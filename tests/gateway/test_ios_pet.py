"""Tests for the iOS pet gateway adapter (pairing, SQLite queue, HTTP surface,
SSE cursor + Last-Event-Id, session→platform routing, and toolset exclusions).

All tests pin ``HERMES_HOME`` to a tmp dir so they never touch the user's real
``~/.hermes/ios_pet.db``.
"""

import asyncio
import json
import socket
import uuid
from unittest.mock import patch

import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig, load_gateway_config


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _isolated_hermes_home(tmp_path, monkeypatch):
    """Every test gets its own HERMES_HOME so the shared SQLite DB is clean."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    yield


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def _start_adapter(admin_key: str = "test-admin-key-ok"):
    from gateway.platforms.ios_pet import IOSPetAdapter

    port = _free_port()
    cfg = PlatformConfig(
        enabled=True,
        extra={"host": "127.0.0.1", "port": port, "admin_key": admin_key},
    )
    adapter = IOSPetAdapter(cfg)
    with patch.object(IOSPetAdapter, "_acquire_platform_lock", return_value=True), \
         patch.object(IOSPetAdapter, "_release_platform_lock", return_value=None):
        ok = await adapter.connect()
    assert ok is True
    return adapter, port


async def _pair(port: int, admin_key: str, device_name: str = "TestPhone") -> dict:
    import aiohttp

    admin = {"Authorization": f"Bearer {admin_key}"}
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"http://127.0.0.1:{port}/v1/ios_pet/pair/start", headers=admin
        ) as resp:
            assert resp.status == 200
            start = await resp.json()
        async with session.post(
            f"http://127.0.0.1:{port}/v1/ios_pet/pair/complete",
            json={
                "pair_code": start["pair_code"],
                "pair_token": start["pair_token"],
                "device_name": device_name,
            },
        ) as resp:
            assert resp.status == 200
            done = await resp.json()
    return done


# ---------------------------------------------------------------------------
# Module-level sanity
# ---------------------------------------------------------------------------

class TestRequirements:
    def test_check_ios_pet(self):
        from gateway.platforms.ios_pet import check_ios_pet_requirements
        assert check_ios_pet_requirements() is True


class TestPlatformEnum:
    def test_ios_pet_value(self):
        assert Platform.IOS_PET.value == "ios_pet"


class TestEnvOverrides:
    def test_ios_pet_enabled_via_env(self, monkeypatch):
        monkeypatch.setenv("IOS_PET_ENABLED", "true")
        cfg = load_gateway_config()
        assert Platform.IOS_PET in cfg.platforms
        assert cfg.platforms[Platform.IOS_PET].enabled is True

    def test_ios_pet_home_channel_env_applies_to_yaml_platform(self, monkeypatch):
        from gateway.config import _apply_env_overrides

        monkeypatch.setenv("IOS_PET_HOME_CHANNEL", "device-home-123")
        cfg = GatewayConfig(platforms={Platform.IOS_PET: PlatformConfig(enabled=True)})

        _apply_env_overrides(cfg)

        assert cfg.platforms[Platform.IOS_PET].home_channel is not None
        assert cfg.platforms[Platform.IOS_PET].home_channel.chat_id == "device-home-123"


# ---------------------------------------------------------------------------
# Toolset shape — P2-B regression (no arbitrary code execution)
# ---------------------------------------------------------------------------

class TestIosPetToolset:
    def test_toolset_excludes_terminal_file_and_code_execution(self):
        """The pet surface must omit terminal, raw file IO, and execute_code.

        execute_code would let the model spawn subprocesses / read arbitrary
        files via Python stdlib APIs, and cronjob would let the pet schedule a
        future cron-agent run with broader tools. Regression test for the
        restricted iOS Pet surface.
        """
        from toolsets import resolve_toolset

        tools = resolve_toolset("hermes-ios-pet")
        # Things we want
        assert "web_search" in tools
        assert "send_message" in tools
        assert "memory" in tools
        # Things that must not leak in
        for banned in ("terminal", "process",
                       "read_file", "write_file", "patch", "search_files",
                       "execute_code", "cronjob"):
            assert banned not in tools, f"{banned} must not be in hermes-ios-pet"


# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------

class TestSqliteHelpers:
    def test_enqueue_and_session(self):
        from gateway.platforms import ios_pet as ip

        device_id = f"dev-test-{uuid.uuid4().hex[:12]}"
        conn = ip._ensure_db()
        try:
            conn.execute(
                """INSERT INTO devices (id, name, bearer_hash, session_id, paired_at, last_seen, active)
                   VALUES (?, 't', 'x', 'sess-1', 0, 0, 1)""",
                (device_id,),
            )
            conn.commit()
        finally:
            conn.close()

        assert ip.get_ios_pet_session_id(device_id) == "sess-1"
        assert ip.get_ios_pet_device_for_session("sess-1") == device_id
        assert ip.get_ios_pet_device_for_session("") is None
        assert ip.get_ios_pet_device_for_session("does-not-exist") is None

        assert ip.enqueue_ios_pet_event(device_id, {"kind": "message", "text": "hi"})
        conn = ip._ensure_db()
        try:
            row = conn.execute(
                "SELECT payload_json FROM events WHERE device_id = ? ORDER BY id DESC LIMIT 1",
                (device_id,),
            ).fetchone()
        finally:
            conn.close()
        assert row is not None
        assert json.loads(row["payload_json"])["text"] == "hi"

    def test_inactive_device_is_invisible_to_session_lookup(self):
        from gateway.platforms import ios_pet as ip

        device_id = f"dev-inactive-{uuid.uuid4().hex[:8]}"
        conn = ip._ensure_db()
        try:
            conn.execute(
                """INSERT INTO devices (id, name, bearer_hash, session_id, paired_at, last_seen, active)
                   VALUES (?, 't', 'x', 'sess-inactive', 0, 0, 0)""",
                (device_id,),
            )
            conn.commit()
        finally:
            conn.close()

        assert ip.get_ios_pet_session_id(device_id) is None
        assert ip.get_ios_pet_device_for_session("sess-inactive") is None

    def test_enqueue_runs_retention_cleanup(self):
        from gateway.platforms import ios_pet as ip

        device_id = f"dev-retention-{uuid.uuid4().hex[:8]}"
        conn = ip._ensure_db()
        try:
            conn.execute(
                """INSERT INTO devices (id, name, bearer_hash, session_id, paired_at, last_seen, active)
                   VALUES (?, 't', 'x', 'sess-retention', 0, 0, 1)""",
                (device_id,),
            )
            conn.execute(
                "INSERT INTO pair_sessions (pair_code, pair_token_hash, expires_at, used) VALUES (?, ?, ?, 0)",
                ("expired-pair", "hash", 0),
            )
            conn.execute(
                "INSERT INTO events (device_id, payload_json, created_at, delivered) VALUES (?, ?, 0, 1)",
                (device_id, json.dumps({"kind": "old"})),
            )
            conn.commit()
        finally:
            conn.close()

        assert ip.enqueue_ios_pet_event(device_id, {"kind": "message", "text": "new"})

        conn = ip._ensure_db()
        try:
            old_events = conn.execute(
                "SELECT COUNT(*) FROM events WHERE device_id = ? AND delivered = 1",
                (device_id,),
            ).fetchone()[0]
            expired_pairs = conn.execute(
                "SELECT COUNT(*) FROM pair_sessions WHERE pair_code = 'expired-pair'"
            ).fetchone()[0]
        finally:
            conn.close()
        assert old_events == 0
        assert expired_pairs == 0


# ---------------------------------------------------------------------------
# HTTP surface — pairing, admin, test push
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pair_complete_and_admin_devices():
    import aiohttp

    adapter, port = await _start_adapter()
    try:
        done = await _pair(port, "test-admin-key-ok", "TestPhone")
        device_id = done["device_id"]
        bearer = done["bearer_token"]

        from gateway.platforms.ios_pet import verify_ios_pet_bearer
        assert verify_ios_pet_bearer(bearer) == device_id

        admin = {"Authorization": "Bearer test-admin-key-ok"}
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"http://127.0.0.1:{port}/v1/ios_pet/devices", headers=admin
            ) as resp:
                assert resp.status == 200
                data = await resp.json()
            assert any(d["id"] == device_id for d in data["devices"])

            async with session.post(
                f"http://127.0.0.1:{port}/v1/ios_pet/test_push",
                headers=admin,
                json={"device_id": device_id, "text": "ping"},
            ) as resp:
                assert resp.status == 200
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_test_push_reports_enqueue_failure(monkeypatch):
    import aiohttp
    from gateway.platforms import ios_pet as ip

    adapter, port = await _start_adapter("enqueue-fail-admin-key")
    try:
        done = await _pair(port, "enqueue-fail-admin-key", "FailPhone")
        monkeypatch.setattr(ip, "enqueue_ios_pet_event", lambda *args, **kwargs: False)

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"http://127.0.0.1:{port}/v1/ios_pet/test_push",
                headers={"Authorization": "Bearer enqueue-fail-admin-key"},
                json={"device_id": done["device_id"], "text": "ping"},
            ) as resp:
                assert resp.status == 500
                data = await resp.json()
                assert "enqueue" in data["error"].lower()
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_pair_complete_rejects_replay_and_bad_token():
    import aiohttp

    adapter, port = await _start_adapter("replay-admin-key-123")
    try:
        async with aiohttp.ClientSession() as session:
            # Legitimate start
            async with session.post(
                f"http://127.0.0.1:{port}/v1/ios_pet/pair/start",
                headers={"Authorization": "Bearer replay-admin-key-123"},
            ) as resp:
                start = await resp.json()

            # First completion succeeds
            async with session.post(
                f"http://127.0.0.1:{port}/v1/ios_pet/pair/complete",
                json={
                    "pair_code": start["pair_code"],
                    "pair_token": start["pair_token"],
                    "device_name": "A",
                },
            ) as resp:
                assert resp.status == 200

            # Replay of the same code must fail — generic error message, no
            # distinction between "used" / "expired" / "bad token".
            async with session.post(
                f"http://127.0.0.1:{port}/v1/ios_pet/pair/complete",
                json={
                    "pair_code": start["pair_code"],
                    "pair_token": start["pair_token"],
                    "device_name": "B",
                },
            ) as resp:
                assert resp.status == 400
                err = await resp.json()
                assert "credentials" in err["error"].lower()
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_concurrent_pair_complete_only_produces_one_device():
    """Atomicity check: hammer pair/complete with the same code concurrently.

    The previous implementation had a TOCTOU between the SELECT and UPDATE,
    so two in-flight completions on the same code could both insert devices.
    """
    import aiohttp

    adapter, port = await _start_adapter("race-admin-key-abc")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"http://127.0.0.1:{port}/v1/ios_pet/pair/start",
                headers={"Authorization": "Bearer race-admin-key-abc"},
            ) as resp:
                start = await resp.json()

            body = {
                "pair_code": start["pair_code"],
                "pair_token": start["pair_token"],
                "device_name": "RaceCandidate",
            }

            async def _try():
                async with session.post(
                    f"http://127.0.0.1:{port}/v1/ios_pet/pair/complete",
                    json=body,
                ) as r:
                    return r.status, await r.json()

            # Fire several concurrent completions.
            results = await asyncio.gather(*[_try() for _ in range(8)])

        succeeded = [r for r in results if r[0] == 200]
        failed = [r for r in results if r[0] != 200]
        # Exactly one wins; the rest get the generic 400.
        assert len(succeeded) == 1, results
        assert all(r[0] == 400 for r in failed), results
    finally:
        await adapter.disconnect()


# ---------------------------------------------------------------------------
# SSE stream — no duplicate replays, Last-Event-Id resume
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sse_stream_does_not_replay_events_within_a_single_connection():
    """Regression test for the "event storm" bug.

    The stream handler must not resend events each loop iteration until ACK;
    it must keep a local cursor so a held connection sees each event exactly
    once, even if the client takes its time to ACK.
    """
    import aiohttp

    adapter, port = await _start_adapter("sse-admin-key-xyz")
    try:
        done = await _pair(port, "sse-admin-key-xyz", "SSEPhone")
        bearer = done["bearer_token"]
        device_id = done["device_id"]

        from gateway.platforms.ios_pet import enqueue_ios_pet_event
        for i in range(3):
            enqueue_ios_pet_event(device_id, {"kind": "message", "text": f"msg-{i}"})

        seen_ids: list[int] = []
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"http://127.0.0.1:{port}/v1/ios_pet/stream",
                headers={"Authorization": f"Bearer {bearer}"},
                timeout=aiohttp.ClientTimeout(total=3),
            ) as resp:
                assert resp.status == 200
                # Read for ~2s, enough for the 400ms polling loop to tick
                # multiple times. With the bug, we'd see each id repeated.
                try:
                    async for raw in resp.content:
                        line = raw.decode("utf-8", errors="ignore").strip()
                        if line.startswith("id:"):
                            seen_ids.append(int(line.split(":", 1)[1].strip()))
                        if len(seen_ids) >= 3:
                            # Give the server an extra tick; if it re-sends,
                            # more ids would arrive before timeout.
                            try:
                                await asyncio.wait_for(resp.content.read(1), timeout=1.0)
                            except asyncio.TimeoutError:
                                pass
                            break
                except asyncio.TimeoutError:
                    pass

        assert len(seen_ids) == 3, f"expected exactly 3 events, saw {seen_ids}"
        assert seen_ids == sorted(set(seen_ids)), f"duplicate/out-of-order ids: {seen_ids}"
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_sse_stream_resumes_from_last_event_id():
    """With Last-Event-Id, reconnects skip events the client already processed."""
    import aiohttp

    adapter, port = await _start_adapter("resume-admin-key")
    try:
        done = await _pair(port, "resume-admin-key", "ResumePhone")
        bearer = done["bearer_token"]
        device_id = done["device_id"]

        from gateway.platforms.ios_pet import enqueue_ios_pet_event, _ensure_db

        # Enqueue two events and grab their ids.
        for text in ("first", "second", "third"):
            enqueue_ios_pet_event(device_id, {"kind": "message", "text": text})
        conn = _ensure_db()
        try:
            ids = [r["id"] for r in conn.execute(
                "SELECT id FROM events WHERE device_id = ? ORDER BY id",
                (device_id,),
            ).fetchall()]
        finally:
            conn.close()
        assert len(ids) == 3
        cutoff = ids[0]  # pretend the client already saw the first event

        seen: list[int] = []
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"http://127.0.0.1:{port}/v1/ios_pet/stream",
                headers={
                    "Authorization": f"Bearer {bearer}",
                    "Last-Event-Id": str(cutoff),
                },
                timeout=aiohttp.ClientTimeout(total=3),
            ) as resp:
                assert resp.status == 200
                try:
                    async for raw in resp.content:
                        line = raw.decode("utf-8", errors="ignore").strip()
                        if line.startswith("id:"):
                            seen.append(int(line.split(":", 1)[1].strip()))
                        if len(seen) >= 2:
                            break
                except asyncio.TimeoutError:
                    pass

        assert cutoff not in seen, f"resume failed, saw the skipped id: {seen}"
        assert seen[:2] == ids[1:3], f"expected {ids[1:3]}, saw {seen}"
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_stream_rejects_missing_bearer():
    import aiohttp

    adapter, port = await _start_adapter("auth-admin-key")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"http://127.0.0.1:{port}/v1/ios_pet/stream",
                timeout=aiohttp.ClientTimeout(total=3),
            ) as resp:
                assert resp.status == 401
            async with session.get(
                f"http://127.0.0.1:{port}/v1/ios_pet/stream",
                headers={"Authorization": "Bearer not-a-real-token"},
                timeout=aiohttp.ClientTimeout(total=3),
            ) as resp:
                assert resp.status == 401
    finally:
        await adapter.disconnect()


# ---------------------------------------------------------------------------
# Adapter.send — carries reply_to and metadata into the SSE payload
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_adapter_send_enqueues_with_metadata():
    adapter, port = await _start_adapter("meta-admin-key")
    try:
        done = await _pair(port, "meta-admin-key", "D2")
        device_id = done["device_id"]

        result = await adapter.send(
            device_id,
            "hello from adapter",
            reply_to="prev-msg-id",
            metadata={"mood": "happy", "source": "cron"},
        )
        assert result.success is True

        from gateway.platforms.ios_pet import _ensure_db
        conn = _ensure_db()
        try:
            row = conn.execute(
                "SELECT payload_json FROM events WHERE device_id = ? ORDER BY id DESC LIMIT 1",
                (device_id,),
            ).fetchone()
        finally:
            conn.close()
        payload = json.loads(row["payload_json"])
        assert payload["text"] == "hello from adapter"
        assert payload["reply_to"] == "prev-msg-id"
        assert payload["metadata"] == {"mood": "happy", "source": "cron"}
    finally:
        await adapter.disconnect()


# ---------------------------------------------------------------------------
# send_message tool
# ---------------------------------------------------------------------------

def test_send_message_unknown_ios_pet_device(monkeypatch):
    from tools.send_message_tool import _handle_send

    monkeypatch.setenv("IOS_PET_ENABLED", "true")
    out = _handle_send({
        "target": "ios_pet:00000000-0000-0000-0000-000000000001",
        "message": "hi",
    })
    data = json.loads(out)
    assert data.get("error") or not data.get("success")


# ---------------------------------------------------------------------------
# /v1/runs routing — paired iOS Pet sessions use the restricted toolset
# ---------------------------------------------------------------------------

class TestApiServerPlatformRouting:
    def test_resolve_client_platform_defaults_to_api_server(self):
        from gateway.platforms.api_server import APIServerAdapter
        assert APIServerAdapter._resolve_client_platform(None) == "api_server"
        assert APIServerAdapter._resolve_client_platform("") == "api_server"
        assert APIServerAdapter._resolve_client_platform("not-a-paired-session") == "api_server"

    def test_resolve_client_platform_detects_paired_ios_pet_session(self):
        """A session_id that matches a paired iOS Pet device must resolve to
        the ``ios_pet`` platform so /v1/runs picks the restricted toolset."""
        from gateway.platforms import ios_pet as ip
        from gateway.platforms.api_server import APIServerAdapter

        device_id = f"dev-route-{uuid.uuid4().hex[:8]}"
        session_id = f"sess-route-{uuid.uuid4().hex[:8]}"
        conn = ip._ensure_db()
        try:
            conn.execute(
                """INSERT INTO devices (id, name, bearer_hash, session_id, paired_at, last_seen, active)
                   VALUES (?, 't', 'x', ?, 0, 0, 1)""",
                (device_id, session_id),
            )
            conn.commit()
        finally:
            conn.close()

        assert APIServerAdapter._resolve_client_platform(session_id) == "ios_pet"


# ---------------------------------------------------------------------------
# Platform registration for toolset / skills
# ---------------------------------------------------------------------------

class TestIosPetPlatformConfig:
    def test_platforms_dict_includes_ios_pet(self):
        from hermes_cli.tools_config import PLATFORMS
        assert "ios_pet" in PLATFORMS
        assert PLATFORMS["ios_pet"]["default_toolset"] == "hermes-ios-pet"
