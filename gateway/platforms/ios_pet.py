"""
iOS Pet companion adapter — pairing, device registry, and SSE push stream.

Exposes a small aiohttp server (default http://127.0.0.1:8643) with:
- POST /v1/ios_pet/pair/start   (admin)
- POST /v1/ios_pet/pair/complete
- POST /v1/ios_pet/unregister
- GET  /v1/ios_pet/stream       (SSE, device bearer)
- POST /v1/ios_pet/ack
- POST /v1/ios_pet/test_push    (admin)
- GET  /v1/ios_pet/devices      (admin)
- GET  /health

Chat uses the separate API server (/v1/runs). This adapter only handles
pairing and proactive event delivery over SSE (APNs deferred).
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import secrets
import socket as _socket
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional, Union

try:
    from aiohttp import web
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False
    web = None  # type: ignore[assignment]

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult, is_network_accessible

logger = logging.getLogger(__name__)

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8643
PAIR_TTL_S = 300
# Keep delivered (ACKed) events around briefly so that a reconnecting client
# using Last-Event-Id can still skip past them without receiving duplicates.
DELIVERED_RETENTION_S = 24 * 3600
HEARTBEAT_INTERVAL_S = 15.0
_DB_NAME = "ios_pet.db"
_db_lock = threading.Lock()


def check_ios_pet_requirements() -> bool:
    return bool(AIOHTTP_AVAILABLE)


def _db_path() -> Path:
    from hermes_constants import get_hermes_home
    return get_hermes_home() / _DB_NAME


def _connect() -> sqlite3.Connection:
    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # Better concurrency than default rollback-journal + DEFERRED mode: WAL
    # allows a reader and a writer to progress simultaneously, which matters
    # here because the SSE stream reads in a loop while other endpoints write.
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.DatabaseError:
        pass
    return conn


def _housekeep(conn: sqlite3.Connection) -> None:
    """Drop expired pair sessions and ancient delivered events. Best-effort."""
    try:
        now = time.time()
        conn.execute(
            "DELETE FROM pair_sessions WHERE expires_at < ? OR used = 1",
            (now - 60,),
        )
        conn.execute(
            "DELETE FROM events WHERE delivered = 1 AND created_at < ?",
            (now - DELIVERED_RETENTION_S,),
        )
        conn.commit()
    except sqlite3.DatabaseError as e:
        logger.debug("[ios_pet] housekeep skipped: %s", e)


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS devices (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            bearer_hash TEXT NOT NULL,
            session_id TEXT NOT NULL,
            paired_at REAL NOT NULL,
            last_seen REAL,
            active INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS pair_sessions (
            pair_code TEXT PRIMARY KEY,
            pair_token_hash TEXT NOT NULL,
            expires_at REAL NOT NULL,
            used INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at REAL NOT NULL,
            delivered INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(device_id) REFERENCES devices(id)
        );
        CREATE INDEX IF NOT EXISTS idx_events_device_undelivered
            ON events(device_id, delivered, id);
        """
    )
    conn.commit()


def _ensure_db() -> sqlite3.Connection:
    with _db_lock:
        conn = _connect()
        _init_schema(conn)
        return conn


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def enqueue_ios_pet_event(
    device_id: str,
    payload: Dict[str, Any],
    *,
    conn: Optional[sqlite3.Connection] = None,
) -> bool:
    """Insert a push event for *device_id* (used by adapter and send_message tool)."""
    own = conn is None
    if own:
        conn = _ensure_db()
    try:
        payload.setdefault("ts", time.time())
        conn.execute(
            "INSERT INTO events (device_id, payload_json, created_at, delivered) VALUES (?, ?, ?, 0)",
            (device_id, json.dumps(payload, separators=(",", ":"), ensure_ascii=False), time.time()),
        )
        conn.commit()
        _housekeep(conn)
        return True
    except Exception as e:
        logger.warning("[ios_pet] enqueue failed: %s", e)
        return False
    finally:
        if own:
            conn.close()


def get_ios_pet_session_id(device_id: str) -> Optional[str]:
    """Return *session_id* for an active device, or None."""
    conn = _ensure_db()
    try:
        row = conn.execute(
            "SELECT session_id FROM devices WHERE id = ? AND active = 1",
            (str(device_id),),
        ).fetchone()
        return str(row["session_id"]) if row else None
    finally:
        conn.close()


def get_ios_pet_device_for_session(session_id: str) -> Optional[str]:
    """Return device_id if *session_id* belongs to an active iOS Pet device."""
    if not session_id:
        return None
    conn = _ensure_db()
    try:
        row = conn.execute(
            "SELECT id FROM devices WHERE session_id = ? AND active = 1",
            (str(session_id),),
        ).fetchone()
        return str(row["id"]) if row else None
    finally:
        conn.close()


def verify_ios_pet_bearer(token: str) -> Optional[str]:
    """Return device_id if *token* matches an active device, else None."""
    if not token:
        return None
    h = _hash_token(token)
    conn = _ensure_db()
    try:
        row = conn.execute(
            "SELECT id FROM devices WHERE bearer_hash = ? AND active = 1",
            (h,),
        ).fetchone()
        return str(row["id"]) if row else None
    finally:
        conn.close()


class IOSPetAdapter(BasePlatformAdapter):
    """HTTP server for iOS pet pairing and SSE event delivery."""

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.IOS_PET)
        extra = config.extra or {}
        self._host: str = extra.get("host", os.getenv("IOS_PET_HOST", DEFAULT_HOST))
        self._port: int = int(extra.get("port", os.getenv("IOS_PET_PORT", str(DEFAULT_PORT))))
        self._admin_key: str = (
            extra.get("admin_key")
            or os.getenv("IOS_PET_ADMIN_KEY", "")
            or os.getenv("API_SERVER_KEY", "")
        )
        self._app: Optional["web.Application"] = None
        self._runner: Optional["web.AppRunner"] = None
        self._site: Optional["web.TCPSite"] = None

    def _admin_ok(self, request: "web.Request") -> bool:
        if not self._admin_key:
            return False
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            tok = auth[7:].strip()
            return hmac.compare_digest(tok, self._admin_key)
        return False

    def _device_from_request(self, request: "web.Request") -> Optional[str]:
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return None
        tok = auth[7:].strip()
        if self._admin_key and hmac.compare_digest(tok, self._admin_key):
            return None  # admin token — not a device
        return verify_ios_pet_bearer(tok)

    async def _handle_health(self, request: "web.Request") -> "web.Response":
        return web.json_response({"status": "ok", "platform": "ios_pet"})

    async def _handle_pair_start(self, request: "web.Request") -> "web.Response":
        if not self._admin_ok(request):
            return web.json_response({"error": "Unauthorized"}, status=401)
        pair_code = secrets.token_hex(4)
        pair_token = secrets.token_hex(32)
        expires_at = time.time() + PAIR_TTL_S
        conn = _ensure_db()
        try:
            conn.execute(
                "INSERT INTO pair_sessions (pair_code, pair_token_hash, expires_at, used) VALUES (?, ?, ?, 0)",
                (pair_code, _hash_token(pair_token), expires_at),
            )
            conn.commit()
            _housekeep(conn)
        finally:
            conn.close()
        return web.json_response({
            "pair_code": pair_code,
            "pair_token": pair_token,
            "expires_at": expires_at,
            "ttl_seconds": PAIR_TTL_S,
        })

    async def _handle_pair_complete(self, request: "web.Request") -> "web.Response":
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        pair_code = (body.get("pair_code") or "").strip()
        pair_token = (body.get("pair_token") or "").strip()
        device_name = (body.get("device_name") or "iOS Device").strip() or "iOS Device"
        # Bound device_name so a pathological client can't write megabytes to disk.
        device_name = device_name[:128]
        if not pair_code or not pair_token:
            return web.json_response({"error": "pair_code and pair_token required"}, status=400)

        generic_err = {"error": "Invalid or expired pair credentials"}

        conn = _ensure_db()
        try:
            # Atomic consume: SELECT + UPDATE under an IMMEDIATE transaction
            # so two concurrent pair_complete calls on the same code can't both
            # produce a device row. compare_digest happens before the UPDATE
            # but inside the write lock, and the UPDATE's WHERE used = 0 is
            # the actual serialization point.
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT pair_token_hash, expires_at FROM pair_sessions WHERE pair_code = ? AND used = 0",
                (pair_code,),
            ).fetchone()
            if not row or time.time() > row["expires_at"]:
                conn.rollback()
                return web.json_response(generic_err, status=400)
            if not hmac.compare_digest(_hash_token(pair_token), row["pair_token_hash"]):
                conn.rollback()
                return web.json_response(generic_err, status=400)

            cur = conn.execute(
                "UPDATE pair_sessions SET used = 1 WHERE pair_code = ? AND used = 0",
                (pair_code,),
            )
            if cur.rowcount != 1:
                # Another request already consumed this code between SELECT and UPDATE.
                conn.rollback()
                return web.json_response(generic_err, status=400)

            device_id = str(uuid.uuid4())
            session_id = str(uuid.uuid4())
            bearer = secrets.token_hex(32)
            conn.execute(
                """INSERT INTO devices (id, name, bearer_hash, session_id, paired_at, last_seen, active)
                   VALUES (?, ?, ?, ?, ?, ?, 1)""",
                (device_id, device_name, _hash_token(bearer), session_id, time.time(), time.time()),
            )
            conn.commit()
            _housekeep(conn)
        except sqlite3.DatabaseError as e:
            logger.warning("[ios_pet] pair_complete failed: %s", e)
            try:
                conn.rollback()
            except sqlite3.DatabaseError:
                pass
            return web.json_response({"error": "Pairing failed"}, status=500)
        finally:
            conn.close()

        return web.json_response({
            "device_id": device_id,
            "bearer_token": bearer,
            "session_id": session_id,
        })

    async def _handle_unregister(self, request: "web.Request") -> "web.Response":
        try:
            body = await request.json()
        except Exception:
            body = {}
        device_id = (body.get("device_id") or "").strip()
        if not device_id:
            return web.json_response({"error": "device_id required"}, status=400)

        if self._admin_ok(request):
            pass
        else:
            dev = self._device_from_request(request)
            if not dev or dev != device_id:
                return web.json_response({"error": "Unauthorized"}, status=401)

        conn = _ensure_db()
        try:
            conn.execute("UPDATE devices SET active = 0 WHERE id = ?", (device_id,))
            conn.commit()
        finally:
            conn.close()
        return web.json_response({"ok": True})

    async def _handle_devices(self, request: "web.Request") -> "web.Response":
        if not self._admin_ok(request):
            return web.json_response({"error": "Unauthorized"}, status=401)
        conn = _ensure_db()
        try:
            rows = conn.execute(
                "SELECT id, name, session_id, paired_at, last_seen, active FROM devices ORDER BY paired_at DESC"
            ).fetchall()
        finally:
            conn.close()
        return web.json_response({
            "devices": [
                {
                    "id": r["id"],
                    "name": r["name"],
                    "session_id": r["session_id"],
                    "paired_at": r["paired_at"],
                    "last_seen": r["last_seen"],
                    "active": bool(r["active"]),
                }
                for r in rows
            ]
        })

    async def _handle_test_push(self, request: "web.Request") -> "web.Response":
        if not self._admin_ok(request):
            return web.json_response({"error": "Unauthorized"}, status=401)
        try:
            body = await request.json()
        except Exception:
            body = {}
        device_id = (body.get("device_id") or "").strip()
        if not device_id:
            return web.json_response({"error": "device_id required"}, status=400)
        text = (body.get("text") or "Hermes iOS pet test ping").strip()
        conn = _ensure_db()
        try:
            row = conn.execute(
                "SELECT session_id FROM devices WHERE id = ? AND active = 1",
                (device_id,),
            ).fetchone()
        finally:
            conn.close()
        if not row:
            return web.json_response({"error": "Device not found"}, status=404)
        session_id = row["session_id"]
        enqueued = enqueue_ios_pet_event(
            device_id,
            {
                "kind": "nudge",
                "preview": text[:200],
                "text": text,
                "session_id": session_id,
                "deep_link": f"hermespet://open?session={session_id}",
            },
        )
        if not enqueued:
            return web.json_response({"error": "Failed to enqueue test push"}, status=500)
        return web.json_response({"ok": True, "enqueued": True})

    async def _handle_ack(self, request: "web.Request") -> "web.Response":
        device_id = self._device_from_request(request)
        if not device_id:
            return web.json_response({"error": "Unauthorized"}, status=401)
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        last_id = body.get("last_event_id")
        try:
            last_id = int(last_id)
        except (TypeError, ValueError):
            return web.json_response({"error": "last_event_id must be an integer"}, status=400)
        conn = _ensure_db()
        try:
            conn.execute(
                "UPDATE events SET delivered = 1 WHERE device_id = ? AND id <= ? AND delivered = 0",
                (device_id, last_id),
            )
            conn.commit()
            _housekeep(conn)
        finally:
            conn.close()
        return web.json_response({"ok": True})

    async def _handle_stream(self, request: "web.Request") -> Union["web.StreamResponse", "web.Response"]:
        device_id = self._device_from_request(request)
        if not device_id:
            return web.json_response({"error": "Unauthorized"}, status=401)

        # Initialize the local cursor from Last-Event-Id so reconnects don't
        # replay events the client has already seen. Without this (and the
        # cursor below), the loop would re-send every undelivered event every
        # tick until the client POSTed /ack, producing a duplicate-event storm.
        try:
            last_streamed_id = int(request.headers.get("Last-Event-Id", "0") or 0)
        except (TypeError, ValueError):
            last_streamed_id = 0

        response = web.StreamResponse(
            status=200,
            reason="OK",
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
                # Apply security headers directly: the middleware pass runs
                # after prepare() for a StreamResponse, which is too late.
                "X-Content-Type-Options": "nosniff",
                "Referrer-Policy": "no-referrer",
            },
        )
        await response.prepare(request)

        conn = _ensure_db()
        try:
            conn.execute(
                "UPDATE devices SET last_seen = ? WHERE id = ?",
                (time.time(), device_id),
            )
            conn.commit()
        finally:
            conn.close()

        last_heartbeat = time.time()
        last_touch = time.time()
        try:
            while True:
                conn = _ensure_db()
                try:
                    rows = conn.execute(
                        """SELECT id, payload_json FROM events
                           WHERE device_id = ? AND id > ? AND delivered = 0
                           ORDER BY id LIMIT 50""",
                        (device_id, last_streamed_id),
                    ).fetchall()
                finally:
                    conn.close()

                for r in rows:
                    eid = int(r["id"])
                    payload = json.loads(r["payload_json"])
                    chunk = f"id: {eid}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
                    await response.write(chunk.encode("utf-8"))
                    last_streamed_id = eid
                    last_heartbeat = time.time()

                now = time.time()
                # SSE comment line as keep-alive: ignored by clients but keeps
                # NATs / reverse proxies from silently closing the connection.
                if now - last_heartbeat >= HEARTBEAT_INTERVAL_S:
                    await response.write(b": keepalive\n\n")
                    last_heartbeat = now

                # Refresh last_seen every ~30s for a long-lived stream.
                if now - last_touch >= 30.0:
                    conn = _ensure_db()
                    try:
                        conn.execute(
                            "UPDATE devices SET last_seen = ? WHERE id = ?",
                            (now, device_id),
                        )
                        conn.commit()
                    finally:
                        conn.close()
                    last_touch = now

                await asyncio.sleep(0.4)
        except asyncio.CancelledError:
            raise
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception as e:
            logger.debug("[ios_pet] stream ended: %s", e)
        return response

    async def connect(self) -> bool:
        if not AIOHTTP_AVAILABLE:
            logger.warning("[%s] aiohttp not installed", self.name)
            return False

        if is_network_accessible(self._host) and not self._admin_key:
            logger.error(
                "[%s] Refusing to start: binding to %s requires IOS_PET_ADMIN_KEY or API_SERVER_KEY.",
                self.name, self._host,
            )
            return False

        if is_network_accessible(self._host) and self._admin_key:
            try:
                from hermes_cli.auth import has_usable_secret
                if not has_usable_secret(self._admin_key, min_length=8):
                    logger.error(
                        "[%s] Refusing to start: admin key is a placeholder. "
                        "Set a real secret before exposing on %s.",
                        self.name, self._host,
                    )
                    return False
            except ImportError:
                pass

        lock_id = f"{self._host}:{self._port}"
        if not self._acquire_platform_lock("ios-pet-endpoint", lock_id, "iOS Pet HTTP endpoint"):
            return False

        try:
            mws = [mw for mw in (body_limit_middleware, security_headers_middleware) if mw is not None]
            self._app = web.Application(middlewares=mws)
            self._app.router.add_get("/health", self._handle_health)
            self._app.router.add_post("/v1/ios_pet/pair/start", self._handle_pair_start)
            self._app.router.add_post("/v1/ios_pet/pair/complete", self._handle_pair_complete)
            self._app.router.add_post("/v1/ios_pet/unregister", self._handle_unregister)
            self._app.router.add_get("/v1/ios_pet/stream", self._handle_stream)
            self._app.router.add_post("/v1/ios_pet/ack", self._handle_ack)
            self._app.router.add_post("/v1/ios_pet/test_push", self._handle_test_push)
            self._app.router.add_get("/v1/ios_pet/devices", self._handle_devices)

            # Non-blocking port probe: don't stall the event loop with a
            # synchronous socket.connect while aiohttp sites are being set up.
            port_check_host = self._host if self._host not in ("0.0.0.0", "::") else "127.0.0.1"
            port_in_use = await self._port_in_use(port_check_host, self._port)
            if port_in_use:
                logger.error(
                    "[%s] Port %d already in use. Set IOS_PET_PORT to a free port.",
                    self.name, self._port,
                )
                self._release_platform_lock()
                return False

            # Drop expired pair sessions and old delivered events on startup.
            conn = _ensure_db()
            try:
                _housekeep(conn)
            finally:
                conn.close()

            self._runner = web.AppRunner(self._app)
            await self._runner.setup()
            self._site = web.TCPSite(self._runner, self._host, self._port)
            await self._site.start()
            self._mark_connected()
            logger.info("[%s] listening on http://%s:%d", self.name, self._host, self._port)
            return True
        except Exception as e:
            logger.error("[%s] Failed to start: %s", self.name, e)
            self._release_platform_lock()
            return False

    @staticmethod
    async def _port_in_use(host: str, port: int) -> bool:
        """Return True iff *host:port* refuses no connection within 500ms."""
        try:
            fut = asyncio.open_connection(host=host, port=port)
            reader, writer = await asyncio.wait_for(fut, timeout=0.5)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return True
        except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
            return False

    async def disconnect(self) -> None:
        self._mark_disconnected()
        if self._site:
            await self._site.stop()
            self._site = None
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
        self._app = None
        self._release_platform_lock()
        logger.info("[%s] stopped", self.name)

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        device_id = str(chat_id)
        conn = _ensure_db()
        try:
            row = conn.execute(
                "SELECT session_id FROM devices WHERE id = ? AND active = 1",
                (device_id,),
            ).fetchone()
        finally:
            conn.close()
        if not row:
            return SendResult(success=False, error=f"Unknown or inactive device: {device_id}")
        session_id = row["session_id"]
        payload: Dict[str, Any] = {
            "kind": "message",
            "text": content,
            "session_id": session_id,
        }
        if reply_to:
            payload["reply_to"] = str(reply_to)
        if metadata:
            payload["metadata"] = metadata
        ok = enqueue_ios_pet_event(device_id, payload)
        if ok:
            return SendResult(success=True)
        return SendResult(success=False, error="Failed to enqueue event")

    async def send_typing(self, chat_id: str) -> None:
        device_id = str(chat_id)
        conn = _ensure_db()
        try:
            row = conn.execute(
                "SELECT session_id FROM devices WHERE id = ? AND active = 1",
                (device_id,),
            ).fetchone()
        finally:
            conn.close()
        if not row:
            return
        enqueue_ios_pet_event(
            device_id,
            {"kind": "typing", "session_id": row["session_id"]},
        )

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        conn = _ensure_db()
        try:
            row = conn.execute(
                "SELECT id, name, session_id, active FROM devices WHERE id = ?",
                (str(chat_id),),
            ).fetchone()
        finally:
            conn.close()
        if not row:
            return {"name": chat_id, "type": "dm", "chat_id": str(chat_id)}
        return {
            "name": row["name"] or "iOS Pet",
            "type": "dm",
            "chat_id": row["id"],
            "session_id": row["session_id"],
            "active": bool(row["active"]),
        }


# Middleware (match api_server style)
MAX_REQUEST_BYTES = 1_000_000

if AIOHTTP_AVAILABLE:
    @web.middleware
    async def body_limit_middleware(request, handler):
        if request.method in ("POST", "PUT", "PATCH"):
            cl = request.headers.get("Content-Length")
            if cl is not None:
                try:
                    if int(cl) > MAX_REQUEST_BYTES:
                        return web.json_response({"error": "body too large"}, status=413)
                except ValueError:
                    return web.json_response({"error": "Invalid Content-Length"}, status=400)
        return await handler(request)
else:
    body_limit_middleware = None  # type: ignore[assignment]

_SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
}

if AIOHTTP_AVAILABLE:
    @web.middleware
    async def security_headers_middleware(request, handler):
        response = await handler(request)
        for k, v in _SECURITY_HEADERS.items():
            response.headers.setdefault(k, v)
        return response
else:
    security_headers_middleware = None  # type: ignore[assignment]
