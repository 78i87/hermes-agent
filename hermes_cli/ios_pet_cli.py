"""CLI for iOS pet pairing — QR code and HTTP helpers against the local ios_pet adapter."""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional


def _base_url() -> str:
    host = os.getenv("IOS_PET_HOST", "127.0.0.1").strip()
    port = os.getenv("IOS_PET_PORT", "8643").strip()
    return os.getenv("IOS_PET_URL", f"http://{host}:{port}").rstrip("/")


def _admin_headers() -> Dict[str, str]:
    key = (
        os.getenv("IOS_PET_ADMIN_KEY", "").strip()
        or os.getenv("API_SERVER_KEY", "").strip()
    )
    if not key:
        print(
            "Set IOS_PET_ADMIN_KEY or API_SERVER_KEY in ~/.hermes/.env for admin API calls.",
            file=sys.stderr,
        )
        sys.exit(1)
    return {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }


def _request_json(
    method: str,
    path: str,
    *,
    body: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    url = _base_url() + path
    data = None
    h = dict(headers or {})
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        h.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(err_body)
        except json.JSONDecodeError:
            parsed = {"error": err_body or str(e)}
        print(json.dumps(parsed, indent=2), file=sys.stderr)
        sys.exit(e.code if isinstance(e.code, int) else 1)
    except urllib.error.URLError as e:
        print(f"Request failed: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_pair(args) -> None:
    """Start pairing, print QR with server URL + pair credentials."""
    start = _request_json("POST", "/v1/ios_pet/pair/start", headers=_admin_headers())
    pair_code = start.get("pair_code", "")
    pair_token = start.get("pair_token", "")
    expires_at = start.get("expires_at", 0)
    payload = {
        "v": 1,
        "base_url": _base_url(),
        "pair_code": pair_code,
        "pair_token": pair_token,
        "expires_at": expires_at,
    }
    blob = json.dumps(payload, separators=(",", ":"))
    print("Scan this QR with the iOS app (or paste the JSON below):\n")
    try:
        import qrcode

        qr = qrcode.QRCode(border=2)
        qr.add_data(blob)
        qr.make(fit=True)
        qr.print_ascii(invert=True)
    except ImportError:
        print("(Install `qrcode` for ASCII QR: pip install 'qrcode>=7')", file=sys.stderr)
    print("\nJSON payload:\n", blob, "\n", sep="")

    if getattr(args, "wait", False):
        before = {d["id"] for d in _request_json("GET", "/v1/ios_pet/devices", headers=_admin_headers()).get("devices", [])}
        print("Waiting for new device to register (Ctrl+C to stop)...")
        deadline = time.time() + float(getattr(args, "wait_timeout", 300) or 300)
        while time.time() < deadline:
            time.sleep(2)
            devices = _request_json("GET", "/v1/ios_pet/devices", headers=_admin_headers()).get("devices", [])
            for d in devices:
                if d["id"] not in before and d.get("active"):
                    print(f"Paired: {d.get('name', '?')}  device_id={d['id']}")
                    return
        print("Timed out waiting for pairing.", file=sys.stderr)
        sys.exit(1)


def cmd_list(_args) -> None:
    data = _request_json("GET", "/v1/ios_pet/devices", headers=_admin_headers())
    print(json.dumps(data, indent=2))


def cmd_remove(args) -> None:
    device_id = args.device_id.strip()
    _request_json(
        "POST",
        "/v1/ios_pet/unregister",
        headers=_admin_headers(),
        body={"device_id": device_id},
    )
    print(json.dumps({"ok": True, "removed": device_id}, indent=2))


def cmd_test(args) -> None:
    device_id = args.device_id.strip()
    text = getattr(args, "text", None) or "Hermes iOS pet test ping"
    out = _request_json(
        "POST",
        "/v1/ios_pet/test_push",
        headers=_admin_headers(),
        body={"device_id": device_id, "text": text},
    )
    print(json.dumps(out, indent=2))


def ios_pet_command(args) -> None:
    sub = getattr(args, "ios_pet_command", None) or getattr(args, "subcommand", None)
    if sub == "pair":
        cmd_pair(args)
    elif sub == "list":
        cmd_list(args)
    elif sub == "remove":
        cmd_remove(args)
    elif sub == "test":
        cmd_test(args)
    else:
        print("Unknown ios-pet subcommand", file=sys.stderr)
        sys.exit(1)
