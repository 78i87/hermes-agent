---
sidebar_position: 21
title: "iOS Pet companion"
description: "Pair an iOS app with Hermes for SSE push events alongside the API server"
---

# iOS Pet companion

The **iOS Pet** platform adapter runs a small HTTP server (default **8643**) for:

- **Pairing** — register a device and obtain a per-device bearer token
- **SSE push** — stream proactive events (`message`, `typing`, `nudge`) to the app while it is in the foreground

**Chat** still uses the [OpenAI-compatible API server](./open-webui.md) (`POST /v1/runs` and SSE on port **8642**). The iOS Pet server does not replace the API server.

## Enable

Add to `~/.hermes/.env`:

```bash
IOS_PET_ENABLED=true
# Same secret family as API_SERVER_KEY — used for admin endpoints (pair/start, devices, test_push)
IOS_PET_ADMIN_KEY=your-long-random-secret
# Optional — defaults shown
# IOS_PET_HOST=127.0.0.1
# IOS_PET_PORT=8643
```

If you bind to a non-loopback address (e.g. `0.0.0.0` on a home server), set a strong `IOS_PET_ADMIN_KEY` or reuse `API_SERVER_KEY`.

Start the gateway:

```bash
hermes gateway
```

You should see a log line similar to:

```
[ios_pet] listening on http://127.0.0.1:8643
```

## CLI pairing

From a machine that can reach the iOS Pet HTTP port:

```bash
hermes ios-pet pair
```

This calls `POST /v1/ios_pet/pair/start` with your admin key, prints an ASCII QR code, and shows the JSON payload for the app to scan.

Optional:

```bash
hermes ios-pet pair --wait
```

List devices:

```bash
hermes ios-pet list
```

Test push:

```bash
hermes ios-pet test <device-uuid>
```

Unregister:

```bash
hermes ios-pet remove <device-uuid>
```

Environment variables for the CLI: `IOS_PET_HOST`, `IOS_PET_PORT`, or `IOS_PET_URL` (base URL override), plus `IOS_PET_ADMIN_KEY` or `API_SERVER_KEY`.

## Wire protocol (app contract)

1. **Pairing** — App scans QR JSON `{ base_url, pair_code, pair_token, expires_at }`, then `POST {base_url}/v1/ios_pet/pair/complete` with `{ pair_code, pair_token, device_name }`. Store `bearer_token`, `device_id`, and `session_id`.
2. **Chat** — `POST http://<api-host>:8642/v1/runs` with `Authorization: Bearer <API_SERVER_KEY>` and the paired `session_id`. Either form works:
   - `X-Hermes-Session-Id: <session_id>` request header (preferred, mirrors `/v1/chat/completions`), **or**
   - `"session_id": "<session_id>"` in the JSON body.

   The response echoes the session back as `X-Hermes-Session-Id` and in the JSON body. When the session matches a paired iOS Pet device, the agent runs with the restricted `hermes-ios-pet` toolset — no terminal, no raw file reads/writes, and no arbitrary Python execution — regardless of what the API server's default toolset is.
3. **Push** — `GET {base_url}/v1/ios_pet/stream` with `Authorization: Bearer <bearer_token>`. Events are SSE: `id:` line plus `data: {json}`. Send `Last-Event-Id: <n>` on reconnect to resume after the last event you processed.
4. **Ack** — `POST /v1/ios_pet/ack` with `{ "last_event_id": <int> }` to mark events delivered and let the server purge old rows. During a live stream the server keeps a local cursor so events are not re-sent until the client reconnects, but ACK is still required so events won't be redelivered after a disconnect.

## `send_message` target

Use `ios_pet:<device-uuid>` or set `IOS_PET_HOME_CHANNEL` to a device UUID and send to target `ios_pet` (home channel).

## APNs

Push notifications when the app is backgrounded are **not** in v1; the SSE stream covers foreground delivery. A future version can attach APNs to the same event queue.
