# Cloudflare idempotent ingest example

This example demonstrates a Cloudflare Workers flow using Durable Objects for
idempotent sequencing, Workers KV for payload storage, and OTLP/HTTP dispatch
for telemetry.

## Routes

- `POST /ingest`
- `GET /events?tenant=...&stream=...&cursor=...&limit=...`
- `GET /snapshot?tenant=...&stream=...`
- `GET /stream?tenant=...&stream=...&cursor=...&limit=...` (SSE)

## Environment variables

- `OTEL_EXPORTER_OTLP_ENDPOINT` (required for OTel dispatch)
- `OTEL_EXPORTER_OTLP_HEADERS` (optional, `key=value,key2=value2`)
- `IDEM_TTL_SECS` (default: 86400)
- `EVENT_TTL_SECS` (default: 604800)
- `SNAPSHOT_TTL_SECS` (default: 2592000)
- `SNAPSHOT_INTERVAL` (default: 10)
- `LIST_LIMIT` (default: 100)
- `STREAM_PAGE_LIMIT` (default: 100)
- `STREAM_MAX_PAGES` (default: 5)
- `STREAM_PAGE_DELAY_MS` (default: 200)

## OTel dispatch behavior

- Uses OTLP/HTTP JSON with `POST` to `OTEL_EXPORTER_OTLP_ENDPOINT`.
- Adds headers from `OTEL_EXPORTER_OTLP_HEADERS` when provided.
- Payload bytes are truncated to 4096 bytes and encoded as base64 in attributes.
- Dispatch is best-effort: failures are logged in the response but do not abort flows.
- `ingest` events are emitted only when `inserted=true`.
- `stream_open` and `stream_close` events are emitted on SSE start/close.

## Notes

- Payloads are accepted as JSON arrays of bytes (e.g. `[1,2,3]`).
- Event keys are stored as `evt:{tenant}:{stream}:{seq}` with zero-padded `seq`.
- Snapshots are stored as `snap:{tenant}:{stream}` with metadata in KV.
