# cap-http-workers

`cap-http-workers` provides a Cloudflare Workers fetch-backed implementation of the HTTP read/write capabilities for wasm deployments.

## Surface
- Implements `HttpRead` and `HttpWrite` using `workers-rs` (`worker` crate).
- Provider kind: `http.workers`.

## Configuration
- Provider config is intentionally minimal in 0.1.
- Optional Cloudflare request settings may be introduced later as provider config fields.

## Status
- Phase 1: stub crate + design doc + test scaffolding.
