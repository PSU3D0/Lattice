# cap-http-workers (design)

## Goal
Provide a wasm-native HTTP capability for Cloudflare Workers that implements `HttpRead` and `HttpWrite` using the `workers-rs` fetch API. The provider kind is `http.workers`, and it is designed to satisfy `resource::http`, `resource::http::read`, and `resource::http::write` hints during preflight.

## Scope (0.1)
- Implement `HttpRead`/`HttpWrite` using Workers `fetch`.
- Support request headers, body (bytes), method, and response status/headers/body.
- Map optional `timeout_ms` to a client-side timeout using `worker::Delay`.
- Keep config schema open-ended for optional Cloudflare request settings.

## Non-goals (0.1)
- Per-request CF settings embedded in the Flow IR (future, possibly via node config).
- Streaming response handling (current capability surface returns `Vec<u8>`).
- Provider registry integration beyond the crate (registry work is separate).

## Provider kind
- Kind: `http.workers`
- Provides: `resource::http`, `resource::http::read`, `resource::http::write`
- `connect` schema: empty
- `config` schema: optional object reserved for future Cloudflare `cf` fetch settings

## Mapping
- `HttpRequest.method` → `worker::Method`
- `HttpRequest.headers` → `worker::Headers` (append)
- `HttpRequest.body` → `RequestInit.body` (`Uint8Array`)
- `HttpRequest.timeout_ms` → timeout via `Delay` + `select`
- `Response.status_code()` → `HttpResponse.status`
- `Response.headers().entries()` → `HttpHeaders`
- `Response.bytes().await` → `HttpResponse.body`

## Error handling
- `worker::Error::BodyUsed` and `RustError` → `HttpError::InvalidResponse`
- All other worker errors → `HttpError::Transport`
- Timeout → `HttpError::Timeout(timeout_ms)`

## Tests (phase 1)
- wasm unit test validates `capabilities::http::ensure_registered()` is called (registration surfaced).
- Further integration tests will require a Workers test harness to mock `fetch`.

## Phased implementation plan
1) **Phase 1: crate + registration test**
   - Create crate scaffold + `WorkersHttpClient` stub.
   - Implement `HttpRead`/`HttpWrite` using `Fetch::send`.
   - Add wasm test for registration.
2) **Phase 2: functional fetch tests**
   - Add Workers test harness for outbound fetch (mock or local service binding).
   - Tests: GET body, POST body + headers, timeout mapping.
3) **Phase 3: config surface**
   - Introduce optional provider config struct for CF request settings (cache, resolve_override).
   - Validate serialization + unit tests for config parsing.
4) **Phase 4: provider registry integration**
   - Add `http.workers` to provider registry once schema registry is defined.
   - Wire into bindings lock resolution.
