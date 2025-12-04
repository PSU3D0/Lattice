# host-web-axum

`host-web-axum` exposes workflows over HTTP using Axum. It wires HttpTrigger nodes into routes, manages SSE/streaming responses, and enforces deadlines and rate limits for the Web profile.

## Surface
- Route mounting APIs for workflows and health endpoints.
- Respond bridge that connects kernel channels to HTTP bodies (including SSE).
- Middleware hooks for request facets (sessions, tracing, auth).

## Invocation Metadata
`host-web-axum` enriches each invocation with HTTP-specific metadata so environment plugins
can observe authentication, tracing, and request context:

- Labels
  - `http.method`, `http.path`, `http.version`, `http.host`, `http.query_raw`
- Extensions
  - `http.headers` — JSON object mapping lower-case header names to string arrays
  - `http.query` — JSON object mapping query parameter names to decoded value arrays
  - `auth.user` — Parsed JSON from the `X-Auth-User` header (handy for mock Auth0 style middleware)

See `host-inproc::EnvironmentPlugin` for hook details.

## Next steps
- Implement the route builder and integrate with `kernel-exec` run handles.
- Add integration tests covering S1/S2 (echo + marketing site) scenarios.
- Provide examples demonstrating streaming and cancellation propagation.

## Depends on
- Runtime primitives from `kernel-exec` and capability wiring from `capabilities`.
- HTTP capability adapters (`cap-http-reqwest`) for outbound calls.
