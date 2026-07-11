# Hunter connector — local flow example

Minimal `flow!` exercising `connector.hunter.verify_email` end-to-end through
the real executor (scoped enforcement on).

Run against a bindings lock (mock or real endpoints):

```sh
flows run local --example connector_hunter_local_flow \
  --bindings-lock <lock.json> \
  --payload '{"email":"ada@leads.test"}'
```

Hand-author a lock with an `endpoint.profile.static` handle for
`endpoint_profile.hunter_default` and an `auth.static_bearer` handle for
`outbound_auth.hunter_api_key_auth`. The bearer secret is the Hunter API key —
it is appended as the `api_key` query parameter (Hunter authenticates with a
query parameter, not a header).

Offline tests: `cargo test -p example-connector-hunter-local-flow`.
