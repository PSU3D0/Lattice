# Airtable connector — local flow example

Minimal `flow!` exercising `connector.airtable.create_record` end-to-end
through the real executor (scoped enforcement on).

Run against a bindings lock (mock or real endpoints):

```sh
flows run local --example connector_airtable_local_flow \
  --bindings-lock <lock.json> \
  --payload '{"base_id":"appXXXX","table":"Transcripts","fields":{"Call ID":"c1"},"typecast":null}'
```

Generate a lock with `flows bindings lock generate`, or hand-author one with an
`endpoint.profile.static` handle for `endpoint_profile.airtable_default`
and an `auth.static_bearer` handle for `outbound_auth.airtable_token_auth`.

Offline tests: `cargo test -p example-connector-airtable-local-flow`.
