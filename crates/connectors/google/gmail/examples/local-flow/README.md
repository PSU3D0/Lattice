# Gmail connector — local flow example

Minimal `flow!` exercising `connector.google.gmail.send_message` end-to-end
through the real executor (scoped enforcement on).

Run against a bindings lock (mock or real endpoints):

```sh
flows run local --example connector_google_gmail_local_flow \
  --bindings-lock <lock.json> \
  --payload '{"to":"ops@example.test","subject":"hi","text_body":"hello"}'
```

Generate a lock with `flows bindings lock generate`, or hand-author one with an
`endpoint.profile.static` handle for `endpoint_profile.google_gmail_default`
and an `auth.static_bearer` handle for `outbound_auth.google_workspace_auth`.

Offline tests: `cargo test -p example-connector-google-gmail-local-flow`.
