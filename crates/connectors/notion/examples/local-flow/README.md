# Notion connector — local flow example

Minimal `flow!` exercising `connector.notion.create_page` end-to-end through
the real executor (scoped enforcement on).

Run against a bindings lock (mock or real endpoints):

```sh
flows run local --example connector_notion_local_flow \
  --bindings-lock <lock.json> \
  --payload '{"database_id":"db-1","title":"call summary"}'
```

Generate a lock with `flows bindings lock generate`, or hand-author one with an
`endpoint.profile.static` handle for `endpoint_profile.notion_default` and an
`auth.static_bearer` handle for `outbound_auth.notion_api_auth`.

Offline tests: `cargo test -p example-connector-notion-local-flow`.
