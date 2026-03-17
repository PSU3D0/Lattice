# example-connector-google-sheets-local-flow

Local mock-first example flow for `connector_google_sheets`.

The example proves:
- connector-owned runnable flow packaging
- canonical `connector.google.sheets.upsert_row` usage in a real flow
- local env-runtime smoke testing by default
- compatibility with host-owned connector bindings when launched through `flows run local --bindings-lock`

Default behavior:
- if `LATTICE_CONNECTOR_ENDPOINT_GOOGLE_SHEETS_DEFAULT_BASE_URL` is unset, the example can run against a local mock server in tests
- if `LATTICE_CONNECTOR_AUTH_GOOGLE_WORKSPACE_AUTH` is unset in that mock-first path, the example uses a demo bearer token

Representative production path:
- run via `flows run local --example connector_google_sheets_local_flow --bindings-lock <lock.json>`
- bind `connector.google.sheets` to either:
  - `auth.service_account_jwt`, or
  - `auth.oauth2.refresh`
