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

Representative production paths:
- one-shot local execution:
  - `flows run local --example connector_google_sheets_local_flow --bindings-lock <lock.json>`
- Axum/server-hosted route:
  - `flows run serve --example connector_google_sheets_local_flow --bindings-lock <lock.json>`
  - then POST JSON to `/google/sheets/local`
- bind `connector.google.sheets` to either:
  - `auth.service_account_jwt`, or
  - `auth.oauth2.refresh`

The flow is now **self-bootstrapping**:
- if you provide `spreadsheet_id`, it reuses that workbook
- if you omit `spreadsheet_id`, it creates a spreadsheet, ensures the target
  sheet exists, ensures the CRM header row exists, and then upserts the lead
- for repeatable/idempotent follow-on runs, persist the returned
  `spreadsheet_id` and pass it back on later requests

## Helper: generate a live service-account bindings lock

This example includes a small helper that generates a valid `bindings.lock.json`
for the Google Sheets example and computes the required `content_hash`.

From `codebase/`:

```bash
uv run crates/connectors/google/sheets/examples/local-flow/generate_live_bindings_lock.py \
  --out /tmp/google-sheets-live.bindings.lock.json
```

Default assumptions:
- service-account email secret ref: `google_sheets_sa_email`
- private-key secret ref: `google_sheets_sa_private_key`
- token URL: `https://oauth2.googleapis.com/token`
- endpoint base URL: `https://sheets.googleapis.com`
- scope: `https://www.googleapis.com/auth/spreadsheets`

Override examples:

```bash
uv run crates/connectors/google/sheets/examples/local-flow/generate_live_bindings_lock.py \
  --out /tmp/google-sheets-live.bindings.lock.json \
  --service-account-email-ref my_google_sa_email \
  --private-key-ref my_google_sa_private_key
```

## Live smoke checklist

1. Create a Google Cloud service account and enable the Google Sheets API.
2. Export the expected env vars:

```bash
export google_sheets_sa_email='your-service-account@project.iam.gserviceaccount.com'
export google_sheets_sa_private_key='<private-key-pem>'
```

3. Generate the live bindings lock:

```bash
uv run crates/connectors/google/sheets/examples/local-flow/generate_live_bindings_lock.py \
  --out /tmp/google-sheets-live.bindings.lock.json
```

4. Run locally with **no spreadsheet_id** so the flow creates the workbook,
   sheet, and header row for you:

```bash
cargo run -p flows-cli -- run local \
  --example connector_google_sheets_local_flow \
  --bindings-lock /tmp/google-sheets-live.bindings.lock.json \
  --payload '{"spreadsheet_title":"Lattice CRM Smoke","sheet":"Leads","email":"ada@example.test","name":"Ada Lovelace","summary":"live smoke"}'
```

The response will include:
- `spreadsheet_id`
- `spreadsheet_url`
- `created_spreadsheet`
- `created_sheet`
- `initialized_headers`
- upsert result fields such as `action`, `row_index`, and `updated_range`

5. For repeatable follow-on runs, pass the returned `spreadsheet_id` back in:

```bash
cargo run -p flows-cli -- run local \
  --example connector_google_sheets_local_flow \
  --bindings-lock /tmp/google-sheets-live.bindings.lock.json \
  --payload '{"spreadsheet_id":"YOUR_SPREADSHEET_ID","sheet":"Leads","email":"ada@example.test","name":"Ada Lovelace","summary":"follow-on upsert"}'
```

6. Or serve over Axum:

```bash
cargo run -p flows-cli -- run serve \
  --example connector_google_sheets_local_flow \
  --bindings-lock /tmp/google-sheets-live.bindings.lock.json \
  --addr 127.0.0.1:8080

curl -sS -X POST http://127.0.0.1:8080/google/sheets/local \
  -H 'content-type: application/json' \
  -d '{"spreadsheet_title":"Lattice CRM Smoke","sheet":"Leads","email":"ada@example.test","name":"Ada Lovelace","summary":"served live smoke"}'
```
