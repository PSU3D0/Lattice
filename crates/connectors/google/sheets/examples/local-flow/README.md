# example-connector-google-sheets-local-flow

Portable self-bootstrapping CRM flow example for `connector_google_sheets`.

## What it proves

This example proves:
- connector-owned runnable flow packaging
- canonical `connector.google.sheets` usage in a real flow
- standard native execution through `flows run local --example ...`
- standard Axum hosting through `flows run serve --example ...`
- compatibility with host-owned connector bindings when launched through `--bindings-lock`
- portable package shape for `flows bundle -p ... --wasm`

The flow is now shaped like the top-level examples:
- `dag_macros::flow!` is the source of truth
- `bundle()` is the standard macro-generated host-bundle path
- helper CRM nodes stay in the flow crate
- native mock/runtime harness logic is test-only rather than the public execution surface

## Native execution

Representative production paths:
- one-shot local execution:
  - `flows run local --example connector_google_sheets_local_flow --bindings-lock <lock.json>`
- Axum/server-hosted route:
  - `flows run serve --example connector_google_sheets_local_flow --bindings-lock <lock.json>`
  - then POST JSON to `/google/sheets/local`
- bind `connector.google.sheets` to either:
  - `auth.service_account_jwt`, or
  - `auth.oauth2.refresh`

The flow is **self-bootstrapping**:
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

5. For repeatable follow-on runs, pass the returned `spreadsheet_id` back in.

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

## WASM / bundle note

This package is now shaped so `flows bundle -p example-connector-google-sheets-local-flow --wasm` is the intended portable artifact path.

Actual runtime execution of HTTP/connector-heavy bundles still depends on the current host-side WASM capability/runtime support; the package shape is now portable even where every host runtime is not yet feature-parity complete.
