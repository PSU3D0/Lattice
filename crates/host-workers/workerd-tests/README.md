# host-workers/workerd-tests

This crate provides a deployable Workers fixture used for host-workers E2E,
resume evidence validation, and workspace backend integration coverage.

## Covered routes

- `/health`, `/echo`, `/stream`, `/cancel`
- `/timer` for halt/resume durability
- `/workspace` for workspace read/write/list/delete semantics
- `/workspace-resume` for workspace continuity across halt/resume
- `/workspace-retained` for retained-cleanup/alarm-path validation
- `/workspace-quota` for workers-side quota enforcement checks
- `/workspace-invalid-path` for traversal rejection checks
- `/workspace-mutation` for overwrite/delete accounting checks
- `/workspace-blocked-prefix` for blocked-prefix/path-policy checks
- `/__test/workspace/objects` for R2 object inspection
- `/__test/workspace/run-retained-cleanup` for retained-cleanup test dispatch

## Config files

- `wrangler.toml`
  - portable default: internal alarm dispatch via service binding
  - no committed account-specific URL/token literals
- `wrangler.service-binding.template.toml`
  - rendered by wrapper script for account-specific deploy
- `wrangler.cpu-proof.template.toml`
  - rendered for airtight CPU/no-idle proof (`?source=alarm` dispatch tag)

Rendered configs are written under `.wrangler/` and should remain local.

## Wrapper-run workflow

From wrapper root:

```bash
# deploy fixture + set secret
MODE=service-binding EXPECTED_ACCOUNT_ID=<account-id> \
  ./ops/scripts/cloudflare-resume-proof-init.sh

# gather evidence
./ops/scripts/lat-000047-workers-e2e.sh

# optional airtight CPU/no-idle proof
MODE=cpu-proof EXPECTED_ACCOUNT_ID=<account-id> BASE_URL=https://<worker>.<subdomain>.workers.dev \
  ./ops/scripts/cloudflare-resume-proof-init.sh
./ops/scripts/lat-000047-cpu-proof.sh
```
