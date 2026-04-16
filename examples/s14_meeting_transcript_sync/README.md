# S14 meeting transcript sync

Serious first-party workflow example for a polling/reconciliation transcript sync.

## What it proves in this tranche

- explicit outer reconcile topology for meeting transcript sync
- flow-local ownership of meeting-key, retry, waiting, ambiguity, and upload policy
- typed config, domain, and state modules
- flow-local fake adapters and fake-driven semantic tests
- serious example crate shape with standard `flow()` / `validated_ir()` / `bundle()` entrypoints, while keeping the current runtime-installation limitation explicit

## Current flow shape

- trigger
- `fetch_recent_completed_meetings`
- `upsert_meeting_jobs`
- `select_due_jobs`
- `reconcile_due_jobs`
- capture

Important honest note:
- per-meeting classify / resolve / fetch / upload semantics are implemented explicitly in the example crate's reconciliation engine
- they are not yet split into graph-visible per-item nodes because this tranche intentionally stops before a truthful scheduled-wrapper / fanout expansion

## Declared invocation contract

The flow preserves route metadata for:
- `POST /meeting-transcript-sync/run`

Current payload examples:
- `payloads/sample.json`
- `payloads/backfill-sample.json`

Current honest note:
- this route is part of the example's bundle/entrypoint contract
- crate tests exercise it only after installing a flow-local runtime seam in-process
- there is not yet a public non-test runtime installation path for the example crate, so advertising a standalone local/manual serve path here would be misleading
- outside that test harness, effectful execution currently fails with `meeting transcript sync runtime is not installed`

The intended production posture remains:
- Cloudflare Cron trigger
- thin scheduled wrapper
- D1 job ledger
- R2 transcript destination

Those are intentionally deferred to the later tranche.

## Current boundaries

Implemented here:
- flow-local reconciliation semantics
- meeting source classification policy
- transcript source resolution / ambiguity handling semantics
- retry vs permanent failure behavior
- upload retry semantics
- idempotent discovered-job upsert and due-job selection

Deferred here:
- real Google Calendar / Drive / Docs integration
- real Zoom transcript retrieval
- D1-backed job store
- R2-backed uploader
- scheduled Worker wrapper
- host-wasmtime or workerd proofing

## Provider groundwork usage

This example consumes the narrow provider groundwork only at the primitive boundary:
- Google Doc transcript refs carry Google Docs / Drive MIME metadata from `connector_google_platform`
- Zoom transcript refs carry connector metadata from `connector_zoom_platform`

Business policy remains flow-local.

## Verification

Current honest proof path:
- crate semantic tests with flow-local fake adapters and test-installed runtime seam
- shape/bundle checks only; no first-class standalone local/manual route proof yet

Targeted checks:

```bash
cargo check -p example-s14-meeting-transcript-sync
cargo test -p example-s14-meeting-transcript-sync -- --test-threads=1
cargo check -p example-s14-meeting-transcript-sync --target wasm32-unknown-unknown --no-default-features
```

Current honest bundleability note:
- the crate has the standard bundleable shape and `bundle()` entrypoint
- but the serious deploy/runtime path still depends on later D1/R2 + scheduled-wrapper work
