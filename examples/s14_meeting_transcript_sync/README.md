# S14 meeting transcript sync

Serious first-party workflow example for a polling/reconciliation transcript sync.

## What this tranche lands honestly

- explicit outer reconcile topology for meeting transcript sync
- flow-local ownership of meeting-key, retry, waiting, ambiguity, and upload policy
- typed config, domain, state, and scheduled-trigger modules
- a safe example-owned execution seam: `TranscriptSyncExecutor`
- example-local Cloudflare storage helpers for a D1-shaped job ledger and an R2-shaped transcript uploader
- a thin example-owned scheduled wrapper path:
  - generic `ScheduledTick` -> `TranscriptSyncExecutor::execute_scheduled_tick(...)`
  - Cloudflare adapter surface: `worker::ScheduledEvent` -> `cloudflare::execute_scheduled_event(...)` (compile-checked; not yet runtime-proved)
- standard `flow()` / `validated_ir()` / `bundle()` entrypoints, with truthful limits on what the bundle path does today

## Current flow shape

- trigger
- `fetch_recent_completed_meetings`
- `upsert_meeting_jobs`
- `select_due_jobs`
- `reconcile_due_jobs`
- capture

Important honest note:
- per-meeting classify / resolve / fetch / upload semantics are implemented explicitly in the example crate's reconciliation engine
- they are not yet split into graph-visible per-item nodes because this example still keeps the per-item reconciliation loop local rather than inventing a broader scheduler/fanout platform

## Safe execution seam

Use `TranscriptSyncExecutor` when you want to run the workflow with supplied config and services:

```rust
let services = TranscriptSyncServices::new(
    meeting_source,
    resolver,
    fetcher,
    uploader,
    store,
);
let executor = TranscriptSyncExecutor::new(config, services);
let summary = executor.execute(request).await?;
```

Properties of this seam:
- config and services are owned by the executor instance
- overlapping executions do not share a process-global installed runtime
- the seam is example-local and does not change host/runtime core crates
- the same seam is used by the scheduled helper path

## Scheduled wrapper path

The landed thin scheduled path is:

```rust
let tick = ScheduledTick::new("2026-04-16T10:00:00Z", "*/5 * * * *");
let summary = executor.execute_scheduled_tick(&tick).await?;
```

For Cloudflare Workers, the example also exposes:
- `cloudflare::scheduled_tick_from_event(...)`
- `cloudflare::execute_scheduled_event(...)`

Those helpers convert `worker::ScheduledEvent` into the same `ScheduledTick` shape and then delegate to `TranscriptSyncExecutor`.

Important honest limits:
- the generic scheduled composition seam is runtime-proved in crate tests
- the Cloudflare-specific `worker::ScheduledEvent` adapter is currently compile-checked, not separately runtime-proved
- the remaining deployment blocker is narrow: this example still does not ship real Google Calendar / Docs / Drive or Zoom meeting-source / resolver / fetcher implementations, so deploy code must still compose those services before calling the wrapper

## Bundle and route contract

The flow preserves route metadata for:
- `POST /meeting-transcript-sync/run`

Current payload examples:
- `payloads/sample.json`
- `payloads/backfill-sample.json`

Important honest note:
- the bundle remains a truthful topology/bundle artifact
- the effectful production execution seam is `TranscriptSyncExecutor`, not a process-global installed runtime behind `bundle()`
- executing the bundle directly returns an explicit error pointing callers at `TranscriptSyncExecutor`

## Cloudflare storage slice

Implemented here:
- `cloudflare::D1TranscriptJobStore`
  - example-local D1-shaped ledger implementation under `src/cloudflare/d1_store.rs`
  - native tests run against a real SQLite schema shaped to the intended D1 tables/indexes
  - wasm builds expose `from_env(...)` / `from_database(...)` for Cloudflare D1 bindings
  - due-job selection is filtered by `org_scope`
- `cloudflare::R2TranscriptUploader`
  - example-local uploader under `src/cloudflare/r2_uploader.rs`
  - native tests write the real transcript object layout to a local bucket-shaped directory
  - wasm builds expose `from_env(...)` / `from_bucket(...)` for Cloudflare R2 bindings

Current R2 object layout:

```text
transcripts/<org>/<yyyy>/<mm>/<meeting_key>/
  source.json
  transcript.txt
  transcript.normalized.json
```

`UploadedTranscript.destination_uri` points at the canonical `transcript.txt` object.

## Current boundaries

Implemented here:
- flow-local reconciliation semantics
- meeting source classification policy
- transcript source resolution / ambiguity handling semantics
- retry vs permanent failure behavior
- upload retry semantics
- idempotent discovered-job upsert and org-scoped due-job selection
- example-owned direct execution seam and scheduled-tick composition seam
- D1/R2 integration adapters kept local to the example crate

Deferred here:
- real Google Calendar / Drive / Docs integration
- real Zoom transcript retrieval
- a fully assembled deployable Worker entrypoint with live provider services wired from env/bindings
- host-wasmtime or workerd proofing

## Provider groundwork usage

This example consumes the narrow provider groundwork only at the primitive boundary:
- Google Doc transcript refs carry Google Docs / Drive MIME metadata from `connector_google_platform`
- Zoom transcript refs carry connector metadata from `connector_zoom_platform`

Business policy remains flow-local.

## Verification

Current honest proof path:
- crate semantic tests through `TranscriptSyncExecutor`
- scheduled-tick tests proving the thin wrapper path
- example-local D1/R2 tests under `src/cloudflare/`
- native crate checks plus wasm guest check without default features

Targeted checks:

```bash
cargo check -p example-s14-meeting-transcript-sync
cargo test -p example-s14-meeting-transcript-sync -- --test-threads=1
cargo check -p example-s14-meeting-transcript-sync --target wasm32-unknown-unknown --no-default-features
```
