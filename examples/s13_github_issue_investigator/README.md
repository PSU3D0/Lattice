# S13 GitHub issue investigator

This example is the first phased slice of a more ambitious Lattice-native issue
investigation workflow.

## What the current phase proves

- explicit GitHub-issue-like workflow topology
- typed AI triage output
- explicit outbound sandbox dispatch envelope construction
- halt at the dispatch boundary
- tokenized local/native callback resume proof
- Workers/workerd callback-resume proof through `POST /__lattice/resume`
- standard bundleable example shape

Implementation notes:
- the current phase uses a **combined halting dispatch node** (`dispatch_investigation_job`) rather than a separate standalone `std.callback.wait` node, while still following the source-of-truth callback-resume contract
- dispatch now goes through the extracted minimal `dispatch-backend` seam rather than hand-rolled HTTP in the example
- the workerd proof uses the test harness' mocked OpenAI + sandbox HTTP path and a test-only bundle selector so the real example bundle can be exercised under the shared worker fixture
- the wait/resume shape is split into distinct inner states:
  - `InvestigationDispatchState` for the halted dispatch boundary
  - `InvestigationResumeState` for the resumed semantic result
  - wrapped by `InvestigationAwaitOutput` so the flow edge remains type-consistent

## What it does **not** prove yet

Not in this phase:
- host-wasmtime/bundle truth-in-advertising for the halt/resume path
- GitHub mutation actions (comment/labels)
- stronger callback hardening (HMAC/attestation)
- real sandbox artifact upload protocol

Those are planned in later phases from:
- `ops/s13-github-issue-investigator-phased-plan-2026-04-03.md`

## Route

- `POST /github/issues`

## Sample payload

See:
- `payloads/sample.json`

## Current flow shape

- trigger
- `triage_issue_agent`
- branch:
  - `prepare_investigation_request`
  - `request_more_info`
- investigation branch:
  - `dispatch_investigation_job` (halts)
  - `review_investigation_result`
- capture

## Current honest operator paths

### 1. Native proof path

This is the simplest current proof path for the example logic itself:

```bash
cargo check -p example-s13-github-issue-investigator
cargo test -p example-s13-github-issue-investigator -- --test-threads=1
cargo check -p example-s13-github-issue-investigator --target wasm32-unknown-unknown --no-default-features
cargo test -p dispatch-backend
```

This proves:
- typed triage
- outbound dispatch envelope construction
- halt at the dispatch boundary
- typed resume payload handling in native/inproc tests

### 2. Bundle export proof

The example bundles as wasm:

```bash
cargo run -p flows-cli -- bundle -p example-s13-github-issue-investigator --wasm --dev --out-dir ../scratch/s13.bundle
```

Current honest note:
- bundle export works,
- but the general operator-facing `flows run bundle ...` path is not yet smooth for this example because the serious connector-runtime / bound-connection wiring still lives mostly behind test/runtime seams.

### 3. Workers/workerd callback-resume proof

The strongest current deploy-like proof is the shared workerd/miniflare fixture:

```bash
cd crates/host-workers/workerd-tests && npm run test -- --run -t "s13 GitHub issue investigator"
```

That path proves:
- `POST /github/issues`
- halt at `dispatch_investigation_job`
- callback-style `POST /__lattice/resume`
- final typed resumed result

## Current operator boundary

The runtime proof is real, but the shortest operator-facing path still stops at a few seams:

- this example is not yet a built-in CLI example target
- the obvious bundle execution path still needs a clearer public connector-runtime / bindings story
- the workerd proof currently depends on the shared test fixture's mocked HTTP + test-only bundle selector
- host-wasmtime/bundle truth-in-advertising for the full halt/resume lifecycle is still not proved

## Verification

Current targeted checks:

```bash
cargo check -p example-s13-github-issue-investigator
cargo test -p example-s13-github-issue-investigator -- --test-threads=1
cargo check -p example-s13-github-issue-investigator --target wasm32-unknown-unknown --no-default-features
cargo test -p dispatch-backend
cargo run -p flows-cli -- bundle -p example-s13-github-issue-investigator --wasm --dev --out-dir ../scratch/s13.bundle
cd crates/host-workers/workerd-tests && npm run test -- --run -t "s13 GitHub issue investigator"
```
