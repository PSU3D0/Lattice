# S11 lead intake

Representative AI-first business workflow proving a lead-intake flow with:
- typed extraction
- explicit branch topology
- outreach drafting
- image generation
- workspace artifact persistence

## What it proves

- explicit HTTP-triggered business workflow shape
- typed AI extraction and drafting over the current Lattice HTTP bridge
- workspace-backed artifact persistence for generated images
- native live smoke against real OpenAI
- wasm bundle export
- host-wasmtime execution proof in repo tests
- workerd/miniflare proof in the shared Workers test fixture

## Route

- `POST /leads`

## Sample payload

See:
- `payloads/live-sample.json`

## Current honest operator paths

### 1. Native live smoke against real OpenAI

This is the most honest current operator path for this example.

Required:
- `OPENAI_API_KEY`

Optional:
- `OPENAI_BASE_URL`
- `OPENAI_TEXT_MODEL`
- `OPENAI_IMAGE_MODEL`
- `S11_LIVE_OUTPUT_DIR`

Run:

```bash
cargo run -p example-s11-lead-intake --bin live_smoke --features native-smoke
```

What it writes:
- `input.json`
- `email_package.json`
- `workspace_entries.json`
- `summary.json`
- `hero.png`

Default output root:
- `scratch/s11-lead-intake-live/<timestamp>/`

The smoke binary also creates a run-scoped filesystem workspace under that output root and marks it complete at the end.

### 2. Bundle export proof

The example can be bundled as wasm:

```bash
cargo run -p flows-cli -- bundle -p example-s11-lead-intake --wasm --dev --out-dir ../scratch/s11.bundle
```

This is a real repo-backed proof path, and the repo already has tests covering the host-wasmtime roundtrip for this example.

## Current boundaries / what is not yet smooth

- There is still no built-in `flows run local --example s11_lead_intake` path; this example is not wired into the built-in CLI example registry yet.
- The most operator-friendly path today is the native `live_smoke` binary, not a fully polished generic deploy/bind/invoke path.
- The Workers proof exists in the shared `host-workers/workerd-tests` fixture, but it is still a repo test-harness proof rather than a documented first-class deploy command.
- This example is a strong sync/operator-attended path today, but not yet the canonical “deploy anywhere with one standard bindings story” example.

## Verification

Current targeted checks:

```bash
cargo check -p example-s11-lead-intake
cargo test -p example-s11-lead-intake
cargo check -p example-s11-lead-intake --target wasm32-unknown-unknown --no-default-features
cargo test -p flows-cli --test bundle run_bundle_s11_lead_intake_wasmtime_roundtrip -- --nocapture
cd crates/host-workers/workerd-tests && npm run test -- --run -t "s11 lead intake example"
```
