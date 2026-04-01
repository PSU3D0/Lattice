Status: Draft
Purpose: dispatch-brief
Owner: Runtime
Last reviewed: 2026-03-31

# AI Next-Phase Dispatch Brief

This brief exists so future sessions or subagents can continue AI/example work
without re-litigating the architecture.

## Read first

1. `impl-docs/spec/ai-surface-and-layering.md`
2. `impl-docs/spec/agent-loop-runtime.md`
3. `impl-docs/spec/connector-op-reuse-and-node-declaration.md`
4. `impl-docs/spec/llm-implementation-plan.md`
5. `impl-docs/spec/llm-lead-intake-example.md`

## Current architectural stance

- HTTP remains the raw host capability.
- AI operations are a Lattice semantic layer above HTTP.
- `llm-*` crates are provider/protocol substrate.
- `llm-lattice` is the integration/bridge layer, not the final semantic family.
- The first example should use **explicit graph-visible AI steps**.
- A bounded agent loop is a **later semantic node-local runtime**, not the first
  example.

## Status note

The original sequence below has now been completed for `example-s11-lead-intake`.
It remains useful as a historical implementation pattern for future examples.

What is now landed:
- `llm-lattice`
- OpenAI structured-output + image-generation wiring
- `example-s11-lead-intake`
- native tests
- real native live smoke
- host-wasmtime execution proof
- workerd/miniflare proof

## Immediate implementation sequence

## Slice 1 — `crates/llm-lattice/`

Build the bridge crate.

### Goal
Implement a minimal `LatticeHttpClient` that adapts:
- `llm_types::http_client::HttpClientExt`
- to Lattice `capabilities::http::{HttpRead, HttpWrite}`

### Constraints
- non-streaming first
- multipart may return unsupported initially if not needed by the example
- no reqwest/tokio/wasm-bindgen
- must compile for native and wasm32

### Verification
- `cargo check -p llm-lattice`
- `cargo check -p llm-lattice --target wasm32-unknown-unknown`
- adapter unit tests for request/response conversion

## Slice 2 — OpenAI image-generation wiring fix

### Goal
Make the already-ported OpenAI image-generation surface consumable through the
client capability path.

### Expected tasks
- verify `llm-agent` feature gating around image generation
- wire `ImageGeneration` capability exposure cleanly in
  `llm-provider-openai`
- add focused tests for image-generation request/response plumbing

### Verification
- `cargo test -p llm-provider-openai`
- explicit contract or unit test proving DALL-E 3 request/response path

## Slice 3 — structured-output proof

### Goal
Add one focused proof that `output_schema` is correctly mapped to OpenAI strict
JSON-schema request format.

### Verification
- contract test in `llm-provider-openai`
- verify `response_format.type = "json_schema"`
- verify strict mode and sanitized schema behavior

## Slice 4 — scaffold `examples/s11_lead_intake/`

### Goal
Build the first explicit topological AI example.

### Scope
- `extract_lead`
- branch on priority
- `draft_outreach`
- `generate_image`
- `store_image`
- `compose_email`
- `template_response`
- capture

### Constraints
- use OpenAI end-to-end for first pass
- current proved image path is `gpt-image-1.5`
- store generated image bytes in workspace
- explicit topology first, not bounded agent loop

## Slice 5 — native + wasmtime proofs

### Goal
Prove the example on:
- native local execution
- wasm bundle execution via wasmtime

### Verification
- unit tests for pure nodes
- integration test with mock HTTP server
- wasm bundle proof with mock HTTP + MemoryWorkspace

## Slice 6 — Workers/miniflare proof

### Goal
Run the example under `host-workers` with workspace backed by R2/DO.

### Scope
- reuse or extract `crates/host-workers/workerd-tests/` infrastructure
- mock provider HTTP if possible
- validate workspace artifact persistence path

### Verification
- miniflare/workerd integration test
- route invocation proves end-to-end flow execution
- workspace artifact inspection proves image persistence

## Subagent breakdown recommendation

Use **fresh** subagents with narrow scopes.

### Recommended dispatches

1. **Bridge subagent**
   - build `crates/llm-lattice/`
   - no example work

2. **OpenAI image-wiring subagent**
   - patch/wire image-generation capability exposure
   - add provider tests

3. **Example scaffold subagent**
   - build `examples/s11_lead_intake/`
   - native tests first

4. **Bundle proof subagent**
   - add wasm bundle execution proof
   - no Workers work yet

5. **Workers proof subagent**
   - extract/reuse workerd fixture pieces
   - add miniflare test path for the example

## Guardrails for subagents

- Do not push to remote automatically.
- Keep slices narrow and testable.
- Prefer explicit graph-visible AI nodes first.
- Do not turn the first example into an agent-loop implementation project.
- Treat Workers secrets/bindings as host-owned concerns, not guest `std::env`
  assumptions.
- Reuse existing workspace/runtime infrastructure where possible.

## Cloudflare-ready end state for the example

A worked example is considered ready when all are true:
- native test proof passes
- real native live smoke passes
- wasm bundle proof passes
- full host-wasmtime execution proof passes
- workerd/miniflare proof passes
- workspace artifact path is exercised on Workers via R2/DO
- OpenAI completion + image generation both flow through the Lattice bridge
- docs explain any retention requirement for inspecting workspace artifacts
