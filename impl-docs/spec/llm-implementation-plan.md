Status: Active
Purpose: implementation-plan
Owner: Runtime
Last reviewed: 2026-03-31

# LLM Crate Implementation Plan

Phased, incremental, test-driven plan for building Lattice's LLM crate family from rig source.

**Pre-requisites:**
- Read `impl-docs/spec/llm-crate-topology.md` first
- Reference source is at `ref-libs/rig/rig/rig-core/src/` (MIT licensed, v0.33.0)
- All work happens in `codebase/crates/llm-*/`

## Phase 1: llm-types foundation

**Goal:** A compiling crate with the core types, traits, and HTTP abstraction.

### Steps

1. **Copy wholesale** (zero edits needed):
   - `completion/message.rs` → `llm-types/src/completion/message.rs`
   - `completion/request.rs` → `llm-types/src/completion/request.rs`
   - `completion/mod.rs` → `llm-types/src/completion/mod.rs`
   - `tool/mod.rs` → `llm-types/src/tool.rs`
   - `one_or_many.rs` → `llm-types/src/one_or_many.rs`
   - `json_utils.rs` → `llm-types/src/json_utils.rs`
   - `wasm_compat.rs` → `llm-types/src/wasm_compat.rs`
   - `streaming.rs` → `llm-types/src/streaming.rs`

2. **Copy + strip reqwest** (keep trait def, remove reqwest impl):
   - `http_client/mod.rs` → `llm-types/src/http_client/mod.rs`
     - Keep: `HttpClientExt` trait, `Error` enum, `LazyBody`/`LazyBytes`/`StreamingResponse` types, `NoBody`, helper functions (`text()`, `make_auth_header()`, `bearer_auth_header()`, `with_bearer_auth()`)
     - Remove: `use reqwest::Body`, `impl HttpClientExt for reqwest::Client`, `impl HttpClientExt for reqwest_middleware::ClientWithMiddleware`, `pub use reqwest::Client as ReqwestClient`, `impl From<NoBody> for reqwest::Body`, mock module (move to tests)
     - Replace: `NoBody` → `impl From<NoBody> for Bytes` (keep), remove `impl From<NoBody> for Body`
   - `http_client/multipart.rs` → `llm-types/src/http_client/multipart.rs`
     - Remove: `impl From<MultipartForm> for reqwest::multipart::Form` block
   - `http_client/sse.rs` → `llm-types/src/http_client/sse.rs`
     - One comment ref to reqwest, trivial
   - `http_client/retry.rs` → `llm-types/src/http_client/retry.rs`
     - Zero edits

3. **Write `llm-types/src/lib.rs`** as the module root re-exporting everything.

4. **Write `llm-types/Cargo.toml`** with deps:
   ```toml
   [dependencies]
   serde = { version = "1", features = ["derive"] }
   serde_json = "1"
   schemars = "1"
   thiserror = "2"
   tracing = "0.1"
   bytes = "1"
   futures = "0.3"
   http = "1"
   async-stream = "0.3"
   eventsource-stream = "0.2"
   pin-project-lite = "0.2"
   ordered-float = "5"
   mime = "0.3"
   base64 = "0.22"
   url = "2"
   ```

5. **Add MIT license notice** to `llm-types/src/lib.rs`:
   ```rust
   //! LLM types and traits for Lattice.
   //!
   //! Derived from [rig](https://github.com/0xPlaygrounds/rig) (MIT license).
   //! Copyright (c) 2024, Playgrounds Analytics Inc.
   ```

6. **Fix imports:** All rig internal imports (`use crate::...`) need to be adjusted for the new module structure. The main changes:
   - `crate::wasm_compat::*` paths stay the same if wasm_compat is at crate root
   - `crate::completion::*` stays the same
   - `crate::tool::*` becomes `crate::tool::*`
   - `crate::embeddings::*` stays the same
   - `crate::streaming::*` stays the same

7. **Compile check:**
   - `cargo check -p llm-types`
   - `cargo check -p llm-types --target wasm32-unknown-unknown`

8. **Copy and adapt tests from rig** — rig has inline tests in many of these files. They should come along with the copy.

### Verification
- `cargo test -p llm-types`
- `cargo check -p llm-types --target wasm32-unknown-unknown`
- No reqwest, no wasm_bindgen, no tokio in the dep tree

## Phase 2: llm-agent

**Goal:** The agent tool-use loop, extractor, and client builder compile and pass tests.

### Steps

1. **Copy wholesale** (zero edits):
   - `agent/mod.rs` → `llm-agent/src/agent/mod.rs`
   - `agent/builder.rs` → `llm-agent/src/agent/builder.rs`
   - `agent/completion.rs` → `llm-agent/src/agent/completion.rs`
   - `agent/tool.rs` → `llm-agent/src/agent/tool.rs`
   - `agent/prompt_request/mod.rs` → `llm-agent/src/agent/prompt_request/mod.rs`
   - `agent/prompt_request/hooks.rs` → `llm-agent/src/agent/prompt_request/hooks.rs`
   - `agent/prompt_request/streaming.rs` → `llm-agent/src/agent/prompt_request/streaming.rs`
   - `extractor.rs` → `llm-agent/src/extractor.rs`
   - `tools/mod.rs` + `tools/think.rs` → `llm-agent/src/tools/`

2. **Copy + edit** (replace `= reqwest::Client` defaults):
   - `client/mod.rs` → `llm-agent/src/client/mod.rs`
   - `client/builder.rs` → `llm-agent/src/client/builder.rs`
   - `client/completion.rs`, `client/embeddings.rs`, etc. → `llm-agent/src/client/`
   - `client/model_listing.rs` → `llm-agent/src/client/model_listing.rs`
   - Edit: remove all `= reqwest::Client` type parameter defaults, or replace with a generic-only form
   - Edit: change `use crate::` imports to `use llm_types::` where needed

3. **Write `llm-agent/Cargo.toml`:**
   ```toml
   [dependencies]
   llm-types = { path = "../llm-types" }
   serde = { version = "1", features = ["derive"] }
   serde_json = "1"
   schemars = "1"
   futures = "0.3"
   tracing = "0.1"
   ```

4. **Write `llm-agent/src/lib.rs`** with module declarations and re-exports.

5. **Compile check** for both native and wasm32.

### Verification
- `cargo test -p llm-agent`
- `cargo check -p llm-agent --target wasm32-unknown-unknown`

## Phase 3: llm-provider-openai

**Goal:** OpenAI Chat Completions provider compiles and has contract tests.

### Steps

1. **Copy** `providers/openai/` → `llm-provider-openai/src/`
   - `client.rs` — the OpenAI client builder
   - `completion/mod.rs` — Chat Completions API impl
   - `completion/streaming.rs` — streaming response parser
   - `embedding.rs` — embedding model
   - `mod.rs` — module root + types
   - **Skip initially:** `responses_api/` (Responses API + websocket, can add later), `audio_generation.rs`, `image_generation.rs`, `transcription.rs`

2. **Edit:** Replace all `= reqwest::Client` with no default. Change `use crate::` to `use llm_types::` / `use llm_agent::`.

3. **Write `llm-provider-openai/Cargo.toml`:**
   ```toml
   [dependencies]
   llm-types = { path = "../llm-types" }
   serde = { version = "1", features = ["derive"] }
   serde_json = "1"
   tracing = "0.1"
   base64 = "0.22"
   ```

4. **Write contract tests:**
   - Serialize a CompletionRequest to the expected OpenAI JSON format
   - Deserialize a sample OpenAI response
   - Deserialize a streaming SSE chunk sequence
   - Verify tool_choice serialization
   - Verify structured output schema serialization

### Verification
- `cargo test -p llm-provider-openai`
- `cargo check -p llm-provider-openai --target wasm32-unknown-unknown`

## Phase 4: llm-provider-anthropic

**Goal:** Anthropic Messages API provider compiles and has contract tests.

### Steps

1. **Copy** `providers/anthropic/` → `llm-provider-anthropic/src/`
   - `client.rs`, `completion.rs`, `streaming.rs`, `mod.rs`
   - `decoders/` — JSONL, line, SSE decoders

2. **Edit:** Same pattern as OpenAI — replace defaults, fix imports.

3. **Contract tests:** Same pattern — serialize/deserialize request/response.

### Verification
- `cargo test -p llm-provider-anthropic`
- `cargo check -p llm-provider-anthropic --target wasm32-unknown-unknown`

## Phase 5: Lattice bridge + explicit AI example path

**Goal:** Land the Lattice integration seam and the first explicit topological AI example.

This phase follows the architecture in:
- `impl-docs/spec/ai-surface-and-layering.md`
- `impl-docs/spec/agent-loop-runtime.md`
- `impl-docs/spec/llm-lead-intake-example.md`

The important decision is:
- `llm-*` crates are the provider/protocol substrate,
- `llm-lattice` is the bridge/integration layer,
- the first worked example should use **explicit graph-visible AI steps**, not
  a bounded agent loop.

### Phase 5a — `llm-lattice` bridge crate

1. **LatticeHttpClient adapter** (new `crates/llm-lattice/`):
   - `impl HttpClientExt for LatticeHttpClient`
   - Converts `http::Request<T>` → `capabilities::http::HttpRequest`
   - Calls Lattice `HttpRead` / `HttpWrite`
   - Converts `capabilities::http::HttpResponse` → `http::Response<LazyBody<U>>`

2. **Scope constraints:**
   - non-streaming first
   - multipart may be unsupported initially if the first example does not need it
   - no reqwest/tokio/wasm-bindgen

### Phase 5b — OpenAI image-generation wiring fix

1. Verify and repair client capability exposure so the already-ported OpenAI
   image-generation implementation is cleanly consumable through the provider
   client path.
2. Current proved image path uses `gpt-image-1.5`.
3. Add focused tests for image-generation request/response handling.

### Phase 5c — structured-output contract proof

1. Add a targeted OpenAI contract test proving that `output_schema` is mapped to
   strict OpenAI `response_format = json_schema` payloads.
2. This reduces risk before the first example depends heavily on structured
   extraction.

### Phase 5d — first serious example: `examples/s11_lead_intake/`

Build the first explicit AI example with:
- trigger → `extract_lead` → branch → `draft_outreach` → `generate_image` →
  `store_image` → `compose_email` → capture
- OpenAI end-to-end for first pass
- generated image persisted to workspace
- explicit graph-visible nodes first; bounded agent loop is later work

### Phase 5e — native + wasm bundle proof

1. Native proof with mock HTTP server returning canned provider responses.
2. Wasmtime bundle proof with mock HTTP + `MemoryWorkspace`.
3. Validate the example through standard CLI bundle paths.
4. Real native live smoke with OpenAI credentials and artifact output.

### Phase 5f — Workers/miniflare proof

1. Reuse or extract `crates/host-workers/workerd-tests/` infrastructure.
2. Run the example under `host-workers` with workspace backed by R2 + DO.
3. Validate route execution and workspace image persistence under workerd/miniflare.

### Verification
- `cargo check -p llm-lattice`
- `cargo check -p llm-lattice --target wasm32-unknown-unknown`
- `cargo test -p llm-provider-openai`
- native example tests
- wasm bundle proof for the example
- full host-wasmtime execution proof for the example
- workerd/miniflare proof for the example
- real native live smoke with artifact output

### Phase 5 status snapshot

As of the current state, the following are landed:
- `llm-lattice` bridge crate
- OpenAI structured-output and image-generation proofs
- `example-s11-lead-intake`
- bundle metadata/load proof
- full host-wasmtime execution proof for s11
- workerd/miniflare proof for s11
- real native live smoke for s11 using `gpt-5.4-mini` + `gpt-image-1.5`

The remaining follow-on is no longer “make s11 run”, but rather broader AI
surface/product work: more examples, bounded agent loop runtime, additional
providers, and deployment UX.

## Phase 6 (optional): Additional providers

Port remaining rig providers as needed:
- `providers/gemini/` — Google Gemini (important for Vertex AI)
- `providers/groq/` — Groq (fast inference)
- `providers/ollama/` — Ollama (local models)
- `providers/deepseek/` — DeepSeek
- Each is a separate crate: `llm-provider-gemini`, `llm-provider-groq`, etc.

## Phase 7 (optional): Telemetry

Evaluate rig's `telemetry/` module. If it follows OpenTelemetry spans/traces standard and the emerging agent telemetry conventions, adapt it. Otherwise build Lattice-native telemetry using existing tracing infrastructure.

## Dispatch guidance for subagents

Each phase can be dispatched as a **fresh** subagent task. The subagent should:

1. **Read first:** `impl-docs/spec/llm-crate-topology.md`, this file, and when working on post-extraction integration also read:
   - `impl-docs/spec/ai-surface-and-layering.md`
   - `impl-docs/spec/agent-loop-runtime.md`
   - `impl-docs/spec/ai-phase-next-dispatch.md`
2. **Reference source:** `ref-libs/rig/rig/rig-core/src/` — read actual files before copying
3. **Copy then fix:** Copy files first, then fix imports/reqwest refs, then compile
4. **Test incrementally:** `cargo check` after each file group, `cargo test` before declaring done
5. **Both targets:** Always verify both `cargo check -p <crate>` and `cargo check -p <crate> --target wasm32-unknown-unknown`
6. **MIT notice:** Include copyright notice in lib.rs of each crate
7. **Don't push:** Commit but don't push — parent session reviews first
