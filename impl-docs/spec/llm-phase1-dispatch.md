Status: Ready
Purpose: subagent-dispatch-brief
Owner: Runtime

# Phase 1 Dispatch: llm-types foundation

## Pre-read (REQUIRED)
1. `impl-docs/spec/llm-crate-topology.md` — architecture and source mapping
2. `impl-docs/spec/llm-implementation-plan.md` — full phase plan

## Working directory
`/home/psu3d0/Projects/psu3d0/coltec-codespaces/nexus/codespaces/lattice/latticeflow-lib-dev/codebase`

## Reference source
`ref-libs/rig/rig/rig-core/src/` (relative to workspace root at `..`)

Note: the rig repo is one level up from `codebase/` — reference paths are:
`/home/psu3d0/Projects/psu3d0/coltec-codespaces/nexus/codespaces/lattice/latticeflow-lib-dev/ref-libs/rig/rig/rig-core/src/`

## Goal
Populate `crates/llm-types/` with rig's core types and traits, stripped of reqwest dependency, compiling for both native and wasm32-unknown-unknown.

## Existing state
- `crates/llm-types/Cargo.toml` already has correct dependencies
- `crates/llm-types/src/lib.rs` is a stub with TODO comment
- Workspace already includes the crate
- Compiles for both targets (empty)

## What to copy

### 1. wasm_compat.rs (copy wholesale)
Source: `wasm_compat.rs` → `crates/llm-types/src/wasm_compat.rs`
Zero edits needed.

### 2. one_or_many.rs (copy wholesale)
Source: `one_or_many.rs` → `crates/llm-types/src/one_or_many.rs`
Zero edits needed. Remove any `use crate::` imports that reference modules we don't have.

### 3. json_utils.rs (copy wholesale)
Source: `json_utils.rs` → `crates/llm-types/src/json_utils.rs`
Zero edits needed.

### 4. completion/ (copy wholesale)
Source: `completion/mod.rs`, `completion/message.rs`, `completion/request.rs`
Target: `crates/llm-types/src/completion/`

Edits needed:
- Fix `use crate::` imports to point to sibling modules in llm-types
- `completion/request.rs` references `crate::wasm_compat`, `crate::tool`, `crate::completion::message`, `crate::one_or_many`, `crate::streaming`, `crate::json_utils`, `crate::embeddings` — these all need to resolve within llm-types

### 5. tool/mod.rs (copy, may need adaptation)
Source: `tool/mod.rs` → `crates/llm-types/src/tool.rs` (flatten from dir to file)

Check imports — it references `crate::embeddings::embed`, `crate::embeddings::tool`, `crate::completion::message`, `crate::json_utils`, `crate::wasm_compat`, `crate::one_or_many`.

Note: tool/rmcp.rs and tool/server.rs are optional features (MCP protocol). Skip them initially — they can be added behind feature gates later.

### 6. streaming.rs (copy wholesale)
Source: `streaming.rs` → `crates/llm-types/src/streaming.rs`
Check imports.

### 7. http_client/ (copy + strip reqwest)
Source: `http_client/` → `crates/llm-types/src/http_client/`

Files: `mod.rs`, `multipart.rs`, `sse.rs`, `retry.rs`

Edits for `mod.rs`:
- Remove `use reqwest::Body;`
- Remove `pub use reqwest::Client as ReqwestClient;`
- Remove `impl From<NoBody> for Body` (keep `impl From<NoBody> for Bytes`)
- Remove the entire `impl HttpClientExt for reqwest::Client` block (~60 lines)
- Remove the entire `impl HttpClientExt for reqwest_middleware::ClientWithMiddleware` block (behind cfg, ~60 lines)
- Remove the `#[cfg(test)] pub(crate) mod mock` block (move mock to test module if needed)

Edits for `multipart.rs`:
- Remove `impl From<MultipartForm> for reqwest::multipart::Form` block (~30 lines)

Edits for `sse.rs`:
- One comment reference to reqwest — remove or change to "HTTP client"

### 8. embeddings/ (copy wholesale)
Source: `embeddings/` → `crates/llm-types/src/embeddings/`
Files: `mod.rs`, `embed.rs`, `embedding.rs`, `distance.rs`, `builder.rs`, `tool.rs`
Check imports for `crate::` references.

### 9. Write lib.rs
Declare all modules and set up re-exports:
```rust
pub mod wasm_compat;
pub mod one_or_many;
pub mod json_utils;
pub mod completion;
pub mod tool;
pub mod streaming;
pub mod http_client;
pub mod embeddings;
```

## Key import patterns to fix

Rig uses `crate::` to reference sibling modules. In llm-types, these become:
- `crate::wasm_compat` → `crate::wasm_compat` (same)
- `crate::completion` → `crate::completion` (same)
- `crate::tool` → `crate::tool` (same)
- `crate::streaming` → `crate::streaming` (same)
- `crate::one_or_many` → `crate::one_or_many` (same)
- `crate::json_utils` → `crate::json_utils` (same)
- `crate::embeddings` → `crate::embeddings` (same)
- `crate::http_client` → `crate::http_client` (same)
- `crate::agent::*` → NOT in this crate (that's llm-agent Phase 2)
- `crate::client::*` → NOT in this crate
- `crate::extractor` → NOT in this crate
- `crate::providers::*` → NOT in this crate

If any file references `crate::agent`, `crate::client`, `crate::extractor`, or `crate::providers`, those items need to either:
- Be behind `#[cfg(feature = "...")]` gates that we don't enable
- Be removed or gated behind a TODO marker
- Be refactored into a trait boundary that llm-agent will satisfy

## Incremental compilation strategy

Copy files in this order, running `cargo check -p llm-types` after each group:

1. `wasm_compat.rs` → check
2. `one_or_many.rs` → check
3. `json_utils.rs` → check
4. `completion/` → check (this may need tool.rs first due to imports)
5. `tool.rs` → check
6. `streaming.rs` → check
7. `http_client/` → check
8. `embeddings/` → check

After all compile: `cargo check -p llm-types --target wasm32-unknown-unknown`

## Tests
- All inline `#[cfg(test)]` tests from rig should come along with the copied files
- Run `cargo test -p llm-types` after each group
- If tests fail due to missing imports from modules not yet copied, mark them `#[ignore]` with a comment

## Commit
Use: `feat(llm-types): populate core types and traits from rig`
Do NOT push.

## What NOT to do
- Do NOT add reqwest as a dependency
- Do NOT add tokio as a dependency
- Do NOT add wasm-bindgen as a dependency
- Do NOT add nanoid/fastrand/getrandom
- Do NOT copy files from `providers/`, `pipeline/`, `vector_store/`, `loaders/`, `evals.rs`, `telemetry/`
- Do NOT try to make the agent loop work (that's Phase 2)
