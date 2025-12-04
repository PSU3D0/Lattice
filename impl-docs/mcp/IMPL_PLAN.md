# MCP Plugin Implementation Plan

## Phase 0 — Research & Alignment
- Review `ref-crates/rust-sdk` to confirm expected transport surfaces (stdio, WebSocket, SSE) and session/resource APIs.
- Audit Flow IR metadata to confirm we can attach MCP annotations without schema changes (leverage existing `metadata` maps).
- Validate no conflicts with current RFC sections (notably §4.9 control flow annotations and §6 host adapter contracts).

## Phase 1 — Foundations
1. **Metadata Annotations**
   - Add `#[mcp(...)]` attribute support in `dag-macros` to capture tool names, descriptions, namespaces, streaming flags, and resource descriptors.
   - Extend Flow IR schema to include optional `mcp` metadata on workflow definitions; update docs (`impl-docs/impl-plan.md`, RFC §6).
   - Tests: trybuild fixtures validating attribute parsing; schema round-trip asserting metadata serialization.
2. **Catalog Builder**
   - Implement `plugin-mcp::catalog::{McpCatalog, ToolDescriptor, ResourceDescriptor}`.
   - Source data from Flow registry (workflows + metadata) and generate MCP `Tool` structures (JSON Schema references + descriptions).
   - Tests: unit tests ensuring grouping/namespacing; snapshot test for sample S1/S2 flows.

## Phase 2 — Server Handler & Execution Bridge
1. **Executor Bridge**
   - Define `ExecutorHandle` trait (wraps kernel execution) with `invoke_value` and `invoke_stream`.
   - Map MCP tool inputs into executor payloads (respect JSON schema validation).
   - Wire cancellation from MCP client → kernel cancellation token.
   - Tests: unit tests with mocked executor verifying cancellation and streaming behaviour.
2. **Session Mapping**
   - Create `RequestScopeFacet` storing `mcp_session_id`, attach when invoking workflows.
   - Provide helper to access session metadata inside nodes/capabilities.
   - Tests: ensure session id propagates and is cleared on completion.
3. **Server Handler**
   - Implement `rmcp::service::ServerHandler` with handlers for `list_tools`, `call_tool`, `subscribe`, `list_resources`, `read_resource`.
   - Support automatic progress notifications based on kernel metrics (hooks stubbed until metrics task finalised).
   - Tests: integration test using rmcp’s in-process transport invoking S1 workflow (value) and S2 (stream).

## Phase 3 — Resource Integration
- Register artifact/resource providers; allow workflows to expose resources via metadata.
- Implement lazy fetching via capabilities (BlobStore, File).
- Tests: end-to-end scenario where a workflow emits an artifact, MCP `read_resource` returns expected payload.

## Phase 4 — Gateway & Host Adapters
- Add optional `plugin-mcp::gateway` helper that resolves workflow location and wraps auth/quotas.
- Provide adapters/examples:
  - Axum host + MCP endpoint (reuse existing host-web-axum).
  - Cloudflare Workers example demonstrating deployment pipeline.
- Tests: CLI integration (`flows gateway mcp serve --example s1_echo`) once gateway CLI exists.

## Phase 5 — Documentation & Developer Experience
- Author `impl-docs/mcp/USER_STORIES.md` scenarios, README updates, tutorial showing MCP integration.
- Provide template project (similar to Workers templates) demonstrating plugin usage.
- Validate `cargo doc` for plugin crate; ensure docs.rs friendly.

## Out of Scope / Future
- Advanced prompt management or agent orchestration.
- Cross-session memory stores (left to capabilities).
- Non-Rust MCP transports (Python, JS).
