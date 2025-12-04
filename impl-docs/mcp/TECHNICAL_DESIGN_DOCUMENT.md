# Model Context Protocol Plugin — Technical Design

## Purpose & Scope
- Provide a reusable `plugin-mcp` crate that bridges Flow IR workflows into Model Context Protocol (MCP) tool servers without baking protocol details into individual hosts.
- Support both self-hosted deployments (Axum, Workers, queue) and the managed invocation gateway described in `impl-docs/rust-workflow-tdd-rfc.md` §§6.4–6.4.1 by sharing a single catalog surface.
- Keep core runtime agnostic to MCP; the plugin consumes Flow IR metadata and emits MCP tool/session/resource handlers while delegating execution to existing hosts.

## Design Goals
- **Thin integration**: Reuse the official `rmcp` SDK (`ref-crates/rust-sdk`) for protocol transport; `plugin-mcp` supplies glue, not a new runtime.
- **Catalog parity**: Automatically derive MCP tool metadata (namespaced identifiers, descriptions, streaming flags, JSON schemas) from Flow IR so documentation and policy remain in sync.
- **Session continuity**: Map MCP session identifiers into `RequestScope` facets, enabling workflows to opt into conversational state via capabilities (Durable Object, KV) without the plugin owning persistence.
- **Resource exposure**: Translate Flow IR artifact declarations into MCP resource listings and stream content via existing capability adapters (BlobStore, File, etc.).
- **Gateway alignment**: Allow the managed gateway to load the same catalog, enforce tenancy/auth, and route requests to heterogeneous hosts (Workers, Axum, Temporal).

## High-Level Architecture
```
rmcp transport (stdio | WebSocket | HTTP SSE)
          │
   plugin-mcp (ServerHandler impl)
          │  uses FlowRegistry + ExecutorHandle
          ▼
     Flow runtime (kernel + host adapters)
```
- `plugin-mcp` owns an `McpCatalog` built from Flow IR manifests (`schemas/flow_ir.schema.json`). Each catalog entry references a workflow trigger, annotated with MCP metadata (`#[mcp(...)]` attributes) that are compiled into the IR.
- For each MCP tool invocation, the plugin:
  1. Resolves the workflow + target host via the Flow registry (delegating to the invocation gateway when present).
  2. Validates payloads against the JSON schema derived from Flow IR input types.
  3. Launches execution through the kernel executor; if the workflow emits `ExecutionResult::Stream`, the plugin forwards chunks via MCP streaming responses.
- Sessions use rmcp’s session managers (`streamable_http_server::session`) and store `SessionId` inside `RequestScope::session_id`, allowing downstream nodes/capabilities to correlate state.
- Resources map to Flow IR artifacts or capability handles. Flow authors tag outputs with `#[artifact(mcp_resource = "...")]`; the plugin registers providers that fetch data through capabilities.

## Key Modules
- `catalog`: Builds and caches MCP tool/resource registries from Flow IR (`impl-docs/rust-workflow-tdd-rfc.md` §§4.7–4.9).
- `annotations`: Proc-macros / helper APIs that inject MCP metadata into Flow IR during macro expansion without leaking MCP types into user code.
- `session`: Adapter that exposes rmcp session lifecycle to the runtime (`RequestScope`) and publishes cancellation tokens for early termination.
- `service`: Implements `rmcp::service::ServerHandler`, calling into the kernel executor and multiplexing responses (value vs stream).
- `gateway`: Optional facade used by the managed invocation gateway; wraps catalog lookup, tenancy filters, and host routing.

## Compatibility & Interactions
- **Hosts**: Axum host remains a turnkey HTTP server; Workers/Temporal hosts are unaffected. The gateway may embed the plugin to expose MCP endpoints alongside REST.
- **Capabilities**: No protocol-specific capability is required. MCP metadata references existing capabilities (e.g., `cap-http`, `cap-cache`). Policy checks (EFFECT/DET) still apply because tool annotations reuse Flow IR hints.
- **Schema evolution**: `plugin-mcp` emits tool schemas that reference Flow IR `$id` values. Breaking changes reuse the existing capability/versioning flow (§6.3.1 of the RFC).
- **Security**: The plugin exposes hooks for auth/allow-list checks. Self-hosted deployments can register guards; the gateway enforces tenant policies centrally.
- **Back-pressure & streaming**: Streaming flows require `#[mcp(streaming = true)]`. The plugin enforces that flag and forwards cancellation when MCP clients drop the stream.

## Open Questions & Future Enhancements
- Multi-tenant namespace strategy: default to `tenant.workflow_name`; allow overrides via annotations; investigate hierarchical tool groups for large catalogs.
- Prompt/resource surfacing: Decision whether to auto-map Flow IR templates into MCP prompts or leave optional for future work.
- Telemetry: Determine if we surface MCP request/latency metrics via the existing metrics plan (`impl-docs/current_task/task_metrics_instrumentation.md`).
