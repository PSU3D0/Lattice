# MCP Plugin User Stories

## 1. Agent Platform Operator
- **As** a platform engineer running the managed invocation gateway  
- **I want** to expose all tenant-approved workflows as MCP tools via a single endpoint  
- **So that** AI agent clients (e.g., Claude, ChatGPT) can discover and invoke flows without bespoke integration.  
- **Acceptance**  
  - Gateway loads `plugin-mcp` catalog, filters by tenant visibility, and lists tools with namespaced identifiers.  
  - Invoking a tool fans out to the correct host (Axum, Workers, Temporal) and streams results when flagged.  
  - Catalog updates when new workflows are deployed, without restarting the gateway.

## 2. Workflow Author (Self-Hosted)
- **As** an engineer building internal automation flows  
- **I want** to declare MCP metadata alongside my `#[flow]` definitions  
- **So that** I can ship a standalone MCP server on Axum for my team’s agent without hand-writing protocol code.  
- **Acceptance**  
  - Adding `plugin-mcp` and tagging flows produces a runnable MCP binary.  
  - JSON schemas match the Flow IR definition, including optional fields and enums.  
  - Cancelling a request from the MCP client cancels the underlying workflow execution.

## 3. Multi-Session Agent Integrator
- **As** an AI tooling engineer  
- **I want** MCP session identifiers propagated into my workflow context  
- **So that** I can store per-session state (via KV/Durable Object) and resume conversations.  
- **Acceptance**  
  - Workflows receive `ctx.facets().mcp_session_id()` (or equivalent helper).  
  - Session end triggers cancellation hooks, allowing me to release resources.  
  - Concurrent sessions remain isolated; cross-session leakage causes validation failure.

## 4. Artifact Consumer
- **As** a compliance analyst  
- **I want** workflows to publish generated documents as MCP resources  
- **So that** the agent can fetch artefacts (PDF, text) after a tool call.  
- **Acceptance**  
  - Workflows tagged with `#[artifact(mcp_resource = "...")]` appear in `list_resources`.  
  - `read_resource` streams blobs through existing capability adapters (e.g., BlobStore).  
  - Access control respects capability policies (sensitive resources can be gated).

## 5. Cloudflare Workers Deployment
- **As** a developer targeting Cloudflare Workers + Neon  
- **I want** to deploy the MCP server on Workers while still invoking Neon-backed workflows  
- **So that** I have a global, low-latency MCP endpoint reusing the same flows.  
- **Acceptance**  
  - `plugin-mcp` integrates with the Workers host adapter and compiles to `wasm32-unknown-unknown`.  
  - Tool invocations route to Workers-hosted flows or remote hosts as configured.  
  - Streaming and resource APIs operate within Workers’ limits (SSE emulation where needed).
