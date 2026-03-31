Status: Active
Purpose: architecture-spec
Owner: Runtime
Last reviewed: 2026-03-31

# LLM Crate Topology

## Overview

Lattice's LLM support is built by extracting and adapting code from [rig](https://github.com/0xplaygrounds/rig) (MIT license, v0.33.0). Rig's core abstractions are excellent but shipped as a monolith crate with a hard reqwest dependency, making it unusable in Lattice's wasm32/wasmtime/Workers environments. We split rig's code into properly separated crates that integrate with Lattice's capability system.

## Why not depend on rig-core directly?

1. **reqwest is a hard non-optional dependency.** On wasm32, reqwest uses `web_sys::fetch()` via `wasm_bindgen`, which requires a browser/JS host. Lattice's wasmtime sandbox and Cloudflare Workers don't provide that.
2. **tokio is a hard dependency.** No-op on wasm32 but adds weight.
3. **futures-timer on wasm uses gloo-timers → js_sys::setTimeout.** Same browser-only problem.
4. **60MB+ rlib on wasm32.** Too heavy for flow bundles.
5. **rig's crate topology is monolithic.** The useful traits, the reqwest impl, and 20+ providers are all in one crate. You can't take the traits without taking reqwest.

## Crate topology

```
crates/llm-types/            # Foundation types + traits (zero platform deps)
crates/llm-agent/            # Agent loop, extractor, hooks (depends on llm-types)
crates/llm-provider-openai/  # OpenAI provider (depends on llm-types)
crates/llm-provider-anthropic/ # Anthropic provider (depends on llm-types)
```

### llm-types

The foundation. Contains:
- `CompletionModel` trait (generic over HTTP client)
- `CompletionRequest` / `CompletionResponse` structs
- `Message` types (system, user, assistant, tool)
- `Tool` trait with typed `Args: JsonSchema + Deserialize`, `Output: Serialize`, `Error`
- `ToolDefinition` / `ToolChoice` / `ToolSet`
- `HttpClientExt` trait (the pluggable HTTP layer — without any reqwest impl)
- SSE streaming types and parser
- `OneOrMany<T>` utility type
- JSON Schema manipulation helpers
- Embedding types
- `WasmCompatSend` / `WasmCompatSync` compatibility traits

**Dependencies:** serde, serde_json, schemars (1.x), thiserror, tracing, bytes, futures, http, async-stream, eventsource-stream, pin-project-lite, ordered-float, mime, base64, url

**NOT included:** nanoid (uses getrandom → fails on wasm32-unknown-unknown without JS), fastrand (only used in vector_store), mime_guess (not needed in core types). Provider crates that need ID generation should use `nanoid` with a `getrandom` feature gate or use a simpler ID scheme.

**Platform deps:** NONE. Compiles to wasm32-unknown-unknown.

### llm-agent

The agent runtime. Contains:
- `AgentBuilder` — configure model + preamble + tools + dynamic context + hooks
- `PromptRequest` — the bounded tool-use loop with `max_turns`, concurrent tool execution
- `PromptHook` trait — `on_completion_call`, `on_tool_call`, `on_tool_result` with Continue/Terminate
- `Extractor<M, T>` — structured output extraction via forced tool_choice to a submit tool
- Streaming agent support
- Client builder scaffolding

**Dependencies:** llm-types, serde, serde_json, futures, tracing

**Platform deps:** NONE.

### llm-provider-openai

OpenAI Chat Completions + Responses API implementation. Contains:
- `CompletionModel` impl for OpenAI
- Request/response serialization for OpenAI's API format
- Streaming response parsing
- Model listing

**Dependencies:** llm-types, serde, serde_json, tracing, base64

**Platform deps:** NONE. Generic over `H: HttpClientExt`.

### llm-provider-anthropic

Anthropic Messages API implementation. Contains:
- `CompletionModel` impl for Anthropic
- Request/response serialization for Anthropic's API format
- SSE/JSONL streaming decoders

**Dependencies:** llm-types, serde, serde_json, tracing

**Platform deps:** NONE. Generic over `H: HttpClientExt`.

## Integration with Lattice

### HttpClientExt adapter

A ~50 line adapter bridges rig's `HttpClientExt` trait to Lattice's `capabilities::http::HttpWrite`:

```rust
struct LatticeHttpClient {
    write: Arc<dyn capabilities::http::HttpWrite>,
}

impl llm_types::HttpClientExt for LatticeHttpClient {
    // Convert http::Request → capabilities::http::HttpRequest
    // Call self.write.send(request).await
    // Convert capabilities::http::HttpResponse → http::Response<LazyBody<U>>
}
```

This adapter lives in whatever crate provides the Lattice LLM node definitions (likely a `llm-lattice` or just in node crate code).

### Auth / API keys

Provider API keys come from Lattice's connector binding model, not env vars. The provider crates accept API keys as constructor parameters (they already do — `rig::providers::openai::Client::new(api_key)` takes a string). The Lattice connector runtime resolves the key from bindings-lock.

### Portability

Because all four crates have zero platform deps:
- Native flows: LatticeHttpClient wraps `cap_http_reqwest::ReqwestHttpClient`
- Wasm bundles: LatticeHttpClient wraps `RemoteHttpWrite` (opcode bridge to host)
- Workers: LatticeHttpClient wraps `cap_http_workers::WorkersHttpClient`

Same flow, different hosts, same LLM provider code.

## Source mapping from rig

### llm-types (copy wholesale from rig-core/src/)

| Destination | Source | Lines | Edits |
|-------------|--------|-------|-------|
| completion/ | completion/ | 2,541 | none |
| tool/mod.rs | tool/mod.rs | 550 | none |
| streaming.rs | streaming.rs | 996 | none |
| one_or_many.rs | one_or_many.rs | 730 | none |
| json_utils.rs | json_utils.rs | 215 | none |
| wasm_compat.rs | wasm_compat.rs | 76 | none |
| embeddings/ | embeddings/ | 1,079 | none |
| http_client/ | http_client/ | 1,229 | strip reqwest impl (~150 lines), replace Body type |

### llm-agent (copy wholesale from rig-core/src/)

| Destination | Source | Lines | Edits |
|-------------|--------|-------|-------|
| agent/ | agent/ | 3,362 | none |
| extractor.rs | extractor.rs | 433 | none |
| client/ | client/ | 2,080 | replace `= reqwest::Client` defaults |
| tools/ | tools/ | 97 | none |

### llm-provider-openai (copy + replace defaults)

| Source | Lines | Edits |
|--------|-------|-------|
| providers/openai/ | 8,122 | ~16 `= reqwest::Client` → remove default |

Skip: `responses_api/websocket.rs` (2,000 lines, needs tokio-tungstenite, native-only)

### llm-provider-anthropic (copy + replace defaults)

| Source | Lines | Edits |
|--------|-------|-------|
| providers/anthropic/ | 4,093 | ~3 `= reqwest::Client` → remove default |

## schemars version note

Rig uses schemars 1.x. Lattice's `dag-core` uses schemars 0.8. The LLM crates use schemars 1.x independently — the two schema ecosystems don't intersect (LLM types don't derive dag-core's `JsonSchema`, they use schemars for tool parameter schema generation which is a different concern). Both versions can coexist in the same workspace via Cargo's semver resolution.

## License

All code derived from rig carries the MIT license notice from Playgrounds Analytics Inc. Each crate's lib.rs includes the copyright notice as required.

## Cross-references

- `impl-docs/spec/node-vs-capability-surface.md` (LLM nodes as semantic boundaries)
- `private/impl-docs/roadmap/workflow-studio-and-agentic-authoring-2026-03-28.md` (agentic loop patterns)
- `ref-libs/rig/` (upstream reference, not a build dependency)
