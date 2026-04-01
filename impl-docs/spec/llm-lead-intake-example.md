Status: Draft
Purpose: example-design
Owner: Runtime
Last reviewed: 2026-03-31

# s11_lead_intake: LLM-powered lead intake flow

## Overview

A webhook-triggered flow that:
1. Receives free-text lead submissions
2. Extracts structured `LeadInfo` via LLM (structured output)
3. Branches on priority
4. High-priority: generates personalized outreach draft + hero image, composes email package
5. Low-priority: generates standard acknowledgment
6. Captures the result

This is the first Lattice example that uses AI/LLM capabilities, and the first
designed for Cloudflare Workers deployment from day one.

It is intentionally an **explicit topological example**:
- graph-visible AI steps come first,
- bounded agent-loop behavior is a later follow-on example,
- the purpose of this example is to prove transport, structured output,
  workspace persistence, wasm bundling, and Workers execution.

## Flow topology

```
trigger (POST /leads)
    ↓
extract_lead [LLM structured output → LeadInfo]
    ↓
branch on lead.priority
    ├── "high"
    │     ↓
    │   draft_outreach [LLM completion, uses LeadInfo context]
    │     ↓
    │   generate_image [LLM image generation → bytes]
    │     ↓
    │   store_image [workspace.write("images/hero.png", bytes)]
    │     ↓
    │   compose_email [Pure, assembles outreach + workspace path → EmailPackage]
    │     ↓
    │   capture
    │
    └── "low" / default
          ↓
        template_response [Pure, standard acknowledgment → EmailPackage]
          ↓
        capture
```

## Types

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeadSubmission {
    name: String,
    email: String,
    message: String,   // free-text, unstructured
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct LeadInfo {
    name: String,
    email: String,
    priority: Priority,
    product_interest: String,
    seat_count: Option<u32>,
    timeline: Option<String>,
    summary: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, schemars::JsonSchema, PartialEq)]
#[serde(rename_all = "lowercase")]
enum Priority {
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OutreachDraft {
    subject: String,
    body: String,
    tone: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EmailPackage {
    to: String,
    subject: String,
    body: String,
    image_artifact_path: Option<String>,
    priority: Priority,
}
```

## Node definitions

### extract_lead
- Effects: Effectful
- Determinism: Nondeterministic (LLM output varies)
- Resources: http_write (for provider API call)
- Implementation: Uses typed structured output (`prompt_typed::<LeadInfo>(...)`) so the OpenAI provider emits `response_format = json_schema` with a strict schema-constrained response.

### draft_outreach
- Effects: Effectful
- Determinism: Nondeterministic
- Resources: http_write
- Implementation: Uses `CompletionModel::completion()` with a preamble that
  includes the extracted `LeadInfo` as context. Returns `OutreachDraft`.

### generate_image
- Effects: Effectful (costs money, creates an asset)
- Determinism: Nondeterministic
- Resources: http_write
- Idempotency: key = "lead.email", scope = Node
- Implementation: Uses `llm_provider_openai::image_generation::ImageGenerationModel`
  with `gpt-image-1.5`. Returns raw image bytes.

### store_image
- Effects: Effectful
- Determinism: BestEffort
- Resources: workspace_write
- Implementation: Uses `std.workspace.write` to persist image bytes at
  `images/hero.png`. On Workers this goes to R2 via `cap-workspace-workers`.
  On native it goes to filesystem via `cap-workspace-fs`.

### compose_email
- Effects: Pure
- Determinism: Strict
- No resources needed
- Assembles `OutreachDraft` + workspace image path into `EmailPackage`
  (`image_artifact_path` rather than pretending the output is a permanent URL)

### template_response
- Effects: Pure
- Determinism: Strict
- Generates standard `EmailPackage` from `LeadInfo`

## LLM provider integration

### The HttpClientExt adapter

The flow doesn't depend on reqwest, tokio, or any platform-specific HTTP.
Instead, each LLM node gets its HTTP capability from Lattice's resource system:

```rust
// In the flow crate or a shared llm-lattice adapter crate:
use llm_types::http_client::HttpClientExt;
use capabilities::http::HttpWrite;

struct LatticeHttpClient {
    write: Arc<dyn HttpWrite>,
}

impl HttpClientExt for LatticeHttpClient {
    fn send<T, U>(&self, req: http::Request<T>) -> impl Future<...> {
        async move {
            // Convert http::Request<T> → capabilities::http::HttpRequest
            let cap_req = convert_request(req);
            // Call Lattice's HTTP capability
            let cap_resp = self.write.send(cap_req).await?;
            // Convert capabilities::http::HttpResponse → http::Response<LazyBody<U>>
            convert_response(cap_resp)
        }
    }
    // streaming + multipart similarly
}
```

### API key resolution

The long-term goal is host-owned auth/binding resolution in the same spirit as
other connector families.

For the implemented path now:
- native/mock execution can still use an env-backed development seam,
- real native smoke uses that path with `OPENAI_API_KEY`,
- wasm/workerd and wasmtime execution prefer host-provided connector-runtime endpoint/auth resolution when present,
- guest code still keeps env fallback only as a pragmatic local/dev bridge.

So this example now supports both:
1. a temporary native dev/live path via env vars, and
2. a host-provided/binding-backed path for wasm-hosted execution.

For the mock test path, the API key is a static string and HTTP goes to a local
mock server.

## Cloudflare Workers deployment

### Architecture

```
Internet → Cloudflare Worker (host-workers)
              ↓
           Flow bundle (wasm, compiled from s11_lead_intake)
              ↓
           LLM nodes call capabilities::http::HttpWrite
              ↓
           cap-http-workers::WorkersHttpClient (fetch API)
              ↓
           OpenAI / Anthropic / DALL-E APIs
```

### Workers-specific considerations

**Execution time:** Workers have a 30s CPU time limit (paid plan) but LLM calls
are I/O-wait, not CPU. A typical flow execution:
- extract_lead: ~2-5s wall time (LLM call), ~1ms CPU
- draft_outreach: ~2-5s wall time, ~1ms CPU
- generate_image: ~10-30s wall time, ~1ms CPU
- compose_email: ~0ms wall time, ~0.1ms CPU
Total CPU time is well under 30s. Wall time could be 15-40s for high-priority
path, which is fine for Workers (they wait on I/O without counting CPU).

**Secrets:** OpenAI API key stored as a Workers secret:
```bash
npx wrangler secret put OPENAI_API_KEY
```
Mapped through bindings lock to the connector handle.

**No flow-level halt/resume Durable Object path is needed** for this flow — it is
single-shot request/response. However, the Workers workspace backend still uses
R2 + Durable Object indexing/lifecycle under `cap-workspace-workers`, so the
Cloudflare proof should assume those bindings are present.

**Streaming:** If we want to stream the LLM completion back to the client as it
generates (SSE), that's supported by Workers and by host-workers' streaming
response path. This would be a nice stretch goal.

### Wrangler configuration

```toml
name = "lead-intake"
main = "build/index.js"
compatibility_date = "2026-03-31"

[vars]
LATTICE_BUNDLE_ID = "s11-lead-intake-v1"

# Connector bindings (or use wrangler secrets for API keys)
```

### Build + deploy

```bash
# Build the wasm bundle
flows bundle -p example-s11-lead-intake --wasm --out-dir dist/

# Local dev with wrangler
npx wrangler dev

# Deploy
npx wrangler deploy
```

## Phased proof order

The intended implementation/proof order is:
1. native mock-server proof,
2. wasm bundle proof via wasmtime,
3. workerd/miniflare proof,
4. real Cloudflare deploy later.

This keeps the first example focused on portability layers before account-bound
cloud deployment concerns.

## Test strategy

### Unit tests (in flow crate)
- `extract_lead` with canned LLM response → verify `LeadInfo` extraction
- `draft_outreach` with canned LLM response → verify `OutreachDraft` shape
- Branch routing: high priority → outreach path, low → template path
- `compose_email` pure logic
- `template_response` pure logic

### Integration test (CLI, mock server)
- Start mock HTTP server returning canned OpenAI responses
- `flows run local --example s11_lead_intake --bindings-lock test.lock.json --payload '...'`
- Assert output is a valid `EmailPackage`
- Test both high and low priority paths

### Bundle test (wasmtime)
- Build wasm bundle
- `flows run bundle --bundle dist/ --bindings-lock test.lock.json --payload '...'`
- Same assertions

### Workers test (workerd/miniflare)
- If feasible, test with miniflare + mock HTTP
- Otherwise, manual deploy + curl test

## Crate structure

```
crates/llm-lattice/                  # Adapter crate (shared by all LLM-using flows)
├── Cargo.toml                       # depends on llm-types, capabilities
├── src/
│   └── lib.rs                       # LatticeHttpClient: impl HttpClientExt

examples/s11_lead_intake/
├── Cargo.toml                       # depends on dag-macros, llm-lattice, llm-provider-openai,
│                                    #   capabilities, stdlib
├── src/
│   └── lib.rs                       # flow definition, node definitions, types
├── tests/
│   └── integration.rs               # mock-server-based tests
└── bindings.lock.json               # test bindings lock with mock endpoints
```

## Resolved decisions

1. **Image generation:** OpenAI `gpt-image-1.5` is the current proved image model.
   The ported implementation in `llm-provider-openai::image_generation`
   returns raw bytes (base64-decoded).

2. **Image storage:** Use **workspace** (not blob) to persist the generated image.
   Workspace is run-scoped, works on all three hosts (cap-workspace-fs native,
   cap-workspace-workers R2 on Workers, memory in tests). DALL-E URLs are
   temporary (~1hr), so storing bytes in workspace makes the result durable.
   There is no `cap-blob-r2` for Workers yet, and the image is a run-scoped
   artifact — workspace is the right tool.

3. **Structured extraction shape:** the example now proves the stricter OpenAI
   `json_schema` structured-output path rather than the earlier tool-call
   extractor path.

4. **AI surface shape:** this example uses explicit graph-visible AI steps first.
   It is not the first bounded agent-loop example.

5. **Auth path:** native env-backed live proof exists, but wasm-hosted execution
   now prefers host-provided connector-runtime endpoint/auth resolution when
   available. Full generalized connection management remains a follow-on.

6. **Adapter crate:** Dedicated `crates/llm-lattice/` crate for the
   `LatticeHttpClient` adapter + Lattice-specific provider/client helpers.

7. **Workers testing:** miniflare/workerd first. Reuse or extract existing
   host-workers workerd test infrastructure. Real Cloudflare deploy is a
   follow-on.

8. **Wasm execution proof:** both bundle metadata/load proof and full
   host-wasmtime execution proof now exist for the example.

9. **Streaming:** Non-streaming for the first version. Workers SSE streaming
   of LLM completions is a nice follow-on.

10. **Email sending:** The flow produces an `EmailPackage` but doesn't send it.
   SendGrid/SES connector is a follow-on.

## Cross-references

- `impl-docs/spec/ai-surface-and-layering.md`
- `impl-docs/spec/agent-loop-runtime.md`
- `impl-docs/spec/llm-crate-topology.md`
- `impl-docs/spec/node-vs-capability-surface.md`
- `private/impl-docs/roadmap/workflow-studio-and-agentic-authoring-2026-03-28.md`
- `ops/next-steps-general-2026-03-28.md` (lead_capture_crm in recommended examples)
