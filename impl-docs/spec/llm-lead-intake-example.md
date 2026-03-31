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

This is the first Lattice example that uses LLM capabilities, and the first
designed for Cloudflare Workers deployment from day one.

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
    │   generate_image [LLM image generation, hero image for outreach]
    │     ↓
    │   compose_email [Pure, assembles outreach + image URL → EmailPackage]
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
    image_url: Option<String>,
    priority: Priority,
}
```

## Node definitions

### extract_lead
- Effects: ReadOnly (LLM call reads, doesn't mutate external state)
- Determinism: Nondeterministic (LLM output varies)
- Resources: http_write (for LLM API call)
- Implementation: Uses `llm_agent::extractor::Extractor<M, LeadInfo>` with
  `schemars::JsonSchema` derived on `LeadInfo`. The LLM is forced to return
  structured output matching the schema.

### draft_outreach
- Effects: ReadOnly
- Determinism: Nondeterministic
- Resources: http_write
- Implementation: Uses `CompletionModel::completion()` with a preamble that
  includes the extracted `LeadInfo` as context. Returns `OutreachDraft`.

### generate_image
- Effects: Effectful (costs money, creates an asset)
- Determinism: Nondeterministic
- Resources: http_write
- Idempotency: key = "lead.email", scope = Node
- Implementation: Uses OpenAI image generation (DALL-E) or similar. Returns
  a URL to the generated image.

### compose_email
- Effects: Pure
- Determinism: Strict
- No resources needed
- Assembles `OutreachDraft` + image URL into `EmailPackage`

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

The LLM provider API key comes from the connector bindings model, not env vars.
In the flow, the node builds the provider client with a key from the connector
runtime:

```rust
let api_key = resources.connector_runtime()
    .resolve_secret("openai_api_key")
    .await?;
let http = LatticeHttpClient { write: resources.http_write() };
let client = llm_provider_openai::Client::new(&api_key, http);
let model = client.completion_model("gpt-4o");
```

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

**No Durable Objects needed** for this flow — it's a single-shot request/response
with no halt/resume. If we wanted to add approval before sending the email,
that would introduce halt/resume and need DO support.

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
examples/s11_lead_intake/
├── Cargo.toml          # depends on dag-macros, llm-types, llm-agent, llm-provider-openai
├── src/
│   ├── lib.rs          # flow definition, node definitions, types
│   └── adapter.rs      # LatticeHttpClient impl (or in a shared crate)
├── tests/
│   └── integration.rs  # mock-server-based tests
└── bindings.lock.json  # test bindings lock with mock endpoints
```

The `LatticeHttpClient` adapter (~50-80 lines) should probably live in a shared
`crates/llm-lattice/` crate so it's reusable across all LLM-using flows.
Alternatively, it could live in `llm-types` behind a `lattice` feature flag.

## Open questions

1. **Image generation API choice:** DALL-E 3 via OpenAI, or a different provider?
   DALL-E is already in the OpenAI provider we ported.

2. **Streaming the extraction:** Should `extract_lead` stream tokens while
   extracting, or just return the final structured output? For Workers, the
   non-streaming path is simpler and more reliable.

3. **Email sending:** The flow produces an `EmailPackage` but doesn't send it.
   Sending would be a connector operation (SendGrid, SES, etc.) — nice follow-on
   but not in scope for the first example.

4. **Workspace usage:** Should the generated image be stored in workspace? This
   would exercise workspace + LLM together. The image URL from DALL-E is
   temporary (~1 hour), so storing the bytes in workspace would be more durable.

## Cross-references

- `impl-docs/spec/llm-crate-topology.md`
- `impl-docs/spec/node-vs-capability-surface.md`
- `private/impl-docs/roadmap/workflow-studio-and-agentic-authoring-2026-03-28.md`
- `ops/next-steps-general-2026-03-28.md` (lead_capture_crm in recommended examples)
