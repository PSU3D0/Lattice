Status: Draft
Purpose: architecture-decision / spec
Owner: Runtime
Last reviewed: 2026-03-31

# AI Surface and Layering (0.1.x)

This document defines how AI/LLM-style functionality fits into Lattice.

It answers a recurring architectural question:

- is AI in Lattice just raw HTTP to model providers, or
- should Lattice expose a higher semantic AI surface above HTTP?

## Decision summary

Lattice should treat AI as a **semantic platform surface above HTTP**, not as a
new host capability and not as mere raw transport wiring.

The intended layering is:

1. **Host capabilities**
   - `http_read`
   - `http_write`

2. **Provider/protocol SDK layer**
   - `llm-types`
   - `llm-agent`
   - `llm-provider-openai`
   - `llm-provider-anthropic`
   - future provider crates

3. **Lattice integration/bridge layer**
   - currently expected as `llm-lattice`
   - adapts Lattice resource/context APIs to the provider SDK layer

4. **Lattice semantic AI operation layer**
   - provider-agnostic operations such as:
     - `complete`
     - `extract_structured`
     - `embed`
     - `generate_image`
   - future additions may include:
     - `rerank`
     - `transcribe`
     - `tool_call`
     - bounded agent-loop orchestration helpers

5. **Graph-visible canonical nodes and custom-node reuse**
   - thin canonical nodes for generator/LLM/simple composition use
   - reusable typed operations for arbitrary Rust nodes

## What this means

### AI is not a host capability

HTTP remains the raw host capability boundary.

AI operations are built **on top of** HTTP plus host-owned auth/endpoint
resolution. They are therefore **not** a new lowest-level runtime capability in
0.1.x.

### AI is connector-like

AI provider families should follow the same broad architecture as other
connector families:
- host-owned auth and endpoint resolution,
- semantic operations,
- canonical node wrappers,
- reusable in-node usage.

However, AI should not be forced into the narrowest possible “REST action only”
shape. AI providers often need:
- structured-output schema handling,
- tool-calling semantics,
- SSE streaming,
- image-byte responses,
- provider-specific chat/message formats,
- bounded step-loop orchestration above single request/response calls.

So the right posture is:
- **reuse connector runtime/binding principles**, but
- **allow richer semantic AI operations above raw HTTP descriptors**.

## Layer responsibilities

## 1. Host capability layer

The host layer owns:
- HTTP transport,
- runtime policy,
- secret/binding resolution,
- execution environment differences (native, wasmtime, Workers).

The host does **not** own provider-specific prompt/message semantics.

## 2. Provider/protocol SDK layer

The current `llm-*` crates are the portable provider SDK substrate.

They own:
- provider request/response types,
- structured-output request mapping,
- SSE decoding,
- completion/embedding/image-generation model traits,
- provider-specific JSON and auth/header shapes.

They should remain:
- portable to wasm32,
- independent of reqwest/tokio/browser-only wasm assumptions,
- usable directly by advanced authors.

This layer is **not yet** the Lattice semantic AI surface.

## 3. Lattice bridge layer

`llm-lattice` is currently the expected bridge crate.

Its role is to:
- adapt `llm_types::http_client::HttpClientExt` to Lattice HTTP resources,
- construct provider clients from the current Lattice execution context,
- mediate host-owned auth/endpoint configuration into provider SDK clients,
- provide the reusable integration seam used by examples, canonical nodes, and
  future AI connector families.

`llm-lattice` is therefore:
- **integration plumbing**, not
- the final conceptual provider-agnostic connector surface.

## 4. Lattice semantic AI operation layer

Lattice should expose provider-agnostic operations above provider SDKs.

Canonical early operations:
- `complete`
- `extract_structured<T>`
- `embed`
- `generate_image`

These operations are:
- not raw capabilities,
- not mere HTTP wrappers,
- semantic platform constructs implemented by provider families.

### Provider subsets

Not every provider supports every operation.

Therefore Lattice should prefer:
- capability-specific traits / operation families,
- provider-declared subsets,
- optional provider-specific extensions,

rather than one giant mandatory “AI provider” trait.

## 5. Graph-visible nodes vs in-node usage

The AI surface follows the existing rule from
`impl-docs/spec/node-vs-capability-surface.md`:
- graph-visible nodes remain the unit of topology, retry, visibility, and
  deployment reasoning,
- arbitrary Rust nodes remain the unit of composition-local implementation.

Therefore AI functionality should be reusable in two ways:

1. **canonical graph-visible AI nodes**
   - good for examples, generators, simple composition, operator visibility

2. **typed in-node AI operations**
   - good when multiple AI calls belong to one semantic retry unit

This means:
- a flow may use explicit `ai.extract_structured` and `ai.generate_image` nodes,
- or a custom Rust node may internally invoke multiple AI operations,
- but custom-node usage should remain declared and governable.

## Lower-level API availability

The lower-level provider SDK surface should remain public and supported.

This is valuable for:
- advanced/provider-specific tuning,
- experimentation,
- unsupported edge features,
- cases where Lattice’s provider-agnostic operation layer is intentionally
  narrower than the provider’s raw API.

So the intended model is:
- **high-level semantic AI API available**, and
- **lower-level provider API still available**.

## Semantic envelopes

Absent explicit caching/pinning, AI operations should default to conservative
semantics.

### Default posture
- `complete` → `Effectful + Nondeterministic`
- `extract_structured` → `Effectful + Nondeterministic`
- `embed` → `Effectful + Nondeterministic`
- `generate_image` → `Effectful + Nondeterministic`

Rationale:
- they perform external billable network operations,
- outputs can vary across retries,
- the current effects lattice treats external network calls as effectful.

### Stronger semantics only when justified

Stronger semantics such as `Stable` or claims of effective idempotency should be
reserved for explicitly designed wrappers, for example:
- host caching,
- pinned model/version,
- prompt/schema hashing,
- durable replay of captured provider responses.

## Provider-specific families

Provider-specific families should be possible and expected.

Examples:
- `connector.openai.*`
- `connector.anthropic.*`

These families may implement some or all of the provider-agnostic AI operation
families.

They may also expose provider-specific extensions when those do not fit the
portable common surface.

## Relationship to agent loops

Single AI operations such as completion or image generation are distinct from
bounded multi-step agent loops.

Agent loops should sit **above** the single-operation layer.
They are covered in:
- `impl-docs/spec/agent-loop-runtime.md`

## Non-goals for 0.1.x

This document does **not** require:
- AI to become a new host opcode family beyond HTTP for first-pass support,
- every provider feature to appear in the provider-agnostic API,
- graph topology to represent every internal model/tool step,
- subflows to become the primary representation of bounded agent loops.

## Open questions

1. **Naming**
   - keep `llm-lattice` as the bridge crate name, or later generalize to an
     `ai-*` family?

2. **Packaging**
   - should provider-specific semantic operations live in dedicated
     `connector-openai` / `connector-anthropic` crates, or initially live near
     the provider crates?

3. **Bindings/auth**
   - should early Workers support use a minimal AI-specific binding seam first,
     or immediately reuse the broader connector-runtime role model?

4. **Canonical operation catalog**
   - which provider-agnostic operations become first-class nodes in 0.1.x, and
     which remain lower-level helpers initially?

## Cross-references

- `impl-docs/spec/node-vs-capability-surface.md`
- `impl-docs/spec/connector-crate-surface.md`
- `impl-docs/spec/connector-op-reuse-and-node-declaration.md`
- `impl-docs/spec/agent-loop-runtime.md`
- `impl-docs/spec/llm-implementation-plan.md`
- `impl-docs/spec/llm-lead-intake-example.md`
