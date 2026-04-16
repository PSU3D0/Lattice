Status: Draft
Purpose: architecture-decision / backend-runtime contract
Owner: Runtime
Last reviewed: 2026-04-03

# Dispatch Backend Runtime (0.1.x)

This document defines how **dispatch backends** fit into Lattice.

It answers a design question raised by external sandbox / callback-resume work:

- should Lattice treat external job execution as just another raw capability,
- should it treat each backend as a flow-level primitive,
- or should it define a bindable backend family above existing primitives?

## Decision summary

Dispatch backends should be modeled as a **host-owned, bindable semantic family
above existing primitives**, not as:
- a new lowest-level raw capability,
- and not as backend-specific flow topology.

The intended layering is:

1. **Primitive host services / capabilities**
   - HTTP
   - durability services (`CheckpointStore`, `ResumeSignalSource`, optional
     `ResumeScheduler`)
   - optional blob/artifact storage
   - optional host-owned receipt/tracking state

2. **Host-agnostic backend adapter contract**
   - submit external work
   - observe completion
   - normalize result

3. **Flow-visible semantic node/subflow family**
   - dispatch + await + resume
   - typed semantic result contract
   - truthful effect/idempotency declaration

This keeps Lattice aligned with its existing primitive/semantic split:
- connectors are above HTTP,
- AI is above HTTP,
- workspace nodes are above workspace capability,
- dispatch backends are above HTTP + durability host services.

## Core stance

A dispatch backend is:
- **connector-like in packaging discipline**,
- but **not just an ordinary connector action**.

Why connector-like:
- typed request/response models
- bindable runtime/endpoint/auth configuration
- reusable host/runtime adapter seam
- declarative effect/determinism/resource contract

Why not just an ordinary connector:
- it participates in halt/resume lifecycle
- it may use callback or polling completion
- it may own cancellation / timeout handling
- it may normalize artifacts and terminal job states
- it behaves more like runtime/control-plane orchestration than a single CRUD
  request/response action

## Primary rule

Flows should await a **normalized semantic completion**, not a vendor-specific
job substrate.

Good:
- await `InvestigationResult`
- await `DocumentAnalysisResult`
- await `ApprovalDecision`

Bad:
- await GitHub Actions run id
- await Kubernetes Job name
- await Codex Action output file path
- await backend-specific status enums in flow payloads

## What the flow should see

The flow should see:
- a semantic request shape
- a semantic result shape
- one explicit dispatch/await semantic boundary
- explicit effectfulness and idempotency declarations

The flow should **not** directly own:
- backend receipts as its primary result contract
- polling loops
- callback transport details
- vendor-specific tracking APIs

## What the backend adapter should see

A backend adapter should be free to manage:
- submission to its substrate
- callback or polling completion
- terminal-state detection
- cancellation
- artifact lookup or normalization
- substrate-specific auth/runtime details

That logic belongs behind the backend adapter seam, not in flow topology.

## Host portability model

A dispatch backend should be portable across hosts **provided the host can
satisfy the backend's required host-service traits**.

That means:
- a backend adapter should depend on a **small host service bundle trait**,
  not on one concrete host implementation
- Workers, native, and future hosts can all support the same backend family if
  they provide the required services

## Conceptual host service bundle

```rust
trait DispatchBackendHost {
    fn http_read(&self) -> Option<&dyn HttpRead>;
    fn http_write(&self) -> Option<&dyn HttpWrite>;

    fn checkpoint_store(&self) -> Option<&dyn CheckpointStore>;
    fn resume_signal_source(&self) -> Option<&dyn ResumeSignalSource>;
    fn resume_scheduler(&self) -> Option<&dyn ResumeScheduler>;

    fn blob(&self) -> Option<&dyn BlobStore>;
    fn dispatch_receipt_store(&self) -> Option<&dyn DispatchReceiptStore>;
}
```

This is conceptual only.

The important idea is:
- the backend adapter depends on a **portable host service contract**
- not directly on Workers internals, Axum internals, GitHub-specific APIs, etc.

## Flow-visible forms

There are two plausible flow-visible shapes.

## 1. Semantic node wrapper
Example conceptual surface:
- `sandbox.dispatch_and_wait`
- `external_completion.await_result`

Use when:
- the dispatch/await step is one clear semantic unit
- we want compact topology
- we want generator/agent ergonomics

## 2. Standard subflow pattern
Example conceptual surface:
- `subflow.external_completion.dispatch_and_wait`

Use when:
- we want transparency over internals
- we want to build first on existing primitives
- we want to learn before freezing a first-class node API

### Current recommendation
Start **subflow-or-semantic-family-first** using current primitives and the new
callback-resume contract, then later decide whether a dedicated first-class node
wrapper is justified.

## Callback vs polling

Callback vs polling should be modeled as a **backend/runtime property**, not a
flow-topology distinction.

### Callback-first backend
- backend or external job calls Lattice resume endpoint directly
- good for direct sandbox services or job runtimes that can callback reliably

### Polling backend
- host scheduler polls backend status and resumes when terminal
- useful for substrates that do not support reliable callback

### Hybrid backend
- callback preferred, polling fallback
- often the best long-term real-world posture

The flow should not need different topology for these modes.

## Binding model

Dispatch backends should be **bindable** at deployment/runtime, similar in
spirit to connectors.

A flow should be able to say, semantically:
- “use the default issue investigation backend”

without embedding:
- secrets
- endpoint URLs
- backend-specific API quirks

Conceptually, a backend binding might carry things like:
- backend kind (`github_actions`, `self_hosted_runner`, `http_dispatch`)
- dispatch endpoint/profile
- auth handles
- artifact handling profile
- completion mode preference
- timeout defaults

This should remain host-owned binding state, not Flow IR secrets or config
payloads.

## Effectfulness and idempotency

Dispatch-backend use is **effectful by default**.

Why:
- it submits work to an external system
- it may allocate remote state or queue entries
- it may consume external capacity or budget

Therefore flows using it should normally declare:
- `Effects::Effectful`
- `Determinism::Nondeterministic` or `BestEffort`
- explicit idempotency on the dispatch boundary

This is especially important on resume paths.

### Idempotency rule
The dispatching step must have an explicit idempotency story.

Examples:
- stable semantic idempotency key in the request
- backend-level dedupe key
- host-side dispatch receipt keyed to one semantic job

Do **not** assume ordinary retries are harmless.

## Why this is not a raw capability

A raw capability should be closer to:
- HTTP
- KV
- blob
- workspace
- clock
- resume token service

Dispatch backends are too orchestration-shaped for that role.
They already combine:
- external submission
- suspend/resume participation
- timeout/cancellation semantics
- semantic result normalization

So they should be treated as **capability-backed semantic runtime families**.

## Wrong architectures to avoid

## Wrong 1 — backend-specific flow primitives
Bad examples:
- `github_actions_wait`
- `codex_wait`
- `claude_action_wait`
- `k8s_job_wait`

Why bad:
- backend mechanics leak into topology
- hard to swap substrates

## Wrong 2 — “just use raw HTTP and custom code everywhere”
Bad examples:
- every flow hand-rolls callback tokens, timeouts, and resume handling
- no shared result normalization model
- no shared binding/runtime seam

Why bad:
- duplicated logic
- inconsistent guarantees
- weak authoring ergonomics

## Wrong 3 — backend bypasses workflow shell
Bad examples:
- backend directly comments on GitHub by default
- backend directly mutates repo/PR without returning to workflow

Why bad:
- loses explicit approvals and final policy checks
- weakens Lattice’s orchestration value

## Wrong 4 — treat it as ambient shell power
Bad examples:
- arbitrary nodes can launch undeclared backends/sandboxes
- no typed request/result contract

Why bad:
- governance collapse
- unclear effect/idempotency envelope

## The right architecture

The right architecture is:

### Primitive layer
- HTTP
- durability services
- optional blob/artifact store
- optional host-owned receipt/tracking state

### Backend adapter layer
- GitHub Actions backend
- self-hosted runner backend
- custom sandbox service backend
- future job backends

### Flow-visible semantic family
- dispatch + await + resume
- semantic typed result contract
- explicit effect/idempotency boundary

### Execution engine inside backend
Examples:
- Claude Code Action
- Codex Action
- custom shell harness
- containerized sandbox

This keeps:
- execution engines replaceable under one backend family
- backends replaceable under one orchestration model

## Relationship to current external-completion work

This document complements:
- `impl-docs/spec/external-completion-runtime.md`
  - broad runtime split between flow-facing semantics and backend-facing
    completion transport
- `impl-docs/spec/external-sandbox-dispatch-and-callback-resume.md`
  - concrete callback-token contract for the current external sandbox pattern

This document clarifies the additional point that:
- dispatch backends should be a **bindable runtime family** above current
  primitives
- not merely an ad hoc pattern per backend

## Working conclusion

Dispatch backends should be modeled as:

- **bindable semantic backend adapters**
- **above existing primitives**
- **connector-like in discipline**
- **topology-visible at the semantic boundary**
- **effectful and idempotency-aware by default**

This preserves Lattice’s current philosophy:
- keep primitives small and honest
- build semantic families above them
- keep topology meaningful
- keep backend mechanics out of the main workflow contract

## Cross references

- `impl-docs/spec/external-completion-runtime.md`
- `impl-docs/spec/external-sandbox-dispatch-and-callback-resume.md`
- `impl-docs/spec/checkpointing-and-durability.md`
- `impl-docs/spec/capabilities-and-binding.md`
- `impl-docs/spec/connector-crate-surface.md`
- `impl-docs/spec/node-vs-capability-surface.md`
- `impl-docs/spec/connector-op-reuse-and-node-declaration.md`
