Status: Draft
Purpose: architecture-decision / runtime contract
Owner: Runtime
Last reviewed: 2026-04-03

# External Completion Runtime (0.1.x)

This document defines how Lattice should model workflows that:
- dispatch work to an external system,
- await completion without burning host CPU,
- and resume with a typed result.

It exists to answer a broader architectural question:

- does Lattice own polling/event plumbing for external jobs, or
- should that seam live entirely outside Lattice?

## Decision summary

Lattice should own **external completion orchestration**, but not necessarily
**backend-specific completion transport**.

In practice:
- Lattice **does own**:
  - halt / await / resume semantics
  - resume token issuance and validation
  - timeout / cancellation / idempotency behavior
  - typed resume payload contracts
  - stable internal resume endpoint semantics
  - host-side continuation after completion
- Lattice does **not need to own**, in the first pass:
  - every backend-specific polling loop
  - vendor-specific run state models in Flow IR
  - one distinct wait primitive per backend

So the clean split is:
- **Lattice owns “await external completion and resume safely”**
- **backend adapters own “how completion is detected”**

## Why this distinction matters

If Lattice owns too little, it becomes just a thin callback wrapper and loses:
- timeout semantics
- cancellation semantics
- run/checkpoint ownership
- auditability
- policy/control-plane value

If Lattice owns too much, backend-specific mechanics leak into workflow
semantics:
- GitHub Actions run IDs in flow payloads
- one polling/wait shape per backend
- topology polluted by substrate mechanics

The right architecture preserves:
- workflow-level semantic clarity
- host/runtime ownership of durability
- backend flexibility
- future replacement of one execution substrate with another

## Two contract layers

## 1. Flow-facing semantic contract

This is the contract that user-authored flows and examples should reason about.

A flow should think in terms of:
- dispatch semantic external work
- await completion
- resume with a typed semantic result

Examples of flow-facing result types:
- `InvestigationResult`
- `DocumentAnalysisResult`
- `ApprovalDecision`
- `SandboxReviewResult`

The flow should **not** primarily reason in terms of backend receipts like:
- GitHub Actions run IDs
- workflow job URLs
- Kubernetes Job names
- provider-specific artifact APIs

Those are operational/backend details.

## 2. Backend-facing operational contract

This is the contract implemented by a host-side backend adapter.

A backend adapter is responsible for:
- submission to the external substrate
- tracking / terminal-state detection
- collecting and normalizing results
- optionally cancellation
- optionally artifact resolution

The backend adapter may use:
- callback-only completion
- polling-only completion
- callback-preferred with polling fallback

This should remain a **host/runtime concern**, not a flow-topology concern.

## Core rule

A workflow should await a **normalized external job completion**, not await a
specific vendor runtime.

Good:
- “await investigation result”
- “await document analysis result”
- “await approval decision from external reviewer”

Bad:
- “await GitHub Actions run 12345”
- “await Codex workflow output file”
- “poll Claude action status until complete”

## Conceptual host-side contracts

These are conceptual runtime seams, not final crate/API commitments.

### Flow-facing semantics

```rust
pub struct ExternalCompletionRequest<T> {
    pub kind: String,
    pub payload: T,
    pub policy: CompletionPolicy,
}

pub struct CompletionPolicy {
    pub timeout_ms: Option<u64>,
    pub cancellation: CancellationPolicy,
    pub expected_mode: CompletionModePreference,
}

pub enum CompletionModePreference {
    CallbackPreferred,
    PollPreferred,
    CallbackOnly,
    PollOnly,
}
```

```rust
pub struct ExternalCompletionResult<R> {
    pub status: CompletionTerminalStatus,
    pub result: R,
    pub artifacts: Vec<ArtifactRef>,
}

pub enum CompletionTerminalStatus {
    Succeeded,
    Failed,
    Cancelled,
}
```

### Backend-facing operations

```rust
pub trait ExternalCompletionBackend {
    async fn dispatch(
        &self,
        req: DispatchRequest,
    ) -> Result<DispatchReceipt, DispatchError>;

    async fn status(
        &self,
        receipt: &DispatchReceipt,
    ) -> Result<JobStatus, DispatchError>;

    async fn cancel(
        &self,
        receipt: &DispatchReceipt,
    ) -> Result<(), DispatchError>;

    async fn collect_result(
        &self,
        receipt: &DispatchReceipt,
    ) -> Result<JobCompletion, DispatchError>;
}
```

```rust
pub struct DispatchReceipt {
    pub backend_kind: String,
    pub job_id: String,
    pub tracking_mode: TrackingMode,
    pub metadata: serde_json::Value,
}

pub enum TrackingMode {
    CallbackOnly,
    PollOnly,
    CallbackPreferredPollFallback,
}

pub enum JobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

pub struct JobCompletion {
    pub terminal_status: JobStatus,
    pub result_payload: serde_json::Value,
    pub artifacts: Vec<ArtifactRef>,
}
```

The important point is not exact naming.
The important point is that these are **host/backend contracts**, not flow IR
payload contracts.

## Callback vs polling

## Callback-first backends

Examples:
- custom sandbox service
- self-hosted execution service
- GitHub Actions workflow that calls the Lattice resume endpoint explicitly

### Shape
1. dispatch external job
2. flow halts
3. backend calls stable internal resume endpoint
4. host validates callback and resumes flow

### Characteristics
- lower latency
- direct completion path
- simpler host logic if callback is reliable

## Polling backends

Examples:
- backends that expose a status API but cannot easily callback
- systems where callback trust/auth is awkward
- certain hosted job systems where polling is the native control pattern

### Shape
1. dispatch external job
2. flow halts
3. host-owned scheduler polls backend via adapter
4. on terminal completion, host resumes flow with normalized result

### Characteristics
- completion transport remains host-owned
- flow does not see polling details
- backend-specific state model stays behind adapter

## Hybrid backends

Examples:
- GitHub Actions backend where the workflow *tries* to callback, but host can
  poll as a fallback
- any external job substrate with imperfect callback reliability

### Shape
1. dispatch external job
2. flow halts
3. callback preferred
4. host polling fallback if callback not seen in expected window

This is likely the best long-term posture for some real-world backends.

## What Lattice should own

Lattice should own, at the host/runtime layer:
- checkpoint creation for external-completion waits
- resume token issuance / validation
- timeout handling
- cancellation semantics
- stable internal resume endpoint semantics
- mapping accepted completion to resumed execution
- optional storage of backend receipt/tracking metadata
- normalization boundary between backend result and flow-facing result

This is the real orchestration value.

## What backend adapters should own

Backend adapters should own:
- submission to the substrate
- substrate-specific auth/endpoint/runtime details
- run/job ID handling
- polling or callback transport mechanics
- backend artifact lookup
- result normalization from substrate-native formats into semantic results

## Where backend state should live

Backend tracking state should live in host/runtime-owned state such as:
- checkpoint metadata
- token metadata
- dispatch receipt store
- scheduler-owned poll state
- adapter-managed tracking metadata

It should **not** become the main workflow payload abstraction.

## Connector-like, but not just another connector

Dispatch backends should reuse good connector-style architecture principles:
- typed request/response models
- host-owned auth and endpoint resolution
- deployment/runtime binding
- reusable semantic operations

But they should **not** be treated as ordinary end-user connector actions.

Why:
- they participate in halt/resume lifecycle
- they may involve timeouts and cancellation
- they may involve host polling schedulers or resume tokens
- they are closer to control-plane/runtime orchestration than to ordinary CRUD

A good mental model is:
- **connector-like packaging discipline**
- **runtime/control-plane semantics**

## Wrong architectures to avoid

## Wrong 1 — backend specifics in flow semantics
Bad examples:
- flow payload includes GitHub Actions run IDs as the main result contract
- flow branches on vendor-specific job states
- flow directly understands Actions artifact APIs

Why bad:
- substrate leakage
- hard to swap backends
- topology becomes operational plumbing

## Wrong 2 — one wait primitive per backend
Bad examples:
- one node for callback waits
- one node for GitHub polling
- one node for queue-based completion
- one node for custom service completion

Why bad:
- workflow semantics become transport-specific
- wrong abstraction boundary

## Wrong 3 — backend bypasses Lattice on completion
Bad examples:
- backend posts final GitHub comment directly by default
- backend opens PR / mutates repo without returning through workflow shell

Why bad:
- loses explicit workflow governance
- weakens auditability and approvals
- makes Lattice incidental rather than central

## Wrong 4 — ambient external job powers
Bad examples:
- arbitrary nodes can launch undeclared sandboxes/jobs
- no declared budget / timeout / result contract

Why bad:
- governance collapse
- poor operator trust

## Right architecture

A healthy architecture looks like:

### Flow layer
- dispatch semantic external work
- halt / await
- resume with semantic result

### Host runtime layer
- manage checkpoint and token lifecycle
- manage timeout / cancellation
- optionally drive polling
- resume flow safely

### Backend adapter layer
- GitHub Actions adapter
- self-hosted sandbox adapter
- HTTP callback service adapter
- future job-system adapters

### Backend execution engine layer
Examples:
- Claude Code Action
- Codex Action
- custom harness
- self-hosted agent shell
- containerized runner

This keeps execution engines replaceable under one backend family and keeps
backends replaceable under one orchestration model.

## Implications for current GitHub issue investigator direction

For the current `s13` direction:
- Lattice should own the dispatch → halt → resume shell
- the callback result should be a normalized `InvestigationResult`
- GitHub Actions, Claude Code Action, Codex Action, or a custom sandbox should
  be backend implementations, not workflow semantics

This preserves the key product claim:
- Lattice is the typed orchestration shell around bounded external agent work

## Recommended phased buildout

## Phase 1
- callback-first orchestration
- no general polling runtime yet
- typed semantic result resumes the flow

## Phase 2
- introduce a host-side backend adapter seam
- adapters may be callback-only or polling-backed
- keep this invisible to flow topology

## Phase 3
- add optional shared completion-driver support in hosts
- allow callback, polling, and hybrid fallback
- still do not surface transport differences in flow topology

## Working conclusion

The right source-of-truth stance is:

- Lattice **does** own external completion orchestration
- Lattice does **not** need to own every backend-specific completion transport
- callback vs polling is a backend/runtime concern
- halt / await / resume is a Lattice concern
- flows should resume with **semantic results**, not substrate-native receipts

## Cross references

- `impl-docs/spec/dispatch-backend-runtime.md`
- `impl-docs/spec/external-sandbox-dispatch-and-callback-resume.md`
- `impl-docs/spec/checkpointing-and-durability.md`
- `impl-docs/spec/agent-loop-runtime.md`
- `impl-docs/spec/capabilities-and-binding.md`
