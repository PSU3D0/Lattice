Status: Draft
Purpose: architecture-decision / contract
Owner: Runtime
Last reviewed: 2026-04-03

# External Sandbox Dispatch and Callback Resume (0.1.x)

This document defines the contract for a Lattice flow that:

1. dispatches work to an external sandbox or agent harness,
2. halts without burning host CPU,
3. receives a callback from that external system,
4. and resumes the original flow with the callback payload.

This is the intended contract for workflows such as:
- GitHub issue investigation,
- repo-local code analysis,
- bounded external coding/investigation jobs,
- or other heavyweight sandboxed work that should not run inside Workers.

## Decision summary

The preferred 0.1.x shape is:

- **Lattice host (Workers or native) owns orchestration**
- **external sandbox owns heavyweight repo/filesystem/bash work**
- callback resume uses a **stable host-owned endpoint**
- authorization uses an **opaque single-use resume token**
- token is a **one-time capability to resume one halted workflow**
- callback payload is **schema-validated before resume**
- HMAC/signature schemes are **optional later hardening**, not required for V1

This keeps the model simple, portable, and aligned with the existing durability
spec.

## Why this exists

Workers is a good place to host:
- ingress,
- lightweight triage,
- durable orchestration,
- callback handling,
- and final mutation steps.

Workers is **not** the right place to pretend that heavyweight code
investigation is native. Deep repo-local work often needs:
- real filesystem access,
- git clone,
- shell commands,
- test runs,
- and larger artifact generation.

The right boundary is therefore:
- Lattice orchestrates,
- sandbox executes,
- halt/resume bridges them.

## Relationship to other specs

This document refines and applies:
- `impl-docs/spec/external-completion-runtime.md`
- `impl-docs/spec/checkpointing-and-durability.md`
- `impl-docs/spec/agent-loop-runtime.md`
- `impl-docs/spec/public-io-contract.md`
- `impl-docs/spec/capabilities-and-binding.md`

In particular:
- resume token creation/resolution remains owned by `ResumeSignalSource`
- checkpoint persistence remains owned by `CheckpointStore`
- the callback endpoint is a **host resume surface**, not a flow trigger surface
- bounded agentic behavior remains **node-local and declared**, even when one
  node dispatches to an external sandbox

## Non-goals

This document does **not** require:
- a new generic shell capability for ordinary flow nodes
- Workers-native git/filesystem/bash execution
- per-job temporary route allocation
- HMAC signatures as the only callback auth model
- a full artifact upload protocol in V1
- general public exposure of internal resume endpoints

## Actors

### 1. Lattice orchestrator host
The host running the flow (Workers or native). Owns:
- checkpointing,
- resume token creation,
- outbound dispatch,
- callback validation,
- resume execution,
- final workflow continuation.

### 2. External sandbox dispatcher
The external HTTP service that accepts investigation jobs.
It may enqueue or directly launch sandbox work.

### 3. External sandbox worker
The sandboxed runtime that performs heavyweight investigation work:
- clone repo,
- inspect files,
- run bounded commands/tests,
- produce findings/artifacts,
- send callback.

### 4. Resume signal source
Host durability service responsible for:
- creating opaque resume tokens,
- resolving tokens back to checkpoint handles,
- revoking/marking used tokens.

## Core lifecycle

## 1. Flow dispatches external job
A flow reaches a semantic step such as:
- `github_issue_investigation_dispatch`

That step:
1. creates a checkpoint,
2. creates a single-use resume token,
3. constructs an outbound dispatch payload,
4. sends it to the external sandbox dispatcher,
5. halts awaiting callback.

## 2. External sandbox performs work
The sandbox uses the provided job payload to:
- perform bounded repo-local work,
- gather findings,
- optionally upload artifacts elsewhere,
- prepare a structured callback result.

## 3. External sandbox calls back
The sandbox calls the stable host-owned resume endpoint with:
- the resume token,
- the structured result payload,
- optional advisory routing fields.

## 4. Host validates and resumes
The host:
1. resolves the token,
2. validates token freshness and single-use rules,
3. validates callback body schema,
4. acquires checkpoint lease,
5. resumes the original flow with the callback payload.

## Stable callback endpoint

The preferred cross-host callback surface is:

```http
POST /__lattice/resume
```

This aligns with the broader cross-host internal resume endpoint direction.

This endpoint is:
- **host-owned**
- **not** a user-authored flow trigger
- **not** a public product API by default
- **stable** across jobs

The endpoint itself stays stable; the **token** is the ephemeral capability.

## Authentication model (V1)

### Preferred V1 model
Use:
- HTTPS
- opaque high-entropy token
- single-use token
- short TTL
- token bound to one checkpoint/run

### Recommended transport
Preferred request auth:

```http
Authorization: Bearer <resume-token>
```

The token MAY also appear in the body for compatibility with existing internal
resume contract work, but bearer header is the preferred shape for sandbox
callbacks.

### Why token-first
A token-first capability model is enough for V1 and avoids early complexity
around:
- body canonicalization,
- HMAC key distribution,
- replay-signature windows,
- timestamp skew handling,
- and signature-debug ergonomics.

### Later hardening
Later versions MAY add optional:
- HMAC body signing,
- sandbox identity attestation,
- callback request idempotency keys,
- signed artifact manifests.

These are follow-ons, not prerequisites.

## Token semantics

Resume tokens used for sandbox callbacks should be created with semantics
roughly equivalent to:

```rust
TokenConfig {
    ttl: Some(...),
    single_use: true,
    metadata: Some({
        "purpose": "external_sandbox_callback",
        "callback_kind": "investigation",
        "flow_id": "...",
        "run_id": "...",
    }),
}
```

### Required properties
- opaque / non-guessable
- bound to one checkpoint handle
- single-use on successful acceptance
- revocable on cancellation/timeout
- expiring

### Important rule
The external sandbox must receive a **resume token**, not a raw checkpoint
handle.

## Outbound dispatch contract

The external dispatch payload should be explicit and versioned.

### Recommended envelope

```json
{
  "contract_version": "0.1",
  "job_kind": "github_issue_investigation",
  "job_id": "job_01H...",
  "request": {
    "repo": {
      "owner": "org",
      "name": "repo",
      "ref": "main"
    },
    "issue": {
      "number": 417,
      "title": "panic when config is empty",
      "body": "..."
    },
    "policy": {
      "max_steps": 8,
      "allow_shell": true,
      "allow_test_runs": true,
      "allow_patch_proposal": true,
      "max_wall_clock_seconds": 900
    }
  },
  "callback": {
    "url": "https://host.example.com/__lattice/resume",
    "auth": {
      "kind": "bearer_resume_token",
      "token": "opaque-single-use-token"
    },
    "expires_at": "2026-04-03T18:00:00Z",
    "source": "sandbox_dispatch"
  }
}
```

### Notes
- `job_kind` identifies the external work contract, not the internal flow node
- `job_id` is sandbox-facing and useful for observability, but advisory to the
  host
- `policy` fields are allowed-tool/budget hints for the sandbox runtime
- the callback contract is explicit; the sandbox does not need to discover flow
  topology

## Callback request contract

The callback body should also be versioned and typed.

### Request

```http
POST /__lattice/resume
Authorization: Bearer <resume-token>
Content-Type: application/json
```

```json
{
  "contract_version": "0.1",
  "source": "sandbox_dispatch",
  "job_id": "job_01H...",
  "status": "completed",
  "result": {
    "summary": "Likely null dereference in config parser when file is absent.",
    "confidence": 0.83,
    "findings": [
      {
        "kind": "root_cause",
        "path": "src/config/parser.rs",
        "detail": "Unchecked unwrap after optional read path"
      }
    ],
    "proposed_actions": [
      {
        "kind": "comment",
        "body": "I investigated and found..."
      }
    ],
    "artifacts": []
  }
}
```

### Status values
Initial recommended values:
- `completed`
- `failed`
- `cancelled`

These are sandbox job statuses, not HTTP statuses.

## Response contract

### Success
- `200 OK` — resumed and completed to a non-halt result
- `202 Accepted` — callback accepted; resumed flow halted again or continued
  asynchronously

### Client errors
- `400 Bad Request` — malformed callback body
- `401 Unauthorized` — missing/invalid token
- `404 Not Found` — token expired or unknown
- `409 Conflict` — token already consumed or checkpoint lease conflict
- `413 Payload Too Large` — callback body exceeds allowed size

### Error body
Use the standard host error envelope with:
- `code`
- `details`

## Callback payload rules

### Required V1 rules
- callback payload must be JSON
- payload must validate against the expected resume/result schema
- payload size must be bounded by host policy
- tokens must not be logged in plaintext

### Token consumption rule
A token should be marked consumed only after:
1. token resolution succeeds,
2. request body passes schema validation,
3. the host accepts the resume request.

This allows retry on accidental malformed payloads without forcing a new token
issuance.

### Large artifacts
Large logs, patches, or binary artifacts should **not** be embedded in the V1
callback body.

V1 guidance:
- keep callback body modest
- upload large artifacts elsewhere first if needed
- return typed artifact references in callback payload

The artifact upload protocol is intentionally left for later work.

## Resume behavior

On accepted callback:
1. resolve token -> checkpoint handle
2. acquire lease on checkpoint
3. build resume invocation metadata
4. inject callback payload as the resume input for the halted node/frontier
5. continue the flow from the stored frontier

The resumed flow then decides what to do next, for example:
- post GitHub comment
- add labels
- create follow-up task
- request human review
- discard low-confidence result

## Failure and timeout semantics

## Dispatch failure before halt
If outbound dispatch fails before the flow successfully halts, the flow should
follow its ordinary error path. Any created resume token should be revoked if
possible.

## No callback received
The recommended pattern is to pair callback wait with timeout semantics, so a
missing callback becomes a normal workflow branch rather than an infinite wait.

## Invalid callback payload
Return `400` and do not consume the token.

## Expired token
Return `404` or equivalent deterministic token-expired error.

## Duplicate callback after success
Return `409` or token-already-used semantics.
Do not resume twice.

## If resumed flow fails later
The token remains consumed. The workflow has already resumed; subsequent errors
are ordinary flow execution failures.

## Security posture

### V1 required posture
- HTTPS only
- opaque bearer token
- single-use token
- bounded TTL
- stable host-owned callback endpoint
- bounded body size
- schema validation before resume
- no raw checkpoint handles exposed externally

### V1 explicit non-requirements
- no HMAC requirement
- no source-IP trust model
- no per-job route allocation
- no general public callback API promise

### V2+ hardening options
- HMAC body signing
- attested sandbox identity
- replay-protected signed envelopes
- artifact signing / provenance chain

## Example application: GitHub issue investigation

A representative flow can use this contract as:

1. GitHub issue ingress on Workers
2. bounded triage/classification node
3. if worth investigating:
   - dispatch sandbox job with repo/ref + issue context
   - halt
4. sandbox clones repo and investigates
5. sandbox callback resumes flow
6. resumed flow posts comment / routes for human review / stores artifacts

This pattern intentionally flexes:
- connectors
- AI
- bounded agentics
- workspace/blob artifacts
- durability and halt/resume
- host separation between orchestration and heavy execution

## Working conclusion

The 0.1.x source-of-truth contract is:

- use a **stable host-owned resume endpoint**
- authorize callbacks with a **single-use opaque resume token**
- treat the token as a **one-time capability to resume one halted flow**
- keep heavy repo-local work in an **external sandbox**, not inside Workers
- let Lattice remain the **typed orchestration shell** around that external
  agent work

## Cross references

- `impl-docs/spec/external-completion-runtime.md`
- `impl-docs/spec/checkpointing-and-durability.md`
- `impl-docs/spec/agent-loop-runtime.md`
- `impl-docs/spec/public-io-contract.md`
- `impl-docs/spec/capabilities-and-binding.md`
