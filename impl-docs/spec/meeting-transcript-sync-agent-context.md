# Meeting transcript sync — shared agent context

Status: dispatch support doc  
Date: 2026-04-16  
Audience: subagents implementing the meeting transcript workflow and related provider layers

This document is the shared recontextualization brief for all subagents working on the meeting-transcript-sync build.

Companion docs:
- `/home/psu3d0/Projects/psu3d0/coltec-codespaces/nexus/codespaces/lattice/latticeflow-lib-dev/ops/meeting-transcript-sync-technical-design-2026-04-14.md`
- `/home/psu3d0/Projects/psu3d0/coltec-codespaces/nexus/codespaces/lattice/latticeflow-lib-dev/ops/meeting-transcript-sync-build-plan-2026-04-14.md`
- `/home/psu3d0/Projects/psu3d0/coltec-codespaces/nexus/codespaces/lattice/latticeflow-lib-dev/ops/meeting-transcript-sync-dispatch-plan-2026-04-16.md`
- `impl-docs/spec/example-authoring-conventions.md`

---

## 1. Workflow target

We are building a first serious workflow for:
- detecting recently completed meetings from Google Calendar
- determining Zoom vs Google Meet / Docs-backed transcript source
- fetching transcript artifacts
- storing durable state in D1
- storing transcript artifacts in R2
- reconciling/retrying safely over time

The intended deployment posture is Cloudflare-first, but the implementation order is:
1. reusable provider groundwork
2. explicit workflow crate
3. D1/R2 integration
4. thin scheduled wrapper

Do **not** invert that order.

---

## 2. Non-goals for this tranche

Do **not** use this work to start or redesign:
- Lattice core scheduling control planes
- generalized trigger runtime frameworks
- generic transcript-domain abstractions in core crates
- giant provider-farming efforts
- self-healing implementation
- broad Studio/operator-product work

Keep the current build narrow and honest.

---

## 3. Layering rules

### A. Workflow semantics stay in the workflow/example crate
The flow crate owns business semantics such as:
- meeting-key/idempotency policy
- transcript source resolution policy
- waiting vs retry vs permanent-failure semantics
- transcript upload policy
- run summary semantics

### B. Reusable provider logic may be extracted upward
Reusable provider logic may live in provider-facing crates, such as:
- existing `crates/connectors/google/platform/`
- narrowly justified new Google crates under `crates/connectors/google/`
- a narrowly justified future Zoom platform/helper crate

Provider crates may own reusable concerns such as:
- auth plumbing
- endpoint/path/query helpers
- DTOs if clearly reusable
- export/lookup helpers
- connector-level mocked HTTP contract tests

### C. Do not push transcript business policy into provider crates
Do not bake these into provider crates:
- transcript reconciliation policy
- meeting retry policy
- meeting job ledger semantics
- transcript destination layout policy
- end-to-end workflow state machine logic

---

## 4. Existing repo priors to follow

### Example shape
This workflow should follow `impl-docs/spec/example-authoring-conventions.md`.
Treat it as a serious first-party example, not an ad hoc test harness.

Expected shape:
- `examples/s14_meeting_transcript_sync/`
- `Cargo.toml`
- `README.md`
- `payloads/`
- `src/lib.rs`
- explicit, truthful workflow topology
- bundleable shape if the crate is intended to be portable/provable from day one

### Provider priors
Existing useful baselines:
- `crates/connectors/google/platform/`
- `crates/connectors/google/sheets/`
- `examples/s11_lead_intake/`
- `examples/s13_github_issue_investigator/`

Use these for style and layering priors.
Do not cargo-cult their exact domain semantics.

---

## 5. Practical implementation order

### Tranche 1 — Google provider groundwork
Goal:
- create the minimal reusable Google provider surface needed for this workflow

Likely scope:
- Calendar event fetch helpers
- Docs/Drive transcript lookup/export helpers
- service-account auth plumbing

### Tranche 2 — Zoom provider groundwork
Goal:
- create the minimal reusable Zoom provider surface needed for this workflow

Likely scope:
- meeting/transcript lookup helpers
- ready vs not-ready outcome mapping
- auth request construction

### Tranche 3 — Workflow example crate
Goal:
- build the actual transcript workflow as a serious example crate
- keep orchestration semantics local to the flow

### Tranche 4 — D1/R2 + scheduled wrapper
Goal:
- add persistence/storage and a thin Cloudflare scheduled wrapper
- avoid building a generalized scheduler platform

---

## 6. Test discipline

Every write tranche should be test-driven.

Expected pattern:
1. add failing tests first or clearly identify the missing test target
2. make the smallest implementation change needed
3. run targeted tests first
4. run one broader confidence command
5. stop when tranche acceptance is met

Do not broaden scope “while here.”

---

## 7. Provider-specific context

### Google context
Current design stance:
- Google Meet transcript handling should be treated primarily as a Docs/Drive resolution problem informed by Calendar meeting metadata
- not as an unspecified “Meet magic” problem

Useful questions:
- which Calendar fields best support transcript-doc resolution?
- what Drive/Docs lookup/export path is minimally sufficient?
- what belongs in `google/platform` vs a focused `google/docs` or `google/drive` crate?

### Zoom context
Current design stance:
- Zoom transcript handling should be modeled as a bounded provider-layer capability
- ready/not-ready semantics matter more than exhaustive Zoom API coverage

Useful questions:
- what minimal response shapes express transcript available vs not ready?
- what auth/path/query helpers are worth codifying now?
- what should remain reference-only for later expansion?

---

## 8. Acceptance philosophy

A tranche is successful when it is:
- small
- test-backed
- layered correctly
- easy to review
- honest about what is still missing

A tranche is **not** more successful because it added extra abstractions.

---

## 9. Reviewer checklist

Reviewers should explicitly check for:
- transcript-domain leakage into core or provider crates
- missing failing tests for the claimed behavior
- over-ambitious provider surface design
- hidden deployment assumptions in the wrong layer
- example-crate drift from authoring conventions
- “while here” opportunistic changes outside the tranche

---

## 10. Default stop rule

If you hit a design branch that would require:
- changing Lattice core semantics,
- introducing a generic scheduler framework,
- or expanding into a broad connector family,

stop and report the blocker instead of inventing the next platform.
