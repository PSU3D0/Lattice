Status: Draft
Purpose: adr
Owner: Core
Last reviewed: 2025-12-12

# ADR-0004: Retry Ownership (Connector vs Orchestrator)

## Context

Retries can occur in multiple layers:
- connector libraries (HTTP clients, SDKs)
- capability providers
- orchestrators/hosts (queue workers, Temporal, web adapters)

Double-retry causes:
- duplicate side effects
- unbounded tail latency
- cost blowups
- broken idempotency assumptions

We need a clear default that aligns with Flow IR delivery semantics.

## Decision

Default: orchestrator-owned retries.

- The runtime/orchestrator owns retry policy for node execution.
- Capability providers SHOULD avoid internal retries, especially for effectful writes.
- Safe exception (bounded): read-only operations MAY retry on transient failures when it is semantically safe.

Effectful write guidance:
- Any retry of effectful writes must be gated by idempotency (either provider-level idempotency keys or LatticeFlow idempotency spec).

Representation in 0.1:
- Documented policy + future control surfaces (`on_error!`) and connector metadata.

## Alternatives Considered

- Connector-owned retries by default.
  - Rejected: harder to analyze and often leads to hidden retry storms.
- Mixed ownership (per connector) without a default.
  - Rejected: creates inconsistency and surprises.

## Consequences

- Retry behavior becomes analyzable and consistent across hosts.
- Connector crates become thinner and easier to certify.

## Follow-ups

- Encode retry ownership in future connector specs (registry/marketplace).
- Add linting hooks for "retry hidden in connector" (future; best-effort).
