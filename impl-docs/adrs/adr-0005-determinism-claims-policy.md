Status: Draft
Purpose: adr
Owner: Core
Last reviewed: 2025-12-12

# ADR-0005: Determinism Claims Policy

## Context

Nodes declare a determinism level:
- `strict`
- `stable`
- `best_effort`
- `nondeterministic`

Capability hints constrain determinism (e.g., HTTP implies BestEffort).

We need consistent semantics so:
- validators can enforce minimum constraints
- certification can later prove stronger claims
- agents/importers can choose conservative defaults

## Decision

Determinism semantics:

- Strict:
  - output is a pure function of input
  - no clock/rng/network/db reads unless fully pinned and modeled
- Stable:
  - deterministic given pinned resources (e.g., blob reads by hash, cached HTTP responses)
  - the "pin" mechanism may be implicit in 0.1 and becomes explicit via facets/evidence later
- BestEffort:
  - expected drift across retries (default for external calls)
- Nondeterministic:
  - explicitly allows nondeterminism (must be declared; useful for sampling, LLMs without replay, etc)

0.1 enforcement:
- Hints constrain minimum determinism via `dag-core` registries.
- Stable claims are allowed but should be used conservatively; certification later upgrades/downgrades with evidence.

## Alternatives Considered

- Only allow Strict/BestEffort in 0.1.
  - Rejected: Stable is important for pinned reads and future replay semantics.
- Treat determinism as purely informational.
  - Rejected: determinism is one of the core differentiators.

## Consequences

- Importers/agents can default to BestEffort when unsure.
- Future certification can attach replay evidence without changing the lattice semantics.

## Follow-ups

- Define what constitutes "pinned" resources once capability facets land.
- Add certification harness proofs for Stable claims (later epic).
