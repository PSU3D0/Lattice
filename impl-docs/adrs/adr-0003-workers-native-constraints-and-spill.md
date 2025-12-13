Status: Draft
Purpose: adr
Owner: Runtime
Last reviewed: 2025-12-12

# ADR-0003: Workers-Native Constraints Profile and Spill Strategy

## Context

Cloudflare Workers (wasm32-unknown-unknown) is a primary deployment target.

Workers constraints:
- no filesystem semantics equivalent to Linux containers
- no multi-threaded runtime assumptions
- network access should be mediated through platform APIs (fetch)

The current executor has OS-coupled behavior (notably filesystem-backed spill via `tempfile` in `kernel-exec`).

## Decision

Define a Workers-native execution profile for 0.1 with explicit constraints:

- Hosts/executor MUST NOT rely on filesystem spill in wasm builds.
- For 0.1 Workers deployments:
  - spill is disabled OR
  - spill requires an explicit blob capability tier (future) and is otherwise rejected at preflight.

Runtime dependencies:
- Prefer wasm-compatible async primitives.
- HTTP egress from nodes must go through a Workers-backed HTTP capability (fetch), not `reqwest`.

## Alternatives Considered

- Keep spill as temp-fs in Workers by emulating a filesystem.
  - Rejected: fragile and not aligned with Workers reality.
- Require blob-backed spill immediately.
  - Deferred: good long-term, but too much for first Workers milestone.

## Consequences

- We can ship Workers-native execution without accidentally depending on Linux-only behavior.
- Spill semantics stay portable; the blob-backed spill path remains open.

## Follow-ups

- Add feature gating in `kernel-exec` for wasm builds.
- Document the Workers profile constraints in `impl-docs/spec/capabilities-and-binding.md`.
