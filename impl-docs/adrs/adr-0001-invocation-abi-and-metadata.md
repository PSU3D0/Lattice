Status: Draft
Purpose: adr
Owner: Core
Last reviewed: 2025-12-12

# ADR-0001: Invocation ABI and Metadata Keys

## Context

We need a stable execution boundary shared across hosts and bridges (Axum, Workers, queue bridge, future Temporal).

The codebase already has a practical boundary:
- `host-inproc::Invocation` / `InvocationParts` (`crates/host-inproc/src/lib.rs`)
- `kernel-exec::ExecutionResult` / `ExecutionError` (`crates/kernel-exec/src/lib.rs`)

Without a stable ABI, bridges will drift (different payload envelopes, missing metadata, inconsistent cancellation and error mapping).

## Decision

- The canonical invocation envelope for 0.1.x is `host_inproc::InvocationParts`.
- Payload is untyped JSON (`serde_json::Value`) at the ABI boundary.
- Metadata is carried as:
  - `labels: BTreeMap<String, String>` (cheap strings)
  - `extensions: BTreeMap<String, JsonValue>` (structured)

Reserved key spaces:
- `lf.*`: Lattice cross-host stable keys.
- `http.*`: HTTP host adapters.

0.1 target "core" keys (hosts SHOULD set when available):
- `lf.run_id`: unique id per invocation.
- `lf.flow_id`: Flow IR id.
- `lf.trigger_alias`
- `lf.capture_alias`
- `lf.event_id`: upstream event identifier.
- `lf.idem_key`: derived idempotency key where applicable.

Hosts may add additional keys; consumers must ignore unknown keys.

## Alternatives Considered

- A fully typed protobuf/flatbuffer ABI.
  - Rejected for 0.1: slows iteration and makes importer-driven flows harder.
- A host-specific payload envelope per host.
  - Rejected: breaks portability and makes bridges brittle.

## Consequences

- Bridges can persist and forward invocations without embedding host-specific assumptions.
- Observability and policy hooks have a stable place to live.
- The ABI stays compatible with agent-generated/importer-generated flows (JSON-first).

## Follow-ups

- Ensure Axum host and Workers host populate at least `lf.run_id`.
- Update `impl-docs/spec/invocation-abi.md` if the reserved key set changes.
