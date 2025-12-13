Status: Draft
Purpose: spec
Owner: Core
Last reviewed: 2025-12-12

# Invocation ABI (0.1.x)

This document defines the host-agnostic invocation boundary used to execute a validated Flow IR.

Source of truth:
- Invocation types: `crates/host-inproc/src/lib.rs`
- Executor results/errors: `crates/kernel-exec/src/lib.rs`
- Web mapping example: `crates/host-web-axum/src/lib.rs`

## Invocation

In 0.1.x, **flow selection** (which `FlowIR`) is a host/deployment concern and is out-of-band.

An invocation identifies (within the selected flow):
- which trigger node to run (`trigger_alias`)
- which node output to capture (`capture_alias`)
- the input payload (`payload`)
- optional deadline (`deadline`)
- arbitrary metadata (`metadata`)

Canonical shape: `host_inproc::InvocationParts`.

Semantics:
- `trigger_alias` MUST exist in the `ValidatedIR` nodes list.
- `capture_alias` MUST exist in the `ValidatedIR` nodes list.
- `payload` is JSON and is passed as the trigger node input.
- `deadline` is an optional time budget for reaching the capture node.

## Metadata

Metadata is carried alongside an invocation and is intended for:
- tracing/observability
- policy decisions
- connector idempotency and dedupe correlation

Shape:
- `labels: BTreeMap<String, String>` (cheap, indexable strings)
- `extensions: BTreeMap<String, JsonValue>` (structured data)

Namespace conventions:
- `lf.*`: reserved for LatticeFlow (cross-host stable keys)
- `http.*`: reserved for HTTP host adapters

0.1 target reserved keys (hosts SHOULD set when available):
- `lf.run_id`: globally unique execution id for this invocation.
- `lf.flow_id`: FlowIR id.
- `lf.trigger_alias`: alias.
- `lf.capture_alias`: alias.
- `lf.event_id`: upstream event id (webhook message id, queue message id, etc).
- `lf.idem_key`: computed idempotency key (string form) when relevant.

HTTP adapters MAY populate (examples exist in Axum host):
- labels: `http.method`, `http.path`, `http.host`, `http.query_raw`, `http.version`
- extensions: `http.query`, `http.headers`

## Results

The executor returns `kernel_exec::ExecutionResult`:
- `Value(JsonValue)`: single value completion.
- `Stream(StreamHandle)`: streaming completion.

Streaming semantics:
- Each stream item is a JSON value.
- Cancellation MUST be propagated if the client disconnects or the stream is dropped.

## Errors

The executor returns `kernel_exec::ExecutionError` (examples):
- unknown trigger/capture alias
- cancelled
- deadline exceeded
- node failed
- handler not registered

Host mapping (0.1 contract):
- Non-streaming errors MUST serialize as JSON with at least:
  - `error` (string): human-readable summary.
- Hosts SHOULD include when classification exists:
  - `code` (string): stable diagnostic code (see `impl-docs/error-codes.md`).
- Hosts MAY add additional fields over time:
  - `details` (object): structured context (e.g. `node_alias`, `trigger_alias`, `capture_alias`, `retryable`, `timeout_ms`).
  - `diagnostics` (array): structured diagnostics payloads for agent tooling.
- Consumers MUST ignore unknown fields.

HTTP status mapping (non-normative guidance; as implemented in Axum host today):
- `DeadlineExceeded` -> 504
- `Cancelled` -> 503
- most other execution failures -> 500

Streaming error mapping (0.1 contract):
- SSE event type `error` with JSON payload matching the same error object (at minimum `{ "error": "..." }`).

## Cancellation

Cancellation is cooperative.

- Hosts SHOULD cancel execution when:
  - the client disconnects (HTTP streaming)
  - the invocation deadline is exceeded
  - the platform aborts the request
- Nodes can observe cancellation via their execution context (see `capabilities::context` usage patterns).
