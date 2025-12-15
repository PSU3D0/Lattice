Status: Draft
Purpose: spec
Owner: Core
Last reviewed: 2025-12-12

# Control Surfaces (0.1.x)

Control surfaces are declarative orchestration metadata embedded in Flow IR.

Source of truth:
- IR structures: `crates/dag-core/src/ir.rs` (`ControlSurfaceIR`, `EdgeIR`)

0.1 principle:
- Some orchestration is edge-native (delivery/buffer/timeout).
- Some is control-surface native (if/switch routing).
- Complex surfaces are spec'd early but may be lint-only or runtime-rejected until implemented.

## 0.1 Runtime-Semantic Surfaces

### Delivery (EdgeIR.delivery)

Field: `EdgeIR.delivery`.

Allowed values:
- `at_least_once` (default)
- `at_most_once`
- `exactly_once`

Validator requirements (implemented):
- `exactly_once` requires a dedupe capability and idempotency metadata (see `kernel-plan`).

### Buffer / Spill (EdgeIR.buffer)

Field: `EdgeIR.buffer` (`BufferPolicy`).

Semantics:
- `max_items`: max in-memory queued items (must be positive when present: `CTRL102`).
- `spill_threshold_bytes` + `spill_tier`: if configured, executor may spill payloads exceeding threshold.

Notes:
- Spill tiers are runtime-configured; IR only names the tier.

### Timeout (EdgeIR.timeout_ms)

Field: `EdgeIR.timeout_ms`.

Semantics:
- Optional budget for the edge hop.
- Must be positive when present (`CTRL101`).
- Hosts and/or executor may enforce it.

### If (ControlSurfaceKind::If)

`ControlSurfaceIR.kind = "if"`.

Config shape (JSON object):

- `v` (number): version, must be `1`.
- `source` (string): node alias whose output is inspected.
- `selector_pointer` (string): JSON Pointer (RFC 6901) into the source output.
- `then` (string): node alias to schedule when predicate is true.
- `else` (string): node alias to schedule when predicate is false.

Predicate:
- The value at `selector_pointer` MUST be a JSON boolean.

Targets:
- `ControlSurfaceIR.targets` MUST include at least: `source`, `then`, `else`.

Structural requirements (implemented by validator/runtime):
- `selector_pointer` MUST be a valid JSON Pointer string (empty or starting with `/`) (`CTRL120`).
- For both `then` and `else`, an edge `source -> target` MUST exist (`CTRL121`).
- `ControlSurfaceIR.targets` MUST include `source`, `then`, and `else` (`CTRL120`).
- A flow MUST NOT define multiple if surfaces for the same `source` (`CTRL122`).

### Switch (ControlSurfaceKind::Switch)

`ControlSurfaceIR.kind = "switch"`.

Config shape (JSON object):

- `v` (number): version, must be `1`.
- `source` (string): node alias whose output is inspected.
- `selector_pointer` (string): JSON Pointer (RFC 6901) into the source output.
- `cases` (object): map of `string -> node alias`.
- `default` (string, optional): node alias to schedule when no cases match.

Selector:
- The value at `selector_pointer` MUST be a JSON string.
- Matching is exact string equality.

Routing semantics:
- After `source` produces a value, the runtime selects exactly one target:
  - if `cases[selector_value]` exists: schedule that target
  - else if `default` exists: schedule `default`
  - else: schedule no targets

Structural requirements (implemented by validator/runtime):
- `selector_pointer` MUST be a valid JSON Pointer string (empty or starting with `/`) (`CTRL110`).
- For every case target (and `default` when present), an edge `source -> target` MUST exist (`CTRL111`).
- `ControlSurfaceIR.targets` MUST include `source`, every case target, and `default` when present (`CTRL110`).
- A flow MUST NOT define multiple switch surfaces for the same `source` (`CTRL112`).

## 0.1 Spec-Only / Lint-Only Surfaces (Reserved)

These are reserved shapes. In 0.1 they MAY be validated structurally.

Runtime handling (0.1):
- If a reserved *runtime-semantic* surface is present and the selected host/runtime does not implement it for the active `profile`, execution MUST fail deterministically.
- Metadata-only reserved surfaces (e.g. `Partition`, where `EdgeIR.partition_key` is canonical) MUST NOT change runtime behavior in 0.1 and MAY be ignored.
- Hosts SHOULD surface diagnostic code `CTRL901` and include the surface `id`/`kind` in error details when available.

### Error Handling (ControlSurfaceKind::ErrorHandler)

Config shape (reserved):
- `v` (number): `1`
- `scope` (object): `{ "nodes": ["alias", ...] }` (or future edge scopes)
- `strategy` (string): `retry|fail|continue`
- `max_retries` (number, optional)
- `backoff_ms` (number, optional)

### Rate Limit (ControlSurfaceKind::RateLimit)

Config shape (reserved):
- `v` (number): `1`
- `target` (string): node alias
- `qps` (number)
- `burst` (number)

### Partition (ControlSurfaceKind::Partition)

Config shape (reserved):
- `v` (number): `1`
- `edge` (object): `{ "from": "alias", "to": "alias" }`
- `key` (string): opaque partition key expression

Note:
- `EdgeIR.partition_key` is the canonical representation in 0.1; this control surface is optional metadata.

### ForEach (ControlSurfaceKind::ForEach)

Config shape (reserved):
- `v` (number): `1`
- `source` (string): node alias producing an array
- `items_pointer` (string): JSON Pointer selecting the array within the output
- `body_entry` (string): node alias to schedule per element

### Window (ControlSurfaceKind::Window)

Config shape (reserved):
- `v` (number): `1`
- `event_time_pointer` (string): JSON Pointer selecting event-time field
- `size_ms` (number)
- `allowed_lateness_ms` (number)
- `watermark` (string): reserved, implementation-defined

### Await / Pause (Checkpoint-based, reserved)

This is the "pause and wait for external event" primitive (not necessarily human-in-the-loop).

0.1 representation uses checkpoints + an optional control surface (reserved).

Config shape (reserved):
- `v` (number): `1`
- `checkpoint_id` (string)
- `correlation_key` (string): opaque key used to match resume events
- `timeout_ms` (number, optional)

## Lint Policy Hook

`FlowPolicies.lint.require_control_hints` can be used by validators/policy engines to require that flows include explicit control surfaces (rather than embedding orchestration in opaque node code).
