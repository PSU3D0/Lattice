Status: Canonical
Purpose: spec
Owner: Core
Last reviewed: 2025-12-12

# Error Code Registry

This registry tracks the diagnostic identifiers emitted across LatticeFlow crates. Each
entry lists the canonical short description, the primary subsystem, and the default
severity. Diagnostic producers must reference these codes verbatim; tests in `dag-core`
ensure the registry stays in sync with the implementation.

| Code      | Subsystem      | Default | Summary |
|-----------|----------------|---------|---------|
| DAG001    | Macros         | Error   | Missing or unknown port type on a node definition. |
| DAG002    | Macros         | Error   | Node parameters could not be reflected into a schema. |
| DAG003    | Macros         | Error   | Referenced resource or capability is undefined. |
| DAG004    | Validation     | Error   | Effectful node lacks a valid idempotency declaration. |
| DAG005    | Validation     | Error   | Node concurrency hints exceed allowed bounds. |
| DAG006    | Validation     | Error   | Conflicting batch configuration detected on node. |
| DAG101    | Validation     | Error   | Trigger definition does not expose an output port. |
| DAG102    | Validation     | Error   | Trigger respond configuration incompatible with profile. |
| DAG103    | Validation     | Error   | Trigger route/method conflicts with an existing trigger. |
| DAG200    | Validation     | Error   | Cycle detected in workflow graph. |
| DAG201    | Validation     | Error   | Port type mismatch between connected nodes. |
| DAG202    | Validation     | Error   | Referenced workflow variable is undefined. |
| DAG203    | Validation     | Error   | Credential binding missing for capability requirements. |
| DAG204    | Validation     | Error   | Workflow terminates without a responder for trigger. |
| DAG205    | Validation     | Error   | Duplicate node alias encountered in workflow. |
| DAG206    | Validation     | Error   | Requested exporter unavailable for the workflow. |
| DAG300    | Validation     | Error   | Human-in-the-loop surface attached to non node/edge scope. |
| DAG301    | Validation     | Error   | View/export binding cannot be serialised. |
| DAG320    | Validation     | Error   | Invalid or non-positive rate limit configuration. |
| DAG330    | Validation     | Error   | Provider credentials missing for connector usage. |
| DAG331    | Validation     | Error   | Provider scopes fall outside declared policy. |
| DAG340    | Validation     | Error   | Variable shadowing detected in workflow definition. |
| DAG341    | Validation     | Error   | Variable binding has incompatible type. |
| EFFECT201 | Validation     | Error   | Declared effects do not match bound capabilities. |
| DET301    | Validation     | Error   | Determinism claim conflicts with resource usage. |
| DET302    | Validation     | Error   | Declared determinism conflicts with registered resource hints. |
| CTRL001   | Lint           | Warn    | Control-flow surface hint recommended for branching/loop. |
| CTRL901   | Runtime        | Error   | Reserved control surface not supported by this host/profile. |
| IDEM020   | Validation     | Error   | Effectful sink missing partition key and idempotency key. |
| IDEM025   | Validation     | Error   | Idempotency key references non-deterministic fields. |
| IDEM099   | Runtime        | Fatal   | Possible idempotency key collision detected. |
| CACHE001  | Validation     | Warn    | Strict node missing cache specification. |
| CACHE002  | Validation     | Error   | Stable node missing pinned inputs or cache policy. |
| RETRY010  | Validation     | Error   | Retry ownership declared on both connector and orchestrator. |
| RETRY011  | Validation     | Error   | Retry ownership violates capability policy defaults. |
| EXACT001  | Validation     | Error   | Exactly-once delivery requested without dedupe binding. |
| EXACT002  | Validation     | Error   | Exactly-once delivery requested without idempotency metadata. |
| EXACT003  | Validation     | Error   | Exactly-once delivery requested without a sufficient dedupe TTL. |
| EXACT005  | Validation     | Error   | Exactly-once sink lacks documented idempotent contract. |
| SPILL001  | Validation     | Error   | Spill tiers must declare an in-memory `max_items` budget. |
| SPILL002  | Validation     | Error   | Spill tiers require a blob capability binding. |
| TIME015   | Runtime        | Error   | Execution exceeded configured timeout budget. |
| RUN030    | Runtime        | Warn    | Workflow experiencing sustained backpressure stall. |
| RUN040    | Runtime        | Info    | Buffer spill to blob tier triggered. |
| RUN050    | Runtime        | Error   | Hard memory limit exceeded during execution. |
| WFLOW010  | Lowering       | Error   | Inline segment contains non deterministic operations. |
| DATA101   | Validation     | Error   | Data-class annotations missing for emitted schema. |
| EGRESS101 | Validation     | Error   | No allow-listed egress domains for connector. |
| SECR201   | Validation     | Error   | Credential binding grants scopes outside requested set. |
| SAGA201   | Validation     | Error   | Compensation node incompatible with effect node output. |

> **Note:** The default severity column indicates how diagnostics are surfaced in the
> absence of policy overrides. Individual organisations may escalate or demote specific
> codes via policy configuration, but the canonical registry values must remain stable.
