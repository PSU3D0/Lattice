# Checkpointing and Durability Specification

Status: Draft  
Version: 0.1.x  
Last updated: 2026-01-29

## 1. Overview

This specification defines the model for **checkpointing**, **durability**, and **resumability** in latticeflow. It enables flows to:

- Persist execution state at defined boundaries (checkpoints).
- Resume execution after process restart, host migration, or deliberate suspension.
- Support wait/timer/HITL (human-in-the-loop) nodes without burning compute resources.
- Operate across diverse host environments (native, Cloudflare Workers, etc.).

### 1.1 Scope

This document covers:

- Durability modes and their guarantees.
- Durability profiles for checkpoint compatibility.
- Host trait surfaces for checkpoint storage, resume scheduling, and blob spilling.
- Checkpoint record schema and lifecycle.
- Timer and wait node design for the standard library.
- Validator rules for durability enforcement.

Implementation guidance, host-specific details, and examples live in the Epic 04 roadmap doc.

### 1.2 Relationship to Other Specs

- **Flow IR**: Uses `CheckpointIR` (frontier + state). Durability policy lives under `FlowIR.policies`.
  Durability profiles are **registry metadata** (stdlib or external registry), not required fields in Flow IR.
- **Invocation ABI**: Resume triggers produce a new invocation that carries checkpoint context.
  This spec defines the required resume metadata and labels.
- **Capabilities and Binding**: Introduces host durability traits (`CheckpointStore`, `ResumeScheduler`,
  `ResumeSignalSource`, `CheckpointBlobStore`). These are **host-internal** capabilities and are not exposed as flow nodes.

### 1.3 Key Terms

| Term | Definition |
|------|------------|
| **Checkpoint** | A persistent snapshot of execution state at a node boundary. |
| **Frontier** | The set of node outputs that represent "where we are" in execution. |
| **Durability mode** | A flow-level policy governing checkpoint frequency and coverage. |
| **Resume token** | An opaque identifier that maps to a checkpoint for external resume triggers. |
| **Resume signal** | A host-delivered event that causes a checkpoint to resume (timer alarm, webhook, queue message). |
| **Halt node** | A node that requires checkpoint + external resume (timer, HITL, callback). |

---

## 2. Durability Model

### 2.1 Design Principle: Checkpointable by Default

All nodes are **assumed checkpointable** unless they explicitly declare incompatibility. This inverts the traditional "opt-in" model:

- Authors don't annotate every node with checkpoint metadata.
- Incompatible nodes are the exception, not the rule.
- Validator enforces durability based on node durability profiles.

### 2.2 Durability Modes

A flow declares its durability mode in `FlowIR.policies.durability`:

```
durability = "off" | "partial" | "strong"
```

| Mode | Checkpoint behavior | Resume guarantee |
|------|---------------------|------------------|
| `off` | No checkpoints written. | No resume; restart from trigger. |
| `partial` | Checkpoints at halt nodes only. | Resume at declared halt boundaries. |
| `strong` | Checkpoints at every node boundary (or configurable interval). | Resume at any completed node. |

**Default**: `partial` (checkpoint only where required).

### 2.3 Strong vs Partial Durability

**Strong durability** requires:
- Every node in the flow is checkpoint-compatible.
- State is persisted after each node completes.
- Resume can occur at any frontier.

**Partial durability** allows:
- Some nodes to be checkpoint-incompatible.
- State is persisted only at explicit halt nodes.
- Resume can only occur at halt boundaries.

If `strong` is requested but the flow contains incompatible nodes, the validator emits an error. A future option may allow downgrade to `partial` with a warning.

### 2.4 Durability Report

The validator produces a **durability report** for each flow:

```
DurabilityReport {
  mode: DurabilityMode,
  coverage: f32,                    // 0.0 to 1.0
  incompatible_nodes: Vec<String>,  // node aliases
  resumable_segments: Vec<Segment>, // contiguous resumable regions
  halt_nodes: Vec<String>,          // nodes requiring checkpoint
}
```

This enables tooling to show "85% durable" and highlight which nodes block full coverage.

---

## 3. Durability Profile

### 3.1 Profile Fields

Every node has a **durability profile** provided by its registry metadata (stdlib or external registry).
Flow IR does not require these fields; they are resolved at planning/validation time. Flows may optionally
override a node's profile in rare cases (e.g., host-specific implementations), but the default is registry-derived.

```rust
pub struct DurabilityProfile {
    /// Node state can be serialized and resumed.
    /// Default: true
    pub checkpointable: bool,
    
    /// For streaming nodes: output can be replayed from a cursor.
    /// Default: true for non-streaming, false for unbounded streams.
    pub replayable: bool,
    
    /// Node is a halting boundary requiring suspend + resume.
    /// True for timer/callback/approval nodes.
    pub halts: bool,
}
```

### 3.2 Default Profile

For ordinary nodes (no special declarations):

```rust
DurabilityProfile {
    checkpointable: true,  // default assumption
    replayable: true,      // for non-streaming nodes
    halts: false,
}
```

### 3.3 Incompatibility Conditions

A node is **checkpoint-incompatible** when:

| Condition | Reason |
|-----------|--------|
| Unbounded streaming without replay contract | Cannot resume mid-stream safely. |
| Non-serializable internal state | Holds open sockets, file handles, or in-memory references. |
| Non-idempotent effects without replay protection | Side effects would repeat unsafely on resume. |
| Depends on nondeterministic input without capture | Cannot reconstruct execution context. |

### 3.4 Declaring Incompatibility

Nodes opt out via macro attribute or registry entry:

**Rust macro**:
```rust
#[node(checkpointable = false, reason = "holds open websocket")]
async fn realtime_feed(input: FeedConfig) -> NodeResult<FeedStream> { ... }
```

**Registry entry** (for external nodes):
```json
{
  "id": "external.realtime_feed",
  "durability": {
    "checkpointable": false,
    "reason": "holds open websocket"
  }
}
```

### 3.5 Halt Nodes

Halt nodes declare `halts = true`. The runtime **must** persist a checkpoint and suspend when entering these nodes. Examples:

- `timer.wait` — wait for duration or absolute time.
- `hitl.approval` — wait for human approval.
- `callback.external` — wait for external webhook.

### 3.6 Profile Resolution

1. Load registry profile for each node.
2. Apply any flow overrides (if present).
3. Derive effectfulness/determinism from `effects` and `determinism`.
4. Derive `replayable` for streams (default false for unbounded streams).
5. Validate against durability mode.

---

## 4. Checkpoint Record Schema

### 4.1 CheckpointRecord

```rust
pub struct CheckpointRecord {
    /// Unique identifier for this checkpoint.
    pub checkpoint_id: String,
    
    /// Flow identity.
    pub flow_id: FlowId,
    pub flow_version: String,
    
    /// Execution identity.
    pub run_id: RunId,
    pub parent_run_id: Option<RunId>,
    
    /// Execution frontier (where we stopped).
    pub frontier: FlowFrontier,
    
    /// Serialized node outputs needed for resume.
    pub state: SerializedState,
    
    /// Idempotency tokens for effectful nodes.
    pub idempotency: IdempotencyState,
    
    /// Checkpoint metadata.
    pub created_at: DateTime<Utc>,
    pub resume_after: Option<DateTime<Utc>>,
    pub ttl: Option<Duration>,
    
    /// Schema version for forward compatibility.
    pub version: u32,
}
```

**Note:** Resume tokens are managed by `ResumeSignalSource` and do not need to be stored inside the
checkpoint record. The record may include `resume_after` but token storage is host-specific.

### 4.2 FlowFrontier

Represents the execution position:

```rust
pub struct FlowFrontier {
    /// Completed node(s) at the frontier.
    pub completed: Vec<FrontierEntry>,
    
    /// Next node(s) to execute on resume.
    pub pending: Vec<String>,
}

pub struct FrontierEntry {
    pub node_alias: String,
    pub output_port: String,
    pub cursor: Option<String>,  // for streaming nodes
}
```

### 4.3 SerializedState

```rust
pub struct SerializedState {
    /// Inline JSON for small payloads.
    pub data: JsonValue,
    
    /// References to spilled blobs for large payloads.
    pub blobs: Vec<BlobRef>,
}

pub struct BlobRef {
    pub ref_id: String,      // e.g., "blob://checkpoint/{checkpoint_id}/{field}"
    pub size_bytes: u64,
    pub content_hash: Option<String>,
}
```

### 4.4 IdempotencyState

Preserves idempotency tokens for effectful nodes:

```rust
pub struct IdempotencyState {
    /// Map of node_alias -> idempotency token(s) consumed.
    pub consumed: HashMap<String, Vec<String>>,
    
    /// Map of node_alias -> idempotency token to use on resume.
    pub pending: HashMap<String, String>,
}
```

---

## 5. Host Trait Surfaces

Hosts implement these traits to enable durable execution. These are **host-internal services** and are not
exposed as flow nodes. They may be backed by existing storage systems (KV, SQL, blob stores), but flows
must not depend on or access them directly. This keeps durability concerns separate from application
capabilities and avoids coupling checkpoints to flow-level storage nodes.

All traits are optional; the runtime gracefully degrades if a trait is unavailable.

### 5.1 CheckpointStore

Primary storage for checkpoint records.

```rust
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait CheckpointStore: Capability {
    /// Persist a checkpoint record. Returns a handle for retrieval.
    async fn put(&self, record: CheckpointRecord) -> Result<CheckpointHandle, CheckpointError>;
    
    /// Retrieve a checkpoint by handle.
    async fn get(&self, handle: &CheckpointHandle) -> Result<CheckpointRecord, CheckpointError>;
    
    /// Acknowledge completion; checkpoint may be deleted or archived.
    async fn ack(&self, handle: &CheckpointHandle) -> Result<(), CheckpointError>;
    
    /// Acquire a lease to prevent concurrent resume.
    async fn lease(
        &self,
        handle: &CheckpointHandle,
        ttl: Duration,
    ) -> Result<Lease, CheckpointError>;
    
    /// Release a previously acquired lease.
    async fn release_lease(&self, lease: Lease) -> Result<(), CheckpointError>;
    
    /// List checkpoints for a flow/run (for diagnostics).
    async fn list(
        &self,
        filter: CheckpointFilter,
    ) -> Result<Vec<CheckpointHandle>, CheckpointError>;
}

pub struct CheckpointHandle {
    pub checkpoint_id: String,
    pub flow_id: FlowId,
    pub run_id: RunId,
}

pub struct Lease {
    pub lease_id: String,
    pub expires_at: DateTime<Utc>,
}

pub struct CheckpointFilter {
    pub flow_id: Option<FlowId>,
    pub run_id: Option<RunId>,
    pub status: Option<CheckpointStatus>,
}

pub enum CheckpointStatus {
    Active,
    Completed,
    Expired,
}

#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    #[error("checkpoint not found")]
    NotFound,
    #[error("lease conflict: checkpoint is locked by another consumer")]
    LeaseConflict,
    #[error("lease expired")]
    LeaseExpired,
    #[error("storage error: {0}")]
    Storage(String),
}
```

### 5.2 ResumeScheduler

Schedules future resume triggers (timers, alarms).

```rust
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait ResumeScheduler: Capability {
    /// Schedule resume at an absolute time.
    async fn schedule_at(
        &self,
        handle: CheckpointHandle,
        at: DateTime<Utc>,
    ) -> Result<ScheduleId, ScheduleError>;
    
    /// Schedule resume after a delay.
    async fn schedule_after(
        &self,
        handle: CheckpointHandle,
        delay: Duration,
    ) -> Result<ScheduleId, ScheduleError>;
    
    /// Cancel a scheduled resume.
    async fn cancel(&self, schedule_id: ScheduleId) -> Result<(), ScheduleError>;
    
    /// Query schedule status.
    async fn status(&self, schedule_id: ScheduleId) -> Result<ScheduleStatus, ScheduleError>;
}

pub struct ScheduleId(pub String);

pub enum ScheduleStatus {
    Pending { fires_at: DateTime<Utc> },
    Fired { fired_at: DateTime<Utc> },
    Cancelled,
    Expired,
}

#[derive(Debug, thiserror::Error)]
pub enum ScheduleError {
    #[error("schedule not found")]
    NotFound,
    #[error("invalid delay: {0}")]
    InvalidDelay(String),
    #[error("scheduler unavailable: {0}")]
    Unavailable(String),
}
```

### 5.3 ResumeSignalSource

Generates and resolves resume tokens for external triggers (webhooks, callbacks).

```rust
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait ResumeSignalSource: Capability {
    /// Create a resume token for a checkpoint.
    async fn create_token(
        &self,
        handle: &CheckpointHandle,
        config: TokenConfig,
    ) -> Result<ResumeToken, TokenError>;
    
    /// Resolve a token to its checkpoint handle.
    async fn resolve_token(
        &self,
        token: &ResumeToken,
    ) -> Result<CheckpointHandle, TokenError>;
    
    /// Revoke a token (e.g., on flow cancellation).
    async fn revoke_token(&self, token: &ResumeToken) -> Result<(), TokenError>;
}

pub struct ResumeToken(pub String);

pub struct TokenConfig {
    pub ttl: Option<Duration>,
    pub single_use: bool,
    pub metadata: Option<JsonValue>,
}

#[derive(Debug, thiserror::Error)]
pub enum TokenError {
    #[error("token not found or expired")]
    NotFound,
    #[error("token already used")]
    AlreadyUsed,
    #[error("token generation failed: {0}")]
    Generation(String),
}
```

### 5.4 CheckpointBlobStore

Spills large checkpoint state to blob storage. This is a host-level service. A host may implement it by
wrapping an existing blob provider (R2, S3, filesystem), but it is not a flow-exposed capability.

```rust
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait CheckpointBlobStore: BlobStore {
    /// Put a checkpoint blob with automatic cleanup binding.
    async fn put_checkpoint_blob(
        &self,
        checkpoint_id: &str,
        field: &str,
        data: &[u8],
    ) -> Result<BlobRef, BlobError>;
    
    /// Delete all blobs for a checkpoint (on ack or expiry).
    async fn delete_checkpoint_blobs(
        &self,
        checkpoint_id: &str,
    ) -> Result<(), BlobError>;
}
```

### 5.5 Trait Composition: HostDurability

Hosts compose these traits into a durability bundle:

```rust
pub trait HostDurability {
    fn checkpoint_store(&self) -> Option<&dyn CheckpointStore>;
    fn resume_scheduler(&self) -> Option<&dyn ResumeScheduler>;
    fn resume_signal_source(&self) -> Option<&dyn ResumeSignalSource>;
    fn checkpoint_blob_store(&self) -> Option<&dyn CheckpointBlobStore>;
    
    /// Returns the maximum durability mode this host supports.
    fn max_durability_mode(&self) -> DurabilityMode;
}
```

If a host lacks `CheckpointStore`, durability mode is forced to `off`.

---

## 6. Checkpoint Lifecycle

### 6.1 When Checkpoints Are Written

| Trigger | Condition |
|---------|-----------|
| **Halt node entry** | Always (timer, HITL, callback). |
| **Node completion** | If `durability = strong`. |
| **Interval** | If configured (e.g., every 10 nodes or 5 seconds). |
| **Explicit** | Node calls `context.checkpoint()`. |

### 6.2 Checkpoint Flow

```
1. Node completes execution
2. Runtime checks: should checkpoint?
   - Yes if halt node, strong durability, or interval trigger
3. Serialize frontier + state
   - Spill to blob store if state > threshold (default 512 KiB)
4. Write checkpoint record to CheckpointStore
5. If halt node:
   - Schedule resume (timer) or create token (HITL/callback)
   - Return control to host (no CPU burn)
6. On resume trigger:
   - Acquire lease on checkpoint
   - Load checkpoint record + blobs
   - Reconstruct execution context
   - Continue from frontier.pending nodes
7. On completion:
   - Ack checkpoint (allows cleanup)
   - Release lease
```

### 6.3 Lease Semantics

Leases prevent concurrent resume of the same checkpoint:

- Lease TTL should exceed expected execution time.
- If lease expires mid-execution, another consumer may resume.
- Runtime should renew lease periodically for long flows (heartbeat).
- On resume conflict, later consumer receives `LeaseConflict` error.

**Renewal/heartbeat**:
- Hosts SHOULD allow renewing a lease by calling `lease(handle, ttl)` again before expiry.
- Runtime SHOULD renew at a fixed interval (e.g., every ttl/2) while execution is in progress.
- If renewal fails, runtime SHOULD halt execution and return a deterministic error to avoid double-run.

### 6.4 Garbage Collection

| State | Action |
|-------|--------|
| Completed (ack'd) | Delete checkpoint + blobs immediately (default) or after retention period. |
| Expired (TTL) | Delete checkpoint + blobs. |
| Abandoned (lease expired, no progress) | Mark for manual review or auto-delete after grace period. |

**Default retention**: discard on completion. Hosts may retain completed checkpoints for a configured
window (`retain_completed_for`) for debugging or audit.

The `ack` operation is the primary lifecycle hook; hosts should perform retention/cleanup decisions at
ack time. Additional lifecycle hooks are optional and intended for metrics/observability rather than
core semantics.

---

## 7. Resume Mechanics

### 7.0 Resume Signals and Plumbing

Resume is driven by **resume signals** that originate from the host environment. A resume signal is not
itself a flow capability or a flow node; it is a host mechanism that activates a stored checkpoint.

Resume signals are delivered through host durability traits:
- **`ResumeScheduler`** for time-based signals (timer/alarm).
- **`ResumeSignalSource`** for external signals (webhooks, callbacks, queues).

This avoids introducing host-specific flow nodes (e.g., `cap-do-timer`). The standard library timer/HITL
nodes are portable and delegate to these traits. If a host does not implement the required trait, the
validator rejects flows that rely on it.

### 7.1 Resume Triggers

| Trigger type | Source | Resolution |
|--------------|--------|------------|
| **Timer/alarm** | ResumeScheduler fires | CheckpointHandle directly |
| **HTTP webhook** | External system calls resume endpoint | ResumeToken → CheckpointHandle |
| **Queue message** | Message contains checkpoint ID | CheckpointHandle directly |
| **Manual** | Operator invokes resume | CheckpointHandle or ResumeToken |

### 7.1.1 Resume Endpoint (Host Internal)

Hosts expose a private resume endpoint used by timers, webhooks, and operators. This endpoint is not
part of Flow IR triggers and should not be externally discoverable by default. The concrete request/
response shape is host-specific and defined in Epic 04 implementation notes.

### 7.2 Resume Invocation

A resume produces a new `Invocation` with checkpoint context. This does **not** require a separate
flow entrypoint; the runtime uses the stored frontier to continue execution inside the same flow.
If a flow has multiple public entrypoints, resume bypasses them and continues from the frontier only.

```rust
InvocationMetadata {
    labels: {
        "lf.run_id": "<original_run_id>",
        "lf.resume_id": "<new_unique_id>",
        "lf.checkpoint_id": "<checkpoint_id>",
    },
    extensions: {
        "lf.resume": {
            "frontier": { ... },
            "attempt": 1,
            "resumed_at": "2026-01-29T12:00:00Z",
        }
    }
}
```

### 7.3 State Rehydration

```
1. Load CheckpointRecord from store
2. Resolve BlobRefs to actual data
3. Reconstruct SerializedState
4. Rebuild node outputs for frontier.completed
5. Restore idempotency state
6. Create new execution context with restored state
7. Begin execution from frontier.pending nodes
```

### 7.4 Idempotency on Resume

Effectful nodes must handle resume correctly:

- If `idempotency.consumed` contains a token for this node, skip execution (already done).
- If `idempotency.pending` has a token, use it for the current attempt.
- This prevents double-execution of side effects.

### 7.5 Error Handling

| Error | Behavior |
|-------|----------|
| Checkpoint not found | Fail with `ResumeError::CheckpointNotFound` |
| Lease conflict | Retry after backoff or fail |
| Blob fetch failed | Fail with `ResumeError::StateCorrupted` |
| Schema version mismatch | Attempt migration or fail with `ResumeError::IncompatibleVersion` |

---

## 8. Timer and Wait Nodes (Standard Library)

### 8.1 Design Principle: Unified Node, Host-Specific Scheduler

Timer/wait nodes are **single logical nodes** that delegate to the host's `ResumeScheduler`. This avoids proliferating host-specific node variants.

### 8.2 Timer Node (`std.timer.wait`)

```rust
#[node(
    halts = true,
    effectful = false,
    deterministic = false,
)]
pub struct TimerWaitInput {
    /// Duration to wait (mutually exclusive with `until`).
    #[serde(default, with = "humantime_serde")]
    pub duration: Option<Duration>,
    
    /// Absolute time to wait until (mutually exclusive with `duration`).
    pub until: Option<DateTime<Utc>>,
    
    /// Payload to forward on resume.
    #[serde(default)]
    pub payload: JsonValue,

    /// Optional scheduler alias (host-defined). If omitted, uses host default.
    #[serde(default)]
    pub scheduler: Option<String>,
}

pub struct TimerWaitOutput {
    /// Original payload, forwarded.
    pub payload: JsonValue,
    
    /// Actual time resumed.
    pub resumed_at: DateTime<Utc>,
    
    /// Scheduled vs actual drift.
    pub drift_ms: i64,
}
```

**Behavior**:
1. Validate exactly one of `duration` or `until` is set.
2. Compute target time.
3. Checkpoint current state.
4. Resolve scheduler:
   - If `scheduler` is set, use that alias.
   - Otherwise, use the host default scheduler.
5. Call `ResumeScheduler.schedule_at(handle, target_time)` using the resolved scheduler.
5. Return control to host (no CPU burn).
6. On alarm fire: resume invocation, emit `TimerWaitOutput`.

Code-defined usage patterns live in `impl-docs/spec/stdlib-and-node-registry.md` and the Epic 04
implementation notes.

### 8.3 HITL Approval Node (`std.hitl.approval`)

```rust
#[node(
    halts = true,
    effectful = true,  // sends notification
    deterministic = false,
)]
pub struct ApprovalInput {
    /// Context to show approver.
    pub context: JsonValue,
    
    /// Notification channel (email, slack, etc.).
    pub notify: Option<NotifyConfig>,
    
    /// Timeout after which approval auto-rejects.
    #[serde(default, with = "humantime_serde")]
    pub timeout: Option<Duration>,
}

pub struct ApprovalOutput {
    /// Approved or rejected.
    pub approved: bool,
    
    /// Approver identity (if available).
    pub approver: Option<String>,
    
    /// Approver's comment.
    pub comment: Option<String>,
    
    /// Time of decision.
    pub decided_at: DateTime<Utc>,
}
```

**Behavior**:
1. Checkpoint current state.
2. Create resume token via `ResumeSignalSource`.
3. Optionally send notification with approval link (contains token).
4. If timeout set, also schedule timeout alarm.
5. Return control to host.
6. On approval webhook: resume with approved payload.
7. On timeout alarm: resume with auto-reject.

Usage examples for HITL nodes live in the stdlib spec and the Epic 04 implementation notes.

### 8.4 External Callback Node (`std.callback.wait`)

```rust
#[node(
    halts = true,
    effectful = false,
    deterministic = false,
)]
pub struct CallbackWaitInput {
    /// Timeout for callback.
    #[serde(default, with = "humantime_serde")]
    pub timeout: Option<Duration>,
    
    /// Context to embed in token metadata.
    #[serde(default)]
    pub context: JsonValue,
}

pub struct CallbackWaitOutput {
    /// Payload provided by external caller.
    pub payload: JsonValue,
    
    /// Time callback was received.
    pub received_at: DateTime<Utc>,
}
```

**Behavior**:
1. Checkpoint state.
2. Create resume token with context.
3. Emit token in node output (caller must expose it).
4. Return control.
5. On callback: resume with provided payload.

### 8.5 Native vs Workers: Same Node, Different Scheduler

The timer node doesn't know which host it's on. It calls:

```rust
context.durability().resume_scheduler().schedule_after(handle, delay)
```

| Host | ResumeScheduler implementation |
|------|-------------------------------|
| **Native** | Task queue (Tokio timer, cron, SQS delay, etc.) |
| **Workers** | Durable Object alarm |

This keeps node logic portable.

### 8.6 Scheduler Selection and Host Config

Timer/HITL/callback nodes can optionally select a scheduler by name. The scheduler alias is a **host
configuration** concern, not part of Flow IR. The node input schema (derived from the Rust input struct
via the `#[node]` macro) exposes an optional `scheduler` parameter when needed.

**Resolution rules**:
- If `scheduler` is omitted, the host uses its default scheduler.
- If `scheduler` is set, the host must have a matching alias configured.
- If no matching alias exists, validation fails.

Timeouts on HITL/callback nodes use the same scheduler selection logic. Host configuration schemas and
defaults are defined in the Epic 04 implementation notes.

### 8.7 Standard Library Placement and Host Gating

Standard library nodes are registered via node metadata (e.g., `#[node(...)]`) and compiled into hosts.
The stdlib is a logical registry; concrete code may live in a dedicated crate (e.g., `crates/stdlib`) or
be embedded into host crates. Hosts may include/exclude stdlib node sets via feature flags, but the
portable contract remains the same.

Gating rules:
- Nodes declare required host services (e.g., `halts`, `requires_resume_scheduler`).
- Validator checks host-provided durability services and scheduler aliases.
- Host-specific nodes (e.g., `workers.timer.wait`) are allowed as **extensions** but are not part of
  the stdlib and should not be required by portable flows.

---

Implementation guidance for native and Workers hosts is documented in Epic 04.

---

## 9. Checkpoint Storage Design Considerations

### 10.1 State Size and Blob Threshold

- **Default threshold**: 512 KiB.
- State below threshold: inline in `SerializedState.data`.
- State above threshold: spill to `CheckpointBlobStore`, store `BlobRef`.

Configured via Flow IR durability policies (see `impl-docs/spec/flow-ir.md`).

### 10.2 System of Record

| Store | Authoritative for |
|-------|-------------------|
| `CheckpointStore` | Execution position, metadata, idempotency state. |
| `CheckpointBlobStore` | Large payload data. |

**Consistency requirement**: Blob must be written before checkpoint record references it. On resume, blob fetch failure is fatal.

**Separation from flow storage**: Checkpoint stores and blobs are host durability infrastructure. They may
use the same underlying systems as flow-level KV/SQL/blob capabilities, but they are not addressable by
flows. This avoids accidental coupling between business data and runtime state.

### 10.3 Garbage Collection

| Trigger | Action |
|---------|--------|
| Ack (flow completed) | Delete checkpoint + blobs (or archive). |
| TTL expiry | Delete checkpoint + blobs. |
| Lease expiry without progress | Mark abandoned; GC after grace period. |

**Blob orphan cleanup**: Periodic scan for blobs with no referencing checkpoint.

### 10.4 Versioning and Schema Evolution

- `CheckpointRecord.version` tracks schema version.
- On load, runtime checks version compatibility.
- **Forward compatible**: old runtime can skip unknown fields.
- **Backward compatible**: new runtime can read old records.
- Breaking changes require migration or version gate.

---

## 11. Validator Rules

### 11.1 Durability Mode Validation

```
IF flow.durability == "strong":
  FOR each node in flow:
IF NOT node.durability.checkpointable:
      EMIT error DAG-CKPT-001: "Node '{alias}' is not checkpointable; 
                               cannot use durability=strong"
```

### 11.2 Halt Node Validation

```
IF node.durability.halts:
  IF flow.durability == "off":
    EMIT error DAG-CKPT-002: "Halt node '{alias}' requires durability != off"
  IF host.checkpoint_store == None:
    EMIT error DAG-CKPT-003: "Halt node '{alias}' requires CheckpointStore capability"
```

### 11.3 Idempotency on Resume Path

```
FOR each node on potential resume path:
IF node.effects == Effectful AND NOT node.idempotency.key:
    EMIT error DAG-CKPT-004: "Effectful node '{alias}' on resume path 
                             must declare idempotency"
```

### 11.4 Streaming Node Compatibility

```
IF node.output is Stream:
  IF NOT node.durability.replayable:
    node.durability.checkpointable = false
    IF flow.durability == "strong":
      EMIT error DAG-CKPT-005: "Streaming node '{alias}' is not replayable;
                               cannot checkpoint mid-stream"
```

---

Examples, open questions, and implementation details live in the Epic 04 roadmap doc.
