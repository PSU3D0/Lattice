Status: Draft
Purpose: architecture-decision / spec
Owner: Runtime
Last reviewed: 2026-03-14

# Connector Trigger Runtime Contract (0.1.x)

This document defines the runtime contract between:

- **connector crates** that declare polling/webhook trigger semantics, and
- **hosts** that provide the stateful machinery required to activate and run
  those triggers.

It exists to clarify a subtle but important boundary:

- connector crates should be able to describe trigger semantics precisely,
- but polling state, route exposure, remote subscription activation, verifier
  secret resolution, and conflict reconciliation are often **host concerns**.

## Decision summary

Connector trigger runtimes should follow a **trait/service-oriented split**:

- connector crates declare the **semantic trigger contract**,
- hosts implement the **stateful operational contract**.

In particular:
- polling cursor state is host-owned durable state,
- webhook activation/subscription state is host-owned durable state,
- route exposure is host-owned,
- verifier material resolution is host-owned,
- connector crates may provide **defaults/recommendations/hints**,
  but hosts retain final control over scheduling, routing, leases, and storage.

This keeps the system aligned with existing Lattice principles:
- clean Flow IR,
- host-owned lifecycle concerns,
- explicit capability/credential boundaries,
- and semantic nodes rather than topology inflation.

## Why this document is needed

The connector surface spec already captures that a connector family may expose:
- action nodes,
- polling triggers,
- webhook triggers,
- credential roles,
- activation lifecycle kinds.

But that still leaves an ambiguity:

> which parts are declarative connector semantics, and which parts are host
> implementation details?

Without this clarification, we risk either:
- forcing trigger lifecycle into ordinary graph topology, or
- overloading connector manifests with backend-specific runtime mechanics.

## Layered model

## 1. Flow layer

At the Flow IR / bundle level, the core rule is unchanged:
- a trigger is a `NodeKind::Trigger` node,
- ingress is explicit bundle entrypoint metadata,
- hosts own route realization.

This document does not alter the basic rules from:
- `impl-docs/spec/flow-ir.md`

## 2. Connector trigger contract layer

A connector crate declares:
- trigger kind,
- payload/output contract,
- credential roles,
- lifecycle kind,
- required state schema,
- dedupe identity semantics,
- verifier kind,
- conflict/reconciliation policy,
- recommended scheduling/backoff hints.

This is **semantic declaration**, not backend implementation.

## 3. Host runtime/control-plane layer

A host implements:
- durable trigger state storage,
- scheduling,
- leases/activation locks,
- route allocation/exposure,
- remote webhook reconciliation,
- verifier execution,
- credential resolution,
- and delivery/idempotency integration.

This is where DOs, SQLite, Postgres, queues, or other platform-specific
machinery belong.

## Hard contract vs host-overridable hints

One important source of confusion is that not every trigger field should be
treated the same way.

We should distinguish between:
- **hard semantic contract**, and
- **soft defaults / hints**.

### Hard contract

These should be treated as semantically meaningful declarations from the
connector family.

Examples:
- trigger kind: `polling_trigger` vs `webhook_trigger`
- lifecycle kind: `manual_external`, `host_managed_poll`, etc.
- state schema: what cursor/subscription state looks like
- verifier kind: HMAC, query secret, service-specific signature rules
- dedupe identity semantics: what counts as the same remote event
- conflict policy: reject/adopt/replace/reconcile rules
- stability scope: what logical instance a remote subscription belongs to

Hosts should not silently rewrite these into different semantics.

### Soft defaults / hints

These are recommendations a connector family may provide, but the host may
override for safety, policy, or deployment reasons.

Examples:
- recommended poll interval
- recommended backoff policy
- suggested concurrency cap
- route hint/path hint
- suggested subscription refresh cadence
- preferred retention/cleanup window for activation state

This is the trigger/runtime equivalent of other Lattice hint-style surfaces.

## Trigger instance identity

Hosts need a stable logical identity for a connector trigger instance that
survives restarts and deployment changes.

### Why this matters

It is needed for:
- stable activation state lookup,
- remote webhook adoption/replacement decisions,
- route continuity,
- dedupe continuity,
- lease acquisition,
- and host reconciliation after deployment changes.

### Conceptual shape

```rust
struct TriggerInstanceKey {
    connector_id: String,
    trigger_identifier: String,
    deployment_scope: String,
    entrypoint_alias: String,
    binding_scope: String,
    environment_scope: Option<String>,
}
```

Exact serialization is TBD, but the key idea is:
- identity should be based on **logical ownership**, not a currently assigned
  public URL.

## Route stability model

A public route is not the primary identity of a webhook trigger.
It is a host-managed projection of the logical trigger instance.

### Implication

Connector crates may declare:
- a route hint,
- path preference,
- verifier requirements,
- whether setup and default endpoints are distinct.

Hosts decide:
- the actual externally reachable route,
- how it remains stable across deployments,
- whether it is preserved or rotated,
- how it is mapped back to a `TriggerInstanceKey`.

## Lifecycle kinds

The connector surface spec introduced conceptual lifecycle kinds.
This document refines their runtime meaning.

### `none`
No activation-time remote setup is required.

Example:
- pure local/manual trigger semantics

### `host_managed_poll`
Host scheduler periodically invokes the connector poll driver and persists
connector-owned cursor state.

Examples:
- sheet row diffing
- incremental list/update polling

### `host_managed_webhook`
Host exposes a route and handles inbound verification/runtime, but no remote
subscription API is required.

Examples:
- manual webhook configuration in an external SaaS app

### `host_managed_webhook_subscription`
Host owns remote subscription setup/check/delete/reconciliation for the trigger.

Examples:
- Jira webhook registration
- HubSpot app webhook subscription setup
- managed Stripe-like event endpoint creation

### `manual_external`
Host exposes the route, but the external system must be configured manually by
an operator or user.

Examples:
- manual Slack app event configuration
- manual dashboard-entered callback URL

## Polling trigger state model

Polling triggers need durable host-owned state.

### Typical connector-owned state shape

Examples:
- last revision ID
- sync token
- cursor/page token
- last seen timestamp
- dedupe marker/window
- service-specific resume token

This state should be declared by the connector family as a **typed schema**, but
owned by the host runtime.

### Host responsibilities

Hosts should provide:
- persistent state store
- activation lease / poll lease
- scheduler integration
- dedupe persistence when needed
- failure/backoff handling

### Connector responsibilities

Connector crates should provide:
- state schema
- cursor advance logic
- dedupe identity extraction semantics
- minimum safe interval hint
- semantic output contract

## Webhook activation state model

Webhook triggers often need separate activation state beyond normal inbound
payload handling.

### Typical host-owned activation state

Examples:
- remote webhook/subscription ID
- remote endpoint target currently registered
- subscription list/version
- verifier handle reference
- last activation/reconciliation timestamp
- conflict/reconciliation markers

This state belongs to the host activation/control-plane, not the flow payload.

### Host responsibilities

Hosts should provide:
- activation state store
- compare-and-set or lease-based activation lock
- route allocation/exposure
- remote subscription reconciliation runner
- verifier resolution + enforcement

### Connector responsibilities

Connector crates should provide:
- remote setup/check/delete semantics (if applicable)
- conflict policy recommendation
- verifier kind
- route/setup shape expectations
- payload normalization/output shaping

## Conflict and reconciliation policy

Webhook and activation systems frequently encounter conflicts.

### Common conflict cases

- service allows only one remote webhook target per app/account
- same logical trigger redeployed with a new route
- stale remote subscription still exists
- foreign deployment already owns remote registration
- operator manually changed remote subscription
- multiple environments share the same app credentials unexpectedly

### Recommended conceptual policies

```rust
enum ActivationConflictPolicy {
    RejectIfForeign,
    AdoptIfSameInstance,
    ReplaceIfPolicyAllows,
    ReconcileIfStale,
}
```

Connector crates should be able to declare the intended policy.
Hosts should enforce it using host-owned state and leases.

## Dedupe semantics

Polling/webhook triggers should be able to declare **how events are considered
identical**.

Examples:
- remote event ID
- revision ID
- tuple of `(entity_id, updated_at)`
- service cursor + record ID

This is a connector semantic declaration.
The storage and lease mechanics used to enforce it are host concerns.

## Conceptual trait/service split

The following interfaces are conceptual. They describe the desired separation,
not final crate names.

### Connector-side declarations

```rust
trait ConnectorTriggerContract {
    type Output;
    type DurableState;

    fn kind(&self) -> TriggerKind;
    fn lifecycle(&self) -> TriggerLifecycleKind;
    fn conflict_policy(&self) -> ActivationConflictPolicy;
    fn recommended_hints(&self) -> TriggerRuntimeHints;
}
```

```rust
trait PollingTriggerDriver: ConnectorTriggerContract {
    async fn poll(
        &self,
        state: &mut Self::DurableState,
        ctx: &TriggerRuntimeContext,
    ) -> PollOutcome<Self::Output>;
}
```

```rust
trait WebhookTriggerDriver: ConnectorTriggerContract {
    async fn normalize_request(
        &self,
        req: InboundRequest,
        ctx: &TriggerRuntimeContext,
    ) -> anyhow::Result<Self::Output>;
}
```

```rust
trait ManagedWebhookProvisioner {
    async fn check_remote(&self, ctx: &TriggerActivationContext) -> anyhow::Result<RemoteState>;
    async fn create_remote(&self, ctx: &TriggerActivationContext) -> anyhow::Result<RemoteState>;
    async fn delete_remote(&self, ctx: &TriggerActivationContext) -> anyhow::Result<()>;
}
```

### Host-side services

```rust
trait TriggerActivationStore {
    async fn load(instance: &TriggerInstanceKey) -> anyhow::Result<Option<ActivationRecord>>;
    async fn save(instance: &TriggerInstanceKey, record: ActivationRecord) -> anyhow::Result<()>;
    async fn acquire_lease(instance: &TriggerInstanceKey) -> anyhow::Result<ActivationLease>;
}
```

```rust
trait StableRouteProvider {
    async fn ensure_route(instance: &TriggerInstanceKey, hint: RouteHint) -> anyhow::Result<RouteBinding>;
}
```

```rust
trait TriggerScheduler {
    async fn ensure_schedule(instance: &TriggerInstanceKey, hint: ScheduleHint) -> anyhow::Result<()>;
}
```

```rust
trait InboundVerifierProvider {
    async fn verify(
        verifier: &VerifierDescriptor,
        req: &InboundRequest,
    ) -> anyhow::Result<VerificationOutcome>;
}
```

```rust
trait CredentialResolver {
    async fn resolve(role: CredentialRoleRef) -> anyhow::Result<ResolvedCredentialMaterial>;
}
```

These traits are illustrative, but they capture the intended ownership split.

## Native vs Workers examples

### Native host

Possible host implementations:
- activation store: SQLite/Postgres/filesystem
- lease: DB row lease / file lock / distributed lock
- scheduler: process timer / queue / cron integration
- verifier: in-process HMAC/signature checker
- route provider: web server/router mapping

### Workers host

Possible host implementations:
- activation store: Durable Object / D1
- lease: DO-scoped mutex/lease record
- scheduler: DO alarms / queue / cron trigger
- verifier: worker-side HMAC/signature validation
- route provider: workerd route + stable logical binding metadata

The connector contract should be portable across these hosts.

## Examples

## Example A — Google Sheets polling trigger

Connector declares:
- `kind = polling_trigger`
- `lifecycle = host_managed_poll`
- durable state schema `GoogleSheetRevisionCursor`
- dedupe identity `revision_id`
- recommended minimum interval `60s`

Host owns:
- timer/scheduling
- persisted cursor state
- activation lease
- dedupe persistence

## Example B — Slack webhook trigger (manual external)

Connector declares:
- `kind = webhook_trigger`
- `lifecycle = manual_external`
- verifier kind `hmac_sha256`
- outbound auth role for follow-up API calls
- inbound verifier role for signing secret
- route hint `slack/events`

Host owns:
- route allocation/exposure
- verifier material resolution
- inbound verification execution
- stable mapping from route to trigger instance

## Example C — HubSpot/Jira managed webhook subscription

Connector declares:
- `kind = webhook_trigger`
- `lifecycle = host_managed_webhook_subscription`
- provisioning auth role
- verifier role
- conflict policy, likely `RejectIfForeign` or `ReconcileIfStale`
- remote subscription state schema

Host owns:
- activation state store
- stable route binding
- check/create/delete orchestration
- lease/idempotency around activation

## Relationship to LAT-000028 and LAT-000029

### `LAT-000028`
Should use this contract at the design level so the connector manifest/crate
layout is not action-only.

### `LAT-000029`
Should incorporate these role and service implications for:
- provisioning auth handles,
- inbound verifier handles,
- endpoint/environment profiles,
- and binding schema that can support connector trigger activation services.

For the proposed connection-instance model and runtime trait split used to carry
those bindings, see:
- `impl-docs/spec/connector-connection-bindings.md`

This document does **not** require `LAT-000029` to implement the full trigger
activation store immediately. It does require the credential/binding model to
not paint us into a corner.

## Mistakes to avoid

1. Treating activation lifecycle as ordinary runtime flow topology.
2. Treating public route strings as the primary trigger identity.
3. Collapsing outbound auth and inbound verifier into one credential slot.
4. Pushing cursor/subscription state into ordinary flow payloads.
5. Making schedule/backoff/route fields all either hard-coded or all hint-like.
6. Binding connector crates directly to one storage backend abstraction too
   early.

## Cross references

- `impl-docs/spec/connector-crate-surface.md`
- `impl-docs/spec/connector-and-plugin-model.md`
- `impl-docs/spec/credential-provider.md`
- `impl-docs/spec/flow-ir.md`
- `impl-docs/spec/public-io-contract.md`
