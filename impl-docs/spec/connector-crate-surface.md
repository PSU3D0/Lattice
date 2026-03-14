Status: Draft
Purpose: architecture-decision / spec
Owner: Runtime
Last reviewed: 2026-03-13

# Connector Crate Surface (0.1.x)

This document defines the intended surface of **connector crates** in Lattice.

For the host/runtime contract that owns polling state, route exposure,
activation state, verifier execution, and reconciliation mechanics, see:
- `impl-docs/spec/connector-trigger-runtime-contract.md`

It answers a design question that becomes unavoidable once we move from stdlib
primitives toward real ecosystem farming:

- is a connector crate only a source of action nodes, or
- must it also model triggers, webhook lifecycle, verifier/auth requirements,
  and activation-time setup concerns?

## Decision summary

Yes: **trigger/webhook/auth shapes must be designed explicitly now**, even if the
first implementation pass only generates and runs **action nodes**.

We should **not** ship an action-only connector model that has no place to
express:
- polling triggers,
- webhook triggers,
- provisioning/setup credentials,
- inbound verifier secrets,
- external subscription lifecycle,
- or trigger activation state.

However, we also should **not** block the first connector-codegen landing on a
fully generalized trigger runtime.

Therefore the connector model should be:
- **surface-complete at the spec level**, and
- **incremental at the implementation level**.

## Why this decision exists

From a pure Lattice perspective, a trigger is still just:
- a `NodeKind::Trigger` node, plus
- an explicit bundle entrypoint.

That remains true.

But connector triggers add requirements that plain core triggers do not:
- a polling trigger needs persistent cursor state and scheduling policy,
- a webhook trigger needs route/lifecycle/provisioning semantics,
- many webhook triggers require inbound verification material,
- some trigger activation flows need outbound auth distinct from steady-state
  execution,
- some services only allow one remote webhook per app/account and require
  activation-time reconciliation.

If these concerns are not modeled early, action-node codegen will accidentally
bake in the wrong abstraction boundary.

## Primary design rule

A **connector crate** is a package of **semantic node families** built on top of
capabilities and host credential services.

A connector crate may expose:
- **action nodes**,
- **polling triggers**,
- **webhook triggers**,
- and shared typed/auth/transport/lifecycle helpers.

It should not be reduced to:
- a bag of raw HTTP endpoint wrappers, or
- a graph-level representation of every internal capability step.

This is the connector-specific application of:
- `impl-docs/spec/node-vs-capability-surface.md`

## Connector family decomposition

A connector family should be decomposed into the following conceptual surfaces.

### 1. Action node

A semantic external operation that runs in response to upstream flow input.

Examples:
- `connector.github.issues.get`
- `connector.github.issues.list`
- `connector.slack.message.post`
- `connector.highlevel.contact.create`

Action nodes usually need:
- typed input/output,
- outbound auth profile,
- transport mapping,
- optional pagination or upload/download helpers,
- truthful effects/determinism/resource declarations.

### 2. Polling trigger

A trigger whose ingress is produced by periodic external reads.

Examples:
- watch a Google Sheet for row changes,
- poll a CRM for recently updated records,
- poll a queue-ish SaaS endpoint for new events.

Polling triggers need more than a normal trigger node:
- cursor/checkpoint state shape,
- minimum polling interval guidance,
- dedupe/"already seen" semantics,
- scheduling/activation hooks,
- often a distinction between preview/manual test behavior and deployed
  activation behavior.

### 3. Webhook trigger

A trigger whose ingress is delivered to a host-exposed route.

Examples:
- Slack events webhook,
- Stripe event webhook,
- HubSpot/Jira app callback webhook.

Webhook triggers need:
- route shape,
- inbound payload schema,
- verification/authentication semantics,
- optional remote subscription setup/teardown,
- activation-time state and conflict handling,
- sometimes separate setup and default endpoints.

### 4. Outbound auth profile

Credential semantics used for normal outbound API calls.

Examples:
- API key header,
- bearer token,
- OAuth2 access token,
- service-account/JWT,
- signed request.

This aligns with:
- `impl-docs/spec/credential-provider.md`

### 5. Provisioning/setup auth profile

Credential semantics used specifically during activation-time setup,
registration, or remote reconciliation.

Sometimes this is the same as outbound auth.
Sometimes it is different.

Examples:
- HubSpot app developer credentials for webhook subscription setup,
- Jira cloud/server credentials for webhook registration,
- app-level token for event-subscription provisioning.

### 6. Inbound verifier profile

Secret material or verification algorithm used to authenticate inbound webhook
requests.

Examples:
- HMAC signing secret,
- shared query secret,
- service-specific signature validation rules,
- timestamp + signature freshness requirements.

This is **not the same thing** as outbound request auth and should not be forced
into the same slot.

### 7. Endpoint/environment profile

Connector surface often needs environment/tenant/server selection.

Examples:
- sandbox vs production,
- regional Graph API base URL,
- self-hosted domain,
- per-tenant account location.

This belongs in connector configuration/auth profile metadata rather than in the
Flow IR itself.

## Relationship to Flow IR trigger/entrypoint semantics

This design does **not** change the core Lattice rule from `flow-ir.md`:
- triggers are nodes,
- entrypoints are explicit bundle metadata,
- hosts own ingress routing.

Connector trigger modeling adds another layer **outside** raw Flow IR:
- deployment/activation requirements,
- required credential roles,
- polling state requirements,
- webhook verifier requirements,
- remote registration lifecycle.

So the decomposition is:

1. **Flow IR / bundle**
   - trigger node
   - entrypoint binding
2. **connector crate surface**
   - what kind of trigger this is
   - what activation/runtime services it needs
3. **host deployment/binding layer**
   - actual credential handles
   - public route exposure
   - trigger scheduler / state store / verifier implementation

## Credential role decomposition

Connector crates should model credential requirements by **role**, not by a
single flat "credentials" field.

Recommended conceptual roles:
- `outbound_auth`
- `provisioning_auth`
- `inbound_verifier`
- `endpoint_profile`

A connector family may use one or many of these roles.

### Example: Slack events
- `outbound_auth`: bearer access token for Slack API calls
- `inbound_verifier`: signing secret for webhook signature verification
- `provisioning_auth`: often same as outbound, but may be modeled separately if
  activation flows diverge

### Example: HubSpot app webhook
- `provisioning_auth`: app/developer credential used for remote subscription
  management
- `inbound_verifier`: secret used to verify `x-hubspot-signature`

### Example: Google Sheets poller
- `outbound_auth`: OAuth2 or service account
- `endpoint_profile`: optional environment/region/base-url specifics
- no inbound verifier role

## Trigger-specific lifecycle model

Connector triggers should be able to declare whether they need activation-time
lifecycle behavior.

Connector crates declare the lifecycle **kind** and semantic expectations.
Hosts own the stateful implementation details behind that lifecycle.

Recommended conceptual lifecycle kinds:
- `none`
  - no remote setup required
- `host_managed_poll`
  - host scheduler invokes poll routine with connector-owned cursor state
- `host_managed_webhook`
  - host route exists, but no remote subscription API is needed
- `host_managed_webhook_subscription`
  - host must create/check/delete remote subscriptions during activation
- `manual_external`
  - host exposes route, but user/service must configure the external webhook
    manually

These lifecycle declarations are about **activation**, not graph topology.
They should not be turned into ordinary runtime flow nodes.

## State model for triggers

### Polling trigger state

Polling triggers usually require durable cursor state such as:
- last revision ID,
- last seen timestamp,
- page cursor,
- dedupe marker,
- service-specific sync token.

This state is **activation/runtime host state**, not flow payload state.
It should be modeled separately from normal invocation payloads.

### Webhook activation state

Webhook triggers may require persisted activation state such as:
- remote webhook ID,
- subscription list/version,
- verifier secret alias,
- negotiated route metadata,
- conflict/reconciliation markers.

Again: this is host/activation state, not user-authored flow state.

## Recommended connector manifest decomposition

A connector manifest should eventually be able to describe at least:
- connector identity
- action nodes
- polling triggers
- webhook triggers
- credential roles
- lifecycle kind
- endpoint/environment profiles
- pagination strategy
- effect/determinism/resource requirements

### Conceptual action example

```yaml
connector:
  id: connector.github
  version: 0.1.0

profiles:
  outbound_auth:
    github_pat:
      kind: bearer
      provider: http.bearer
  endpoint_profile:
    github_default:
      base_url: https://api.github.com

nodes:
  - kind: action
    identifier: connector.github.issues.list
    family: github.issues
    input: GithubIssuesListInput
    output: GithubIssuesListOutput
    auth: github_pat
    endpoint: github_default
    effects: ReadOnly
    determinism: BestEffort
    resources:
      - http_read(capabilities::http::HttpRead)
    pagination:
      kind: link_header_next
```

### Conceptual polling trigger example

```yaml
connector:
  id: connector.google.sheets
  version: 0.1.0

profiles:
  outbound_auth:
    google_oauth:
      kind: oauth2
      provider: google.oauth2

nodes:
  - kind: polling_trigger
    identifier: connector.google.sheets.rows_watch
    family: google.sheets
    output: GoogleSheetRowChanged
    auth: google_oauth
    lifecycle: host_managed_poll
    poll:
      cursor: GoogleSheetRevisionCursor
      minimum_interval: 60s
      dedupe_key: revision_id
    effects: ReadOnly
    determinism: BestEffort
```

### Conceptual webhook trigger example

```yaml
connector:
  id: connector.slack
  version: 0.1.0

profiles:
  outbound_auth:
    slack_api:
      kind: bearer
      provider: slack.api
  inbound_verifier:
    slack_signing:
      kind: hmac_sha256
      provider: raw.secret

nodes:
  - kind: webhook_trigger
    identifier: connector.slack.events
    family: slack.events
    output: SlackEventEnvelope
    provisioning_auth: slack_api
    verifier: slack_signing
    lifecycle: manual_external
    webhook:
      method: POST
      route_hint: slack/events
      response_mode: on_received
    effects: ReadOnly
    determinism: BestEffort
```

These examples are **conceptual**. They describe the intended surface, not a
final serialized schema.

## Codegen implications

Codegen should not assume every connector family is action-only.

Even if Phase 1 only emits action-node crates, the generated crate layout should
leave room for:
- `actions/`
- `triggers/polling/`
- `triggers/webhook/`
- `auth/`
- `transport/`
- `lifecycle/`

Recommended split:
- generated:
  - typed IO
  - node specs/register helpers
  - simple transport mapping
  - simple auth profile references
  - simple pagination scaffolds
- handwritten/extension hooks:
  - complex auth bootstrap
  - request signing
  - webhook verification
  - remote registration lifecycle
  - advanced polling diff logic
  - binary/multipart edge cases

## Phase guidance for LAT-000028

### Phase 0 / design
The design for actions, polling triggers, webhook triggers, and credential roles
should be made explicit now.

### Phase 1 / implementation
The first runnable implementation may target:
- semantic **action nodes only**,
- simple auth profile references,
- simple pagination,
- one generated example connector crate.

### Phase 2+
Later work can add:
- polling trigger runtime substrate,
- webhook trigger lifecycle substrate,
- verifier profile runtime support,
- richer auth/provider integration,
- plugin-inner execution for complex connectors.

## Mistakes to avoid

1. **Action-only schema dead-end**
   - do not make a manifest that cannot represent triggers later.

2. **Flattened credentials field**
   - outbound auth, provisioning auth, and inbound verifier are different roles.

3. **Treating trigger setup as a runtime node**
   - activation lifecycle belongs to host/runtime activation, not ordinary flow
     execution topology.

4. **Coupling verifier secrets to Flow IR payloads**
   - verifier secrets should be deployment/binding concerns.

5. **Overfitting to n8n UI property DSL**
   - Lattice should model semantic contracts, not recreate all n8n editor UX.

6. **Declarative-only optimism**
   - many real connectors will still need handwritten helpers.

## Cross references

- `impl-docs/spec/connector-and-plugin-model.md`
- `impl-docs/spec/credential-provider.md`
- `impl-docs/spec/node-vs-capability-surface.md`
- `impl-docs/spec/flow-ir.md`
- `impl-docs/spec/public-io-contract.md`
- `impl-docs/spec/subflows.md`
