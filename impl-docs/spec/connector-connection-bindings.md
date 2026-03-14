Status: Draft
Purpose: architecture-decision / spec
Owner: Runtime
Last reviewed: 2026-03-14

# Connector Connection Bindings (0.1.x)

This document defines the host-side binding model for connector credentials,
endpoint profiles, and verifier material.

It exists to turn the existing connector-role design into a concrete deployment
and runtime contract that is:
- host-owned,
- host-agnostic at the connector/runtime contract level,
- compatible with action connectors today,
- and structurally ready for polling/webhook trigger runtimes.

Related docs:
- `impl-docs/spec/credential-provider.md`
- `impl-docs/spec/connector-crate-surface.md`
- `impl-docs/spec/connector-trigger-runtime-contract.md`
- `impl-docs/spec/resource-catalog.md`
- `impl-docs/spec/connector-manifest-phase-b.md`

## Why this document is needed

The current docs already say that connector crates may require distinct roles:
- `outbound_auth`
- `provisioning_auth`
- `inbound_verifier`
- `endpoint_profile`

The current Phase-B implementation also proves that connector manifests and
runtime helpers can carry those roles structurally.

However, a critical piece is still under-specified:

> what is the host-facing deployment unit that maps those role requirements onto
> real secret stores, token refresh, verifier material, and environment
> profiles?

If we only model raw credential handles, we end up with poor ergonomics and a
weak migration story for ecosystems like n8n where one logical integration
frequently includes several credential-like concerns.

## Decision summary

The operator-facing unit should be a **connector connection instance**.

A connector connection instance is:
- a named logical integration binding,
- scoped to one connector family (or a compatible connector set),
- composed of one or more **role bindings**,
- each role binding resolving to a host-managed **handle instance**.

This yields a 3-layer model:

1. **connector crate contract**
   - declares semantic roles and expectations
2. **connection instance**
   - groups role bindings into an operator-facing logical integration
3. **handle/provider instances**
   - resolve secrets, tokens, verifier material, or endpoint config using
     host-specific providers

This keeps connector crates portable while preserving deployment ergonomics.

## Current state

### Landed today

The current codebase already has:
- connector manifests with explicit `outbound_auth` and `endpoint_profiles`
  sections, plus reserved `provisioning_auth` and `inbound_verifiers`
- a generated connector crate surface for action nodes
- docs describing trigger runtime services such as:
  - `CredentialResolver`
  - `InboundVerifierProvider`
  - `TriggerActivationStore`
  - `StableRouteProvider`
  - `TriggerScheduler`
- an out-of-band deployment binding pattern for resource providers via
  `bindings.lock.json`

### Not landed yet

The current runtime still lacks:
- a real connector credential/binding runtime service
- a real connection-instance model
- a binding schema for connector roles
- OAuth2 refresh/caching services
- service-account/JWT services
- inbound verifier providers
- endpoint profile resolution beyond a local env bridge

### Transitional implementation in Phase B

Today, `connectors-std` resolves action auth/profile data through a narrow local
bridge:
- `LATTICE_CONNECTOR_AUTH_<PROFILE>`
- `LATTICE_CONNECTOR_ENDPOINT_<PROFILE>_BASE_URL`

This should be treated as a **dev adapter**, not the final binding model.

## Terms

### Role kind
The semantic category of connector binding required by a surface.

Canonical kinds:
- `outbound_auth`
- `provisioning_auth`
- `inbound_verifier`
- `endpoint_profile`

### Role ref
A portable reference declared by a connector surface.

Example:
- `outbound_auth.github_pat`
- `inbound_verifier.slack_signing`
- `endpoint_profile.github_default`

### Handle kind
The portable semantic shape expected by a role.

Examples:
- `http.bearer`
- `raw.secret`
- `endpoint.profile`
- `oauth2.access_token`
- `verifier.hmac_sha256`

Connector crates should care about **handle kinds**, not host provider brands.

### Provider kind
A host implementation family used to realize a handle.

Examples:
- `auth.static_bearer`
- `auth.oauth2.refresh`
- `auth.service_account_jwt`
- `verifier.hmac_secret`
- `endpoint.profile.static`
- `endpoint.profile.env`

This follows the same philosophy as the provider-kind model in
`resource-catalog.md`.

### Handle instance
A named host binding that can realize one handle kind via one provider kind.

A handle instance may point to:
- secret refs,
- token refresh config,
- endpoint config,
- verifier config,
- host cache policy.

### Connection instance
A named logical connector integration that groups role bindings.

A connection instance is the operator-facing object most similar to an n8n
"credential" or "app connection", except explicitly decomposed by role.

### Binding scope
The deployment/runtime scope used to choose which connection instance a
connector node or trigger uses.

Typical inputs:
- `flow_id`
- `node_alias`
- `node_identifier`
- `connector_id`
- environment/deployment/profile

## Why the connection instance matters

A real connector integration is often not one secret.

### Example: Slack events
A useful Slack integration may require:
- bot token for ordinary API calls (`outbound_auth`)
- app/admin credential for provisioning (`provisioning_auth`)
- signing secret for inbound verification (`inbound_verifier`)

### Example: HubSpot/Jira managed webhook
A managed webhook integration may require:
- setup credential for remote registration
- verifier material for inbound signatures
- ordinary outbound auth for follow-up API calls
- environment profile for regional/self-hosted endpoints

Treating all of this as one flat credential slot loses important structure.
Treating every role as a raw secret handle loses deployment ergonomics.

A connection instance is the middle layer that preserves both structure and
operability.

## Layered model

## 1. Connector crate contract

Connector crates declare:
- connector identity
- surface identifier
- required roles
- handle-kind expectations
- verifier kind
- endpoint/default profile expectations
- lifecycle kind for triggers

This layer must remain portable and host-agnostic.

## 2. Handle instances

Handle instances live in deployment configuration and point to concrete host
providers.

Conceptual shape:

```yaml
connector_handles:
  cred.github_primary_pat:
    provider_kind: auth.static_bearer
    handle_kind: http.bearer
    connect:
      secret_ref: github_primary_pat

  cred.slack_bot_oauth:
    provider_kind: auth.oauth2.refresh
    handle_kind: oauth2.access_token
    connect:
      client_id_ref: slack_client_id
      client_secret_ref: slack_client_secret
      refresh_token_ref: slack_refresh_token
    config:
      token_url: https://slack.com/api/oauth.v2.access

  verifier.slack_marketing:
    provider_kind: verifier.hmac_secret
    handle_kind: verifier.hmac_sha256
    connect:
      secret_ref: slack_signing_secret

  endpoint.github_public:
    provider_kind: endpoint.profile.static
    handle_kind: endpoint.profile
    connect: {}
    config:
      base_url: https://api.github.com
      default_headers:
        X-GitHub-Api-Version: "2022-11-28"
```

## 3. Connection instances

Connection instances group role bindings for one logical integration.

Conceptual shape:

```yaml
connector_connections:
  github_primary:
    connector_id: connector.github.issues
    roles:
      outbound_auth.github_pat: cred.github_primary_pat
      endpoint_profile.github_default: endpoint.github_public

  slack_marketing:
    connector_id: connector.slack.events
    roles:
      outbound_auth.slack_api: cred.slack_bot_oauth
      provisioning_auth.slack_setup: cred.slack_admin_oauth
      inbound_verifier.slack_signing: verifier.slack_marketing
```

## 4. Flow/node binding selection

Deployments still need a way to decide **which connection instance** a given
node or trigger instance should use.

Recommended precedence:
1. explicit node-alias binding
2. flow-local connector default
3. deployment/environment connector default

Conceptual shape:

```yaml
connector_bindings:
  flow://example:
    defaults:
      connector.github.issues: github_primary
    nodes:
      list_open_issues: github_primary
      list_archived_issues: github_archive
```

This keeps raw secret handles out of flow payloads while still supporting
multiple accounts for the same connector family.

## Why node alias matters

For imported n8n-style flows, different nodes in the same flow may use different
connections of the same connector family.

A node-alias binding path is therefore important for migration fidelity.

## Relationship to trigger identity

For polling and webhook triggers, the selected connection instance should become
part of the host-side `binding_scope` used in `TriggerInstanceKey`.

That ensures durable runtime state is scoped correctly across:
- multiple accounts,
- multiple tenants,
- multiple deployments sharing one connector family.

## Recommended runtime traits

The current trigger-runtime doc shows a conceptual `CredentialResolver`. That is
useful, but implementation should likely split runtime services by role shape.

### 1. Binding selection

```rust
struct ConnectorBindingScope {
    flow_id: String,
    node_alias: String,
    node_identifier: String,
    connector_id: &'static str,
    surface_identifier: &'static str,
}
```

```rust
struct ConnectorRoleRef {
    connector_id: &'static str,
    surface_identifier: &'static str,
    role_kind: ConnectorRoleKind,
    role_name: &'static str,
    expected_handle_kind: &'static str,
}
```

```rust
trait ConnectorBindingResolver {
    async fn resolve_connection(
        &self,
        scope: &ConnectorBindingScope,
    ) -> anyhow::Result<ResolvedConnectionRef>;

    async fn resolve_handle(
        &self,
        connection: &ResolvedConnectionRef,
        role: &ConnectorRoleRef,
    ) -> anyhow::Result<ResolvedHandleRef>;
}
```

### 2. Outbound/provisioning auth application

```rust
trait OutboundAuthProvider {
    async fn apply(
        &self,
        handle: &ResolvedHandleRef,
        role: &ConnectorRoleRef,
        request: &mut capabilities::http::HttpRequest,
    ) -> anyhow::Result<()>;
}
```

`provisioning_auth` can reuse the same trait because it is still outbound
request auth, just used during activation/setup.

### 3. Endpoint profile resolution

```rust
struct ResolvedEndpointProfile {
    base_url: String,
    default_headers: Vec<(String, String)>,
}
```

```rust
trait EndpointProfileResolver {
    async fn resolve(
        &self,
        handle: &ResolvedHandleRef,
        role: &ConnectorRoleRef,
    ) -> anyhow::Result<ResolvedEndpointProfile>;
}
```

### 4. Inbound verifier runtime

```rust
trait InboundVerifierProvider {
    async fn verify(
        &self,
        handle: &ResolvedHandleRef,
        role: &ConnectorRoleRef,
        verifier: &VerifierDescriptor,
        req: &InboundRequest,
    ) -> anyhow::Result<VerificationOutcome>;
}
```

### 5. Optional internal host services

These do not need to be connector-facing, but hosts will likely want internal
helpers such as:
- `OAuthTokenManager`
- `ServiceAccountTokenManager`
- `SecretMaterialStore`
- `ConnectorBindingsStore`

Generated connector crates should not depend directly on those internals.

## Why typed services are preferred to one raw material blob

A single generic `ResolvedCredentialMaterial` blob is workable, but it tends to
collapse too many concerns together.

Typed services are better because they let hosts:
- apply bearer tokens without exposing raw secrets broadly
- sign requests without exposing private signing keys
- verify webhook requests without handing signing secrets to arbitrary connector
  code
- enforce policy at role boundaries

A host may still implement these services on top of one generic secret/token
store internally.

## Runtime integration point

The nearest existing runtime seam is `ResourceAccess`.

### Recommended extension

Add an optional connector-runtime accessor to `ResourceAccess`, for example:

```rust
trait ResourceAccess {
    fn connector_runtime(&self) -> Option<&dyn ConnectorRuntime>;
}
```

Where `ConnectorRuntime` is a thin umbrella exposing the typed services above.

### Important runtime requirement

Connector binding selection needs access to the current node scope. The current
`NodeContext` / task-local resource scoping does not expose node alias directly
through `ResourceAccess`.

Therefore hosts/kernel-exec should add a per-node wrapper that carries:
- `flow_id`
- `node_alias`
- `node_identifier`

This wrapper should be host/runtime supplied, not author supplied.

## Deployment binding schema

### Recommendation

Extend the existing `bindings.lock.json` family rather than inventing an
entirely separate deployment concept.

### Proposed new top-level sections

```json
{
  "version": 1,
  "generated_at": "...",
  "content_hash": "sha256:...",
  "instances": { ... },
  "flows": { ... },
  "connector_handles": { ... },
  "connector_connections": { ... },
  "connector_bindings": { ... }
}
```

This preserves one generated deployment artifact model while keeping connector
bindings structurally distinct from `resource::*` capability wiring.

### `connector_handles`

Suggested shape:

```json
"connector_handles": {
  "cred.github_primary_pat": {
    "provider_kind": "auth.static_bearer",
    "handle_kind": "http.bearer",
    "connect": { "secret_ref": "github_primary_pat" },
    "config": {},
    "grants": {}
  }
}
```

### `connector_connections`

Suggested shape:

```json
"connector_connections": {
  "github_primary": {
    "connector_id": "connector.github.issues",
    "roles": {
      "outbound_auth.github_pat": "cred.github_primary_pat",
      "endpoint_profile.github_default": "endpoint.github_public"
    }
  }
}
```

### `connector_bindings`

Suggested shape:

```json
"connector_bindings": {
  "<flow_id>": {
    "defaults": {
      "connector.github.issues": "github_primary"
    },
    "nodes": {
      "list_open_issues": "github_primary"
    }
  }
}
```

## Validation rules

## Existing diagnostics likely reusable

### `DAG330`
Use for missing connector binding/credentials for connector usage.

### `DAG331`
Use when granted scopes fall outside declared policy.

### `SECR201`
Use when a binding grants broader scopes than requested.

## Additional validation that should be added

Recommended new diagnostic families (names illustrative until registered):
- missing required role binding in a connection instance
- handle kind mismatch for a role expectation
- connection bound to incompatible connector family
- ambiguous connector binding resolution with no winner
- webhook trigger missing verifier binding
- managed webhook trigger missing provisioning auth binding
- endpoint profile missing where required by selected surface/deployment

## Concrete validation checks

1. Every connector node/trigger that declares a role must resolve to a
   connection instance at deployment validation time.
2. Every required role in that connection must map to a handle instance.
3. The handle instance must satisfy the role's expected handle kind.
4. The connection instance must be compatible with the connector family.
5. If scopes/policies are declared, granted scopes must be a subset of policy.
6. Webhook triggers requiring verifier material must not be deployable without an
   inbound verifier binding.
7. Managed webhook subscription triggers must not be deployable without
   provisioning auth.

## Native host implementation sketch

Possible first host implementations:
- `EnvConnectorRuntime`
  - wraps the current env bridge as a dev adapter
- `StaticSecretProvider`
  - resolves `secret_ref` names from env/Vault/OS store
- `OAuth2RefreshProvider`
  - caches access tokens in memory or SQLite/Postgres/filesystem
- `ServiceAccountJwtProvider`
  - signs JWT and exchanges tokens on demand
- `StaticEndpointProfileProvider`
  - resolves configured base URL/default headers
- `HmacVerifierProvider`
  - verifies inbound signatures in process

## Workers host implementation sketch

Possible Workers-side implementations:
- static secrets from `wrangler secret` / env bindings
- token cache in Durable Object or D1
- endpoint profiles from env/bindings/lock config
- verifier execution inside the Worker runtime
- provisioning auth using the same outbound auth provider path

The connector/runtime contract remains the same across hosts. Only provider
implementations differ.

## Relationship to n8n migration

This model improves the n8n ingestion story in several ways.

### 1. It matches the operator mental model
Many n8n "credentials" are really logical connector connections. This model
makes that explicit.

### 2. It preserves multi-account use within one flow
Node-alias connection bindings allow two nodes of the same connector family to
use different accounts cleanly.

### 3. It keeps secrets out of imported flow definitions
Imported flows can preserve logical connection references while hosts own the
actual secret material.

### 4. It leaves room for trigger lifecycle reality
Webhook and polling runtimes can share the same connection-instance and
role-resolution substrate instead of inventing separate credential paths.

## What this document does not require immediately

This document does **not** require:
- full webhook activation store implementation
- scheduler implementation
- route allocation implementation
- OAuth2 browser login UX
- a full secrets-management product

It does require the credential/binding model to be compatible with those future
runtime services.

## Recommended implementation sequence

1. Land runtime traits + dev adapter replacing the direct env bridge in
   `connectors-std`
2. Add connector sections to the deployment lock/config model
3. Add native static secret + endpoint providers
4. Add OAuth2 refresh + service-account providers
5. Integrate inbound verifier providers
6. Reuse the same substrate for polling/webhook activation services

## Cross references

- `impl-docs/spec/credential-provider.md`
- `impl-docs/spec/connector-crate-surface.md`
- `impl-docs/spec/connector-trigger-runtime-contract.md`
- `impl-docs/spec/resource-catalog.md`
- `impl-docs/spec/connector-manifest-phase-b.md`
