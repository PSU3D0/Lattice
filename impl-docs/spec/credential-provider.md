Status: Draft
Purpose: spec
Owner: Core
Last reviewed: 2026-03-14

# Credential Provider (0.1.x)

This document defines how connectors obtain credentials without embedding secrets in flows. Credential
resolution is a **host service** and is configured in the binding layer.

## Goals

- Keep secrets out of Flow IR and code.
- Support OAuth2, service accounts, and API tokens.
- Provide a uniform interface for connectors.

## Model

Flows reference **credential handles** in binding configuration. The host resolves these handles to
credential material at runtime.

Connector crates may require different credential roles, for example:
- **outbound auth** for normal API requests,
- **provisioning auth** for activation-time webhook/subscription setup,
- **inbound verifier** material for authenticating webhook requests,
- **endpoint/environment profile** selection.

These roles should not be collapsed into a single flat secret slot.
See also:
- `impl-docs/spec/connector-crate-surface.md`
- `impl-docs/spec/connector-trigger-runtime-contract.md`
- `impl-docs/spec/connector-connection-bindings.md`

Example binding (conceptual):

```yaml
bindings:
  google_sales:
    kind: google.oauth2
    client_id_secret: gcp_client_id
    client_secret_secret: gcp_client_secret
    refresh_token_secret: gcp_refresh_token
```

Nodes reference the handle (e.g., `credential = "google_sales"`). The handle is not stored in Flow IR
unless explicitly configured by the host.

Connector-trigger role example (conceptual):

```yaml
bindings:
  slack_outbound:
    kind: slack.api.bearer
    token_secret: slack_bot_token

  slack_inbound_verifier:
    kind: hmac.secret
    secret_ref: slack_signing_secret

  hubspot_provisioning:
    kind: hubspot.developer.oauth2
    client_id_secret: hubspot_client_id
    client_secret_secret: hubspot_client_secret
    refresh_token_secret: hubspot_refresh_token

  microsoft_graph_us_gov:
    kind: endpoint.profile
    graph_api_base_url: https://graph.microsoft.us
```

A connector trigger or action may reference one or more of these roles by
handle. The host is responsible for mapping those handles into the appropriate
runtime services.

## Credential Types

### OAuth2

- Inputs: client ID/secret, refresh token, scopes.
- Host manages token refresh and caching.
- Tokens are injected per request and never stored in Flow IR.

### Service Account (JWT)

- Inputs: client email, private key, scopes.
- Host signs JWT and exchanges for access token.

### API Token / Static Secret

- Inputs: token or key
- Injected directly into headers or request signing.

### Inbound verifier secret

- Inputs: signing secret, shared query secret, verifier key material, or
  service-specific verification configuration.
- Used to authenticate incoming webhook requests.
- Distinct from outbound request auth even when both happen to originate from
  the same external service.

For the concrete connection-instance model, runtime trait split, and deployment
binding extension proposed for `LAT-000029`, see:
- `impl-docs/spec/connector-connection-bindings.md`

## Runtime Interface (Conceptual)

```
CredentialProvider::resolve(handle) -> CredentialMaterial
```

For connector triggers and richer connector families, hosts may resolve several
role-specific handles rather than a single credential slot, for example:

```
resolve(outbound_auth_handle)
resolve(provisioning_auth_handle)
resolve(inbound_verifier_handle)
resolve(endpoint_profile_handle)
```

`CredentialMaterial` is opaque to flows but may include:
- Access token
- Expiry timestamp
- Additional headers
- Verifier secret material
- Endpoint/profile metadata

## Security and Storage

- Secrets are stored in host-managed stores (Vault, KMS, env secrets).
- Workers: secrets via `wrangler secret` or external secret store; tokens cached in DO.
- Native: secrets via Vault/OS store; tokens cached in memory or DB.

## Validation

- Nodes declare required credential type (e.g., `google.oauth2`).
- Validator checks binding availability in deployment configuration.
- Missing or mismatched credentials are deployment-time errors.
