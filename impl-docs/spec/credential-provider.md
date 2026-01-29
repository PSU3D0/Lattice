Status: Draft
Purpose: spec
Owner: Core
Last reviewed: 2026-01-29

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

## Runtime Interface (Conceptual)

```
CredentialProvider::resolve(handle) -> CredentialMaterial
```

`CredentialMaterial` is opaque to flows but may include:
- Access token
- Expiry timestamp
- Additional headers

## Security and Storage

- Secrets are stored in host-managed stores (Vault, KMS, env secrets).
- Workers: secrets via `wrangler secret` or external secret store; tokens cached in DO.
- Native: secrets via Vault/OS store; tokens cached in memory or DB.

## Validation

- Nodes declare required credential type (e.g., `google.oauth2`).
- Validator checks binding availability in deployment configuration.
- Missing or mismatched credentials are deployment-time errors.
