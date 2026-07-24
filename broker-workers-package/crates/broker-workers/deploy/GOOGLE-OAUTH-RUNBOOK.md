# Google OAuth production readiness

This runbook is intentionally offline until the final operator-gated proof. Do not place credentials, test-user addresses, provider responses, or account identifiers in repository or evidence files.

Use `deploy/scripts/c5-orchestrate.mjs` for the fail-closed top-level reducer that owns the private generic auth-driver, both private Google egress Workers, and both broker Workers. Both it and `deploy/scripts/deploy.mjs` require `--workers-subdomain <account-subdomain>`, `--public-callback-base <exact-origin>`, and `--secrets-file <absolute-path>`; the provider sub-deployer receives the same secrets file. Dry-run executes no remote command or API request. Apply requires all explicit approvals, fetches the account's live Workers subdomain from Cloudflare before any mutation, and fails closed unless it byte-for-byte equals `--workers-subdomain`. Apply classifies each `wrangler deployments list` result as absent, present, or unknown: only an explicit script-not-found response or an empty array proves absence, while malformed output and every other API error abort before mutation. A present target aborts unless an explicit owned-update flow verifies its exact ownership manifest. Cleanup must use `deploy/scripts/c5-cleanup.mjs --ownership-state <absolute c5-ownership.json>`; it verifies all five live deployments before deleting any and preserves non-owned D1.

## 1. Fix the callback

Supply the Cloudflare account's lowercase Workers subdomain label with `--workers-subdomain`; for example, an account subdomain of `account-label` is one label, not a dotted hostname. Choose the public broker origin as `PUBLIC_CALLBACK_BASE` in exactly this shape:

```text
https://<prefix>-broker-public.<account-subdomain>.workers.dev
```

It must be an origin only: no path, query, fragment, credentials, port, or trailing slash. The old bare `https://<prefix>-broker-public.workers.dev` form is invalid. The sole redirect URI is:

```text
https://<prefix>-broker-public.<account-subdomain>.workers.dev/v0.2/credential-callback
```

The deployment renderer derives this single value and threads it to token egress as `GOOGLE_OAUTH_REDIRECT_URI`; it must not accept an independent callback URI. The plan, ownership, qualification, and readiness evidence report the same value. Register exactly that byte string in Google Cloud. Before apply performs any deployment, secret update, or migration, it verifies the supplied label against `GET /accounts/{account_id}/workers/subdomain` using `CLOUDFLARE_API_TOKEN`; mismatch, absence, or API error aborts apply. Dry-run performs no API request. There are no provider-named callback routes.

## 2. Register Google OAuth

1. Configure an External consent screen in Testing status.
2. Add only the operator-selected exact test account(s). Never record those addresses in this repository or sanitized evidence.
3. Create an OAuth 2.0 client of type **Web application**.
4. Configure the one exact authorized redirect URI derived in step 1.
5. Request exactly `https://www.googleapis.com/auth/gmail.send` and `https://www.googleapis.com/auth/spreadsheets`; reject every additional scope.
6. Install the client ID and client secret as `GOOGLE_OAUTH_CLIENT_ID` and `GOOGLE_OAUTH_CLIENT_SECRET` secrets on the private token egress Worker. The broker never receives the client secret.

The token egress Worker calls the pinned Google `tokeninfo` endpoint, validates its opaque `user_id` and exact two-scope result, and returns only the subject to broker custody for a scoped commitment. Email, name, avatar, and raw subject are not control-plane fields or evidence. Revocation uses only the fixed token-egress `/revoke` route backed by `https://oauth2.googleapis.com/revoke`.

## 3. Sign operator artifacts offline

Prepare an operator-owned `operator-artifact-input.json` outside the repository. It names `key_id`, `not_before`, `expires_at`, the pinned `activation_recipient` (`key_id`, raw X25519 `public_key_b64u`, and exact HPKE suite), and the six required artifact groups: `deployment_standing_authority`, `deployment_contract_set`, `registry_definitions`, `registry_decisions`, `historical_inventory`, and `historical_key_evidence`. Each entry names its C1 `schema` and unsigned `value`; historical archives include unsigned validity and revocation evidence. Do not use generated defaults.

Keep the Ed25519 seed or PKCS8 PEM in a mode-0600 operator file, or pipe it on standard input. Never pass key bytes as an argument:

```bash
node deploy/scripts/operator-artifacts.mjs build \
  --config /operator/private/operator-artifact-input.json \
  --key-file /operator/private/deployment-authority.pem \
  --output /operator/evidence/operator-artifact-bundle.json

node deploy/scripts/operator-artifacts.mjs verify \
  --bundle /operator/evidence/operator-artifact-bundle.json \
  --trust-root /operator/evidence/operator-trust-root.json
```

`operator-trust-root.json` contains only `key_id` and `public_key_b64u`. Record the emitted `bundle_hash`; install the byte-exact canonical bundle, hash, key ID, and public key as `OPERATOR_ARTIFACT_BUNDLE_JCS`, `OPERATOR_ARTIFACT_BUNDLE_SHA256`, `OPERATOR_BUNDLE_KEY_ID`, and `OPERATOR_BUNDLE_PUBLIC_KEY_B64U`. Deployment re-verifies every signature, nested historical evidence, hash, validity window, revocation state, and exact installed standing authority/contract set before its first remote command. `/ready` runs the same Rust verification rules over installed bytes and the pinned trust root.

Generate the generic-activation X25519 recipient key offline. Install only its pinned raw public key and key ID as `GENERIC_ACTIVATION_RECIPIENT_PUBLIC_KEY_B64U` and `GENERIC_ACTIVATION_RECIPIENT_KEY_ID`; install the raw private key only through `wrangler secret put GENERIC_ACTIVATION_RECIPIENT_PRIVATE_KEY_B64U`. Submission clients use the public key returned in `NextAction` and the declared C1 HPKE suite/AAD fields. Never record the private key or activation plaintext in artifacts or logs.

## 4. Install Worker secrets

Qualify secret names, never values, against `google-oauth-readiness.json`. Supply private-Worker secret material through `--secrets-file <absolute-path>`. The file must be outside the repository, be a regular mode-0600 JSON file, and have exactly this structure (placeholders denote operator values):

```json
{
  "auth-driver": {
    "AUTH_DRIVER_SERVICE_AUTH": "<operator-value>"
  },
  "google-token-egress": {
    "GOOGLE_EGRESS_SERVICE_AUTH": "<shared-operator-value>",
    "GOOGLE_OAUTH_CLIENT_ID": "<operator-value>",
    "GOOGLE_OAUTH_CLIENT_SECRET": "<operator-value>",
    "GOOGLE_TOKEN_RESULT_KEY": "<64-lowercase-hex>"
  },
  "google-provider-egress": {
    "GOOGLE_EGRESS_SERVICE_AUTH": "<same-shared-operator-value>"
  },
  "broker-private": {
    "ACTIVATION_SERVICE_AUTH": "<operator-value>",
    "AI_GATEWAY_AUTHORIZATION": "<operator-value>",
    "AUTH_DRIVER_SERVICE_AUTH": "<same-auth-driver-value>",
    "BINDING_SIGNING_SEED": "<64-lowercase-hex>",
    "COMMITMENT_KEY": "<64-lowercase-hex>",
    "CUSTODY_ROOT_KEY": "<64-lowercase-hex>",
    "DEPLOYMENT_BOOTSTRAP_AUTH": "<operator-value>",
    "GENERIC_ACTIVATION_RECIPIENT_PRIVATE_KEY_B64U": "<32-byte-canonical-unpadded-base64url>",
    "GOOGLE_EGRESS_SERVICE_AUTH": "<same-shared-operator-value>",
    "INVOKE_SERVICE_AUTH": "<operator-value>",
    "KEY_HASH_PEPPER": "<operator-value>",
    "RECEIPT_SIGNING_SEED": "<64-lowercase-hex>"
  },
  "broker-public": {}
}
```

All five groups must be present even though `broker-public` has no secrets. Missing groups/names, extra groups/names, empty values, mismatched shared auth values, malformed 32-byte lowercase-hex keys, a malformed X25519 private key, or a malformed `GOOGLE_TOKEN_RESULT_KEY` abort locally before any remote mutation. `AUTH_DRIVER_SERVICE_AUTH` must match between auth-driver and broker-private; `GOOGLE_EGRESS_SERVICE_AUTH` must match across both egress Workers and broker-private. `GOOGLE_OAUTH_REDIRECT_URI` is deliberately absent from this file: deployment derives it from the verified callback origin and installs it on token egress. Every value is piped to `wrangler secret put` over standard input; values never appear in argv, logs, plans, ownership manifests, or evidence. Evidence records only secret names and `planned` or `installed_and_verified` status.

Generic non-OAuth activation is available only on the private broker service. Install `ACTIVATION_SERVICE_AUTH` and `AUTH_DRIVER_SERVICE_AUTH`, pin `GENERIC_PROFILE_AUTHORITY_PUBLIC_KEY_B64U`, `GENERIC_ACTIVATION_RECIPIENT_KEY_ID`, and `GENERIC_ACTIVATION_RECIPIENT_PUBLIC_KEY_B64U`, install `GENERIC_ACTIVATION_RECIPIENT_PRIVATE_KEY_B64U` only as a secret, and bind the immutable registry-approved `AUTH_DRIVER_SERVICE`. Static-secret, workload-exchange, and external-custodian submission routes never pass through the public facade.

The flow Worker receives `LATTICE_BROKER_DEPLOYMENT_KEY`, `LATTICE_BROKER_POP_SEED_B64U`, and `LATTICE_BROKER_SERVICE_AUTH` as secrets plus an operator-approved immutable receipt public key and hash. It must not contain Google access/refresh tokens or the OAuth client secret. AI Gateway remains host configuration with payload logging disabled and explicit spend/rate policy. The complete variables, secrets, bindings, and order are enumerated in `google-oauth-readiness.json`.

## 5. Offline qualification

Run without provider or deployment access:

```bash
cd crates/provider-google-workers
npm ci
npm test
npm run dry-run

cd ../broker-workers/workerd-tests
npm ci
npm test
npm run package
npm run package:verify
npm run package:test
npm run deploy:test
```

Also run focused Rust tests, formatting, the hint gate, normative vectors, package hash parity, secret scanning, and the S21 Worker qualification. Mock upstream service bindings are the only Google network used by tests.

## 6. Deploy and cut over

For every Worker role, use the strict sequence: prove the target absent, deploy it, install every required secret through stdin, list and verify the required secret names, then capture exactly one deployment ID and source hash. Apply it in this order: auth-driver; token egress (including the derived `GOOGLE_OAUTH_REDIRECT_URI`); provider egress; broker-private; and finally broker-public, whose required secret map is empty. The first four Workers have `workers_dev: false` and no routes, so none is publicly reachable while secrets are being installed. If secret installation, name verification, or deployment capture fails before migration, delete that just-created Worker and abort; broader rollback may delete only other Workers created by this run. The broker does not render or deploy a service binding until the corresponding dependency has completed secret verification and immutable capture.

After all three private dependencies are verified, deploy broker-private, install and verify its 12 secrets, and capture it before applying D1 migrations through `0003_production_v2_cutover.sql`. Only after migration succeeds may broker-public be re-qualified, deployed, captured, and exposed for readiness checks. A migration attempt is irreversible: on any failure after the attempt begins, preserve D1, broker-private, and egress and emit `forward_fix_required`; creation-scoped rollback deletes only a broker-public Worker created by this run. Reconcile each connection forward through material seal/readback, signed registry/profile verification, V2 binding verification, authoritative fence switch, legacy ciphertext destruction, and completion. Every phase appends a cutover event. After a V2 lease or rotation, only a V2/fence-aware forward fix is permitted; never roll back to executable V1 admission.

After migrations succeed, deploy broker public, then the S21 flow Worker. Qualify all required secrets by name, exact service bindings, Durable Object ownership, D1 identity, callback route, source hashes, and AI Gateway policy. V1 receipt material remains verification-only under archived signed keys; V1 grant, binding, and invoke admission stays disabled.

## 7. User-gated proof and cleanup

Only the user may authorize the first live proof. Use the exact consent-screen test account, verify account commitment behavior, then execute the LLM to Sheets to Gmail flow once. Evidence may contain artifact hashes, receipt references, counts, status codes, and ownership IDs; it must exclude credentials, OAuth codes, tokens, provider bodies, document content, raw account subject, and email.

If the proof is disposable, cleanup is ownership-safe and hash-pinned. Remove public route, flow Worker, broker Workers, both egress Workers/DOs, D1, AI Gateway, and secret names only when the recorded ownership manifest and live deployment/source hashes match. Preserve only sanitized receipt/history verification evidence and ciphertext-destruction confirmations.
