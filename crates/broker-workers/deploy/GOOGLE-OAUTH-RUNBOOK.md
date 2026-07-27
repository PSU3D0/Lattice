# Google OAuth production readiness

This runbook is intentionally offline until the final operator-gated proof. Do not place credentials, test-user addresses, provider responses, or account identifiers in repository or evidence files.

Use `deploy/scripts/c5-orchestrate.mjs` for the fail-closed top-level reducer that owns the private generic auth-driver, both private Google egress Workers, and both broker Workers. Both it and `deploy/scripts/deploy.mjs` require `--workers-subdomain <account-subdomain>`, `--public-callback-base <exact-origin>`, `--d1-id <canonical-lowercase-UUID>`, and `--secrets-file <absolute-path>`; the provider sub-deployer receives the same secrets file. The D1 id must be the exact dashed lowercase `uuid` returned by Cloudflare (for example, `c1a61a80-5d61-4500-aae8-4db034897753`); uppercase, braces, whitespace, and undashed forms are rejected rather than normalized. Dry-run executes no remote command or API request. Apply requires all explicit approvals, fetches the account's live Workers subdomain from Cloudflare before any mutation, and fails closed unless it byte-for-byte equals `--workers-subdomain`. Apply classifies each `wrangler deployments list` result as absent, present, or unknown: only an explicit script-not-found response or an empty array proves absence, while malformed output and every other API error abort before mutation. A present target aborts unless an explicit owned-update flow verifies its exact ownership manifest. Cloudflare's deployments and versions JSON exposes deployment and version UUIDs but no content digest. Evidence therefore keeps those Cloudflare identities separate from `uploaded_source_sha256`, which is computed locally by the operator from the upload input. That digest is an operator assertion, not a Cloudflare attestation, measurement, or verification of the running code. Cleanup must use `deploy/scripts/c5-cleanup.mjs --ownership-state <absolute c5-ownership.json>`; it verifies all five live deployments before deleting any and preserves non-owned D1.

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
5. Request exactly `openid`, `https://www.googleapis.com/auth/gmail.send`, and `https://www.googleapis.com/auth/spreadsheets`; reject every additional scope. `openid` is the connection-level identity scope needed for a stable pseudonymous subject; it is not an operation authority.
6. Install the client ID and client secret as `GOOGLE_OAUTH_CLIENT_ID` and `GOOGLE_OAUTH_CLIENT_SECRET` secrets on the private token egress Worker. The broker never receives the client secret.

The token egress Worker calls the pinned Google `tokeninfo` endpoint, validates its opaque `sub` and exact three-scope result, and returns only the subject to broker custody for a scoped commitment. The token endpoint's OpenID `id_token` is accepted but discarded; it is never logged, stored, forwarded, or placed in evidence. Email, name, avatar, and raw subject are not control-plane fields or evidence. Revocation uses only the fixed token-egress `/revoke` route backed by `https://oauth2.googleapis.com/revoke`.

## 3. Generate and sign operator artifacts offline

Never hand-write `operator-artifact-input.json`. Generate it from the checked-in Google broker descriptors and registry shapes, supplying every deployment identity, validity, budget, assurance, recipient, and deployed module digest explicitly. Digests are exact 64-character lowercase hexadecimal values without a `sha256:` prefix. The assurance argument is exact JSON and must be non-empty, unique, and JCS-lexically sorted. This is the exact generator command shape an operator runs:

```bash
node deploy/scripts/operator-input.mjs \
  --org-id "$ORG_ID" \
  --deployment-id "$DEPLOYMENT_ID" \
  --prefix "$PREFIX" \
  --not-before "$NOT_BEFORE" \
  --expires-at "$EXPIRES_AT" \
  --spend-limit-usd "$SPEND_LIMIT_USD" \
  --rate-limit-per-minute "$RATE_LIMIT_PER_MINUTE" \
  --required-assurance-predicates '[{"kind":"brokered_count","predicate_id":"durable-budget-and-dispatch","required_kernel_controls":["durable_budget_ledger","persisted_dispatch_boundary"]}]' \
  --activation-recipient-key-id "$ACTIVATION_RECIPIENT_KEY_ID" \
  --activation-recipient-public-key-b64u "$ACTIVATION_RECIPIENT_PUBLIC_KEY_B64U" \
  --key-id "$OPERATOR_KEY_ID" \
  --broker-wasm-sha256 "$BROKER_WASM_SHA256" \
  --auth-driver-sha256 "$AUTH_DRIVER_SHA256" \
  --google-token-sha256 "$GOOGLE_TOKEN_SHA256" \
  --google-provider-sha256 "$GOOGLE_PROVIDER_SHA256" \
  --output /operator/private/operator-artifact-input.json
```

The generator verifies the descriptor contract hashes with the same JCS-plus-SHA-256 rule as `connector-spec`, derives all six required artifact groups, emits a deny-all empty historical V1 inventory, and writes mode 0600. It never reads, accepts, or emits a private key. The spend/rate values are signed in standing-authority extensions and the rate also bounds every logical-call budget.

Keep the Ed25519 seed or PKCS8 PEM in a mode-0600 operator file, or pipe it on standard input. Never pass key bytes as an argument. Build signs the ContractSet first, resolves the StandingAuthority pin to the final signed ContractSet hash, signs nested historical evidence, and runs the Rust `broker-artifact-verifier` before returning success:

```bash
node deploy/scripts/operator-artifacts.mjs build \
  --config /operator/private/operator-artifact-input.json \
  --key-file /operator/private/deployment-authority.pem \
  --output /operator/evidence/operator-artifact-bundle.json

node deploy/scripts/operator-artifacts.mjs verify \
  --bundle /operator/evidence/operator-artifact-bundle.json \
  --trust-root /operator/evidence/operator-trust-root.json
```

`operator-trust-root.json` contains only `key_id` and `public_key_b64u`. Record the emitted `bundle_hash`. Worker configuration carries only the constant-size trust pins `OPERATOR_ARTIFACT_BUNDLE_SHA256`, `OPERATOR_BUNDLE_KEY_ID`, and `OPERATOR_BUNDLE_PUBLIC_KEY_B64U`; its size is independent of bundle size. The byte-exact canonical bundle is verified-on-load D1 data, not a Worker text binding and not a generated module, so the deployed Worker stays byte-identical to the hash-verified static package. Deployment re-verifies every signature, nested historical evidence, hash, validity window, revocation state, activation-recipient equality, and exact installed standing authority/contract set before its first remote command. Runtime refuses readiness and authority use unless exactly one D1 row for the pinned digest passes the same verification over the verbatim stored bytes.

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

For every Worker role, use the strict sequence: prove the target absent, capture `run_started_at` before the first mutation, deploy it, install every required secret through stdin, list and verify the required secret names, then capture the deployment list. Each `wrangler secret put` creates another deployment, so require at least one rather than exactly one. Require every entry to have `source: "wrangler"`, canonical lowercase UUIDs in `id` and every `versions[].version_id`, and `created_on >= run_started_at`; select the entry with maximum `created_on` even if the API response is out of order. Record `deployment_id`, selected `version_id`, `deployment_count`, every `workers/triggered_by` annotation with its IDs and timestamp, `run_started_at`, and `uploaded_source_sha256`. Apply it in this order: auth-driver; token egress (including the derived `GOOGLE_OAUTH_REDIRECT_URI`); provider egress; broker-private; and finally broker-public, whose required secret map is empty. The first four Workers have `workers_dev: false` and no routes, so none is publicly reachable while secrets are being installed. If secret installation or name verification fails before migration while creation ownership remains proven, delete that just-created Worker and abort; broader rollback may delete only other Workers created by this run. If final deployment evidence is invalid, stale, or non-Wrangler, ownership is uncertain: abort with zero further mutation rather than risk deleting a Worker that appeared concurrently. The broker does not render or deploy a service binding until the corresponding dependency has completed secret verification and immutable capture.

After all three private dependencies are verified, require an exact byte-for-byte match between `--d1-id` and a `uuid` from `wrangler d1 list --json`, then deploy broker-private, install and verify its 12 secrets, and capture it before applying D1 migrations through `0004_operator_artifact_bundle.sql`. D1 is operator-provisioned: the deploy and cleanup scripts never create or delete it. After migration, seed `operator_artifact_bundles` with the canonical bundle bytes keyed by deployment ID and bundle hash using idempotent `INSERT OR IGNORE`, then read `hex(canonical_bundle_jcs)` back and require byte-exact equality. Only after that readback succeeds may broker-public be re-qualified, deployed, captured, and exposed for readiness checks. The exact order is broker-private deployment and secret verification; forward-only D1 migration; operator-bundle D1 seed; byte-exact D1 readback verification; broker-public deployment; public health/readiness/callback checks. A migration attempt is irreversible: on migration, seed, readback, or any later failure, preserve D1, broker-private, and egress and emit `forward_fix_required`; creation-scoped rollback deletes only a broker-public Worker created by this run. Reconcile each connection forward through material seal/readback, signed registry/profile verification, V2 binding verification, authoritative fence switch, legacy ciphertext destruction, and completion. Every phase appends a cutover event. After a V2 lease or rotation, only a V2/fence-aware forward fix is permitted; never roll back to executable V1 admission.

After migrations succeed, deploy broker public, then the S21 flow Worker. Qualify all required secrets by name, exact service bindings, Durable Object ownership, D1 identity, callback route, operator-computed uploaded-source digests, and AI Gateway policy. V1 receipt material remains verification-only under archived signed keys; V1 grant, binding, and invoke admission stays disabled.

## 7. User-gated proof and cleanup

Only the user may authorize the first live proof. Use the exact consent-screen test account, verify account commitment behavior, then execute the LLM to Sheets to Gmail flow once. Evidence may contain artifact hashes, receipt references, counts, status codes, and ownership IDs; it must exclude credentials, OAuth codes, tokens, provider bodies, document content, raw account subject, and email.

If the proof is disposable, cleanup is ownership-safe and identity-pinned. Remove the public route, flow Worker, broker Workers, both egress Workers/DOs, AI Gateway, and secret names only when the recorded ownership manifest and live Cloudflare deployment/version UUIDs match. The operator-computed uploaded-source digest is checked against local evidence only; it is not available from Cloudflare. Preserve the operator-provisioned D1; the cleanup scripts do not delete it. Retain only sanitized receipt/history verification evidence and ciphertext-destruction confirmations.

## Durable Object migrations on fresh deployments

The production configuration declares a single collapsed `v1` migration creating
exactly the three bound classes (`ConnectionRefreshDurableObject`,
`CredentialStateDurableObject`, `V2AuthorityDurableObject`).

The former `v1..v4` chain created `BrokerLedgerDurableObject` and then deleted it
in `v4-delete-v1-ledger`. That chain cannot be applied to a NEW script: Cloudflare
rejects a delete-class migration for a class that had no previous script version
(`code: 10074`). Because this deploy path only ever creates fresh disposable
deployments (target absence is proven first) and V1 is history-only after cutover,
the collapsed chain is the correct form. An existing deployment carrying tags
`v1..v4` must NOT be migrated with this collapsed chain.

Known cleanup item: the production build still exports `BrokerLedgerDurableObject`
even though it is neither bound nor migrated. It is inert, but a V2-only build
should stop exporting it.
