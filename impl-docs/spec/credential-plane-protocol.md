Status: Normative 0.2 with lifecycle-separated-1 correction
Purpose: normative protocol / migration plan
Owner: Runtime / Connectors / Security
Last reviewed: 2026-07-30

# Provider-Neutral Credential Plane Protocol (0.2)

This document defines the provider-neutral credential control plane, custody
boundary, authorization-claim model, and Broker `0.2` artifact deltas. The key
words MUST, MUST NOT, REQUIRED, SHALL, SHALL NOT, SHOULD, SHOULD NOT,
RECOMMENDED, MAY, and OPTIONAL are to be interpreted as described in RFC 2119
and RFC 8174 when, and only when, they appear in all capitals.

The protocol schema version defined here is exactly `0.2`. This document
preserves the strong invariant kernel in
`impl-docs/spec/broker-protocol.md`--trusted-host scope, bounded grants,
linearizable count/replay state, deterministic effects, fail-closed admission,
and signed receipts--while replacing its OAuth/Google-shaped credential plane.
OAuth is one registered profile, OAuth scopes are one authorization-claim
vocabulary, and Google is one static registry entry. Runtime implementation is
outside this packet.

The accepted authority-lifecycle correction is normative in
`credential-plane-lifecycle-separated-1.md`. It retains public protocol `0.2`
but defines distinct `LS1<class>` schemas, requires critical
`authority_model_revision: "lifecycle-separated-1"`, and freezes new
class-specific domains. Pre-fix classes in this document retain their original
meaning and domains for verification or quarantine only; they MUST NOT be
reinterpreted, default-filled, or accepted as corrected dispatch authority.
Where lifecycle-separated-1 conflicts with pre-fix admission or lifecycle
semantics below, the focused correction governs corrected artifacts.

## 1. Scope, decisions, and relationship to `0.1`

`0.2` standardizes:

- immutable, content-addressed authentication profiles;
- provider-neutral connection activation and connection snapshots;
- privileged secret envelopes and short-lived material leases;
- a strict split among planning, response projection, credential custody,
  authenticated-request construction, and transport;
- typed, profile-owned authorization claims and policy instances;
- exact effect grants and explicitly bounded node leases; and
- provider-neutral V2 bindings, grants, receipts, assurance, and migration.

The following decisions are frozen:

1. A caller selects a registered `auth_profile_ref`; it never supplies an
   authorization URL, token URL, callback route, provider route, requested
   scopes, credential placement, signing algorithm, or egress endpoint.
2. Credentials and authenticated provider requests remain privileged. Capsule,
   flow, node, planner, and response-projector APIs cannot represent them.
3. Unknown required profiles, schemes, claim schemas, evaluators, dynamic
   sources, trust entries, critical fields, or implementation kinds fail
   closed before credential access or dispatch.
4. There is no universal authorization DSL. Each constraint names a registered,
   schema-pinned evaluator.
5. Protocol `0.1` execution artifacts are admitted only when they exactly match
   a row in a signed, enumerable, bounded `LegacyAdmissionInventoryV2`.
   Admission is denied by default and removed after drain. Historical V1
   receipt verification is separate, non-authoritative, and remains available.
6. Custody alone never implies semantic attenuation or provider enforcement.

This document supersedes the provider-, role-, and scope-specific fields of
`0.1` `BindingAttestation`, `ExecutionGrant`, and `InvocationReceipt`. It does
not weaken the encoding, budget, ledger, commitment, privacy, or trusted-host
rules in `impl-docs/spec/broker-protocol.md`.

## 2. Common representation rules

### 2.1 Encoding and bounds

Protocol JSON MUST use RFC 8785 JCS exactly as specified by
`impl-docs/spec/broker-protocol.md` Section 3. Duplicate decoded keys, invalid
UTF-8, non-finite numbers, and non-canonical inputs fail closed. Integer fields
MUST be non-negative unless stated otherwise and MUST be no greater than
`9007199254740991`, the largest integer exactly representable in the JCS
binary64 domain. Ordinals, epochs, generations, counts, and byte limits are
integers, never decimal strings or floating-point values.

Unless a narrower bound is stated:

- an identifier is 1 through 256 printable ASCII bytes;
- a version is 1 through 64 ASCII bytes and follows `[A-Za-z0-9._-]+`;
- an array contains at most 1,024 elements;
- a protocol artifact is at most 1 MiB; and
- a grant or receipt is at most 64 KiB.

A content digest is `sha256:` plus exactly 64 lowercase hexadecimal digits. The
long JSON examples use syntactically valid, internally repeated test literals
to freeze shape and bounds; they are not cryptographic golden vectors unless an
exact preimage/hash is printed with them. The conformance corpus in Section 13
MUST replace illustrative signatures with verifiable fixed-key signatures. A
commitment follows the HMAC construction in
`impl-docs/spec/broker-protocol.md` Section 10. Security identifiers are
compared byte-for-byte after schema validation; implementations MUST NOT apply
implicit Unicode normalization or case folding.

### 2.2 Versioned references

An `AuthProfileRef` has exactly:

```json
{"profile_ref":"auth.synthetic.oauth","version":"1"}
```

`profile_ref` is a stable namespace, not a provider display name. Changing any
security semantic requires a new `version` and registry definition. Artifacts
that authorize work carry this pair inside an `auth_profile` `RegistryPinV2`;
the pin's signed definition hash commits to the descriptor and its distinct
descriptor hash. A registry lookup MUST match the complete pin and current
decision epochs.

A `RegistryPinV2` has exactly:

```json
{
  "entry_ref": "registry.synthetic.auth-driver",
  "version": "1",
  "definition_hash": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "approval_epoch": 3,
  "revocation_epoch": 4
}
```

`definition_hash` hashes the complete signed immutable registry definition; it
is distinct from any binary, module, service-identity, schema, or corpus digest
inside the class payload. Admission MUST resolve the current signed decision,
require exact definition hash and approval/revocation epochs, and require
`approved` plus `active`. A reference without all five fields is not authority.

### 2.3 Critical fields and evolution

Every public `0.2` artifact contains `schema_version: "0.2"`, a sorted,
duplicate-free `critical_fields` JSON Pointer array, and an `extensions`
object. Every V2 schema is closed with `additionalProperties: false`; extension
members can occur only below `/extensions`. A producer MUST list every
security-required extension pointer. A verifier MUST reject an unknown schema
version, required vocabulary, tagged-union variant, registry class, status,
assurance predicate, evidence kind, direct unknown member, or critical
extension. Unknown non-critical extension members remain in JCS hash and
signature inputs and MUST NOT widen authority.

An additive extension may be non-critical only when ignoring it cannot affect
identity, authority, custody, endpoint selection, credential use, dispatch,
assurance, destruction, or verification. A new direct member, union variant, or
changed field meaning requires a new protocol or profile version. No `0.2`
verifier may reinterpret a `0.1` object as `0.2` by filling defaults. Section
20 is the authoritative closed schema catalogue; illustrative objects elsewhere
MUST validate against it.

## 3. Signed trust registry and authentication profiles

### 3.1 Immutable definitions and mutable decisions

Trust authority is two signed closed artifacts:

- `RegistryDefinitionV2` is immutable and contains `entry_ref`, `version`,
  `class`, one class-specific `class_payload`, publisher identity, publication
  time, `critical_fields`, `extensions`, and publisher signature. Its external
  `definition_hash` is SHA-256 of the complete signed JCS bytes.
- `RegistryDecisionV2` names the exact definition hash, approval status and
  epoch, revocation status and epoch, decision authority, validity interval,
  policy hash, `critical_fields`, `extensions`, and authority signature.

Their signature domains are respectively
`lattice.registry-definition.v0.2` and
`lattice.registry-decision.v0.2`, using Section 10.4's omit-signature preimage.
The publisher key and decision-authority key MUST come from an operator trust
root, never from the artifact itself. Approval is `approved`, `suspended`, or
`rejected`; revocation is `active` or `revoked`. Epochs are exact non-negative
integers and MUST increase on every decision change. Admission pins and checks
the current `RegistryPinV2`; a stale decision, a current epoch different from
the pin, or any state other than `approved`/`active` fails closed.

The closed class enum is:

```text
auth_profile | capsule_planner | response_projector | auth_driver |
custodian | transport | privileged_response_firewall | policy_evaluator |
dynamic_source | claim_normalizer | legacy_inventory
```

Class payloads are closed tagged objects:

- `auth_profile`: complete `AuthProfileDescriptor`, descriptor hash, and profile
  conformance-suite hash;
- `capsule_planner` and `response_projector`: interface version, implementation
  kind, implementation artifact digest, supported contract hashes, and
  conformance corpus/result hashes;
- `auth_driver`: interface version, implementation artifact digest, supported
  scheme refs/profile ref+version pairs, privileged-response capabilities, and
  conformance hashes. Profile support does not contain descriptor/definition
  hashes, avoiding a hash cycle; the profile independently pins the driver;
- `custodian`: interface version, local or remote mode, service-identity
  commitment when remote, supported schemes, destruction-evidence kind, and
  conformance hashes;
- `transport`: interface version, local HTTPS or `remote_custody_v1` mode,
  service-identity commitment when remote, TLS policy hash, and conformance
  hashes;
- `privileged_response_firewall`: interface version, implementation digest,
  supported response-policy hashes, maximum response bytes, and conformance
  hashes;
- `policy_evaluator`: interface version, supported policy profile/schema hashes,
  fact-vocabulary/schema hashes, relation schema hashes, output schema hashes,
  implementation digest, and conformance hashes;
- `dynamic_source`: interface version, authenticated source kind, output
  vocabulary/schema hash, binding schema hash, implementation digest, and
  conformance hashes; and
- `claim_normalizer`: input claim vocabulary/schema, normalized output schema,
  equality/subset result schema, implementation digest, and conformance hashes;
  and
- `legacy_inventory`: inventory schema hash, maximum row count, maximum
  admission duration, and exact allowed deployment IDs.

The exact artifacts and conditional payload requirements are in Section 20 and
the `$defs` catalogue.
A class payload digest is never used where a definition hash is required.

### 3.2 `AuthProfileDescriptor`

An authentication profile is public, immutable, credential-free, and available
only through an approved `auth_profile` registry definition. Required fields
are `profile_ref`, `version`, `scheme_ref`, closed `activation_kind`, closed
`scheme_config`, public-config schema hash, private authorization-claims schema
hash, immutable public-claims-projection policy, trusted auth-driver pin,
claim-normalizer pin, endpoint-policy schema hash, lifecycle capabilities, and
extensions.

Full authorization claims are private.
`PublicClaimsProjectionPolicyV2` is immutable profile policy: it is either
`{"kind":"none"}` or `allowlisted` with projection schema hash, projector pin,
and explicit safe JSON pointers. It never contains a connection value or claims
commitment. `PublicClaimsProjectionEvidenceV2` is connection-specific and is
stored in the authority view, snapshot, binding, node lease, and grant. Its
`projected` variant contains the policy hash, schema/projector pins, runtime
value, source-claims commitment, and projection-evidence hash; its `none`
variant contains policy hash and source-claims commitment only. Evidence MUST
match the pinned policy exactly. No full claims appear in a public artifact.

Profile endpoint policy contains logical endpoint keys and exact allowed HTTPS
origin/path constraints, not deployment URLs. Deployment-specific absolute
URLs, callback origin, client metadata references, and non-secret config live
in signed, digest-pinned `DeploymentEndpointSetV2` and
`DeploymentPublicConfigV2` records. The profile pins their schemas. This
removes the prior contradiction between immutable profile definitions and
per-deployment callback URLs.

Lifecycle capabilities are a sorted subset of `activate`, `refresh`, `rotate`,
`revoke`, `destroy`, `discover_principal`, `introspect`, `exchange`, and
`workload_bind`. Unsupported lifecycle requests fail closed.

### 3.3 Closed `scheme_config`

`scheme_config` is a closed tagged union and contains every security semantic
needed by each advertised scheme:

| `kind` | Required configuration |
| --- | --- |
| `oauth2_authorization_code_pkce` | authorization/token/revocation/principal-discovery endpoint keys, response mode, `S256`, token endpoint auth method, client-auth material kind, refresh rotation mode, and exact token/claim response firewall policy |
| `generic_bearer` | exact header name, prefix, whitespace rule, and secret field schema hash |
| `api_key_header` | exact header name/prefix, forbidden-log rule, and secret field schema hash |
| `api_key_query` | exact query name/encoding, authenticated-query redaction rule, and secret field schema hash |
| `http_basic` | username/password field schema hashes, UTF-8 charset, colon rule, and exact `Authorization` placement |
| `service_account_jwt` | allowed signing algorithm, key kind, issuer/subject claim sources, audience endpoint key, assertion lifetime/skew, JWT claim schema hash, exchange endpoint key, token endpoint auth method, and response firewall policy |
| `signed_request` | canonicalization algorithm, key-ID placement, fixed region/service values or trusted config pointers, signed-header allowlist, payload-hash rule, clock/skew rule, and signature placement |
| `oauth_token_exchange_workload_oidc` | trusted issuer set ref/hash, exact audience, subject/requested token types, exchange endpoint key, assertion age/nonce/proof rules, token endpoint auth method, claim mapping, and response firewall policy |
| `external_custodian_reference` | allowed custodian/transport pins, `remote_custody_v1` codec, service-identity policy, and destruction-evidence kind |

`token_endpoint_auth_method` therefore exists only inside the three token
endpoint variants and is exactly `none`, `client_secret_basic`,
`client_secret_post`, `private_key_jwt`, `tls_client_auth`, or
`self_signed_tls_client_auth`. Unknown variants or configuration fields fail
closed. Section 20's `SchemeConfig` definition gives the exact JSON Schema.

## 4. Credential schemes

A scheme defines credential mechanics, not provider identity or authorization
semantics. Baseline references are:

- `credential.oauth2.authorization_code_pkce@1`;
- `credential.bearer.generic@1`;
- `credential.api_key.header@1`;
- `credential.api_key.query@1`;
- `credential.http.basic@1`;
- `credential.service_account.jwt@1`;
- `credential.signed_request@1`;
- `credential.oauth2.token_exchange_workload_oidc@1`; and
- `credential.external_custodian.reference@1`.

The `scheme_ref` and `scheme_config.kind` mapping is one-to-one in Section 20's
`SchemeConfig` definition and is checked semantically.
A generic bearer profile cannot claim refresh or OAuth semantics. An API-key
placement, Basic encoding, service-account assertion, signed-request
canonicalization, workload exchange, or external-custody protocol cannot be
hidden in driver code or an extension: it MUST appear in the closed
`scheme_config` and be covered by the profile definition hash.

## 5. Provider-neutral activation protocol

### 5.1 Authorized activation input

`CreateActivationV2` is a closed authenticated operator message. It names:

- exact `connector_ref`;
- `AuthProfileRef` plus approved auth-profile `RegistryPinV2`;
- opaque `standing_authority_ref` and its canonical hash;
- opaque `contract_set_ref` and its canonical hash;
- `deployment_id`;
- signed `deployment_endpoint_set_ref`/hash and
  `deployment_public_config_ref`/hash;
- execution lane and custody location; and
- a unique request JTI.

The referenced standing-authority record binds org, deployment, exact connector
contracts, maximum budgets, required assurance predicates, and operator policy.
The frozen contract-set record binds sorted contract ID/hash pairs and their
claim requirements. The broker derives requested private authorization claims
by intersecting those two records with the profile normalizer and operator
policy. The request has no claims/scopes, endpoint URL, callback URI, client
secret, credential placement, account selector, or provider route field.

Endpoint/config records are closed signed deployment records. An endpoint set
contains exact absolute HTTPS URLs keyed by profile logical endpoint keys, the
universal callback absolute URI, issuer, key ID, validity, and content hash. It
MUST validate against the profile endpoint-policy schema and exact origin/path
constraints. A public-config record validates against the profile's public
config schema and may contain only non-authority values and opaque privileged
secret-binding references. Neither record may be supplied by flow/node code.

### 5.2 Closed `NextActionV2`

The server returns an opaque `activation_ref` and exactly one variant:

```text
open_url | submit_private_material | bind_external_custodian |
present_workload_assertion | poll | complete
```

All variants include `schema_version`, `critical_fields`, `kind`,
`activation_ref`, `expires_at`, and `extensions`.
`open_url` adds a server-generated URL and single-use `correlation_handle`.
`submit_private_material` adds a private channel ref, recipient key ID, and
submission schema hash. `bind_external_custodian` adds a challenge and sorted
allowed custodian pins. `present_workload_assertion` adds the same private
channel metadata plus exact audience, nonce, and assertion schema hash. `poll`
adds integer `retry_after_ms` from 1 through 300000. `complete` adds
`connection_ref` and `authority_view_hash`. No action accepts response material
inline.

Secret values and workload assertions use `PrivateMaterialSubmissionV2`, the
same authenticated encrypted operator boundary. They MUST NOT use the public
activation JSON body, URL/query fields, logs, ordinary flow APIs, or capsule
APIs. The private submission codec uses the remote-custody cryptographic suite
in Section 7.4, binds org/operator/activation/channel/schema/JTI/expiry as AAD,
and atomically consumes the JTI before decryption.

### 5.3 Universal callback correlation

Every callback profile uses one deployed callback route
`/v0.2/credential-callback`; there are no provider-named routes. The callback
carries only provider response fields and an opaque, unpredictable, single-use
correlation handle. Its server-side record binds org, operator, activation,
profile definition hash and decision epochs, auth-driver pin, endpoint-set
hash, config hash, exact redirect URI, private derived-claims commitment, PKCE
state where applicable, standing-authority hash, contract-set hash, expiry,
lane, custody, and deployment.

The handler atomically claims correlation before exchange. Duplicate delivery
returns a recorded terminal result or sanitized conflict and never repeats a
non-idempotent exchange. Missing, expired, consumed, wrong-profile, wrong-org,
or mismatched state fails closed. Callback fields cannot alter any bound value.

### 5.4 Activation kinds and state machine

`activation_kind` is exactly `oauth_authorization_code_pkce`,
`secret_submission`, `external_custodian_binding`, `workload_binding`, or
`none`, with the conditional fields in Section 20's `ActivationKind`
definition. OAuth requires S256.
Device authorization is deferred and rejected by baseline `0.2`.

```text
created -> awaiting_action -> action_claimed -> validating_material
        -> discovering_principal -> committing_authority -> active
             |                       |                    |
             +-> restart_required <--+--------------------+
             +-> failed
created|awaiting_action|restart_required -> cancelling -> destroying -> destroyed
active -> rotating -> active
active|rotating|blocked -> revoking -> revoked -> destroying -> destroyed
```

Transitions are durable monotonic CAS records. OAuth code exchange is
idempotent by activation ref only when the profile driver proves that property;
ambiguous exchange enters `restart_required` and wipes code/PKCE transient
material. External custody requires signed challenge or mutually authenticated
private-service proof. Workload binding validates private assertion issuer,
audience, age, nonce, and subject before exchange. A partial connection is not
active and cannot issue authority.

Recovery resumes from encrypted private state plus exact signed pins, never
caller assertions. Tests crash before/after action claim, exchange, response
firewall, principal discovery, secret seal, authority commit, connection write,
rotation switch, revoke, and destruction confirmation.

## 6. Connection authority, lifecycle, and private material

### 6.1 Immutable `ConnectionAuthorityViewV2`

Admission pins an immutable authority view, not a mutable lifecycle snapshot.
Its closed fields are org/connection identity, auth-profile ref and registry
pin, endpoint/config record hashes, custodian and transport pins, execution
lane, custody location, broker instance commitment, principal commitments,
private authorization-claims commitment, connection-specific public projection
evidence under the profile's immutable policy, authorization-claims schema
hash, authority epoch, standing-authority hash, contract-set hash, compatible policy
profile hashes, registry decision epochs, creation time, critical fields, and
extensions.

`authority_view_hash` is SHA-256 of the complete JCS authority view. It excludes
status, update time, active/retiring material, material generations, leases,
rotation phases, and destruction confirmations. It changes whenever authority,
profile semantics, principal, normalized claims, endpoint/config authority,
custody path, standing authority, contract set, evaluator compatibility, or a
pinned registry decision changes. Public artifacts carry claim commitments by default. A `projected` evidence
value is allowed only when its policy hash, schema, projector, and value match
the profile's immutable allowlist policy; otherwise evidence kind is `none`.

Principal commitments are sorted by kind and use Section 10.5 contexts. Raw
account IDs, email, subjects, tenants, audiences, resources, permissions, and
full claims are private. Ambiguous or unavailable discovery blocks activation.

### 6.2 `ConnectionSnapshotV2`

The lifecycle snapshot contains `authority_view`, its recomputed
`authority_view_hash`, status, `current_material_generation`, active material
metadata, retiring material metadata, rotation record, destruction
confirmations, created/updated times, CAS version, critical fields, and
extensions. Status is `pending`, `active`, `rotating`, `blocked`, `revoking`,
`revoked`, `destroying`, or `destroyed`. Only `active` can derive new authority.

`authority_epoch` belongs only to the immutable authority view. It increments
on possible widening/narrowing, revoke, principal/claim/profile/endpoint/custody
change, semantic registry decision change, or uncertainty. Material generation
increments on every token/key/secret/external version change. A proven
material-only rotation creates a new authority view only if a pinned registry
decision changes; otherwise its authority-view hash remains stable.

A grant carries `minimum_material_generation`; a privileged lease records one
exact `leased_material_generation`; a receipt records that exact generation.
Admission MAY use a generation greater than the minimum only after matching the
same authority-view hash and epoch. New work never leases retiring material.
This replaces the contradictory mutable snapshot hash.

### 6.3 Durable rotation and refresh phases

Every refresh/rotation has a phase-conditional durable `RotationRecordV2` with
a unique ref, old/new generation, authority-view hash, expected epoch, phase,
CAS version, and only artifacts that durably exist at that phase. Phases are strictly:

```text
prepared -> provider_request_recorded -> provider_result_observed
         -> new_material_sealed -> authority_reconciled -> switched
         -> retirement_enqueued -> old_material_destroyed -> complete
```

`prepared` has only the common fields. Each later phase cumulatively requires:
provider-request record hash; provider-result commitment; sealed-envelope hash;
reconciliation-record hash; switch-record hash; retirement-outbox hash;
destruction-evidence hash; then completion-record hash. Earlier phases MUST NOT
contain later fields. The provider request is recorded before network crossing.
The credential-producing response goes through the privileged firewall before
any projector. The new envelope is authenticated and read back before
`new_material_sealed`; reconciliation precedes the atomic generation switch.
Only then is old material non-leaseable and destruction enqueued.

If provider rotation may have succeeded but response observation or durable
sealing fails, status MUST become `blocked`; neither old nor guessed new
material may be leased. Recovery uses provider idempotency/introspection or
operator reactivation. It MUST NOT fall back to stale material. At most two
retiring generations exist and retirement is at most 300 seconds unless a
stricter profile bound applies.

### 6.4 Private envelopes and leases

`SecretEnvelopeV2` and `CredentialLeaseV2` are closed privileged persistence
codecs, never public protocol values. Envelopes bind scheme/profile definition,
material kind, sealing key ID, org, connection, custody, authority-view hash,
authority epoch, generation, created/not-before/expires times, nonce,
ciphertext, and AAD hash. Leases bind lease ref, exact effect grant hash,
dispatch attempt, exact generation, use limit, issue/expiry, and private
material. A lease is single-dispatch by default; its use limit is 1 through
255.

Secret types have redacted `Debug`/display, zeroize on drop, avoid clone, and
expose material only inside the auth-driver or remote-custody closure. Public
serializers, connector/capsule APIs, receipts, traces, errors, and logs cannot
encode them. A generation disappears only after a matching destruction
confirmation; `destroyed` covers every committed generation.

## 7. SPI, response firewall, and remote custody

### 7.1 Strict responsibility split

```text
CredentialBlindPlanner::plan(typed_input, public_authority_projection)
    -> OpaqueUnauthenticatedPlan
PrivilegedResponseFirewall::classify_and_extract(raw_bounded_response,
                                                  response_policy)
    -> ScrubbedProviderResponse + PrivateCredentialUpdate?
ResponseProjector::project(ScrubbedProviderResponse) -> typed_output
CredentialCustodian::authority_view / lease / rotate / revoke / destroy
TrustedAuthDriver::authorize(OpaqueUnauthenticatedPlan, CredentialLeaseV2,
                             broker_context) -> PrivateAuthenticatedRequest
BrokerTransport::dispatch(PrivateAuthenticatedRequest)
BrokerTransport::authorize_and_dispatch(RemoteCustodyRequestV2)
```

Planner and projector APIs cannot name or represent credentials, credential
references, assertions, authenticated headers/queries, endpoint authority, raw
provider token responses, or private requests. The auth driver alone applies
bearer/basic/API-key auth, signs, creates JWTs, or exchanges tokens. Every
selected component has a current class-correct `RegistryPinV2`; arbitrary
callbacks, caller modules, and class substitution are forbidden.

### 7.2 Privileged response firewall

Every operation contract declares a closed `credential_response_policy`:

- `forbidden` means credential-shaped fields, auth headers, cookies, signed
  URLs, private keys, token types, or configured sensitive JSON pointers cause
  a privileged failure before projection; or
- `privileged_extract` names exact credential JSON/header pointers, material
  kinds, maximum lengths, claim response schema, and destination custodian. It
  is allowed only for registry-approved auth drivers and lifecycle contracts,
  never ordinary capsule contracts.

Raw provider bytes first enter the privileged firewall. It bounds and parses,
extracts credential fields directly into private zeroizing material, removes
those fields plus configured echo fields, and emits a scrubbed response with a
firewall evidence hash. `ResponseProjector` receives only the scrubbed value
and cannot return private audit bytes or initiate another request. A token-
issuing/echoing response that lacks `privileged_extract` fails closed. Public
errors and receipts expose commitments only.

### 7.3 Trust classes

Capsule planners/projectors are trusted for operation semantics but remain
credential blind. Auth drivers, custodians, the response firewall, and
transports are privileged. Policy evaluators and dynamic sources are trusted
only for their pinned vocabularies/relations. A remote combined
custodian/transport is trusted for custody and network crossing and must return
signed codec evidence. Registry decision epochs are checked at binding, lease/
grant derivation, admission, and immediately before dispatch.

### 7.4 Exact `remote_custody_v1` secure codec

Remote custody runs over TLS 1.3 with mutual service authentication pinned by
the transport/custodian registry decisions. Application confidentiality and
binding use HPKE base mode suite X25519-HKDF-SHA256/AES-256-GCM. The sender uses
the current recipient key ID from the signed registry payload. The outer closed
`RemoteCustodyEnvelopeV2` contains schema version, direction (`request` or
`response`), request ref, org, connection, exact effect-grant hash,
logical-effect ID, dispatch attempt, authority-view hash, minimum material
generation, sender/recipient service pins, recipient key ID, HPKE encapsulated
key, nonce/JTI, issued/expiry times, ciphertext, AAD hash, critical fields,
extensions, and sender Ed25519 signature.

AAD is JCS of the envelope with `ciphertext`, `signature`, and `aad_hash`
omitted. `aad_hash` hashes those bytes. The HPKE plaintext request is the closed
`RemoteAuthorizeDispatchPrivateV2`: canonical unauthenticated plan bytes as
unpadded base64url plus hash, contract hash, profile pin, canonical broker-
context bytes as unpadded base64url, endpoint-set hash, response-firewall policy
hash, and exact effect/attempt. The response plaintext is
`RemoteDispatchResultPrivateV2`: outcome, exact leased generation, provider-
crossing state, firewall evidence, canonical scrubbed response bytes as
unpadded base64url, private credential-update envelope hash when retained by
custody, and durable result-record hash.

The sender signs domain `lattice.remote-custody-envelope.v0.2` plus zero plus
the raw 32-byte SHA-256 AAD digest plus decoded base64url ciphertext bytes. The receiver verifies mTLS identity,
registry pins, signature, time, exact request/effect/attempt, and atomically
reserves `(sender, request_ref, nonce_jti)` before HPKE open. Replays return the
same encrypted recorded terminal result. The remote service durably records
`prepared` before provider crossing and `dispatched` immediately before it;
crash after crossing returns `ambiguous`, never retries without contract policy.
Plaintext/authenticated requests and credentials never leave the remote trusted
boundary. Size limits and exact schemas are in Section 20's remote definitions.

## 8. Private claims, evaluable policy, and assurance predicates

### 8.1 Authorization claims and public projection

Full `AuthorizationClaimsPrivateV2` contains vocabulary ref, schema hash, and
normalized value inside custody. OAuth scopes are one vocabulary, not kernel
fields. Claim parsing, canonicalization, equality, and subset/incomparable
results are owned by the approved `claim_normalizer` definition. If subset
cannot be proven it is false. Wildcards/defaults/non-enumerable authority fail
closed unless the exact profile gives bounded semantics.

Public artifacts contain `authorization_claims_commitment` and
`PublicClaimsProjectionEvidenceV2`; they contain no unkeyed normalized-claims
hash. The default evidence kind is `none`. A `projected` variant carries only a
value valid under the immutable `PublicClaimsProjectionPolicyV2`, its policy
hash, schema/projector pins, source-claims commitment, and evidence hash.
Projection safety is part of auth-profile conformance; callers cannot request
additional fields.

Principal discovery is auth-profile behavior. It may validate a token subject,
exact discovery endpoint, service-account issuer, workload subject, API-key
introspection, or external-custodian assertion. Only commitments leave custody.
A subject change increments authority epoch and requires rebinding.

### 8.2 Evaluable `PolicyInstanceV2`

Each instance is a closed object with:

- stable `instance_id` and `instance_hash` over the object with that hash
  omitted;
- policy profile ref/version/schema hash;
- constraint source (`inline_private_commitment` or immutable `reference`),
  resolved constraint hash, and constraint schema hash;
- a non-empty sorted array of `FactSelectorV2` values;
- exact relation ref and relation schema hash;
- evaluator `RegistryPinV2` and evaluator input/output schema hashes;
- zero or more sorted trusted `DynamicSourceBindingV2` values;
- `EvaluatorOutputV2`; and
- critical fields/extensions.

A fact selector names source kind (`authority_fact`, `canonical_input`,
`trusted_dynamic_source`, or `prior_receipt_handle`), source commitment,
vocabulary ref, fact schema hash, and an RFC 6901 pointer. Dynamic-source
bindings name a `dynamic_source` registry pin, authenticated host binding hash,
output schema, freshness interval, and value commitment. Caller assertions are
not sources.

The relation defines what is compared; unrelated claims are not implicitly
intersected. Examples include `set_subset`, `resource_prefix_within`,
`numeric_at_most`, and profile-owned relations. `EvaluatorOutputV2` is exactly
`{decision, selected_facts_commitment, resolved_constraint_commitment,
effective_value_commitment, relation_evidence_hash, evaluated_at}` where
decision is `satisfied` or `denied`. `satisfied` means only that the named
relation holds for selected facts and resolved constraint under the named
schemas. Unknown/revoked evaluator/source, mismatch, stale source,
incomparable relation, or denied output fails closed.

### 8.3 Explicit assurance predicates and evidence

There is no scalar `minimum_assurance` and no implicit ordering. Standing
authority, bindings, leases, and exact grants carry a sorted non-empty
`required_assurance_predicates` array:

```text
brokered_count(predicate_id, required_kernel_controls[])
semantic_policy(predicate_id, required_policy_instance_hashes[])
provider_constraint(predicate_id, constraint_ref, evidence_schema_hash,
                    required_issuer_pin)
```

Grant issuance and dispatch require one canonical evidence object satisfying
each predicate ID exactly. Receipt `assurance_evidence` is a sorted array of the
closed union:

- `brokered_count`: predicate ID, grant hash, authority-view hash/epoch,
  reservation/ledger transition hash, budget before/after, PoP transcript hash,
  and registry-decision-set hash;
- `semantic_policy`: predicate ID, policy instance/evaluator output hashes,
  fact-set and constraint commitments, evaluator pin, and evaluation time; or
- `provider_constraint`: predicate ID, exact provider constraint ref,
  issuer pin, provider/issuer evidence commitment, evidence schema hash, and
  verification result hash.

Every required predicate has exactly one matching satisfied evidence entry;
extra claims are forbidden. Ordinary static, Basic, API-key, OAuth, service-
account, signed-request, and broad workload credentials can satisfy
`brokered_count` only. Semantic evidence requires evaluable policy instances.
Provider enforcement requires verifier-checkable evidence of the exact narrowed
constraint. Custody/authentication alone satisfies neither semantic nor
provider predicates.

## 9. Non-invocable node leases and exact-effect grants

### 9.1 Monotonic derivation

```text
signed standing deployment authority + exact contract set
  intersect current ConnectionAuthorityViewV2 + registry decisions + policy
  -> NodeLeaseV2 (optional, non-invocable derivation authority)
  intersect activation ordinal + semantic slot + canonical input commitment
  -> ExecutionGrantV2 (exact logical effect only)
  -> reserve / plan / reconcile / exact material lease / dispatch / receipt
```

Every step preserves org, explicit deployment ID, profile, claims/principal
commitments, authority-view hash/epoch, custody, contract, policy instance,
assurance predicates, aggregate ceilings, and PoP. It only narrows scope, time,
budget, and material minimum.

### 9.2 `NodeLeaseV2` is not an invocation grant

`NodeLeaseV2` uses its own artifact type, signature domain
`lattice.node-lease.v0.2`, opaque `node_lease_` namespace, authoritative store,
and audience exactly `broker-grant-derivation`. Broker dispatch/admission APIs
MUST reject its ref, hash, audience, or canonical bytes as `ExecutionGrantV2`.
It contains one flow-node-run subject; first/last inclusive activation ordinal;
sorted declared semantic slots; `max_logical_effects`; budgets/aggregate
ceilings; authority-view hash/epoch; minimum material generation; required
policy instances/assurance predicates; PoP channel; issue/expiry; and JTI. Its
maximum lifetime is 300 seconds.

Child derivation atomically reserves a unique `(node_lease_ref,
logical_effect_id)` and consumes one lease unit. A lost/expired/revoked lease,
out-of-range ordinal/slot, duplicate altered input, exhausted aggregate, or
epoch/view mismatch fails before planning.

### 9.3 Exact `ExecutionGrantV2`

Every V2 execution grant has `audience: "broker-execution"` and one
`grant_scope` variant only:

```text
{kind:"logical_effect", logical_effect_id, activation_ordinal,
 semantic_effect_slot}
```

It MUST include `canonical_input_commitment` computed before issuance from the
exact JCS operation bytes using Section 10.5 field name
`grant_canonical_input`, and a `GrantDerivationEvidenceV2`:

- `direct_standing`: standing-authority ref/hash, contract-set ref/hash,
  derivation-record hash, consumed standing budget unit, canonical-input
  commitment; or
- `node_lease`: all direct fields plus parent node-lease ref/hash/JTI,
  parent remaining budget before/after, and atomic child-reservation record
  hash.

The derivation record commits to org, deployment, subject, operation contract,
connection, authority view/epoch, exact effect scope, canonical-input
commitment, policy/assurance requirements, budgets, times, parent hashes, and
PoP session. There is no `node_lease` grant-scope variant. Invocation requires
byte-identical canonical input that reopens the grant commitment; altered
redelivery is fatal replay conflict.

`minimum_material_generation` is an exact non-negative integer. Immediately
before dispatch the privileged custodian returns a lease for one
`leased_material_generation >= minimum_material_generation` under the same
view/epoch. The exact generation and `dispatch_attempt` are persisted in the
ledger before network crossing and copied to the receipt.

PoP binds org, deployment, session, grant ref/hash, body hash, method, normalized
path/query, audience, timestamp, and unique JTI. Aggregate flow, connection,
node, and parent-lease budgets use one linearizable authority. All registry
approval/revocation epochs are rechecked immediately before dispatch.

## 10. V2 artifacts, signatures, and commitments

### 10.1 `BindingAttestationV2`

The closed signed binding pins org, broker issuer/key, explicit deployment ID,
authority-manifest and standing-authority/contract-set refs and hashes,
connection ref, immutable authority-view hash/epoch, auth-profile and all
registry decision pins, claims/principal/broker commitments, connection-specific public projection
evidence, custody/lane, endpoint/config hashes, supported contracts, policy
instance hashes, required assurance predicates, observation/expiry, critical
fields, extensions, and signature. It contains no provider, roles, scopes, full
claims, secret locator, or mutable material list. `minimum_material_generation`
is observational minimum only and does not enter `authority_view_hash`.

### 10.2 `NodeLeaseV2` and `ExecutionGrantV2`

Section 9 defines the distinct non-invocable lease and exact child grant.
`ExecutionGrantV2` always includes exact logical-effect scope,
`canonical_input_commitment`, grant derivation evidence, authority-view hash/
epoch, minimum material generation, registry decision set hash, required policy
instances/assurance predicates, explicit deployment, channel PoP, bounded
budgets, and standing/contract-set hashes. Section 20 contains the complete
schemas.

### 10.3 `InvocationReceiptV2`

Every receipt contains exact logical-effect grant scope and REQUIRED integer
`dispatch_attempt`: zero only for a pre-dispatch rejection and 1 through 255
after a persisted dispatch boundary. Attempt zero requires `pre_dispatch_stage`
=`pre_planning` or `post_planning_pre_dispatch`. Pre-planning records plan,
facts, and firewall evidence as `not_computed/pre_planning_rejection`;
post-planning records plan/facts hashes and firewall evidence as
`not_computed/no_provider_response`. Positive attempts prohibit the stage. It
also contains exact
`leased_material_generation` for dispatched outcomes (tagged `not_leased` when
zero), canonical-input/connection/claims/principal/broker-instance commitments,
authority-view hash/epoch, derivation evidence hash, policy instance/evaluator
output hashes, the exact six-key core implementation map, a separate evaluator
implementation map, request-plan/fact/firewall/response hashes,
budgets, outcome, closed assurance evidence, the five closed V1 honesty
booleans, issued time, critical fields/extensions, and signature.

Historical `plugin_module_sha256` and provider/role/scope fields do not exist.
`ImplementationRefV2` is exactly `declarative_plan`, `native_component`,
`wasm_component`, or `remote_service`; every variant contains a class-correct
registry pin and its distinct artifact/module/binary/service identity digest.
The core map has exactly `planner`, `projector`, `auth_driver`, `custodian`,
`transport`, and `privileged_response_firewall`; each pin is class-correct. The
separate evaluator map has exactly one class-correct `policy_evaluator` entry
for each referenced evaluator output, so its count and key set MUST match those
outputs.

### 10.4 Hash and signature rules

Content hash is `sha256:` plus SHA-256 of complete JCS bytes. Signed preimage is
`ASCII(domain) || 0x00 || JCS(object with only top-level signature omitted)`.
Domains are:

- `lattice.registry-definition.v0.2`;
- `lattice.registry-decision.v0.2`;
- `lattice.deployment-endpoint-set.v0.2`;
- `lattice.deployment-public-config.v0.2`;
- `lattice.standing-authority.v0.2`;
- `lattice.contract-set.v0.2`;
- `lattice.private-material-submission.v0.2`;
- `lattice.legacy-admission-inventory.v0.2`;
- `lattice.legacy-inventory-decision.v0.2`;
- `lattice.historical-key-validity-evidence.v0.2`;
- `lattice.historical-key-revocation-evidence.v0.2`;
- `lattice.historical-verification-key-archive.v0.2`;
- `lattice.binding-attestation.v0.2`;
- `lattice.node-lease.v0.2`;
- `lattice.invocation-receipt.v0.2`; and
- `lattice.remote-custody-envelope.v0.2` uses Section 7.4's ciphertext-bound
  construction.

Execution grants are authoritative ledger records and hashed, not portable
signed bearers. All signatures are Ed25519, exactly 64 decoded bytes in
canonical unpadded base64url. Trust keys are externally pinned. Unknown
extensions remain in JCS.

The normative machine vector contains 17 complete schema-valid signed objects:
one for every ordinary signed artifact/domain, both historical revocation
evidence variants, and the special remote envelope construction. Every object
includes its full signature object/key ID; the key entry includes exact seed and
derived raw public key. Each vector records unsigned JCS or remote AAD JCS,
domain-separated preimage, signature, complete signed JCS, and artifact hash as
bytes/hex. `verify-credential-plane-vectors.py` derives the public key,
re-signs/verifies with OpenSSL Ed25519, and recomputes every byte/hash.

### 10.5 Exhaustive V2 commitment contexts

V2 reuses the two-stage HMAC KDF in `broker-protocol.md` Section 10. For every
row below, `field_name` is exact, `value_bytes` are JCS of the named private
value (UTF-8 bytes only where marked opaque), and `salt_context` is JCS of the
shown array. No other V2 commitment name is valid without a protocol revision.
`effect` means full logical-effect hash and `attempt` is the receipt integer.

| Field name | `value_bytes` | Exact `salt_context` array |
| --- | --- | --- |
| `authorization_claims` | full normalized private claims JCS | `["credential-plane.v0.2","authority_view",connection_ref,authority_epoch,"authorization_claims"]` |
| `principal_account_subject` | typed opaque subject bytes | `["credential-plane.v0.2","authority_view",connection_ref,authority_epoch,"principal","account_subject"]` |
| `dynamic_source_value` | normalized source value JCS | `["credential-plane.v0.2","policy",instance_id,source_ref,"dynamic_value"]` |
| `broker_instance` | opaque broker instance bytes | `["credential-plane.v0.2","authority_view",connection_ref,authority_epoch,"broker_instance"]` |
| `service_identity` | typed service identity bytes | `["credential-plane.v0.2","registry",entry_ref,"service_identity"]` |
| `connection_commitment` | opaque connection-ref bytes | `["credential-plane.v0.2","receipt",issuer,run_id,node_id,effect,attempt,"connection_commitment"]` |
| `grant_canonical_input` | exact canonical input JCS bytes | `["credential-plane.v0.2","grant",issuer,grant_ref,effect,"canonical_input"]` |
| `canonical_input_commitment` | exact canonical input JCS bytes | `["credential-plane.v0.2","receipt",issuer,run_id,node_id,effect,attempt,"canonical_input_commitment"]` |
| `response_commitment` | scrubbed bounded response JCS bytes | `["credential-plane.v0.2","receipt",issuer,run_id,node_id,effect,attempt,"response_commitment"]` |
| `policy_constraint` | resolved constraint JCS | `["credential-plane.v0.2","policy",instance_id,"constraint"]` |
| `selected_facts` | selected fact-set JCS | `["credential-plane.v0.2","policy",instance_id,"selected_facts"]` |
| `effective_policy_value` | evaluator effective value JCS | `["credential-plane.v0.2","policy",instance_id,"effective_value"]` |
| `provider_constraint_evidence` | private provider evidence JCS | `["credential-plane.v0.2","receipt",issuer,run_id,node_id,effect,attempt,"provider_constraint_evidence",predicate_id]` |
| `rotation_provider_result` | complete privileged provider rotation result JCS | `["credential-plane.v0.2","rotation",connection_ref,rotation_ref,"provider_result"]` |

All contexts include org through the KDF itself. Receipt commitments use the
same attempt, including zero. Principal kinds other than `account_subject`
require a new registered field name/protocol revision; they cannot reuse this
one.

Normative vectors use tenant root bytes `00..1f`, org `org-1`, the literal
contexts/values in the vector manifest below. The table gives scoped key then
commitment, both lowercase hex:

| Field | Scoped key | Commitment |
| --- | --- | --- |
| `authorization_claims` | `4e8a863d24491d3e14c90bcb7a825f351df2fa668c9cbeea075157a203cd6480` | `c3a021124fc5c71e3c38fd1136bdbe08123e4687ceda5b1147678fd1e161f386` |
| `principal_account_subject` | `ff46fc46de8655ff243c5e4884a2dc9cffe8d6ee35fe8ffa798f7056664a63b1` | `bf53f4f47bee52244a092553c7106794e5e3bc2e0493e4e41cb4f6e5f67e4839` |
| `dynamic_source_value` | `d22104b68fcd4d9310d4f939c5bfe5076fe6fa712e5869e71df550e362a77ca5` | `b7fb788a07d1a12646401b557ef97227a27bd81f2258ebb2e76ac904ac6d02dc` |
| `broker_instance` | `825840402c99287bdd79e0b6117f8f15c97d285d36ddfc2d2207dbf910a6fa30` | `dab930182234c1ed78329215fb8b4ef58ed865801caed748d32e0cf93c85d524` |
| `service_identity` | `abb4d1f634aa1745912ca1b35d02f4a9217dd5080c85d8654fc5574be6616a9f` | `a9662b408cecb4555c4c2f13d635d3a60319428f550bafc09b4a8b2fb4821a3e` |
| `connection_commitment` | `4c02a2e675a08d97724438e147f349c8065960baf9a0beb8dcd7237e9db1b6c0` | `d0faadcae5f888e1d8982b5366d81c079bd731b3e2e6cea3d8694e3f08ddf45a` |
| `grant_canonical_input` | `656c5042809b69916651fdb3752e5df37c630076f9079832994f40e7e295e850` | `4c9e827cb113735e5569d2d7771db9240ae14e806e28db22dbfcf79b3a020c17` |
| `canonical_input_commitment` | `c61f206801a172b55326d419b66234c05a53f0f91558545e05a00b7c4d4ec70f` | `134e4e7054b33ec05276e5a1adc24422b147852ec36d1c9de905b57a4a70dc8d` |
| `response_commitment` | `638f3be8ef875aecd6635e89f7a5881cf3478bb2a9ae524e1701e01050c4494e` | `3ff8a943abac967c6e61e8269f63d22aaa376ee2a43a18669238d7f4d835b521` |
| `policy_constraint` | `7667d90b391d33643e471445cfeed1d28c9b30fd3d8e558a2e9439706764a643` | `91db3645ac529d9a599018b3802ecbc57af2e8cb1750bf0aad093e0bb15c3dc2` |
| `selected_facts` | `94c8db251d853384ce4ed1cd04c9dcce477c2c45a9920fb59aff4c3a29a1a6fb` | `0b6fa7c844d5f27626ea395e81e2b547c6abca6bc82b463ccc0a20da3324a4e0` |
| `effective_policy_value` | `7f4cfb5444eb1879b1e49f5dda6456bedcd66df50c273805f7af6b15976b1bba` | `679638611b7b30def545427fee4e33c627604ce0b787ba771355235256d68922` |
| `provider_constraint_evidence` | `67d53b4b6030bf25f2819cd6715030927bec35c0564daea2ca2f06c1b558b834` | `d2d427d7dfa3b0d36b91393de97fce2c48bbe4ee4640905dda874e50824b2480` |
| `rotation_provider_result` | `b7f38d291a76d475ff1bede20f68f9e2fe3532c3e04cbe3c8dcfabc3c490dcd7` | `a01df9b810baba606219d7d3e4e5631a93957143b2ddd096721c3352ac938b02` |

Vector private values are, in row order: `{"scope":["mail.send","table.write"]}`,
opaque `subject-123`, `{"project":"demo"}`, opaque broker/service/connection
strings shown by the contexts, grant input `{"x":1}`, receipt input
`{"x":1}`, `{}`, `{"prefix":"projects/demo/"}`, `["projects/demo/a"]`, the same prefix object,
`{"aud":"resource-1"}`, and `{"token_response":"redacted"}`. Implementations
MUST byte-compare every vector.

### 10.6 Tagged V1 commitment mapping

A V1 account commitment is not a V2 principal commitment. Migration stores it
only as closed `LegacyCommitmentV1` with `kind: "legacy_v0_1"`, original
commitment envelope, and exact original context
`[issuer,connection_ref,"account_commitment"]`. It remains verifiable under the
V1 KDF/domain for historical artifacts and enumerable legacy admission.

A native V2 authority view requires custody to rediscover/open the raw immutable
subject and compute `principal_account_subject` under Section 10.5. Rewrapping
or byte-copying the V1 HMAC is forbidden. If rediscovery is impossible the V2
connection is `blocked` and requires reactivation; it cannot issue V2 bindings.

## 11. Assurance labels and scheme eligibility

`brokered_count`, `brokered_semantic`, and `provider_enforced` are evidence-kind
labels, not ordered scalar levels. They are requested only through Section
8.3 predicates and satisfied only by matching evidence union variants. Static,
Basic, API-key, ordinary OAuth, service-account, and signed-request profiles
can produce brokered-count evidence. Semantic evidence additionally requires
satisfied evaluable policy instances. Workload/token exchange or external
custody can produce provider-constraint evidence only when a registry-pinned
verifier validates exact narrowed provider/issuer evidence. Credential custody,
request signing, or token exchange alone never upgrades assurance.

## 12. Migration from `0.1` to `0.2`

### 12.1 Read-old/write-new and exact legacy admission

Every enabled V2 writer emits only V2. After C3 all new activations/connections
are V2; after C4 all new bindings/leases/grants/receipts are V2. V1 executable
admission exists only through a signed `LegacyAdmissionInventoryV2`; there is
no wildcard legacy policy.

The inventory is a sorted enumerable list and may be empty after cutover to express deny-all executable V1 admission. Every admitted V1 row pins exact:
org, deployment, broker issuer and broker key ID/public-key hash, binding
canonical hash, authority-manifest hash, connection ref, provider, lane,
account commitment envelope, roles JCS hash, required/actual scopes JCS hashes,
revocation epoch, allowed contract ID/hashes, and maximum binding expiry. Each
admitted grant row additionally pins grant ref, canonical grant hash, JTI,
channel method/key thumbprint/session, exact subject hash, not-before, expiry,
maximum expiry, logical/dispatch budgets, and allowed receipt key. Inventory
metadata pins inventory ref, issuing migration authority/key, created time,
absolute expiry no later than 30 days, critical fields/extensions, and signature
domain `lattice.legacy-admission-inventory.v0.2`.

Executable admission additionally requires a signed
`LegacyInventoryDecisionV2` under domain
`lattice.legacy-inventory-decision.v0.2`. It pins an approved
`legacy_inventory` registry definition, exact inventory ref/hash, status,
approval/revocation epochs, effective/expiry interval, and decision authority/
key. Only `approved`, unexpired, epoch-current decisions admit an exact row.
Inventory self-signature is provenance, not approval. The inventory cannot
authorize new V1 activations, bindings, or grants or extend recorded expiry.
Removal, revocation, decision expiry, or inventory expiry stops V1 dispatch.

Historical V1 receipt verification is separate and non-authoritative. A
`HistoricalVerificationKeyArchiveV2` contains issuer/key ID, algorithm, raw
base64url public-key bytes, validity interval, a closed signed
`HistoricalKeyValidityEvidenceV2`, and closed signed revocation evidence tagged
`not_revoked_through` or `revoked`. Evidence repeats issuer/key identity and
names its authority/key; the archive authority signs the whole record. Validity
and revocation evidence use domains
`lattice.historical-key-validity-evidence.v0.2` and
`lattice.historical-key-revocation-evidence.v0.2`. A verifier MUST verify all
evidence/archive signatures from operator trust roots, require repeated
identity/key bytes and intervals to match, establish that receipt issue time is
within validity and before effective revocation, then verify immutable V1
receipt bytes under the V1 domain. Inventory expiry cannot disable historical
verification, and the archive cannot authorize execution. Thus cleanup never
destroys receipt verifiability.

### 12.2 Exact current Google mapping

The old connector remains contract metadata. `auth.google.workspace.oauth2@1`
maps to profile ref/version plus an approved auth-profile registry pin. The old
provider and role disappear from V2 authority; the scheme config is OAuth PKCE.
The exact connection scope set is `openid`, Gmail send, and Sheets. `openid`
is a connection-level identity requirement, while the two resource scopes
remain operation-level minimum authority. The set is normalized privately and
committed; no full scope list is public by default. The old account commitment is stored
only as tagged `LegacyCommitmentV1` for inventory/historical verification.
Native V2 activation rediscoveries the subject and computes a new V2 principal
commitment; inability to do so blocks migration.

Old revocation epoch becomes the initial V2 authority epoch only after live
custody reconciliation. Refresh/access state becomes generation 1 unless an
existing monotonic version proves greater. Lane/custody map to semantic broker/
hosted broker. The old refresh DO route becomes a private locator. Old plugin
hashes map to explicit native planner/projector registry payload digests, never
historical plugin fields.

The static Google profile's logical endpoints resolve through a signed
`DeploymentEndpointSetV2` to authorization
`https://accounts.google.com/o/oauth2/v2/auth`, token
`https://oauth2.googleapis.com/token`, revocation
`https://oauth2.googleapis.com/revoke`, principal discovery
`https://oauth2.googleapis.com/tokeninfo`, and the exact deployed universal
callback. PKCE is S256 and OAuth scheme config uses `client_secret_post`. Claims
come from the exact standing-authority/contract-set record plus the profile's
connection-level identity claims, never deployment-wide constants or caller
input. An OpenID `id_token` in the token response is privileged response
material and MUST be discarded without logging, storage, forwarding, or
evidence projection after the access-token fields are validated.

### 12.3 D1 migration

`0002_credential_plane_v2.sql` is additive and transactional. It creates:

1. `credential_plane_schema` version 2 without altering the V1 schema check;
2. signed registry definitions/decisions and deployment endpoint/config records;
3. V2 activation intents with standing-authority/contract-set and private-state
   refs, no OAuth-named generic columns;
4. authority views and lifecycle snapshots in separate tables;
5. rotation records/outboxes and migration journals;
6. V2 bindings, non-invocable node leases, exact grants, and receipts;
7. exact legacy admission inventories/items and historical verification keys;
8. cross-version state-fence projections; and
9. unique replay, JTI, effect-child, authority epoch, receipt, and CAS indexes.

D1 stores no secret envelope, workload assertion, raw claims/principal, or
remote plaintext. A Rust migration worker authenticates old D1/DO state,
rediscovers principal/claims, creates V2 commitments and authority view, and
conditionally inserts a journaled result. Missing/ambiguous state is `blocked`.
Bindings, leases, and grants are reissued, never textually converted.

### 12.4 DO migration and cross-version state fence

Each connection DO stores an atomic `CrossVersionCredentialFenceV2` understood
by every migration-capable V1 and V2 binary:

```text
phase = v1_authoritative | v2_prepared | v2_authoritative
fence_generation, v2_lease_ever_issued, v2_rotation_ever_started,
v1_leasing_disabled, active_v2_generation, CAS version
```

Migration acquires serialization, authenticates V1 state, writes/read-verifies
a V2 envelope, sets `v2_prepared`, and only then reconciles authority. Before
first V2 lease/rotation, rollback may atomically erase unreferenced prepared V2
state and return to `v1_authoritative`. The first V2 credential lease or
`provider_request_recorded` rotation atomically sets `v2_authoritative`, both
ever-bits, and permanently disables V1 leasing before returning material or
crossing the provider boundary.

After that fence, rollback to any V1 material or a fence-unaware binary is
forbidden, even if old ciphertext remains. Recovery is a V2-aware forward fix,
reactivation, revoke, or destruction. Deployment qualification MUST reject a
binary that cannot read/enforce the fence. This prevents stale-token split
brain. Old ciphertext is destroyed only after V1 grants drain and confirmation
is recorded. Grant/count DO namespaces remain version-separated; receipts stay
byte-identical.

### 12.5 Reissue, cutover, and provider switch removal

For each deployment: install signed definitions/decisions and endpoint/config
records; create exact legacy inventory; prepare/reconcile authority view;
reissue V2 binding; stop V1 issuance; drain enumerated V1 grants; issue only V2
node leases/exact grants; verify both receipt versions; expire legacy admission;
retain historical verification keys; destroy old material with confirmation.
Changed principal/claims or uncertain rotation requires reactivation.

`broker-host` removes concrete `connector_google_platform`; `connector-spec` and
`connector-codegen` emit provider-neutral profile/claim/response-firewall and
implementation pins; `broker-workers` removes Google constants, OAuth route
switches, scope columns from V2 logic, and hard-coded binding construction.
`custodian-google` remains a composition package registering signed class
payloads through generic interfaces.

## 13. Conformance

### 13.1 Synthetic provider matrix

All tests use synthetic endpoints and credentials. No live provider setup is
required.

| Synthetic provider | Required positive cases | Required negative/adversarial cases | Maximum assurance demonstrated |
| --- | --- | --- | --- |
| OAuth refresh | PKCE S256 activation, universal callback, exact derived claims, principal commitment, serialized refresh, rotating token CAS, material-only generation change | caller endpoint/scope injection, state replay, code replay, wrong redirect, claim drift, principal drift, refresh-write crash, revoke race | `brokered_count`; semantic only with a separate evaluator |
| Static auth pack | bearer, API-key header, API-key query, and Basic profiles activate through encrypted secret submission and dispatch with no public secret | secret in URL/log/Debug, wrong placement, CR/LF header, query leakage, stale lease, cross-profile envelope, unencrypted submission | `brokered_count` |
| SigV4-shaped signer | exact method/path/query/header/payload hash, broker clock, registered region/service, key generation rotation | planner auth header, caller region/service, duplicate query, unsigned required header, clock skew, altered body, old generation signature | `brokered_count`; no automatic semantic/provider claim |
| Workload/token exchange | issuer/audience/nonce validation, exchange, subject commitment, narrowed audience evidence, remote authorize-and-dispatch | arbitrary issuer, audience confusion, assertion replay, stale assertion, subject change, broad exchange mislabeled narrowed, remote evidence mismatch | `brokered_count`; `provider_enforced` only for proven narrowing |

### 13.2 Golden vectors

A conforming suite MUST publish and byte-compare:

1. every `AuthProfileDescriptor` and its JCS SHA-256;
2. each activation request/`NextAction` variant and callback-correlation record;
3. normalized authorization claims, equality, subset true/false/incomparable;
4. a connection snapshot for activation, rotation overlap, revoke, and
   destruction confirmation;
5. `BindingAttestationV2`, non-invocable `NodeLeaseV2`, exact-effect child
   `ExecutionGrantV2`, and confirmed/failed/ambiguous/rejected
   `InvocationReceiptV2` with dispatch attempts, their unsigned JCS, signature
   preimage hex where signed, signature, and full artifact hash;
6. private-envelope associated-data bytes and seal/open results using test-only
   keys, while proving public serializers cannot encode secret types;
7. all commitment contexts/vectors, all planner/auth-driver/transport paths,
   privileged response firewall extraction/scrubbing, and remote-custody codec
   AAD/HPKE/signature/replay without exposing authenticated bytes;
8. every schema and union branch in Section 20, including private workload
   submission and each signed registry class/decision; and
9. `0.1` Google tagged commitment mapping, exact inventory admission/denial,
   cross-version fence rollback/forward-fix, reissue, and immutable historical
   receipt verification.

Vectors MUST use exact integers including `0`, `1`, `255`, and
`9007199254740991`; `9007199254740992` is rejected. Required encoding
adversaries include duplicate decoded keys, unknown critical field, unknown
union kind, Unicode non-normalization, uppercase hash hex, padded base64url,
invalid timestamps, more than two retiring generations, and malformed exact
origins.

Required security adversaries include wrong org/deployment/profile/schema/
principal/custodian/lane/broker, forged dynamic source, revoked evaluator,
material-generation rollback, epoch drift, attempting to invoke a node lease,
node-lease child over-derivation, missing/wrong parent evidence, altered input
commitment, wrong receipt attempt, PoP JTI replay, transport substitution,
planner-auth smuggling, token-field response smuggling, public claim leakage,
response-triggered second dispatch, V1 fallback after the V2 fence, crash at
every activation/rotation/ledger state, and destruction without confirmation. Every failure occurs before
credential access or provider dispatch where logically possible and emits only
sanitized bounded errors.

## 14. Google readiness exit checklist

Google readiness means configuration is complete enough for a separately
approved live test; this protocol authorizes no live setup or effect.

- [ ] A static `auth.google.workspace.oauth2` version `1` auth-profile
  definition, descriptor hash, signed decision, and exact decision epochs are
  reviewed, approved, and unrevoked.
- [ ] OAuth client metadata is installed as privileged deployment config;
  client ID disclosure policy and client-secret custody are documented.
- [ ] The one exact deployed universal redirect URI is registered with Google
  and byte-equals the profile endpoint policy.
- [ ] Server-derived claims are exactly connection-level `openid` plus the Gmail
  send and Sheets operation scopes listed in Section 12.2; no implicit/extra
  scopes are requested.
- [ ] Account discovery uses the pinned driver/endpoint, produces an immutable
  subject commitment, and has multi-account ambiguity tests.
- [ ] Production token egress is restricted to the pinned token/revocation/
  discovery origins and provider egress to exact Gmail/Sheets contract origins.
- [ ] Custody root, OAuth client secret, signing keys, commitment keys, and PoP
  keys exist in approved secret bindings, have rotation/runbook ownership, and
  pass secret/log scanning.
- [ ] Universal callback reachability, TLS, state TTL, single-use claim, PKCE,
  restart-required recovery, and CSRF/replay behavior pass synthetic tests.
- [ ] A disposable test account/mailbox and spreadsheet are named by the user;
  self-recipient and cleanup constraints are recorded.
- [ ] Live consent, provider mutations, account choice, evidence path, and
  cleanup each remain explicit user-owned gates.

## 15. Security and privacy threat model

Adversaries include malicious flow input, hostile node/capsule code, a caller
attempting endpoint/claim injection, callback CSRF/replay, stolen activation
links, leaked static secrets, workload assertion replay, stale/revoked
connections, rollback to old key material, malicious provider responses,
planner/projector/auth-driver supply-chain compromise, confused custodian or
transport routing, crash-induced duplicate exchange/dispatch, and a database
reader.

The trusted computing base includes authenticated operator and host boundaries,
profile/trust registries and keys, broker kernel and linearizable ledger,
selected policy evaluators, auth driver, custodian and sealing service,
transport, deployment platform, and provider/issuer as an external oracle. A
remote custodian/transport can misuse credentials or lie unless independent
evidence exists. Open source and signed receipts improve inspection and
accountability; they do not make these parties honest.

Required mitigations include exact endpoint pins, no ambient capsule egress,
private secret/request types, encrypted authenticated secret submission,
single-use callback/workload nonces, short leases, purpose-separated keys,
constant-time secret/hash comparison, material generation and authority epoch
anti-rollback, linearizable budgets/replay, redacted errors/Debug/logs,
tenant-keyed commitments, bounded responses, registry approval/revocation,
crash-safe outboxes, and destruction confirmation.

Public APIs, telemetry, receipts, and audit summaries MUST exclude credentials,
authenticated headers/queries, client secrets, refresh/access tokens, private
keys, workload assertions, authorization codes, PKCE verifiers, raw principal
IDs, account email, provider bodies, low-entropy unkeyed hashes, and custodian
locators. An operator-only private audit store MAY retain bounded encrypted
material under a documented retention policy; ordinary flow principals never
receive it.

## 16. Explicit non-goals

`0.2` does not define a universal policy DSL, arbitrary authenticated HTTP
proxy, provider-independent scope ontology, automatic provider discovery,
credential export API, direct secret injection into flows, provider-independent
exactly-once effects, cross-broker global budgets, federation, dashboard, live
Google setup, device authorization, arbitrary dynamic endpoint registration,
proof that a provider durably performed an effect, measured execution, or proof
that broker/custodian/provider is honest.

Inbound webhook verifier material may use the same custody primitives later,
but inbound verification lifecycle is not standardized here. Lane-B local MITM
proxy behavior remains a separately typed compatibility product and cannot use
a semantic-broker connection or receipt claim.

## 17. Migration execution plan (packets C1-C5)

Dependency direction remains:

```text
connector-spec -> dag-core
connector-codegen -> connector-spec
broker-core -> no connector/provider crate
custodian-google -> broker-core
broker-host -> broker-core + connector-spec
broker-workers -> broker-core + broker-host + connector-spec + custodian-google
```

| Packet | Ownership and required closure | Tests/gate | Safe rollback |
| --- | --- | --- | --- |
| **C1 -- exact protocol kernel** | `broker-core` owns the checked-in Section 20 schemas, signed registry definitions/decisions including firewall/legacy-inventory classes, projection policy/evidence, authority view, commitments, phase-conditional rotation, response firewall types, separate non-invocable node lease, exact grant/derivation, staged receipt evidence, historical key evidence, private codecs, V1 verifier, and error mapping. | Run the normative vector verifier; validate every schema/signature/HMAC/union vector; schema negative corpus; compile-fail public secret serialization; exact-effect/attempt/commitment tests. | Disable before any V2 state/lease; V1 remains unchanged. |
| **C2 -- declarations and side-by-side state** | `connector-spec` owns profile/claim/fact/relation/response-firewall requirements and tagged implementation pins; codegen emits deterministic descriptors. `broker-workers` adds registry/config/authority/rotation/lease/grant/receipt, exact legacy inventory/decision/history, journal, and fence storage without dispatch. | Two-build equality; all nine scheme configs; registry class/decision substitution; every rotation phase shape; projection policy/evidence separation; D1 idempotency; DO fence/seal/readback crash matrix; no Google switches. | Stop worker and erase only journal-proven unreferenced V2 prepared state while fence remains `v1_authoritative`/pre-use. |
| **C3 -- generic activation/custody** | `custodian-google` registers generic auth-profile/driver/custodian definitions; Workers implements standing-authority-derived activation, universal callback, private material/workload channel, firewall, authority-view commit, durable rotation, and fence transition. | Four synthetic providers; callback/workload/secret replay; response credential smuggling; rotation uncertainty; claims privacy; remote codec replay/confidentiality/crash. | Allowed only while fence says no V2 lease/rotation ever. Otherwise block and forward-fix; old Google material MUST NOT be revived. |
| **C4 -- host derivation and evidence** | `broker-host` removes concrete Google dependency, consumes current registry decisions, issues/verifies V2 binding, separate NodeLeaseV2, exact child grant and canonical-input commitment, supports local and remote custody, and verifies V1 historical receipts separately. | Non-invocability; parent child CAS; altered-input replay; PoP/aggregate budgets; evaluator/assurance predicates and exact implementation-map key/count checks; both attempt-zero receipt stages and exact generation; signed legacy-inventory decision admission; historical key/evidence verification. | Stop new V2 derivation and drain already issued exact grants. Never cross a V2-authoritative credential fence backward. |
| **C5 -- cutover/cleanup** | Workers writes V2 only, enforces exact legacy inventory, expires executable V1 admission while retaining history keys, removes Google/OAuth switches and scope-shaped V2 columns, reconciles D1/DO, and confirms destruction. | Full schema/conformance corpus, mock Cloudflare E2E, every crash phase, rollback/forward-fix drill, secret/PII scan, hint gate, diff check, links. | Worker rollback only to a V2/fence-aware build. After first V2 lease/rotation, rollback is forward-fix only; artifacts are never down-converted. |

All packets use focused tests and no live provider. C3-C5 require fresh-context
security review. Metrics distinguish prepared versus authoritative fences,
exact legacy admission versus historical verification, V2 leases/rotations,
blocked uncertainty, retiring generations, and destruction confirmations
without private values.

## 18. Error behavior and reserved diagnostics

Credential-plane failures use the reserved `CRED` diagnostics in
`impl-docs/error-codes.md` for deployment/activation reporting and map to the
nearest sanitized `BRK` class at the broker data-plane boundary. Unknown
profile/schema/evaluator maps to `BRK004`; invalid binding/snapshot/claims maps
to `BRK109`; revoked/epoch mismatch maps to `BRK106`; private custody
unavailability maps to `BRK401`; endpoint violation maps to fatal `BRK302`.
Public errors MUST NOT identify which secret, claim, principal, endpoint
candidate, or provider response caused failure.

## 19. User-owned choices remaining

The protocol shape is frozen, but deployments still require explicit owner
choices before implementation/live use:

1. publisher, approval, and revocation trust roots for profile, driver,
   custodian, transport, and evaluator registries;
2. custody location and broker-instance operator for each real connection;
3. sealing/commitment/signing key providers, retention periods, and destruction
   evidence policy;
4. which semantic policy profiles/evaluators, if any, are approved beyond
   `brokered_count`;
5. exact legacy-admission inventory contents and duration per deployment
   (never more than 30 days);
6. production Google OAuth client/account/redirect, exact live test target,
   evidence location, and cleanup approval; and
7. whether a future device-authorization profile is needed.

None of these choices may be inferred from caller payloads or made by a
migration worker.

## 20. Normative closed JSON Schema catalogue

`impl-docs/spec/credential-plane-protocol.schema.json` is normative Draft
2020-12 JSON Schema for every V2 public artifact, union, control message, and
privileged codec named by this document.
The schema catalogue includes:

- signed registry definitions/decisions and every class payload;
- authentication profiles, all activation/scheme-config variants, deployment
  endpoint/config, standing authority, contract sets, callback records,
  `NextActionV2`, and private material submissions;
- projection policy/evidence, policy/evaluator/assurance types, authority views,
  snapshots, phase-conditional rotation, response policies, private codecs;
- bindings, node leases, exact grants/derivation, implementation refs, receipts;
- remote custody envelopes/private payloads; and
- legacy commitments, inventory/decision, historical keys, and version fences.

`impl-docs/spec/credential-plane-protocol-vectors.json` is the normative
machine-readable vector set. Its preserved pre-fix segment contains 17 complete
signed artifacts, 14 commitments, and 105 union fixtures. The normative
`lifecycle_separated_1` segment adds 18 corrected signed artifacts, nine
length-delimited commitments, 59 negatives, seven historical receipt
classifications, 18 corrected root fixtures, and 18 internal phase fixtures,
for 141 union fixtures total.
The claim list is exhaustive and MUST equal schema discovery: the
verifier fails on a missing, duplicate, extra, multi-matching, or invalid branch.
`impl-docs/spec/verify-credential-plane-vectors.py` MUST recompute and validate
all vector fields without a third-party Python package; OpenSSL is required for
Ed25519.

Every object is closed. `x-lattice-sorted` annotations are mandatory semantic
validation after JSON Schema validation and before JCS; arrays so marked are
unique and ascending by their canonical JCS bytes. Hash equality/recomputation,
class/payload equality, scheme/config one-to-one mapping, time ordering,
critical pointer resolution, signature/domain verification, status transition,
dispatch-attempt/outcome/generation conditions, and monotonic authority checks
are mandatory semantic validation in this document, not optional schema
annotations.

C0 freezes the checked-in schema, reproducible cryptographic vectors, and all
claimed root/named/nested-union fixtures. C1 MUST add complete valid fixtures
for every top-level schema and every required rejection mutation without
changing protocol bytes. C1 MUST run:

```text
python3 impl-docs/spec/verify-credential-plane-vectors.py
```

The protocol cannot be implemented from field-name prose instead of these
files.

## 21. References

- `impl-docs/spec/credential-plane-protocol.schema.json`
- `impl-docs/spec/credential-plane-protocol-vectors.json`
- `impl-docs/spec/verify-credential-plane-vectors.py`
- `impl-docs/spec/broker-protocol.md`
- `impl-docs/spec/connector-connection-bindings.md`
- `impl-docs/spec/credential-provider.md`
- `impl-docs/spec/flow-ir.md`
- `impl-docs/error-codes.md`
- `../ops/lattice-auth-broker-and-connector-capsules-design-2026-07-16.md`
- `../ops/broker-v1-evolution-and-subagent-execution-plan-2026-07-19.md`
- RFC 8785, JSON Canonicalization Scheme
- RFC 2119 and RFC 8174, requirement language
- RFC 6749 and RFC 7636, OAuth 2.0 and PKCE
- RFC 8693, OAuth 2.0 Token Exchange
