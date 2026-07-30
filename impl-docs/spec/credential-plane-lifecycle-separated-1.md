# Credential Plane 0.2 Authority Model Revision `lifecycle-separated-1`

Status: normative protocol 0.2 correction

This file freezes the corrected authority model selected by the accepted
connection-authority lifecycle ADR. It is normative together with
`credential-plane-protocol.schema.json`,
`credential-plane-protocol-vectors.json`, and
`verify-credential-plane-vectors.py`.

## Compatibility boundary

The public protocol and every corrected artifact retain
`schema_version: "0.2"`. Every corrected portable artifact also carries
`authority_model_revision: "lifecycle-separated-1"` and lists
`/authority_model_revision` in its exact sorted, duplicate-free
`critical_fields` array.

This revision does not change the meaning of any pre-fix 0.2 class. The schema
uses distinct `LS1<class>` definitions and an exact `artifact_type` plus the
critical revision. A verifier MUST NOT fill a missing revision, alias an old
domain, or convert pre-fix bytes into a corrected artifact. Once an
`AuthorityModelCutover` is accepted, pre-fix authority artifacts are rejected.
A pre-fix terminal receipt may enter only the historical verification path and
can return `valid`, `historically_ambiguous`, or `invalid`; no result from that
path is dispatch authority.

Every corrected portable schema and every nested record is closed with
`additionalProperties: false`. Identifiers, hashes, commitments, timestamps,
integers, arrays, enums, and tagged state values have the exact bounds in the
schema. Unknown direct members, algorithms, enum values, union variants,
critical pointers, and state values fail closed.

## Portable signed classes and domains

Each class has exactly one corrected domain. There is no fallback or alias:

```text
AuthorityModelCutover          lattice.credential-plane.0.2.lifecycle-separated-1.authority-model-cutover
AuthorizationObservation       lattice.credential-plane.0.2.lifecycle-separated-1.authorization-observation
ProviderGrantVersion           lattice.credential-plane.0.2.lifecycle-separated-1.provider-grant-version
ProviderGrantAdoptionRecord    lattice.credential-plane.0.2.lifecycle-separated-1.provider-grant-adoption
ConnectionAliasRecord          lattice.credential-plane.0.2.lifecycle-separated-1.connection-alias-record
StandingAuthority              lattice.credential-plane.0.2.lifecycle-separated-1.standing-authority
ContractSet                    lattice.credential-plane.0.2.lifecycle-separated-1.contract-set
PolicyInstance                 lattice.credential-plane.0.2.lifecycle-separated-1.policy-instance
RegistryDefinition             lattice.credential-plane.0.2.lifecycle-separated-1.registry-definition
RegistryDecision               lattice.credential-plane.0.2.lifecycle-separated-1.registry-decision
RegistryDecisionVector         lattice.credential-plane.0.2.lifecycle-separated-1.registry-decision-vector
CeilingAmendment               lattice.credential-plane.0.2.lifecycle-separated-1.ceiling-amendment
CorrectedBindingAttestation    lattice.credential-plane.0.2.lifecycle-separated-1.binding-attestation
DispatchAdmission              lattice.credential-plane.0.2.lifecycle-separated-1.dispatch-admission
InvocationReceipt              lattice.credential-plane.0.2.lifecycle-separated-1.invocation-receipt
LegacyAttemptInventory         lattice.credential-plane.0.2.lifecycle-separated-1.legacy-attempt-inventory
ReceiptVerificationKeyset      lattice.credential-plane.0.2.lifecycle-separated-1.receipt-verification-keyset
ReceiptKeyCompromiseRecord     lattice.credential-plane.0.2.lifecycle-separated-1.receipt-key-compromise
```

The signature preimage is `ASCII(domain) || 0x00 || JCS(artifact with only the
top-level signature omitted)`. The only accepted algorithm is Ed25519. Keys are
externally pinned; an artifact's key ID is selection metadata, not a trust
root. Every corrected artifact's top-level `key_id` MUST byte-equal
`signature.key_id`; a mismatch fails after signature verification and before
relationship use.

`AuthorizationObservation`, `ProviderGrantAdoptionRecord`,
`ConnectionAliasRecord`, `LegacyAttemptInventory`,
`ReceiptVerificationKeyset`, and `ReceiptKeyCompromiseRecord` explicitly carry
`dispatch_authority: false`. They are portable evidence or accepted control
records, never caller dispatch tokens. Alias continuity is non-authoritative:
bindings, ACLs, admissions, and receipts pin an immutable provider-grant
version and account commitment.

## Lifecycle partitions

`ProviderGrantVersion.provider_grant_lineage_ref` is stable across semantic
versions of one provider authorization lineage. A replacement authorization or
account creates a new lineage. Connection budget partitions key this stable
lineage and therefore do not reset across grant versions, bindings, aliases, or
material refreshes.

Account budget partitions are distinct and key the exact tuple:

```text
(provider, auth_profile_ref, auth_profile_version,
 account_subject_commitment)
```

They aggregate every relevant grant lineage for that account. Material
rotation cannot reset either partition.

Provider-grant acceptance and operation coverage are separate relations.
Google provider-grant acceptance is exactly `exact` in both schema and semantic
validation. The nested acceptance record byte-equals the outer grant's
provider, profile ref/version, OAuth client/audience commitment, account
commitment, stable lineage, accepted-claims profile, and accepted-claims
commitment. Its observed-claims commitment byte-equals the source observation's
normalized-claims commitment. Operation
coverage is separately `subset_or_equal`, allowing an operation to use less
than the already accepted provider grant. A profile-defined normalization is a
separate explicit acceptance relation and MUST NOT be selected for Google by
the corrected verifier.

A corrected binding is the signed intersection of flow requirements,
StandingAuthority, ContractSet, PolicyInstance, the AA-accepted
RegistryDecisionVector, immutable ProviderGrantVersion, account commitment,
and ActorConnectionAcl epoch. Its closed schema intentionally has no alias,
material generation, receipt-key generation, wall-clock bucket, package label,
or unresolved caller hash. It pins the ACL selector hash plus the exact ACL
record commitment and hash. Material generation is selected only in custody and
recorded in terminal evidence.

## Mandatory relationship graph

A valid signature proves bytes, not coherence. Before any artifact can
contribute to admission, the verifier MUST validate the complete graph and
return a named semantic rejection for any inequality:

1. `RegistryDefinition -> RegistryDecision -> RegistryDecisionVector`: tenant
   and deployment match throughout; the decision pins the definition ref,
   semantic definition hash, and exact signed definition artifact hash; one
   vector entry pins that decision's ref, exact signed hash, epoch, definition
   ref/hash, and status. Every vector entry requires backing signed definition
   and decision artifacts; the fixed positive vector therefore contains exactly
   one entry.
2. `AuthorizationObservation -> ProviderGrantVersion`: tenant, provider,
   profile ref/version, OAuth client/audience commitment, account commitment,
   normalized/observed claims commitment, observation ref, and exact signed
   observation hash match.
3. `ProviderGrantVersion -> ProviderGrantAdoptionRecord`: tenant, immutable
   grant ref and exact signed hash, stable lineage, account commitment, custody,
   source observation ref/hash, and provider authority epoch match.
4. `ProviderGrantVersion -> CorrectedBindingAttestation`: immutable grant ref
   and exact signed hash, lineage, and account commitment match. The connection
   budget partition equals the lineage. The account partition equals provider,
   profile ref/version, and account commitment.
5. `ActorConnectionAcl -> CorrectedBindingAttestation`: tenant, deployment,
   actor commitment, immutable grant ref, account commitment, epoch, selector
   hash, record commitment, and record hash match exactly.
6. Standing, contract, policy, registry definition/decision/vector, binding,
   and admission tenant/deployment partitions match. Binding head references
   name exact signed JCS hashes and its registry vector epoch equals the
   accepted vector.
7. `CorrectedBindingAttestation -> DispatchAdmission`: binding ref/hash, grant
   ref/hash, lineage/account, actor and complete ACL pins, both budget
   partitions, and registry vector ref/hash/epoch match. The signed run-budget
   snapshot contains the matching lineage and account ledger partitions.
8. The admitted contract ID/hash is an exact member of the pinned ContractSet.
9. `DispatchAdmission -> InvocationReceipt`: tenant, deployment, run, effect,
   attempt, admission ref/hash, grant ref/hash, lineage/account, actor and ACL
   pins, binding, canonical input, registry vector, and all current-head hashes
   match. Receipt material generation remains terminal custody evidence and is
   never binding identity.

Every `*_hash` edge above is SHA-256 of the complete signed JCS bytes of its
target. No verifier may replace these equalities with presence checks, signature
success, caller hints, aliases, or default-filled fields. The conformance
verifier's relationship function returns stable reason names such as
`grant_nested_accepted_claims_mismatch`, `binding_acl_selector_mismatch`,
`admission_binding_hash_mismatch`, and `receipt_admission_hash_mismatch`.

Security-relevant collections are unique and ascending by JCS bytes. This rule
applies to critical fields, contract entries, registry-vector entries, receipt
keys, and both connection and account run-ledger partitions. The
`x-lattice-sorted` annotation is mandatory semantic validation, not commentary.

## Internal records

The following `LS1` definitions are closed internal records, have the required
critical revision, `portable: false`, and `dispatch_authority: false`, and
cannot be accepted at a portable dispatch-authority boundary:

```text
AuthenticatedActorContext
ActorConnectionAcl
MaterialState
CustodyReservation
CustodyCancellationEvidence
CrossingAuthorization
CrossingEvidence
RunLedger
EffectState
AttemptState
```

`AuthenticatedActorContext` is minted by the authentication boundary after
caller identity headers are stripped. `ActorConnectionAcl` is current
AdmissionAuthority state with a monotonic epoch. `MaterialState` is exclusively
credential-custody state. Run, effect, and attempt records are exclusively
AdmissionAuthority state. Every run-ledger connection/account partition carries
its ceiling, consumed count, reserved count, and ceiling-amendment head.
`EffectState` and `AttemptState` are closed nine-branch `oneOf` unions. Their
nonterminal phases are `proposed`, `reserved`, `admitted`,
`custody_reserved_non_exportable`, `crossing_risk_started`, and
`provider_crossing_observed`. Three terminal branches freeze the boundary:
pre-crossing failure requires cancellation evidence and prohibits crossing
authorization/evidence; crossing-risk terminal ambiguity requires crossing
authorization but permits crossing evidence to be absent; provider-observed
terminal success/failure/ambiguity requires the complete crossing chain.
Earlier branches prohibit every future-phase field. All branches bind tenant,
deployment, run, logical effect, and canonical input. D1 or a provider record
cannot issue or mutate an admission.

`CrossingAuthorization` is not general dispatch authority. It is an AA-owned,
short-lived, exact, one-use authorization for custody to cross the risk
boundary for one already-issued admission and one non-exportable custody
reservation.

## Two-phase crossing

The only valid AA-to-custody sequence is:

1. AA atomically checks corrected artifacts and current heads, reserves the
   exact effect and attempt under the run ledger, and signs one short-lived
   `DispatchAdmission`.
2. Custody verifies that admission and durably reserves the exact provider
   grant/material path while material remains non-exportable. It returns a
   reservation commitment but cannot yet export or invoke.
3. AA verifies the reservation commitment, atomically records
   `crossing_risk_started`, and issues one exact one-use
   `CrossingAuthorization` bound to both commitments.
4. Custody durably consumes that authorization before exporting usable bearer
   material or initiating a remote provider operation, records the leased
   material generation, and performs only the pinned operation.
5. Provider crossing evidence and a terminal AA state produce immutable,
   byte-identical receipt bytes.

Before step 3, AA may release once only with durable custody cancellation proof
that material remained non-exportable and no remote operation started. At or
after step 3, a crash or missing acknowledgement is conservatively
`terminal_ambiguous` and never refunds by assumption. Redelivery returns the
same terminal bytes and consumes neither another effect nor another provider
call.

## Legacy inventory and receipt history

`LegacyAttemptInventory` is AA-owned and seals one completeness-checked,
AA-accepted staging snapshot. `source_attempt_count` must equal the exact entry
count. Every entry is `terminal_success`, `terminal_failure`,
`terminal_ambiguous`, or `quarantined`, has `dispatch_enabled: false`, and pins
exact `source_kind`, `source_record_ref`, and `source_record_hash`. The snapshot
ref/hash and the complete source set must equal the AA-accepted staging
snapshot. Cutover is blocked unless the inventory is complete and sealed; zero
observed effects alone is not sufficient.

A `ReceiptVerificationKeyset` enumerates `valid`,
`historically_ambiguous`, and `invalid`. A `ReceiptKeyCompromiseRecord`
classifies receipts before the affected interval as `valid`, receipts in an
affected or unknown interval as `historically_ambiguous`, and signatures after
revocation as `invalid`. Signed history is append-only and cannot erase or
silently reclassify old bytes. Historical verification selects a key by exact
receipt issuer/key ID, requires the receipt signing time inside that key's
validity interval, verifies the old-domain signature, then applies the signed
compromise interval and revocation time. The result always carries
`dispatch_authority: false`. The distinct
`LS1HistoricalInvocationReceiptVerificationOnly` schema accepts canonical
fractional timestamps without broadening the pre-fix executable receipt schema.
Timestamp comparison parses canonical RFC3339 UTC instants into integer seconds
plus nanoseconds; lexical string ordering is forbidden. Fractional boundary vectors use one, two, four, and nine digits.

## Commitment construction and privacy

The corrected commitment contexts are exactly:

```text
lattice.credential-plane.0.2.lifecycle-separated-1.commitment.account-subject
lattice.credential-plane.0.2.lifecycle-separated-1.commitment.normalized-claims
lattice.credential-plane.0.2.lifecycle-separated-1.commitment.oauth-client-audience
lattice.credential-plane.0.2.lifecycle-separated-1.commitment.provider-grant-ref
lattice.credential-plane.0.2.lifecycle-separated-1.commitment.actor-subject
lattice.credential-plane.0.2.lifecycle-separated-1.commitment.dispatch-admission
lattice.credential-plane.0.2.lifecycle-separated-1.commitment.canonical-input
lattice.credential-plane.0.2.lifecycle-separated-1.commitment.provider-response
lattice.credential-plane.0.2.lifecycle-separated-1.commitment.alias-intent
```

The two-stage HMAC-SHA-256 primitive from protocol 0.2 is retained. For each
context, the ordered fields are exactly `tenant_id`, `issuer`, `provider`,
`auth_profile_ref`, `oauth_client_id`, `audience`, `artifact_ref`, and
`purpose`. Each field is encoded as:

```text
u32be(name_length) || ASCII(name) || u64be(value_length) || value_bytes
```

Their concatenation is `framed_context`. The opening-key preimage is:

```text
"lattice.commitment-opening.v0.1" || 0x00 ||
u32be(context_length) || ASCII(context) ||
u64be(framed_context_length) || framed_context
```

The commitment preimage is:

```text
"lattice.commitment.v0.1" || 0x00 ||
u32be(context_length) || ASCII(context) ||
u64be(framed_context_length) || framed_context ||
u64be(private_value_length) || private_value
```

The scoped key is HMAC-SHA-256 of the opening-key preimage under the tenant
root; the commitment is HMAC-SHA-256 of the commitment preimage under that
scoped key. The context and purpose are identical. No context may be reused as
an alias. The fixed vectors prove that identical private bytes produce nine
different commitments.

Raw subjects, normalized claims, credentials, tokens, provider bodies, and
canonical input values MUST NOT appear in corrected public artifacts, logs,
errors, or telemetry. Only keyed commitments, bounded public metadata, and
sanitized evidence may cross the custody boundary. Commitment vectors contain
synthetic private bytes solely as verifier inputs; they are not public
artifact examples.

## Conformance packet

The machine packet contains one fixed-key positive vector for each of the 18
signed corrected classes, one vector for each of the nine commitment contexts,
59 negative vectors, seven historical classification vectors, and 18 positive
internal phase-union fixtures. Negatives
include a corrected artifact genuinely signed and verified under an old domain
but rejected under the mandatory corrected domain. They also cover missing/unknown
critical revision, revision omitted from critical fields, unknown members,
unsorted/duplicate critical fields, alias/material-generation injection into a
binding, and byte-tampering of lineage, account commitment, ACL epoch, accepted
registry vector, admission commitment, and material generation. Fixed-key
re-signed negatives prove relationship failures across the complete graph,
legacy source/count/snapshot mismatches, and unsorted contract, registry,
keyset, run-ledger, and critical-field collections.

The verifier independently checks strict JSON parsing, closed schemas, root
union uniqueness, supported schema keywords, all `x-lattice-sorted`
annotations, critical pointers, JCS bytes, class/domain bijection, public-key
derivation from the advertised seed, deterministic re-signing, independent
Ed25519 verification, hashes, commitment framing, the complete relationship
graph, every negative, and actual historical verification-only classification.
