Status: Draft
Purpose: architecture-decision / normative spec
Owner: Runtime / Connectors / Security
Last reviewed: 2026-07-19

# Lattice Broker Protocol (0.1)

This document defines the Broker V1 wire artifacts, canonical encoding,
admission semantics, budget ledger, assurance vocabulary, receipts, and public
error classes. The key words MUST, MUST NOT, REQUIRED, SHALL, SHALL NOT,
SHOULD, SHOULD NOT, RECOMMENDED, MAY, and OPTIONAL are to be interpreted as
described in RFC 2119 and RFC 8174 when, and only when, they appear in all
capitals.

The schema version for every artifact in this document is exactly `0.1`.
Runtime implementation is outside this packet.

## 1. Scope and security statement

The brokered path authorizes one semantic operation contract against one bound
connection. It keeps provider credentials and authenticated provider requests
outside flow and node code, applies count, expiry, replay, revocation, contract,
and connection checks in a broker-owned ledger, and signs bounded receipts.

Given a trusted receipt-signing key and an honest implementation of the named
cryptographic algorithms, a valid `InvocationReceipt` proves that the named
broker signed an integrity-protected assertion that:

1. an authenticated trusted host asserted the receipt's exact deployment,
   bundle, Flow IR, binding lock, flow, node, run, operation, connection, and
   logical-effect scope;
2. the broker admitted or rejected that assertion under the committed grant,
   policy, contract, implementation, and ledger state;
3. the broker processed the invocation through the states represented by the
   receipt; and
4. when `claims.provider_dispatch_observed` is `true`, the broker observed its
   own dispatcher cross the provider-dispatch boundary for that attempt.

The receipt does not prove that guest or node code executed, that any particular
machine image was measured, that adapter or policy computation is verifiable,
that a platform terminated or metered an execution in a particular way, that a
remote provider accepted or durably stored an effect, or that the trusted host
or broker is honest. A malicious broker can sign false statements or misuse a
credential it controls. Provider responses are trusted oracle inputs unless the
provider independently signs them.

All workload, scope, and identity fields used for admission MUST come from the
trusted host's scheduler/task-local scope. Guest or node code MUST NOT supply or
override `org_id`, `principal`, deployment, bundle, Flow IR, binding-lock, flow,
node, alias, run, attempt, grant, operation, contract, connection, activation,
or logical-effect identity. If an untrusted payload repeats such a field, the
host MUST ignore it or compare it to the host-owned value and fail closed on a
mismatch.

```text
                         control-plane trust
  bundle + Flow IR ----> binding/standing envelope
          |                         |
          v                         v
+-------------------+ authenticated assertion + PoP + opaque grant ref
| trusted host      |----------------------------------------------+
| scheduler + shim  |                                              |
+---------+---------+                                              v
          | untrusted input                              +-------------------+
          v                                              | broker core       |
+-------------------+ semantic invocation only           | policy + ledger   |
| flow / node code  |----------------------------------->| custodian + signer|
| no credentials    |<-----------------------------------| planner/dispatcher|
+-------------------+ typed result + receipt ref          +---------+---------+
                                                                  |
                                                        authenticated dispatch
                                                                  |
                                                                  v
                                                        +-------------------+
                                                        | remote provider   |
                                                        | trusted oracle    |
                                                        +-------------------+
```

The trusted-host boundary is a software/process authentication boundary in the
initial profile. Native deployments may use mTLS, SPIFFE, or a deployment key.
Workers may use a private service binding plus a deployment signing key; host
shim and flow Wasm can share an isolate, so this is software/import separation,
not measured execution.

## 2. Common protocol conventions

### 2.1 Identifiers, hashes, times, and principals

- Content hashes MUST be `sha256:` followed by exactly 64 lowercase hexadecimal
  digits. They hash the bytes named by the field. Protocol-owned JSON uses JCS;
  externally owned artifact hashes such as `flow_ir_hash`, `binding_lock_hash`,
  and `bundle_id` retain the exact byte/hash rules of their owning specs.
- HMAC commitments MUST be `hmac-sha256:` followed by exactly 64 lowercase
  hexadecimal digits.
- Opaque IDs and references MUST contain 1 to 256 printable ASCII characters,
  MUST NOT contain credentials, and MUST have at least 128 bits of
  cryptographically random entropy when unpredictability is required.
- Contract IDs, vocabulary IDs, field names, and enum values MUST be ASCII.
  Contract IDs are versioned, for example
  `connector.google.gmail.send_message@1`.
- Timestamps MUST use the canonical UTC RFC 3339 form
  `YYYY-MM-DDTHH:MM:SSZ`, with no fractional seconds or offset spelling.
- A `PrincipalRef` has exactly `{"kind": ..., "id": ...}` in `0.1`.
  `kind` is one of `user`, `service`, `deployment`, or `broker`; `id` is an
  opaque, non-secret identifier. Unknown principal kinds fail closed.
- `org_id` and `principal` are routing and audit identity, not authority by
  themselves. They MUST be authenticated and policy-bound.

Unless a field says otherwise, strings are 1 to 1024 UTF-8 bytes, arrays contain
at most 1024 elements, map keys are unique, and counts are non-negative
integers. Security-relevant identifiers MUST be compared byte-for-byte after
schema validation; implementations MUST NOT apply case folding or Unicode
normalization.

### 2.2 Extension and version rule

Each artifact contains `critical_fields`, a sorted, duplicate-free array of
JSON Pointers. A producer MUST list every extension field whose understanding
is required to preserve the producer's security semantics. A verifier MUST fail
closed if any pointer names a field it does not understand, if a pointer is
invalid or absent from the artifact, or if an unknown security vocabulary,
enum value, tagged-union kind, or schema version is encountered. Unknown fields
not named as critical MAY be retained and ignored, but MUST remain in canonical
hash and signature inputs. Ignoring an unknown field can never widen authority.
A security-relevant addition MUST NOT be introduced as non-critical.

Every object described below inherits this rule. Duplicate fields are rejected
before this rule is evaluated.

## 3. Canonical encoding ADR

### 3.1 Decision

All protocol JSON, operation input, protocol-owned hash preimages that name
JSON, signature payloads, and fixed byte-oriented adapter ABI payloads MUST use
the JSON Canonicalization Scheme (JCS), RFC 8785. A protocol hash or signature
MUST consume the UTF-8 JCS bytes, not source JSON, a pretty-printed form, or an
implementation's default map serialization. This does not recanonicalize an
existing Flow IR, binding-lock, or bundle artifact: fields that reference those
artifacts use the exact stored-byte hashes defined by their owning specs.

A decoder MUST:

1. reject a UTF-8 BOM, invalid UTF-8, non-shortest UTF-8, unescaped control
   characters, and lone UTF-16 surrogate code points;
2. reject duplicate object member names, including names that become equal
   after JSON escape decoding (for example `"a"` and `"\u0061"`);
3. reject trailing data and non-JSON extensions such as comments;
4. preserve decoded Unicode scalar values exactly, with no NFC, NFD, case, or
   compatibility normalization; and
5. sort object member names by UTF-16 code units as RFC 8785 requires.

JSON numbers MUST be representable as finite IEEE 754 binary64 values and MUST
be serialized with RFC 8785's ECMAScript number algorithm. NaN, positive or
negative infinity, overflow to infinity, and a nonzero token that underflows to
zero MUST be rejected. A schema field declared as an integer has an `i64` hard
bound and MUST also round-trip exactly through binary64 JCS. Consequently, the
portable exact-integer range is `-9007199254740991` through
`9007199254740991`; an implementation MUST reject larger-magnitude integer
values even when they fit `i64`. A field declared as a floating-point value is
bounded by finite binary64, from the smallest nonzero subnormal magnitude
through `1.7976931348623157e+308`, plus zero. Protocol budgets and ordinals use
non-negative integer subranges, not floating point.

### 3.2 Size limits

Limits are measured after UTF-8 decoding where stated and before allocating an
unbounded container. A deployment MAY advertise smaller limits; it MUST NOT
silently truncate.

| Item | `0.1` maximum |
| --- | ---: |
| Nesting depth | 64 containers |
| Object members | 4,096 per object |
| Array elements | 65,536 per array |
| Object member name | 256 UTF-8 bytes |
| String value | 256 KiB UTF-8 bytes |
| Canonical operation input | 256 KiB |
| Canonical request plan or bounded response handed to an adapter | 256 KiB |
| `FlowAuthorityManifest` or `BindingAttestation` | 1 MiB canonical bytes |
| Authoritative `ExecutionGrant` record | 64 KiB canonical bytes |
| `InvocationReceipt` | 64 KiB canonical bytes |

Exceeding any limit is an encoding or admission failure and MUST occur before
provider dispatch.

### 3.3 Normative canonicalization vectors

The `Input JSON` column is ASCII source text. JSON escapes are decoded before
canonicalization. Hex is the complete canonical byte sequence; SHA-256 is the
lowercase digest of exactly those bytes. In the two Unicode vectors, the first
value is precomposed U+00E9 and the second is U+0065 followed by U+0301. They
MUST remain distinct: input is not normalized and decoded UTF-8 bytes are
preserved.

| Case | Input JSON | Canonical bytes (hex) | SHA-256 |
| --- | --- | --- | --- |
| Empty object | `{}` | `7b7d` | `44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a` |
| Empty array | `[]` | `5b5d` | `4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945` |
| Object ordering | `{"b":1,"a":2}` | `7b2261223a322c2262223a317d` | `d3626ac30a87e6f7a6428233b3c68299976865fa5508e4267c5415c76af7a772` |
| Nested ordering | `{"z":{"b":false,"a":null},"a":[3,2,1]}` | `7b2261223a5b332c322c315d2c227a223a7b2261223a6e756c6c2c2262223a66616c73657d7d` | `96f8d6beb512491404b2479bff1cf19741b07dedb4ad68e7a9ab9e1b0a028ca2` |
| String escaping | `{"s":"line\nquote\""}` | `7b2273223a226c696e655c6e71756f74655c22227d` | `6f122115d79068199d627109d4b9bd60f52696b5dccbc933aa0cf9cd470ffb4d` |
| Precomposed Unicode | `{"u":"\u00e9"}` | `7b2275223a22c3a9227d` | `606ffff9f63ae3058a32788b12169fffef7f4f86e8e34e22cf3056949620ab37` |
| Decomposed Unicode | `{"u":"e\u0301"}` | `7b2275223a2265cc81227d` | `6a5fd66a30d6c934c359406ff8dffdca28f728ed12ae329bccebf933570da4be` |
| Negative zero | `{"n":-0}` | `7b226e223a307d` | `f3013f933b9fb80ab6d995e7ad9da36f683837ba1d81e950c943d40111eac2f0` |
| Fraction | `{"n":1.5}` | `7b226e223a312e357d` | `cb14d55cfe562fd6592d919f5dfacfa8708687b746a1d110c6dd5529c410e772` |
| Exact integer maximum | `{"n":9007199254740991}` | `7b226e223a393030373139393235343734303939317d` | `e1da48c6a6089f06ecb4e0a2259e658e3786b2420f52baccdf929ec6460d7b41` |
| Exact integer minimum | `{"n":-9007199254740991}` | `7b226e223a2d393030373139393235343734303939317d` | `d49d713821fc149f81ef6ca8054beeba696f5da052f0ab3e2d773808c5a9d625` |
| Finite binary64 maximum | `{"n":1.7976931348623157e308}` | `7b226e223a312e37393736393331333438363233313537652b3330387d` | `9599e0f9672bfa5d654b432fa5f9b7ee04f35c607b3a3ff01ce97b070f8f2648` |
| UTF-16 key ordering | `{"\u20ac":1,"1":3,"\r":2}` | `7b225c72223a322c2231223a332c22e282ac223a317d` | `c09bf1b4a80778801254479094b6c4055a41ffbcc98fd721d4a89c27c9d465fa` |
| Empty nested containers | `{"b":{},"a":[]}` | `7b2261223a5b5d2c2262223a7b7d7d` | `9959f7ea5ff37e0cf81634a894845a335eb6e26fbad0877944e9bc009b4f0644` |

The following are mandatory rejection cases and produce no canonical bytes:
duplicate decoded key names, an unpaired surrogate, `9007199254740992` in an
integer-typed field, `1e400`, `1e-400`, NaN, and infinity.

## 4. Artifact schemas

### 4.1 `FlowAuthorityManifest`

This is an optional sibling artifact for a connector-bearing flow. The bundle
manifest references its canonical byte length and `sha256:...` hash; the
artifact itself binds the exact serialized Flow IR through `flow_ir_hash`.
Connector-free flows omit it. It declares a maximum, not consent or a grant.

```json
{
  "schema_version": "0.1",
  "critical_fields": [],
  "org_id": "org_opaque",
  "principal": {"kind": "service", "id": "bundle-builder"},
  "flow_ir_hash": "sha256:...",
  "nodes": {
    "confirm_candidate": {
      "node_id": "node_opaque",
      "operations": [
        {
          "contract_id": "connector.google.gmail.send_message@1",
          "contract_hash": "sha256:...",
          "call_budget": {
            "max_logical_calls": 1,
            "max_dispatch_attempts_per_call": 1
          },
          "minimum_assurance": "brokered_count",
          "required_attenuations": [],
          "connection_aggregate_key": "google-primary"
        }
      ]
    }
  },
  "aggregate_ceilings": {
    "flow": {"max_logical_calls": 4},
    "connections": {
      "google-primary": {"max_logical_calls": 3}
    }
  }
}
```

| Field | Normative constraint |
| --- | --- |
| `schema_version` | REQUIRED and exactly `0.1`. |
| `critical_fields` | REQUIRED; follows Section 2.2. |
| `org_id`, `principal` | REQUIRED build provenance; MUST be authenticated when the artifact is admitted. They are not guest supplied. |
| `flow_ir_hash` | REQUIRED hash of the exact Flow IR artifact bytes used by the bundle. |
| `nodes` | REQUIRED map keyed by validated, unique `NodeIR.alias`; at most 4,096 entries and non-empty for a present manifest. |
| `node_id` | REQUIRED exact `NodeIR.id` corresponding to the map key. |
| `operations` | REQUIRED non-empty array, unique by `contract_hash` within a node. Every operation MUST exist in that node's validated connector metadata. |
| `contract_id`, `contract_hash` | REQUIRED stable major-versioned semantic contract and its canonical contract hash. Both MUST agree with the registry descriptor. |
| `call_budget.max_logical_calls` | REQUIRED integer from 1 through `9007199254740991`, scoped per `run_id` and node. |
| `call_budget.max_dispatch_attempts_per_call` | REQUIRED integer from 1 through 255. |
| `minimum_assurance` | REQUIRED value from Section 8. |
| `required_attenuations` | REQUIRED sorted, duplicate-free array of versioned profile IDs. Unknown required profiles fail closed. It is empty for count-only V1. |
| `connection_aggregate_key` | OPTIONAL author-defined ASCII key connecting this operation to one aggregate ceiling; it is not a provider account or connection reference. |
| `aggregate_ceilings.flow` | OPTIONAL object with positive `max_logical_calls` for all brokered nodes in one run. |
| `aggregate_ceilings.connections` | OPTIONAL map from declared aggregate key to a positive `max_logical_calls` shared by operations later bound to the same key. |

Derivation MUST be pure from validated IR and connector metadata. Unknown node
IDs, aliases, contracts, assurances, required attenuation profiles, fields
listed in `critical_fields`, or schema versions fail closed. Unknown
non-critical fields follow Section 2.2 and cannot widen the declared maximum.
Aggregate-ceiling enforcement MAY be broker/operator policy in `0.1`, but a
producer and verifier MUST preserve and hash the declarations.

### 4.2 `BindingAttestation`

A binding attestation is a broker-signed, public, credential-free statement
that one exact connection/account selection satisfied operation and scope
requirements at observation time. It extends the existing lock model; it does
not create a second connection-selection precedence.

```json
{
  "schema_version": "0.1",
  "critical_fields": [],
  "org_id": "org_opaque",
  "principal": {"kind": "broker", "id": "broker-primary"},
  "issuer": "https://broker.example",
  "broker_key_id": "broker-key-7",
  "lane": "semantic_broker",
  "connection_ref": "conn_opaque_random",
  "provider": "google",
  "account_commitment": {
    "alg": "hmac-sha256",
    "key_id": "tenant-commit-v3",
    "value": "hmac-sha256:..."
  },
  "roles": {
    "outbound_auth.google_workspace_auth": "oauth2.access_token"
  },
  "scope_alignment": {
    "required_scopes": ["https://www.googleapis.com/auth/gmail.send"],
    "actual_scopes": ["https://www.googleapis.com/auth/gmail.send"],
    "satisfied": true
  },
  "supported_contracts": [
    {
      "contract_id": "connector.google.gmail.send_message@1",
      "contract_hash": "sha256:...",
      "observed_plugin_module_sha256": "sha256:...",
      "attenuation_profiles": ["invocation_count@1"]
    }
  ],
  "endpoint_origins": ["https://gmail.googleapis.com"],
  "revocation_epoch": 4,
  "observed_at": "2026-07-19T12:00:00Z",
  "expires_at": "2026-07-19T12:15:00Z",
  "signature": {
    "alg": "Ed25519",
    "key_id": "broker-key-7",
    "value": "base64url-no-pad"
  }
}
```

| Field | Normative constraint |
| --- | --- |
| `schema_version`, `critical_fields`, `org_id`, `principal` | REQUIRED; `principal.kind` MUST be `broker` and MUST identify the issuer's authenticated signing principal. |
| `issuer`, `broker_key_id` | REQUIRED trust-registry issuer and active verification key ID. The `(issuer, broker_key_id)` pair MUST be pinned by the binding lock. |
| `lane` | REQUIRED and exactly `semantic_broker` for this protocol. A connection MUST be bound to exactly one product lane. |
| `connection_ref` | REQUIRED opaque, immutable broker reference; never selected by mutable display text and never supplied by guest code. |
| `provider` | REQUIRED registry provider ID. |
| `account_commitment` | REQUIRED tenant-scoped commitment to the immutable provider account subject. It MUST follow Section 10. |
| `roles` | REQUIRED non-empty map from declared role ref to exact handle kind. It MUST contain every role required by each admitted contract. |
| `scope_alignment` | REQUIRED. Scope arrays MUST be sorted and duplicate-free. `satisfied` MUST be `true`, and each required scope MUST be satisfied under the provider-specific scope relation. Exact equality is not implied. |
| `supported_contracts` | REQUIRED non-empty array unique by `contract_hash`; each entry binds contract ID/hash and supported attenuation profiles. |
| `observed_plugin_module_sha256` | OPTIONAL informational lock-time provenance only. It MUST NOT pin runtime admission to one rebuild. The receipt records the exact executed implementation. |
| `endpoint_origins` | REQUIRED sorted, duplicate-free HTTPS origins approved for the contracts; paths, queries, userinfo, fragments, and wildcard hosts are forbidden. |
| `revocation_epoch` | REQUIRED non-negative integer. Runtime MUST compare it with live connection and policy state. |
| `observed_at`, `expires_at` | REQUIRED freshness interval with `expires_at > observed_at`. Runtime MUST reject an expired attestation and recheck drift/revocation. |
| `signature` | REQUIRED Ed25519 signature. `key_id` MUST equal `broker_key_id`; `value` is canonical base64url without padding. |

The signature preimage is the ASCII domain
`lattice.binding-attestation.v0.1` followed by one zero byte and the JCS bytes
of the entire object with only top-level `signature` omitted. Unknown schema
versions, issuer keys, lanes, contracts, critical fields, required profiles,
or security vocabularies fail closed. Unknown non-critical fields remain in the
signature preimage. A plugin module observed at lock time is informational;
runtime admission MUST resolve the contract through the trust registry and
require the selected implementation to be approved and unrevoked.

### 4.3 `ExecutionGrant`

The JSON below is the broker's authoritative ledger record. The caller receives
only `grant_ref` plus channel-bound proof-of-possession context. It is not a
portable signed bearer token and MUST NOT be accepted from an unauthenticated
channel as authority.

```json
{
  "schema_version": "0.1",
  "critical_fields": [],
  "org_id": "org_opaque",
  "principal": {"kind": "deployment", "id": "deployment-7"},
  "grant_ref": "grant_opaque_random",
  "issuer": "https://broker.example",
  "audience": "broker-execution",
  "channel_binding": {
    "method": "deployment_key",
    "key_thumbprint": "sha256:...",
    "session_id": "session_opaque_random"
  },
  "subject": {
    "kind": "flow_node_run",
    "bundle_id": "bundle_opaque",
    "flow_ir_hash": "sha256:...",
    "binding_lock_hash": "sha256:...",
    "flow_id": "flow_opaque",
    "node_id": "node_opaque",
    "node_alias": "confirm_candidate",
    "run_id": "run_opaque_random"
  },
  "operation_contract": "connector.google.gmail.send_message@1",
  "contract_hash": "sha256:...",
  "connection_ref": "conn_opaque_random",
  "budgets": {
    "logical_calls": 1,
    "dispatch_attempts_per_call": 1
  },
  "aggregate_budgets": {
    "flow_logical_calls": 4,
    "connection_logical_calls": 3
  },
  "minimum_assurance": "brokered_count",
  "required_attenuations": [],
  "revocation_epoch": 4,
  "not_before": "2026-07-19T12:00:00Z",
  "expires_at": "2026-07-19T12:05:00Z",
  "jti": "grant-record-random"
}
```

| Field | Normative constraint |
| --- | --- |
| `schema_version`, `critical_fields`, `org_id`, `principal` | REQUIRED. `principal.kind` MUST be `deployment`; all values come from the authenticated trusted host/control plane, never the node. |
| `grant_ref` | REQUIRED opaque random reference to the authoritative record. Possession without valid channel proof conveys no authority. |
| `issuer`, `audience` | REQUIRED; audience is exactly `broker-execution` in `0.1`. |
| `channel_binding.method` | REQUIRED and one of `mtls`, `spiffe`, `deployment_key`, or `workers_private_binding`. Unknown methods fail closed. |
| `channel_binding.key_thumbprint`, `session_id` | REQUIRED pins to the authenticated short-lived channel/session. The proof key MUST be unavailable to guest code. |
| `subject` | REQUIRED tagged union. `flow_node_run` is the only subject kind defined in `0.1`; its fields are all REQUIRED and host-owned. |
| `operation_contract`, `contract_hash` | REQUIRED exact operation contract pair from the authority manifest and trust registry. |
| `connection_ref` | REQUIRED exact connection from a fresh binding attestation. |
| `budgets.logical_calls` | REQUIRED integer from 1 through the manifest's per-run, per-node maximum. |
| `budgets.dispatch_attempts_per_call` | REQUIRED integer from 1 through 255 and no greater than the manifest maximum. |
| `aggregate_budgets` | OPTIONAL narrowed flow/connection ceilings. Absence does not disable any operator ceiling. Enforcement MAY be policy in `0.1`. |
| `minimum_assurance` | REQUIRED level from Section 8; grant issuance MUST NOT weaken the manifest requirement. |
| `required_attenuations` | REQUIRED sorted subset/intersection that satisfies every manifest-required profile. Unknown required profiles fail closed. |
| `revocation_epoch` | REQUIRED and MUST equal current authoritative policy/connection epoch at admission. |
| `not_before`, `expires_at` | REQUIRED short interval with `expires_at > not_before`; it MUST fit the broker's advertised maximum grant lifetime. The standing envelope, not this grant, survives long waits/checkpoints. |
| `jti` | REQUIRED unique random ledger-record ID, distinct from `grant_ref`. |

The `subject` union is deliberately isolated from common grant claims. Future
schema revisions may add `cli_principal`, `mcp_session`, or `agent_session`
variants without changing or reinterpreting `flow_node_run`. A `0.1` verifier
MUST reject unknown subject kinds; a later verifier can add a variant while
continuing to verify `flow_node_run` byte-for-byte. Subject-specific fields MUST
NOT be moved to the grant top level.

Unknown schema versions, subject kinds, channel-binding methods, assurances,
critical fields, required attenuation profiles, and security vocabularies fail
closed. Unknown non-critical fields follow Section 2.2 and remain in the grant
record hash. Grant issuance is just in time against the durable standing
envelope and fresh binding; checkpoint resume obtains a new grant for the same
deterministic effect identity.

### 4.4 `InvocationReceipt`

```json
{
  "schema_version": "0.1",
  "critical_fields": [],
  "org_id": "org_opaque",
  "principal": {"kind": "broker", "id": "broker-primary"},
  "issuer": "https://broker.example",
  "broker_key_id": "broker-key-7",
  "grant_hash": "sha256:...",
  "policy_hash": "sha256:...",
  "contract_hash": "sha256:...",
  "plugin_module_sha256": "sha256:...",
  "plugin_trust_tier": "lattice_first_party",
  "bundle_id": "bundle_opaque",
  "flow_ir_hash": "sha256:...",
  "binding_lock_hash": "sha256:...",
  "flow_id": "flow_opaque",
  "run_id": "run_opaque_random",
  "node_id": "node_opaque",
  "node_alias": "confirm_candidate",
  "logical_effect_id": "sha256:...",
  "dispatch_attempt": 1,
  "connection_commitment": {
    "alg": "hmac-sha256",
    "key_id": "tenant-commit-v3",
    "verification_tier": "verifier_with_disclosure",
    "value": "hmac-sha256:..."
  },
  "canonical_input_commitment": {
    "alg": "hmac-sha256",
    "key_id": "tenant-commit-v3",
    "verification_tier": "verifier_with_disclosure",
    "value": "hmac-sha256:..."
  },
  "request_plan_hash": "sha256:...",
  "authority_facts_hash": "sha256:...",
  "budget_before": 1,
  "budget_after": 0,
  "provider_request_id": "sanitized-optional-id",
  "response_commitment": {
    "alg": "hmac-sha256",
    "key_id": "tenant-commit-v3",
    "verification_tier": "broker_only",
    "value": "hmac-sha256:..."
  },
  "outcome": "confirmed",
  "claims": {
    "trusted_host_scope_authenticated": true,
    "broker_admission_enforced": true,
    "provider_dispatch_observed": true,
    "remote_durable_state_proven": false,
    "verifiable_execution_proven": false
  },
  "issued_at": "2026-07-19T12:00:02Z",
  "signature": {
    "alg": "Ed25519",
    "key_id": "broker-key-7",
    "value": "base64url-no-pad"
  }
}
```

| Field | Normative constraint |
| --- | --- |
| `schema_version`, `critical_fields`, `org_id`, `principal`, `issuer`, `broker_key_id` | REQUIRED. Principal kind is `broker`; issuer/key MUST resolve in the verifier trust store. |
| `grant_hash` | REQUIRED SHA-256 of the authoritative grant's JCS bytes, never of the opaque reference alone. |
| `policy_hash`, `contract_hash` | REQUIRED canonical content hashes of the admitted policy snapshot and operation contract. |
| `plugin_module_sha256` | REQUIRED exact implementation bytes used for planning/parsing, including trusted in-process adapter build material represented by the registry. The historical name is retained for protocol stability and does not imply Wasm hosting. |
| `plugin_trust_tier` | REQUIRED one of `lattice_first_party`, `operator_approved`, or `signed_third_party`. Development/rejected tiers cannot issue a production receipt. |
| `bundle_id`, `flow_ir_hash`, `binding_lock_hash`, `flow_id`, `run_id`, `node_id`, `node_alias` | REQUIRED trusted-host scope. They MUST match the grant subject byte-for-byte. |
| `logical_effect_id` | REQUIRED value derived by Section 5. |
| `dispatch_attempt` | REQUIRED integer. It is zero for a pre-dispatch rejection/release and 1-based once dispatch is attempted. |
| `connection_commitment`, `canonical_input_commitment`, `response_commitment` | REQUIRED HMAC commitment objects following Section 10. Response commits to the bounded broker-observed response projection, not remote durable state. |
| `request_plan_hash`, `authority_facts_hash` | REQUIRED after planning; OPTIONAL only for an admission rejection before planning. The request-plan hash covers the final broker-substituted plan. |
| `budget_before`, `budget_after` | REQUIRED remaining per-node logical-call units around reservation/release. A pre-dispatch single release may restore one unit; post-dispatch outcomes never do. |
| `provider_request_id` | OPTIONAL, at most 128 printable ASCII bytes, sanitized and approved by the contract. It MUST contain no response body, token, account PII, or parser diagnostic. |
| `outcome` | REQUIRED one of `confirmed`, `rejected`, `failed`, or `ambiguous`. `confirmed` is broker-observed confirmation, not proof of durable provider state. |
| `claims` | REQUIRED with exactly the five booleans shown. The last two MUST be `false` in `0.1`. Dispatch is `true` only after the durable `dispatched` transition. |
| `issued_at` | REQUIRED broker timestamp at receipt issuance. |
| `signature` | REQUIRED Ed25519 signature using the binding-attestation preimage construction with domain `lattice.invocation-receipt.v0.1`. |

Receipt field verification tiers are normative:

| Tier | Receipt fields / meaning |
| --- | --- |
| `public` | Schema/version, org/principal, issuer/key, all content hashes, trust tier, host scope, effect ID, attempt, budgets, sanitized provider request ID, outcome, claims, issuance time, commitment envelopes, and signature. Anyone with the trust key can verify integrity. |
| `verifier_with_disclosure` | Openings for `connection_commitment` and `canonical_input_commitment`. Only an explicitly authorized verifier receives a scoped opening proof/key. |
| `broker_only` | Opening for `response_commitment` and any retained private audit material. Ordinary flow principals never receive it. |

The commitment value and its tier label are public receipt data; the protected
plaintext/opening is not. Unknown schema versions, keys, trust tiers, outcomes,
claim names, critical fields, or security vocabularies fail closed. Claim names
are a closed `0.1` object; missing, additional, or non-boolean claims fail
verification. Unknown non-critical receipt fields remain signed but confer no
claim. Receipt verification MUST NOT reinterpret `ambiguous` as `failed` or
`confirmed`.

## 5. Deterministic logical-effect identity

For every semantic effect, the trusted host derives:

```text
logical_effect_id = "sha256:" + lowercase_hex(SHA-256(preimage))
```

The preimage is exactly:

```text
ASCII("lattice.logical-effect.v0.1") || 0x00 ||
u32be(len(UTF8(run_id))) || UTF8(run_id) ||
u32be(len(UTF8(node_id))) || UTF8(node_id) ||
u64be(activation_ordinal) ||
u32be(len(UTF8(semantic_effect_slot))) || UTF8(semantic_effect_slot)
```

`activation_ordinal` is an unsigned 64-bit ordinal assigned deterministically
by the trusted scheduler and persisted in checkpoint state. It distinguishes
repeated activations of the same node. `semantic_effect_slot` is a non-empty,
1-to-128-byte ASCII identifier declared by the node/capsule for distinct effects
within one activation. Lengths count bytes and are unsigned big-endian. IDs are
not Unicode-normalized. Bounds MUST be checked before encoding.

The trusted host MUST checkpoint either the canonical input bytes or enough
immutable state to reproduce them byte-for-byte. Retry, redelivery, and resume
of the same logical effect MUST present the same `logical_effect_id` and
byte-identical canonical input. The broker persists both the effect ID and a
SHA-256/HMAC-bound input record. Same ID plus altered bytes fails closed as
`BRK203`; semantic JSON equality, reserialization, or a matching unkeyed hash is
not sufficient. A new effect ID consumes another logical-call unit.

Nondeterministic timestamps, random values, multipart boundaries, and provider
idempotency values MUST either be checkpointed as canonical input or be typed
broker-filled slots outside that input. Where a provider supports an
idempotency key, the broker derives it from the full `logical_effect_id` digest
under the contract's registered encoding and fills it after planning. Guest
code and adapters MUST NOT choose or override it. A provider-specific encoding
MUST preserve at least 128 collision-resistant bits and be deterministic.

## 6. Budgets and Flow IR authoring

### 6.1 Semantics

`max_logical_calls` is an authorization upper bound per `run_id`, per node, and
per operation contract. One distinct `logical_effect_id` reserves one unit.
Exact redelivery of an already-known ID does not consume another unit; it
returns or follows the durable recorded state. A pre-dispatch reservation may
be released exactly once as specified in Section 7. Any post-dispatch state
never returns the logical-call unit.

`max_dispatch_attempts_per_call` bounds transitions to `dispatched` for one
logical effect. Planning attempts do not consume it. Each durable transition to
`dispatched` consumes one attempt even if the process loses the response.
`ambiguous` therefore consumes an attempt and cannot be erased by retry.

Flow-level and connection-level aggregate ceilings are admitted by the `0.1`
manifest and grant schemas. A broker MAY enforce them as operator policy in
`0.1`; if it claims enforcement, it MUST use an authoritative strongly
consistent ledger. Their absence never widens the per-node budget or an
operator ceiling.

Authorization is not obligation: a maximum of two calls does not promise that
two calls happen or that two remote effects exist. Unbounded broker authority
is not representable. Standing authority uses renewable leases plus explicit
rate, concurrency, revocation, and operator ceilings.

### 6.2 Fan-out and diagnostics

A brokered node reachable under `for_each`, `loop`, or `window` MUST have an
explicit author-declared upper bound sufficient to derive a finite per-run
budget. Tooling MUST account for nested multiplicative fan-out. If a control
surface is unbounded, data-dependent without a declared maximum, or its product
overflows the exact-integer range, validation MUST emit an error diagnostic and
fail authority-manifest derivation. It MUST NOT infer a bound from sample data,
a timeout, or an analyzer prediction. A separately declared standing/lease
policy may authorize a different deployment profile, but cannot silently turn
an unbounded graph into a finite manifest.

### 6.3 Proposed `NodeIR` carriage

Budget authoring MUST be hoisted into `NodeIR` so the serialized Flow IR hash
covers it and manifest derivation remains pure. The exact proposed additive
field is `broker_authority`, matching the established `snake_case` serde
convention for structured `NodeIR` metadata:

```json
{
  "broker_authority": {
    "operation_budgets": [
      {
        "contract_id": "connector.google.gmail.send_message@1",
        "semantic_effect_slots": ["send_message"],
        "max_logical_calls": 1,
        "max_dispatch_attempts_per_call": 1,
        "connection_aggregate_key": "google-primary"
      }
    ],
    "flow_aggregate_max_logical_calls": 4,
    "connection_aggregate_max_logical_calls": {
      "google-primary": 3
    }
  }
}
```

`operation_budgets` MUST be unique by `contract_id`, and every contract MUST
also appear in the node's `connector_ops` metadata. `semantic_effect_slots`
MUST be sorted, non-empty, and unique; each slot follows Section 5. All maxima are positive exact integers.
Aggregate fields are OPTIONAL. Macro/builder authoring syntax, Rust type
adoption, schema emission, fan-out diagnostics, and runtime enforcement are
B1/B3 implementation matters; this packet changes no Flow IR runtime type.
Until that adoption lands, a producer MUST NOT claim a derived broker authority
manifest from an out-of-band budget declaration.

## 7. Invocation ledger state machine

```text
issued -> reserved -> planned -> dispatched -> confirmed -> receipt_issued
                    |              |          ambiguous -> receipt_issued
                    |              |          failed    -> receipt_issued
                    |              |
                    +-> released_expired  -> receipt_issued
                    +-> released_cancelled -> receipt_issued
```

The release transitions are permitted only from `reserved` or `planned`.
`confirmed`, `ambiguous`, and `failed` are post-dispatch terminal outcomes for
an attempt. An admission rejection before reservation may issue a rejection
receipt without entering this chain.

Before dispatch, the broker MUST atomically reserve the unique tuple:

```text
org / deployment / flow_ir_hash / run / node / logical_effect_id /
operation_contract / connection_ref
```

A reservation MUST have a short, broker-policy-bounded lease and a durable
transition record. Lease expiry or authenticated cancellation before dispatch
MAY move it to the corresponding release state. Release MUST be compare-and-set,
return the logical-call unit exactly once, and issue a receipt. Repeated release
is idempotent and MUST NOT increment budget. A stale worker MUST NOT plan or
dispatch after its lease is lost.

The broker MUST persist `planned` before dispatch, including the exact canonical
input identity, final broker-substituted request-plan hash, facts hash, selected
implementation, endpoint, and next attempt number. It MUST persist `dispatched`
atomically immediately before crossing the dispatcher boundary. Once dispatch
may have occurred, cancellation, timeout, process loss, or response loss MUST
NOT restore budget or erase the attempt.

If the broker cannot determine whether the provider accepted an effect, it MUST
record `ambiguous`. It MUST NOT blindly retry an ambiguous non-idempotent
operation. A later attempt is allowed only when the operation contract declares
a safe retry rule (for example provider-enforced idempotency), canonical input
is byte-identical, and dispatch-attempt budget remains. Otherwise the logical
effect remains blocked for operator/provider reconciliation. A definitive
`failed` attempt follows the contract's explicit retry policy; there is no
implicit executor fallback.

Every transition MUST be durable and monotonic. Recovery replays the record,
not caller assertions. Strict counters require one authoritative linearizable
ledger; a Workers profile uses a Durable Object authority rather than D1 for
strict grant/count state.

## 8. Assurance vocabulary and executor selection

| Level | Normative meaning |
| --- | --- |
| `direct` | A trusted host executes directly and owns provider credentials. It is not broker count enforcement. |
| `brokered_count` | The broker enforces exact operation, connection/account, host scope, count, TTL, replay, and revocation. |
| `brokered_semantic` | `brokered_count` plus enforcement of declared authority facts. The claim is conditional on the named fact-projection implementation and `plugin_trust_tier`; custom projection does not prove fact completeness. |
| `provider_enforced` | The provider accepts a genuinely narrowed credential or provider-native authority. The exact provider constraint MUST be named. |
| `verifiable` | Measured or cryptographic evidence covers specified adapter/policy computation. The covered computation and oracle boundaries MUST be explicit. This level is not produced by Broker V1. |

Assurance ordering is policy-defined; a deployment MUST NOT assume every row is
a strict substitute for every earlier row. A flow declares a minimum and the
binding selects one explicit executor. There MUST be no silent fallback among
local/direct, remote broker, provider-native, or compatibility proxy executors.
Failure of the selected broker path fails closed.

Generic authenticated HTTP is explicitly outside the semantic broker path. A
fixed-template compatibility gateway may emit separately typed
`direct_proxy` observation records, but it MUST NOT claim brokered semantic
assurance, and one connection MUST NOT be exposed through both lanes.

## 9. Broker error taxonomy

Broker APIs return a stable code and class plus an optional bounded retry hint.
They MUST NOT return provider bodies, request payloads, credentials, tokens,
account PII, raw plugin traps, parser paths, source errors, SQL/storage text, or
unbounded diagnostics. Internal detail belongs only in access-controlled audit
material. The following is the closed `0.1` public enum:

| Code | Class | Default | Summary |
| --- | --- | --- | --- |
| `BRK001` | `invalid_encoding` | Error | JSON, canonicalization, size, or numeric constraints failed. |
| `BRK002` | `unsupported_schema` | Error | Artifact schema version is unsupported. |
| `BRK003` | `unknown_critical_field` | Error | A required extension field is not understood. |
| `BRK004` | `unknown_vocabulary` | Error | Required contract, profile, assurance, enum, or vocabulary is unknown. |
| `BRK101` | `host_unauthenticated` | Error | Trusted-host authentication failed. |
| `BRK102` | `proof_of_possession_failed` | Error | Channel-bound proof or session binding failed. |
| `BRK103` | `grant_not_found` | Error | Opaque grant reference is absent or unavailable. |
| `BRK104` | `grant_not_yet_valid` | Error | Grant is before `not_before`. |
| `BRK105` | `grant_expired` | Error | Grant or binding freshness interval expired. |
| `BRK106` | `grant_revoked` | Error | Revocation epoch or live policy rejects the grant. |
| `BRK107` | `scope_mismatch` | Error | Host assertion differs from the grant subject/scope. |
| `BRK108` | `contract_untrusted` | Error | Contract or selected implementation is absent, mismatched, or revoked. |
| `BRK109` | `binding_invalid` | Error | Connection, account, role, origin, scope, lane, or attestation check failed. |
| `BRK201` | `logical_budget_exhausted` | Error | No logical-call unit remains. |
| `BRK202` | `dispatch_budget_exhausted` | Error | No dispatch attempt remains for the logical effect. |
| `BRK203` | `replay_conflict` | Fatal | Same effect identity arrived with altered canonical input or scope. |
| `BRK204` | `reservation_conflict` | Error | Another valid lease owns the effect reservation. |
| `BRK205` | `reservation_expired` | Error | Caller attempted work with an expired/lost reservation. |
| `BRK301` | `planning_failed` | Error | Bounded request planning or fact projection failed. |
| `BRK302` | `origin_denied` | Fatal | Final request origin is outside the approved contract/binding. |
| `BRK303` | `dispatch_failed` | Error | Broker observed a definitive sanitized dispatch failure. |
| `BRK304` | `outcome_ambiguous` | Error | Dispatch may have occurred but provider outcome is unavailable. |
| `BRK305` | `response_invalid` | Error | Bounded response validation/projection failed. |
| `BRK306` | `unsafe_retry_blocked` | Error | Retry is forbidden by ambiguity/idempotency policy. |
| `BRK401` | `internal_unavailable` | Error | Sanitized broker/ledger/custodian availability failure. |

`Fatal` means the same attempt MUST NOT be retried automatically. `Error` is not
a retry promise; retryability is a separate bounded boolean derived from state
and contract policy. Unknown server-side failures collapse to `BRK401`.
Clients encountering an unknown code MUST treat it as non-retryable and fail
closed. Unknown schemas, critical fields, tagged subjects, trust tiers,
assurance values, outcomes, claim fields, contracts, and required vocabularies
MUST fail closed before dispatch.

## 10. Privacy, commitments, and audit

The broker sees plaintext operation input and bounded provider responses. This
protocol reduces disclosure; it does not remove broker trust.

Low-entropy, private, or account-identifying values MUST NOT use an unkeyed
hash as a public commitment. The commitment is:

```text
HMAC-SHA-256(
  commitment_key,
  ASCII("lattice.commitment.v0.1") || 0x00 ||
  u32be(len(UTF8(org_id))) || UTF8(org_id) ||
  u32be(len(UTF8(field_name))) || UTF8(field_name) ||
  u32be(len(salt_context)) || salt_context ||
  u64be(len(value_bytes)) || value_bytes
)
```

`value_bytes` are JCS bytes for JSON values and exact bytes for typed opaque
values. For a receipt field, `salt_context` is the JCS byte encoding of
`[issuer, run_id, node_id, logical_effect_id, dispatch_attempt, field_name]`.
For a binding's `account_commitment`, it is the JCS byte encoding of
`[issuer, connection_ref, "account_commitment"]`. Any later artifact MUST
normatively define an equally domain-specific, non-circular context.
`salt_context` MUST be unique to the commitment purpose, MUST be generated or
derived by the broker, and MUST never be selected by guest code. Commitment
keys MUST be broker- or tenant-scoped, versioned by `key_id`, unavailable to
flow principals, and rotated independently of receipt signing keys. Rotation
MUST preserve a bounded verification path for retained receipts or explicitly
expire it.

A disclosure verifier receives only a scoped derived key/opening proof for the
approved commitment, never the broker root commitment key. Commitment equality
across tenants or purposes MUST NOT be linkable. Receipt signing keys MUST NOT
be reused as HMAC keys.

Runtime evidence and public errors MUST exclude credentials, access/refresh
tokens, authenticated request bodies, provider response bodies, document
content, raw low-entropy hashes, account PII, and parser/plugin diagnostics.
Only contract-approved sanitized provider request IDs may appear. Writes fail
closed when the broker or authoritative ledger is unavailable; degraded read
modes require explicit separate policy and never imply brokered write assurance.

## 11. Threats and residual trust

The initial adversaries include malicious or prompt-injected input, untrusted
node code, replaying clients, over-broad flow authors, hostile provider content,
network attackers, stale/revoked connections, implementation supply-chain
tampering, and ambiguous remote effects. The trusted computing base includes
bundle/lock generation and keys, trusted-host assertion boundary, broker core
and ledger, custodian/secret store, approved planner/adapter, trust registry,
deployment platform, and remote provider.

Direct ambient provider HTTP is a bypass risk. A broker-bound node MUST NOT have
unrestricted provider egress or direct access to the same credential. Per-node
capability views and destination policy remain required. Open source improves
inspection and self-hosting but does not make the broker honest.

## 12. Accepted Broker V1 decisions

| ID | Accepted ADR | One-line rationale |
| --- | --- | --- |
| D1 | Declarative request-plan AST is primary; trusted in-process Rust adapters handle complex encoding; Wasm is not V1-critical. | Minimizes the TCB and structurally links substitution to fact projection. |
| D2 | Extend `connector-spec` and `connector-codegen`; do not create a parallel registry. | Preserves one source of truth and additive migration. |
| D3 | Defer Wasm packaging/hosting while retaining the fixed boundary in the design. | First-party adapters prove the kernel without toolchain delay. |
| D4 | Google Sheets append and Gmail send are the first provider operations. | Matches the approved S21-shaped proof while explicitly accepting broader custodian risk. |
| D5 | Route the LLM contract through Cloudflare AI Gateway with payload logging disabled. | Keeps LLM provider-key custody out of broker scope and centralizes spend/rate policy. |
| D6 | Use opaque grants plus channel-bound proof of possession; defer portable/Biscuit tokens. | Keeps revocation and counters explicit in the authoritative ledger. |
| D7 | Use a Durable Object as Workers grant/count authority and a strongly consistent in-process test ledger. | Strict counters require linearizable state; D1 is not authoritative. |
| D8 | Use RFC 8785 canonical JSON. | Aligns broker artifacts and canonical invocation JSON. |
| D9 | Carry authority as a sibling artifact hash-bound to Flow IR and referenced by the bundle. | Avoids churn in `FlowRequirements` while preserving content binding. |
| D10 | Claim trusted-host assertion only; Workers key separation is a software boundary. | Prevents measured-execution overclaiming. |
| D11 | Ship an API-first control plane; dashboard UI is post-V1. | The API is enforcement-critical and UI is not. |
| D12 | Stable `lbk_` access keys belong only in the deployment environment and exchange into short sessions/grants. | Gives self-hosted deployments durable access without exposing keys to flows. |
| D13 | Reuse only Apache-2.0/MIT/MPL-2.0 code with provenance; treat ELv2/SUL as clean-room references. | Preserves managed-service licensing options. |
| D14 | V1 is single-tenant self-hosted, but every schema carries `org_id` and `principal`. | Avoids a later identity/tenancy migration. |
| D15 | Keep semantic broker and compatibility gateway as separate lanes; bind a connection to exactly one. | Prevents proxy behavior from entering the semantic TCB or enabling dual-mode admission. |
| D16 | Rust owns all V1 control-plane and custody writes via plain sqlx; future Next.js is API-only. | Maintains one enforcement authority and avoids split-brain writers. |

## 13. Conformance requirements

A conforming `0.1` implementation MUST pass the Section 3 vectors, reject all
listed rejection cases, verify signature domains and unknown-critical behavior,
and test wrong host/node/run/account, replay, altered retry input, budget
exhaustion, expiry, revocation, implementation mismatch, crash recovery from
every durable state, single release, and ambiguous non-idempotent stop. Tests
use synthetic custodians and mock dispatchers; no real credentials, tokens, or
provider effects are required by this specification.
