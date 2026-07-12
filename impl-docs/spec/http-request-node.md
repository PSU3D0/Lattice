Status: v1 BUILT (H1–H4 + H2a/H2b landed, branch verifiability-substrate-hardening); v1.1 binary/artifact contract (§16, H5a–H5d) design-final + adversarially reviewed 2026-07-12 (ACCEPT-WITH-REVISIONS, revisions R1–R3 folded — §16.10), ready to build. Original v1 design revised per adversarial review 2026-07-11 (ACCEPT-WITH-REVISIONS: F1 enforcement locus, F2 redirect precondition, F3 Tier-2 guarantee scope).
Purpose: spec
Owner: Core
Last reviewed: 2026-07-12

# Generic HTTP Request Node (`connector.http`, v1)

Design for the generic httpRequest node: the top-ranked Phase 2 runtime
primitive (895 of 2,061 corpus n8n workflows carry a raw `httpRequest` node —
the single largest RED reason in `ops/n8n-demand-analysis.md` §5). This is the
long-tail escape hatch for APIs that will never get a connector family, and
therefore the node that most directly threatens Lattice's thesis of
compile-verifiable effectfulness. The design's job is to buy corpus coverage
without lying about effects.

Related docs:
- `impl-docs/spec/connector-op-reuse-and-node-declaration.md` (three-layer
  connector model this node reuses)
- `impl-docs/spec/connector-connection-bindings.md`, bindings.lock shape
  (`examples/s16_drive_permissions_audit/bindings.lock.mock.json`)
- `impl-docs/spec/flow-requirements.md`; `crates/cli/src/deploy.rs`
  (`FAMILY_MAPPINGS`: `resource::http` → Ambient on Workers)
- `ops/clone-playbook.md` §3 (binary handoff: DEFERRED, workspace-capability
  recommendation), `ops/clone-economics.md` §5 (demand + gap list)
- `impl-docs/error-codes.md` (EFFECT201/202, CAP110, IDEM/EXACT, RETRY010)

## 0. Decision summary

1. **The generic HTTP node is a connector, not a stdlib node.**
   `connector.http`, built on the existing connectors-std transport +
   endpoint_profile/outbound_auth role machinery. The reachable host set and
   the attached credential are **lock-time data**, exactly like every other
   connector — nothing new has to be invented for auth or URL grants, and
   everything the planner can say about a Gmail flow it can say about an
   httpRequest flow.
2. **Method is static; URL host is static; path/query/body/header-values are
   dynamic.** One op per method (`connector.http.get`, `.head`, `.post`,
   `.put`, `.patch`, `.delete` — mirroring the exhaustive
   `capabilities::http::HttpMethod`), so per-op `ConnectorOpMetadata` is
   honest by construction: GET/HEAD ops declare `resource::http::read` +
   `min_effects = ReadOnly`; the rest declare `resource::http::write` +
   `min_effects = Effectful`. This matches the existing GET-vs-non-GET
   routing in `connectors_std::send_request_from_current`, which CAP110
   enforces at runtime.
3. **The URL never arrives whole as data (default).** The node's input
   carries a *path* (+ query map); the origin comes from the bound
   `endpoint.profile` handle in bindings.lock. Dynamic-host use requires an
   explicit, lock-visible opt-out (§2 grant ladder, Tier 2), in which
   **no lock-granted credential can attach** — an outbound-auth role and a
   dynamic host are mutually exclusive by validation, not by convention.
   (Precisely scoped in §7: this governs lock-managed credentials; it
   cannot stop an author moving a secret through data. §10.)
4. **Response typing is a first-class, static choice** (§5): `json`
   (JsonValue), `typed<T>` (serde, fail-closed on decode error), `text`, and
   a `full_response` envelope that moves status in-band. No silent
   fallback-to-raw in typed mode; lossiness is expressed through serde
   attributes the author chooses, not through runtime mode switching.
   Binary/file responses are **v1.1**, contract now finalized in §16
   (minted attenuable workspace-artifact handles; `ByteSource` byte-input
   type; op-per-effect-shape floors). Not yet built (H5a–H5d, §16.8).
5. **Non-2xx = NodeError by default** (status + truncated body, matching
   `decode_json_response_body`), with a static `full_response` opt-in that
   makes status/headers/body ordinary data (n8n `neverError` parity).
6. **No internal retries, no pagination, no OAuth, no multipart in v1.**
   Retry ownership stays with the orchestrator (RETRY010 vocabulary);
   Effectful write ops go through the existing IDEM/EXACT dedupe machinery
   unchanged.
7. **Measured v1 coverage** (§9): ~80% of the 2,128 corpus httpRequest node
   instances; **60–80% of the 895 workflows** (a band, not a point — the
   swing variable is how many predefinedCredentialType workflows, 26% of
   the 895, resolve to token-style auth the lock can already mint). The
   marquee node-level exclusions are binary/multipart (~10% of nodes).

## 1. Goals / non-goals

Goals (v1): a graph-visible node + typed in-node op for arbitrary REST-ish
JSON/text HTTP calls against lock-granted origins, with honest effect
metadata, working on host-inproc and Workers, coverable by the existing
connector verification harness (manifest/contract/runtime/honesty tests,
CAP110 denial tests, duplicate-injection for writes).

Non-goals (v1), each with the demand number that justifies deferral (§9):
- **Pagination/cursors** — out of scope v1 per maintainer direction; corpus
  usage is 30/2128 nodes (1.4%). The connectors-std `PaginationDescriptor`
  exists for request-mapped ops if v2 wants it.
- **Binary/file request bodies and responses** (multipart 82 + binaryData 24
  up; file/binary ~82 down). Blocked on the binary-handoff decision
  (`ops/clone-playbook.md` §3). §5 reserves the mode name and output shape.
- **OAuth *authorization flows*** (consent/token acquisition; oAuth2Api 21 +
  oAuth1 4 + httpCustomAuth 46 in-corpus). Scope stated precisely: the lock
  runtime **already** supports `auth.oauth2.refresh` and
  `auth.service_account_jwt` provider kinds (cli/src/main.rs:2294–2316,
  3112–3123) that mint bearer tokens the node's `Bearer` role consumes — so
  OAuth-*token* services are in scope wherever a refresh token / service
  account can be provisioned into the lock. What v1 does not build is the
  interactive grant/consent machinery; that is an auth-provider problem,
  not an httpRequest problem.
- **Internal retry/backoff, batching, proxies, TLS-verification opt-outs**
  (`allowUnauthorizedCerts` is 34 nodes and is a security opt-out we decline
  to offer).
- **n8n parameter-shape compatibility.** We clone behavior, not the n8n
  parameter JSON; the two n8n shapes (v1–2 `requestMethod` vs v3–4 `method`)
  are a *reading* concern for clone agents, not a Lattice surface.

## 2. The thesis constraint and the grant ladder

Lattice's product is compile-verifiable effectfulness: the renderer/planner
reads `FlowRequirements` + bindings.lock and states what a flow can do
without executing it. A node whose URL and method arrive as runtime data
destroys that statement — it is `eval()` for side effects. The design
resolves the tension by splitting the request into parts with different
binding times:

| request part | binding time | carried in | verifiable claim |
| --- | --- | --- | --- |
| method | authoring (op identity) | Flow IR `connector_ops[].operation_id` | read vs write effect, dedupe obligations |
| scheme + host (+ base path) | lock time | `endpoint.profile` handle `config.base_url` | exhaustive origin list per flow |
| credential | lock time | outbound-auth handle bound to the connection role | which secret can reach which origin |
| path, query, header values, body | runtime (data) | node input | nothing (by design) |
| response mode, status policy | authoring (static) | node type / IR | output shape, error behavior |

Supporting evidence that this split fits real usage: in the corpus, 2,100 of
2,124 non-empty httpRequest URLs are a *single environment placeholder*
(`{{ $env.BASE_URL }}`-style) — n8n users already converge on binding the
URL at deployment time, not runtime. The lock-time endpoint profile is the
same idea with teeth.

### The grant ladder (default → explicit opt-outs)

- **Tier 0 (default): one bound origin.** The node's connection binds one
  `endpoint.profile` handle; input carries a relative path. The flow's
  reachable-host set is exactly the base_urls in its lock. This is the mode
  every generated clone should use.
- **Tier 1: origin selection from a granted set.** A connection may bind
  multiple endpoint-profile roles and the input selects one **by role
  name**. Hosts are still exhaustively lock-declared; only the choice among
  them is data. **This is a real (small) machinery change, not free
  surface** (review finding F6): `EndpointProfileDescriptor` is all
  `&'static str` (capabilities/src/connector.rs:111–119) and
  `resolve_endpoint_profile` resolves the role key from a static
  descriptor (cli/src/main.rs:3139) — a runtime `target: String` cannot
  mint a descriptor. Design decision: the op **pre-declares a bounded
  static role set** (`endpoint_profile.http_target`,
  `http_target_2` … `http_target_N`, N fixed at authoring, all optional
  beyond the first) and the runtime selector maps onto that set; an
  unknown selector is [HTTP002]. Ship only when a real template needs it
  (defer the whole tier from v1 if none does — see §14 Q7).
- **Tier 2: dynamic URL (`connector.http.*_any_origin` op variants).**
  Full n8n parity: input carries an absolute URL. Requires ALL of:
  (a) a distinct op id (so the IR and requirements manifest show it),
  (b) a lock grant — an endpoint handle of new kind
  `endpoint.any_origin` whose presence is a deliberate, reviewable lock
  edit, (c) **no outbound-auth role bound** (validated fail-closed at lock
  preflight, [HTTP003]), and (d) the runtime SSRF guard of §10. The renderer
  emits a NOTES warning naming every node in this tier.

  Two precision notes from review:
  - **What (c) guarantees, exactly:** Tier 2 prevents **lock-granted
    credentials** from attaching to dynamic-host requests. It cannot
    prevent an author from moving a secret through *data* — a token in an
    `X-Api-Key` header value, a query pair, or the body is ordinary flow
    input the runtime cannot distinguish from payload
    (`apply_static_outbound_auth`, connectors-std/src/auth.rs:8–41, is
    only the lock-credential path). Custom headers stay allowed on
    `*_any_origin` (they are needed for content negotiation); the
    credential-header denylist of §10 still applies. Data-borne secrets on
    Tier 2 are an accepted, documented residual risk (§15) — the honest
    claim is "the platform's credentials are confined; the author's data
    is the author's".
  - **The lock-grant gate needs positive validation to be real** (F7):
    today unknown `provider_kind`s pass `validate_connector_handle_provider_config`
    silently (`_ => {}`, cli/src/main.rs:2326) and only resolution
    hard-requires `endpoint.profile` (main.rs:3143–3157) — so until H1
    adds positive recognition of `endpoint.any_origin` + the HTTP003
    check, a stray handle would not be the "deliberate, reviewable edit"
    claimed. H1 makes recognition + rejection-of-unknown explicit for
    this kind.

  Trade-off stated plainly: Tier 2 buys arbitrary-URL parity at the price
  of the planner only being able to say "this node can GET/POST anywhere,
  with no platform-managed credential". We keep the tier because the
  corpus contains genuine dynamic-host cases (URL-from-previous-node), but
  the default is Tier 0 and clone tooling must not reach for Tier 2 to
  avoid writing a lock entry.

**Why method must be static even when the path is dynamic:** effect hints
live in `NodeIR.effect_hints` and `ConnectorOpMetadata` — both static. A
method-as-data node would have to declare `http_write` + `Effectful` always
(making every fetch look like a write: dishonest ceiling, and it drags
ReadOnly GET flows into IDEM/EXACT obligations) or vary effects at runtime
(no such mechanism exists, by design). Corpus check: method is a static
dropdown value in 2,124 of 2,128 nodes (4 expression-valued `=POST`), so
staticness costs ~0.2% of usage.

## 3. Authoring surface

### 3a. Canonical graph node (generated-clone path)

```rust
use connector_http::actions::{http_get, http_post};

dag_macros::flow! {
    // ...
    let fetch_crm = node!(http_get);     // node alias "fetch_crm"
    let push_stats = node!(http_post);
    connect!(fetch_crm -> shape -> push_stats);
}
```

Which API `fetch_crm` talks to is *not* in the flow: it is bound in
bindings.lock via the existing per-node override map
(`connector_bindings.<flow>.nodes.<alias>` → connection), falling back to
the flow default for `connector.http`. Two httpRequest nodes hitting two
APIs = two connections, two node-alias overrides (§8).

Canonical node input/output (JSON-boundary types, `typed_boundary_policy`
markers apply):

```rust
pub struct HttpRequestInput {
    /// Absolute-path reference, must start with '/'. Validated per §10.
    pub path: String,
    /// Query pairs, percent-encoded via the existing append_query_pair.
    #[serde(default)] pub query: Vec<(String, String)>,
    /// Header name → value. Names must be static-shaped tokens and pass the
    /// §10 denylist; values may be dynamic but are CR/LF-rejected.
    #[serde(default)] pub headers: BTreeMap<String, String>,
    /// JSON body; write ops only (input type for GET/HEAD omits it).
    #[serde(default)] pub body: Option<serde_json::Value>,
    /// Tier 1 only: role name of the granted endpoint profile to use.
    #[serde(default)] pub target: Option<String>,
}

pub struct HttpJsonOutput { pub body: serde_json::Value }          // default
pub struct HttpTextOutput { pub body: String }                      // mode text
pub struct HttpFullResponse<B> {                                    // full_response
    pub status: u16,
    /// Allowlisted response headers only (content-type, etag, location,
    /// retry-after, x-request-id); never a full header dump (§10).
    pub headers: BTreeMap<String, String>,
    pub body: B,        // decoded per mode; see §5 for failure rules
}
```

The Tier-2 variant's input replaces `path`/`target` with `url: String`
(absolute, https-required by default).

### 3b. Typed in-node reuse (custom Rust nodes — the serde path)

Follows `connector-op-reuse-and-node-declaration.md`: the op is invocable
inside a custom node that declares it, with a typed deserialization seam.

```rust
#[def_node(
    name = "FetchInvoice",
    identifier = "acme.fetch_invoice",
    connector_ops(connector_http::ops::HttpGet),
)]
async fn fetch_invoice(input: InvoiceRef) -> NodeResult<Invoice> {
    // typed: serde-deserializes the 2xx JSON body straight into Invoice;
    // decode failure => ConnectorRuntimeError => NodeError (§5).
    let invoice: Invoice = HttpGet::invoke_typed(&HttpRequestInput {
        path: format!("/v2/invoices/{}", input.id),
        ..Default::default()
    }).await.map_err(node_err)?;
    Ok(invoice)
}
```

`invoke_typed<T: DeserializeOwned>` is the only new generic surface;
`invoke` (→ `HttpJsonOutput`), `invoke_text`, and `invoke_full` are fixed
shapes over the same transport. Note (s15 finding, `clone-economics` §5
follow-ups): custom-node identifiers are not connector-prefixed, so scope
inference must ride on `connector_ops[].connector_id` — packet H4 confirms
this works for `connector.http` or fixes it.

### 3c. Static vs dynamic, exhaustively

| field | static/dynamic | enforced by |
| --- | --- | --- |
| method | static (op identity) | op selection; no method field exists in input |
| origin / base path | static (lock) | endpoint-profile resolution; §10 composition guard |
| response mode | static (invoke variant / node type) | Rust types |
| status policy (`error_on_non_2xx` vs full_response) | static | node/op variant |
| timeout | static op default (10s, as connectors-std) + per-node IR override | IR field, additive |
| path, query, header values, body, Tier-1 target | dynamic | runtime validation (§10) |
| header names | dynamic values allowed, validated | §10 denylist + token charset |

## 4. Effects, determinism, and enforcement

Per-op metadata (the honest declaration the whole design exists to protect):

| op | effect_hints | min_effects | determinism_hints | max_determinism |
| --- | --- | --- | --- | --- |
| `connector.http.get`, `.head` | `resource::http::read` | ReadOnly | `resource::http` | BestEffort |
| `.post`, `.put`, `.patch`, `.delete` | `resource::http::write` | Effectful | `resource::http` | BestEffort |
| `.*_any_origin` variants | same as method | same | same | same |

How the existing machinery applies, layer by layer:

- **EFFECT201/DET302 (kernel-plan):** `EffectHint::HttpRead` floors effects
  at ReadOnly, `HttpWrite` at Effectful, family `Http` floors determinism at
  BestEffort (`dag-core/src/effect_hint.rs`). A flow author cannot declare a
  POST node Pure or Strict.
- **EFFECT202 / hint-gate:** all hints come from `capabilities::http::HINT_*`
  constants; `scripts/check-hint-literals.sh` forbids stray `"resource::`
  literals in the new crate, same as every connector.
- **CAP110 (runtime, `capabilities/src/scoped.rs`):** the executor grants
  each node exactly its declared hints. The transport already routes
  GET/HEAD through `resources.http_read()` and POST/PUT/PATCH/DELETE through
  `resources.http_write()` (`connectors-std/src/lib.rs`,
  `send_request_from_current`). A GET-declared node that somehow issued a
  POST would hit `http_write()` with no grant → structured CAP110 denial.
  This is the load-bearing runtime backstop and gets a dedicated honesty
  test per the connector verification guide.
- **FlowRequirements:** the ops appear under `connectors[]` with their role
  requirements and `requires_bound_connection = true`; effects union gains
  `http::read`/`http::write` per node. No schema change needed for Tiers
  0–1. Tier 2 is visible through the distinct operation_id (and the lock's
  `endpoint.any_origin` handle), so "what can the planner still say" is:
  Tier 0/1 — exact origin list; Tier 2 — explicit "anywhere" marker, never
  a silent hole.
- **Dedupe/idempotency:** write ops are Effectful; §6 states the normative
  dedupe rule (the default machinery does NOT engage by itself).
- **`min_effects` is a floor, not a ceiling — and that matters in the
  corpus** (review finding F9, confirmed against EFFECT201's `is_at_least`
  check, effect_hint.rs:218–224: "declare ReadOnly **or Effectful**").
  The corpus contains ~116 GET nodes (~11% of GETs) whose URL is a
  `$env.WEBHOOK_URL`-style trigger — GET-with-side-effects. **Normative
  clone-recipe rule: a GET whose semantic purpose is to fire an action
  (webhook ping, trigger URL, "call this to kick off X") MUST be declared
  `effects = Effectful` on the node**, opting it into the write-side
  idempotency obligations, even though the op's hint remains
  `http::read`. Declaring such a node ReadOnly makes replays "safe by
  declaration" — a quiet lie the type system cannot catch, so the recipe
  must. The inverse case (read-semantics POST, e.g. ~46 GraphQL-ish query
  POSTs) gets no relief: it stays Effectful (see §11 alternative 8).

## 5. Response typing (the maintainer's first-class question)

Modes are static. `B` below is the decoded body type. The rule that resolves
every cell: **a mode is a contract about the success shape; anything that
cannot meet the contract is a NodeError — except in `full_response` mode,
where the author has explicitly asked for wire-level truth in-band.**

| condition ↓ / mode → | `json` (JsonValue) | `typed<T>` (serde) | `text` (String) | `full_response<json\|text>` |
| --- | --- | --- | --- | --- |
| transport error (DNS/TLS/timeout) | NodeError | NodeError | NodeError | NodeError |
| non-2xx status | NodeError `[HTTP101]` (status + ≤240-char body excerpt, as `decode_json_response_body`) | NodeError `[HTTP101]` | NodeError `[HTTP101]` | **Ok**: envelope with real status; body decoded leniently (json: parse-else-`null` + `body_text` excerpt) |
| 2xx, body is not valid JSON | NodeError `[HTTP102]` | NodeError `[HTTP102]` | n/a (no parse) | json flavor: `body = null`, `body_text` populated |
| 2xx, JSON but `T` mismatch | n/a | NodeError `[HTTP103]` carrying the serde error path (`missing field \`x\` at line…`) | n/a | n/a (envelope body is JsonValue, not T) |
| 2xx, empty body | `body = Null` | Ok iff `T` is `()`/`Option<_>`-shaped (serde decides); else `[HTTP103]` | `""` | envelope, `body = Null`/`""` |
| 2xx, body not valid UTF-8 | (JSON parse fails) `[HTTP102]` | `[HTTP102]` | NodeError `[HTTP104]` (strict; no lossy mode in v1) | `body_text` lossy-replaced, flagged `utf8_lossy: true` |
| 2xx, binary content | as above — garbage in is an error, not silent bytes | same | `[HTTP104]` | v1: `body_text` lossy + flag. v1.1: opt into `connector.http.get_binary` → streams the body into a workspace artifact and returns `Artifact { handle, content_type, len, content_hash }` (declares `resource::workspace::write`). Contract final in **§16**; built in H5c. |

Decisions embedded in the table, with rationale:

- **Typed decode failure = node error, full stop.** No fallback-to-raw:
  a node typed as `Invoice` that sometimes emits `JsonValue` is a worse lie
  than a failure. "Partial/lossy" is not a runtime mode — serde already
  expresses it precisely (`#[serde(default)]`, `Option`, and by default
  *unknown fields are ignored*, which is the tolerant-reader behavior we
  want; we do NOT recommend `deny_unknown_fields`). This is the "serde has
  good support" answer: lossiness is a compile-time property of `T`.
- **Fallback recovery is composition, not configuration.** An author who
  wants try-typed-else-raw declares `T = serde_json::Value` and shapes
  downstream, or catches the NodeError via normal error routing. The mode
  matrix stays small and every cell stays predictable.
- **`full_response` is the only in-band-status mode** (n8n
  `neverError`/`ignoreResponseCode`/`fullResponse` ≈ 95 corpus nodes
  combined). It deliberately does not offer `typed<T>` bodies: if you want
  wire truth, you get JSON-or-text truth, and you branch on `status`
  yourself.
- **"Fail-closed" is a property of a strict `T`.** The decode rejects what
  `T` rejects, no more: an untagged enum or a `#[serde(default)]`-heavy
  struct will absorb wrong payloads silently. The runtime guarantees the
  serde contract is *enforced*; how strict that contract is, the author
  chooses when defining `T`.
- **Charset policy: UTF-8 only, everywhere.** All decode paths go through
  `serde_json::from_slice`/UTF-8 (`decode_json_response_body` shape); a
  2xx `Content-Type: …; charset=iso-8859-1` body that is not valid UTF-8
  fails closed as [HTTP102]/[HTTP104]. No transcoding in v1 — documented
  as an accepted limitation, not an oversight.
- **Content-type sniffing: none.** n8n auto-detects; we do not. The mode is
  the contract. (Corpus: explicit non-JSON response formats are ~5.5% of
  nodes; JSON-default dominates.)

## 6. Status codes, retries, idempotency

- Default classification: 2xx → Ok; everything else → NodeError. No
  taxonomy of 4xx-vs-5xx into distinct error kinds in v1, but the NodeError
  carries the numeric status so orchestrator retry *policy* can classify
  (that machinery is the orchestrator's, not this node's).
- **The node performs zero internal retries** (RETRY010: retry ownership is
  declared once; for `connector.http` it is orchestrator-owned, always).
  n8n's node-level `retryOnFail` (82 corpus nodes) maps to Lattice retry
  policy on the node, not to connector behavior.
- **Idempotency interaction (the sharp edge, stated honestly):** GET/HEAD
  ops are ReadOnly — replays are safe by declaration *provided the GET is
  semantically a read* (the §4 webhook-GET rule covers the exception).
  Write ops are Effectful, **but the default machinery does not dedupe by
  itself** (review finding F10): EXACT001–003 fire only on exactly-once
  edges (kernel-plan), and the executor reserves no keys — the working
  pattern is author-composed key + `put_if_absent` against the dedupe
  store (see `connectors/discord/tests/honesty.rs` duplicate-injection
  shape). **Normative rule, not guidance: every `http_post` / `http_put` /
  `http_patch` / `http_delete` node in a cloned flow MUST carry an
  exactly-once edge with an idempotency key, a TTL, and a dedupe binding**;
  the H4 example flow and the clone playbook encode this, and the
  connector harness's duplicate-injection test asserts it for the ops
  themselves. What dedupe canNOT do for a generic POST is make the remote
  API idempotent: retrying a failed-after-send POST may double-charge. This
  is identical to every Effectful connector op today (EXACT005 exists for
  exactly this), but a generic node makes it the author's problem more
  often. v1 documents it; v2 may add an opt-in `idempotency_header`
  (`Idempotency-Key: <dedupe key>`) for APIs that support it — reserved,
  not built (see §14 Q4).
- Timeout: 10s default (connectors-std convention), static per-node
  override. TIME015 budget semantics unchanged.

## 7. Auth and binding — serving the long tail without a connector crate

This is the point of the node: the corpus long tail (predefined-credential
services with no Lattice family, plus generic header/query/basic auth) gets
credentials through the **existing** handle machinery, zero new crates per
API:

- The op declares two roles (`ConnectorOpMetadata.roles`):
  `endpoint_profile.http_target` (required, `expected_handle_kind =
  "endpoint.profile"`) and `outbound_auth.http_target_auth`
  (**optional** — see delta below; `expected_handle_kind` = the union the
  runtime supports).
- A one-off API = one lock entry set: an `endpoint.profile.static` handle
  (base_url + default headers, exactly the s16 shape) + optionally an auth
  handle, joined by a `connector_connections` entry, bound to the node
  alias. Cost: JSON in the lock. No Rust.
- Auth kinds v1, mapped from corpus generic-auth demand
  (`apply_static_outbound_auth` today): `Bearer` (httpHeaderAuth's dominant
  shape, 407 nodes), `ApiKeyHeader` (name + optional prefix), `ApiKeyQuery`
  (48), **plus one additive kind: `Basic`** (username+password → base64
  `Authorization: Basic …`; 50 corpus header-basic nodes). Scope corrected
  per review (F8): the apply-side match in `connectors-std/src/auth.rs` is
  the small part; `OutboundAuthKind` is mirrored in **four more places**
  that must all gain the variant or Basic silently breaks on wasm hosts —
  the wasm transport mirror (capabilities/src/connector.rs:205), the
  host-wasmtime mirror + `transport_auth_profile_to_descriptor`
  (host-wasmtime/src/lib.rs:98, :564), the CLI lock-provider match
  (cli/src/main.rs:3105–3128), and handle validation (main.rs:2275+).
  A cross-mirror exhaustiveness test is part of the H2 gate.
  `Unsupported` keeps failing closed.
  Token-in-path auth (telegram-style) stays on the secret-materialization
  hook gap (`clone-economics` §5) — not this node's problem to solve.
- **Auth attachment rule (binding, security-critical):** outbound auth is
  applied by the connector runtime *after* URL composition, and the
  transport asserts the composed URL's origin equals the resolved endpoint
  profile's origin before attaching ([HTTP105] on violation). Belt and
  braces: path-composition already guarantees it in Tiers 0–1, and Tier 2
  cannot bind lock-granted auth at all ([HTTP003]). On **same-trust-domain
  hosts**, a lock-granted credential therefore cannot travel to a host
  outside its granted profile — including via redirects, because redirect
  following is disabled (blocking precondition H2a, §13).

- **Enforcement locus per host (review finding F1 — where that claim is
  actually enforced, verified against source):**

  | host | trust boundary | does the confinement claim hold? |
  | --- | --- | --- |
  | host-inproc / host-web-axum | node code, connectors-std transport, and capabilities are one native trust domain | **Yes, under honest node code.** The HTTP105 assertion, auth application, and send run in the same domain; there is no boundary to smuggle across. (A malicious *Rust* node could bypass connectors-std and call `http_write()` directly — the existing single-trust-domain model for all connectors.) |
  | host-workers | guest wasm and cap-call handlers share the Worker isolate | Same as native: single trust domain in practice; holds under honest guest code. |
  | **host-wasmtime** | guest wasm is *intended* to be untrusted | **Partial — hardened by H2b (landed, origin-pin variant).** Scope is now derived host-side via `require_trusted_scope()` (`resources.connector_scope()`), and a guest-forged scope is rejected before the connector runtime is consulted. A per-invocation `ConnectorAuthGate` records the origins the host resolved from endpoint profiles and refuses to (a) attach a credential to, or (b) send, any request whose origin ∉ those grants — so a hostile guest that extracts the secret and rewrites the URL is stopped host-side at both issuance and egress. Residual (accepted): credential bytes still enter guest memory (the openai connector legitimately reads the bearer token from the returned request to build its client — withholding bytes would break it; full host-side application deferred), but they are unusable outside host-granted origins through this host's egress. Dev caveat: `EnvConnectorRuntime` can fall back to a guest-supplied `base_url` when no env override is set, weakening the pin on that dev adapter only. |

  This hole is **pre-existing for every connector** (gmail, slack, … use
  the same `apply_outbound_auth` round-trip); this design does not
  introduce it, but a generic HTTP node raises its value to an attacker,
  so it is scheduled as hardening packet **H2b** (§13): derive scope from
  `resources.connector_scope()` instead of the guest payload, and either
  apply auth host-side at send time (guest never holds credentialed
  bytes) or origin-pin in `handle_http_send`. Until H2b lands, the honest
  statement is: credential-origin confinement holds under honest node
  code on all hosts, and against a hostile wasmtime guest only after H2b
  (recorded as residual risk (b), §15).

Required machinery delta: `ConnectorRoleRequirement` has no optionality
today (`{kind, name, expected_handle_kind}`), and `ActionDescriptor.auth`
is a static `Option`. v1 adds `required: bool` (default true, additive,
skip-serializing) to the role declaration + IR + requirements manifest, and
the connector.http transport applies auth iff the scope's connection binds
the role. Lock preflight: unbound *required* role = existing failure;
unbound optional role = fine; bound-but-wrong-kind = existing failure.

## 8. Lock and renderer interaction

What bindings.lock carries (all existing schema, one new handle kind):

```jsonc
"connector_handles": {
  "auth.crm_token":   { "provider_kind": "auth.static_bearer", "handle_kind": "http.bearer", "connect": { "secret_ref": "CRM_TOKEN" } },
  "endpoint.crm_api": { "provider_kind": "endpoint.profile.static", "handle_kind": "endpoint.profile",
                        "config": { "base_url": "https://api.crm.example", "default_headers": { "Accept": "application/json" } } },
  "endpoint.anywhere": { "provider_kind": "endpoint.any_origin", "handle_kind": "endpoint.any_origin", "config": {} }  // Tier 2 only
},
"connector_connections": {
  "crm_api": { "connector_id": "connector.http",
               "roles": { "endpoint_profile.http_target": "endpoint.crm_api",
                          "outbound_auth.http_target_auth": "auth.crm_token" } }
},
"connector_bindings": {
  "<flow-id>": {
    "defaults": { "connector.http": "crm_api" },
    // per-node override: FLAT alias -> connection-name map (verified shape:
    // ConnectorFlowBindings.nodes is BTreeMap<String, String>, resolved by
    // bindings.nodes.get(&scope.node_alias) at cli/src/main.rs:2845 with the
    // connector_id cross-checked against the connection at :2869).
    "nodes": { "push_stats": "metrics_api" },
    "resolved_effect_hints": { "fetch_crm": [], "push_stats": [] }
  }
}
```

Renderer (`flows deploy render`) consequences:
- `resource::http` family → `Ambient` on Workers (existing FAMILY_MAPPINGS
  row): no new bindings emitted.
- New NOTES output: for every flow using `connector.http`, list
  `node alias → origin` from the lock (`the reachable-origin audit line` —
  planner v0 for HTTP). Tier-2 nodes render as a warning note naming the
  node and stating "any origin, no outbound auth".
- Secrets stay `wrangler secret put` refs exactly as today; render never
  inlines them.
- Follow-up already on record (`clone-economics` §5): render should verify
  lock `content_hash`; unchanged by this design but more valuable once
  origin grants are security-relevant — fold into packet H4.

## 9. Corpus grounding (measured 2026-07-11)

Method: all 2,128 httpRequest node instances across the 895 workflows in
`ref-libs/n8n-corpus-zie619/workflows/` were extracted (jq over every
`type == "n8n-nodes-base.httpRequest"` node; both the v1–2 `requestMethod`
and v3–4 `method` parameter shapes counted). Representative nodes were read
in ~15 files. Key distributions:

- **Method:** GET ≈ 1,016 (48%, incl. 1,004 method-absent defaults), POST
  ≈ 1,002 (47%), PUT 50, DELETE 33, PATCH 27. GET+POST = 95%. Expression-
  valued methods: 4 (0.2%) — the static-method decision costs ~nothing.
- **URL:** 2,100/2,124 non-empty URLs are a single `$env` placeholder — the
  sanitized corpus **cannot** measure static-host+dynamic-path vs
  fully-dynamic hosts, and no hostname ranking is recoverable. It does show
  users already deployment-bind whole URLs (supports Tier 0). Treat any
  dynamic-URL prevalence claim as unmeasurable from this corpus.
- **Auth:** none 48%; genericCredentialType 27% (of which httpHeaderAuth
  407, httpBasicAuth 50, httpQueryAuth 48, httpCustomAuth 46, oAuth2Api
  21); predefinedCredentialType 23% (service-specific creds — coverable by
  a bearer/api-key handle whenever the service is token-authed, else needs
  a connector family or OAuth).
- **Body:** sendBody 46%; JSON ≈ 829, multipart 82, raw 31, binary 24,
  form-urlencoded 22.
- **Headers/query:** custom headers 31% (181 with dynamic values); query
  params 15%.
- **Response:** JSON-default dominant; file/binary ≈ 82, text ≈ 36;
  fullResponse 78, neverError/ignoreResponseCode ≈ 17.
- **Long tail ≤1.6% each:** pagination 30, batching 35,
  allowUnauthorizedCerts 34, timeout 15, redirect options 56, proxy 0.

**Recommended v1 scope** = §0: six method ops, Tier 0/1 grants + Tier 2
unauthenticated variants, auth {none, Bearer, ApiKeyHeader, ApiKeyQuery,
Basic}, JSON body (+ form-urlencoded encode — 22 nodes, trivial), headers +
query with dynamic values, response modes {json, typed, text,
full_response}.

**Coverage estimate:** excluded node features — multipart/binary bodies
(~106), file/binary responses (~82), OAuth-grant/custom generic auth
(~71), pagination (30), batching (35) — overlap-adjusted ≈ 300–350 nodes,
so v1 addresses **≈ 80% of node instances (~1,750/2,128)** (node math
verified exact in review). At workflow granularity (one blocked node
blocks a clone) the honest number is a **band: 60–80% of the 895
workflows**, because the node-set→workflow join was not computed exactly
and **the swing variable is predefinedCredentialType**: 231 workflows
(26% of the 895) use service-specific predefined credentials. Counting
all of them blocked gives ~60%; counting all coverable gives ~80%. The
truth is in between and better than first drafted: the lock runtime
already mints bearer tokens from `auth.oauth2.refresh` and
`auth.service_account_jwt` handles (§1), so every predefined-credential
service whose API takes a bearer/api-key token is coverable without new
machinery. v1.1 binary mode + multipart is the single biggest node-level
coverage increment (~10% of nodes).

## 10. Security

- **SSRF.** Tier 0/1: the origin is lock-pinned; input `path` is validated —
  must start with `/`, must not start with `//`, no `\`, no CR/LF/NUL, no
  `..` segment after normalization, percent-encoding of path segments uses
  the existing `encode_component`; the composed URL is re-parsed and its
  origin asserted equal to the profile's ([HTTP105]). Query pairs go
  through `append_query_pair` (NON_ALPHANUMERIC percent-encoding). Tier 2:
  https-only by default; hostname denylist (localhost, `*.localhost`,
  literal loopback/RFC1918/link-local/CGNAT IPs, `169.254.169.254`
  metadata); no auth attachable. Residual risk stated honestly: DNS
  rebinding and redirect-laundering defeat hostname checks — which is why
  Tier 2 additionally requires (v1) **redirects disabled** and carries a
  renderer warning; a resolve-then-pin guard is a native-host v2 hardening
  option, and on Workers the platform itself cannot reach the deployer's
  private network. Tier 2 is a trusted-author mode and is documented as
  such.
- **Redirects.** Disabled for all connector.http requests in v1 (3xx +
  `full_response` = data; 3xx otherwise = [HTTP101] error). **This
  behavior does not exist yet in either provider** and two of this
  section's arguments depend on it, so it is a *blocking precondition*
  (packet H2a, §13): cap-http-reqwest builds a default `Client`
  (src/lib.rs:32–35 — follows up to 10 redirects) and cap-http-workers
  builds `RequestInit` without setting the redirect knob
  (src/lib.rs:69–82 — `fetch` default follows). Rationale for disabling:
  following would let a granted origin bounce a credentialed request
  elsewhere. Re-enabling same-origin-only redirects is a v2 option.
- **Header injection.** Header values: reject CR/LF/NUL. Header names:
  RFC 7230 token charset + case-insensitive denylist {`authorization`,
  `proxy-authorization`, `host`, `content-length`, `transfer-encoding`,
  `connection`, `cookie`} → [HTTP106]. `Authorization` must come from an
  auth handle, never from node data — this keeps secrets out of Flow IR,
  node payloads, checkpoints, and logs. (Corpus reality check: n8n users
  DO put `Authorization: Bearer {{...}}` in header parameters — example 3
  in the corpus sample; the clone recipe is: move it to an
  `auth.static_bearer` handle. The denylist makes the honest path the only
  path.)
- **Secret exfiltration via attacker-controlled URLs.** For lock-granted
  credentials on same-trust-domain hosts (§7 truth table): auth attaches
  only when the composed origin equals the granted profile origin; Tier 2
  cannot bind lock auth; redirects are off (H2a); `ApiKeyQuery` secrets
  never appear in output because response data never echoes the request
  URL and the error excerpt is response-body-only. Explicitly NOT covered:
  data-borne secrets an author routes through headers/query/body on a
  Tier-2 op (§2, residual risk (c)), and a hostile host-wasmtime guest
  until H2b lands (§7, residual risk (b)).
- **Response header leakage.** `full_response` returns an allowlisted
  header subset, not the full map (Set-Cookie never crosses into flow
  data).
- **Logging.** NodeError body excerpts are capped at 240 chars (existing
  convention); request URLs logged without query strings when an
  `ApiKeyQuery` auth is bound.

## 11. Alternatives considered (rejected)

1. **Stdlib node with raw `http_read`/`http_write` capability (s7
   `otel_dispatch` style), URL fully from data.** Rejected: no lock-time
   origin story at all — the planner could only say "talks to the
   internet"; auth would be ad-hoc env plumbing; duplicates the connector
   auth machinery the moment credentials appear. This is exactly the
   escape hatch the thesis forbids as a *default*.
2. **One op with method as data (validated at runtime).** Rejected: static
   effect metadata would have to declare the write ceiling for everything
   (dishonest for the 48% GET population, drags reads into Effectful
   dedupe obligations) — see §2. Cost of rejection measured at 0.2% of
   corpus nodes.
3. **One op with method as a static macro parameter** (two ops read/write,
   method literal checked at expansion). Viable and fewer op ids, but
   requires a new "static op parameter" concept in the macro/IR layer,
   whereas op-per-method reuses `ConnectorOpMetadata` exactly as-is and
   makes the method visible in `operation_id` for requirements/audit.
   Chosen: op-per-method (6 tiny descriptors is cheaper than one new
   concept).
4. **Domain allowlist as a runtime-config regex/pattern list** (n8n-ish).
   Rejected: patterns are not grants; the lock's exhaustive origin list is
   auditable and hashable, a regex is neither. Tier 1 covers the
   multi-origin need with named grants.
5. **Auto content-type/format sniffing for responses.** Rejected: makes
   output type depend on remote behavior; kills typed-boundary guarantees.
6. **Typed mode with fallback-to-raw on decode error.** Rejected (§5): the
   output type becomes a lie; composition (JsonValue mode + downstream
   shaping) expresses the same intent honestly.
7. **Implement n8n's parameter schema directly for 1:1 template import.**
   Rejected: licensing rule (`clone-playbook` §0) plus we translate
   behavior, not surfaces; two n8n parameter generations would ossify into
   our public API.
8. **Declared-ReadOnly POST (an author attestation that a POST is
   semantically a read — GraphQL queries, search endpoints).** Corpus
   demand ~46 GraphQL-ish query POSTs. Rejected for v1: it would require a
   per-node override of the op's `http::write` floor, i.e. a mechanism for
   nodes to *weaken* declared effects on attestation — precisely the
   trust-me hole the effect system exists to remove, and CAP110 could no
   longer distinguish it from a lying write. Cost accepted: ~46 nodes
   (~2%) carry Effectful ceilings (idempotency-key ceremony) they do not
   semantically need. Revisit only if a corpus-significant GraphQL surface
   materializes (v2 could consider a distinct `connector.http.graphql_query`
   op with its own audited semantics rather than a generic override).

## 12. New diagnostics (register in `impl-docs/error-codes.md` + `dag-core/src/diagnostics.rs`)

| code | subsystem | default | summary |
| --- | --- | --- | --- |
| HTTP001 | Validation | Error | connector.http input `path` malformed (missing `/`, `//`, `..`, control chars) — checked at runtime pre-send; fail closed. |
| HTTP002 | Validation | Error | Tier-1 `target` names an endpoint-profile role not bound on the node's connection. |
| HTTP003 | Lock preflight | Error | `any_origin` op with an outbound-auth role bound (auth × dynamic host is forbidden). |
| HTTP004 | Lock preflight | Error | Required role unbound / handle-kind mismatch for connector.http (existing failure, connector.http-attributed message). |
| HTTP101 | Runtime | Error | Non-2xx response in error-on-status mode (carries status + ≤240-char excerpt). |
| HTTP102 | Runtime | Error | 2xx body not valid JSON in a JSON mode. |
| HTTP103 | Runtime | Error | 2xx JSON did not match typed output `T` (carries serde path). |
| HTTP104 | Runtime | Error | 2xx body not valid UTF-8 in text mode. |
| HTTP105 | Runtime | Fatal | Composed URL origin ≠ granted profile origin (invariant breach — bug or attack; never retried). |
| HTTP106 | Runtime | Error | Forbidden/malformed header name or CR/LF in header value. |

## 13. Packet decomposition (subagent-dispatchable)

- **H1 (S) — role optionality + handle kind.** dag-core:
  `ConnectorRoleRequirement.required: bool` (additive, default true,
  skip-serializing; review-confirmed cleanly additive, goldens
  byte-identical) through IR + `ConnectorRoleRequirementIR` +
  requirements manifest + schema regen. **Positive** recognition of the
  new `endpoint.any_origin` handle kind in lock parsing/preflight — today
  unknown provider kinds pass validation silently
  (`validate_connector_handle_provider_config` `_ => {}`,
  cli/src/main.rs:2326), so H1 must add explicit accept-and-validate for
  this kind plus the HTTP003 auth-exclusion check; the Tier-2
  "deliberate, reviewable lock edit" claim is not true until this lands.
  HTTP001–004 + HTTP101–106 registered (diagnostics registry test keeps
  doc in sync).
- **H2a (S, BLOCKING precondition for H2) — no-redirect per-request
  field.** Additive `redirect: Off` (serde-default) on
  `capabilities::http::HttpRequest`, honored by BOTH providers:
  cap-http-reqwest (today builds a default `Client`, src/lib.rs:32–35 —
  follows up to 10 redirects; use `redirect::Policy::none()`) and
  cap-http-workers (today sets no knob on `RequestInit`,
  src/lib.rs:69–82; worker 0.8.1 exposes `redirect: manual`).
  **Definition of done: a 3xx test on each provider path proving the
  redirect is surfaced (HTTP101 / envelope data), never followed.**
  connector.http does not ship without this.
- **H2 (L) — `crates/connectors/http` crate.** Mirror the family layout
  (`connector.yaml` handwritten_semantic, ops/actions/runtime/generated,
  six method ops + `get_any_origin`/`post_any_origin`…, `HttpApi`
  transport: path validation, origin assertion, optional-auth application,
  §5 decode modes, `invoke_typed`). Adds `Basic` to `OutboundAuthKind` —
  scope per F8: apply-side (`connectors-std/src/auth.rs`) **plus all four
  mirrors** (capabilities wasm transport mirror connector.rs:205;
  host-wasmtime mirror lib.rs:98 + `transport_auth_profile_to_descriptor`
  :564; CLI provider match main.rs:3105–3128; handle validation
  main.rs:2275+), with a cross-mirror exhaustiveness test so a missed
  mirror fails CI instead of silently breaking wasm hosts. Full harness:
  manifest/contract/runtime (auth-header assertion, non-2xx mapping,
  decode-failure fixtures, 3xx-not-followed via H2a)/honesty (exact
  grants zero-denial; empty grants CAP110 `MissingHttpRead` and
  `MissingHttpWrite`)/live_smoke gate; duplicate-injection +
  `verify_dedupe_store` for every write op; SSRF unit vectors (path
  traversal, `//host`, CRLF, denylisted headers, origin mismatch).
- **H2b (M) — host-wasmtime auth-boundary hardening (pre-existing hole,
  §7 truth table).** `handle_connector_apply_outbound_auth` must derive
  scope from `resources.connector_scope()` instead of trusting the
  guest-supplied payload scope; then either apply outbound auth host-side
  at send time (credentialed bytes never enter guest memory — preferred)
  or origin-pin the URL in `handle_http_send` against the scope's
  resolved endpoint profile. Benefits every connector, not just
  connector.http. Can land in parallel with H2; v1 GA on host-wasmtime
  for untrusted guests is gated on it (native + Workers are not).
- **H3 (M) — lock preflight + renderer.** Optional-role satisfaction
  rules; HTTP003 fail-closed check (with H1); per-node connection
  override for connector.http uses the verified flat
  `nodes: {alias → connection}` map (main.rs:2845/2869) — golden
  fixtures must encode that shape (F5); renderer NOTES (origin audit
  lines, Tier-2 warning); fold in the render-verifies-`content_hash`
  follow-up. Golden render fixtures.
- **H4 (M) — example flow + corpus acceptance.** New `sNN_http_longtail`
  example: schedule/webhook trigger → `http_get` (JSON, typed) → pure
  shape → `http_post` (Effectful, **exactly-once edge + idempotency key +
  TTL + dedupe binding per the §6 normative rule**, dedupe-asserted)
  against httpmock; a Tier-2 unauthenticated GET node; one
  webhook-style GET declared Effectful (the §4 rule, exercised);
  hand-authored bindings.lock.mock.json (flat `nodes` map); CLI
  end-to-end golden + `flows deploy render` clean; scope-inference check
  for custom nodes using `connector_ops(connector_http::ops::…)` (s15
  finding). Updates `ops/clone-playbook.md` with the §4/§6 normative
  recipe rules.
- **H5 (v1.1 binary mode) — contract FINAL (§16), decomposed into H5a–H5d
  (§16.8).** Binary-handoff decision resolved: minted attenuable
  workspace-artifact handles (`Handle<S>`), `Artifact` as the data-plane
  value, `ByteSource` as the universal byte-input, op-per-effect-shape
  floors (`get_binary` +workspace_write, `post_multipart` +workspace_read).
  Order H5a → H5b → (H5c ∥ H5d). Reviewed 2026-07-12 (ACCEPT-WITH-REVISIONS,
  §16.10): H5b raised to L (handle-only surface + workspace read/write split —
  the enforcement refactor that makes the floors real).

Suggested order: H1 ∥ H2a → H2 (∥ H2b) → (H3 ∥ H4) → H5. H2 is the only
L; it is exemplar-cloning of the existing connector shape, which Phase 1
measured at ~95% one-shot rate.

## 14. Open questions (maintainer), each with a recommendation

1. **Tier 2 in v1 at all?** Recommendation: ship the unauthenticated
   any-origin variants in v1 (real corpus demand exists and forcing lock
   edits for every host teaches bad workarounds like proxying), but keep
   the lock-grant + renderer-warning + no-auth triple mandatory. Veto to
   "v2" costs an estimated single-digit % of workflows and simplifies H2.
2. **`Basic` auth kind now or later?** Recommendation: now — 50 corpus
   nodes, ~15 lines, closes the generic-auth triangle
   (header/query/basic). Note it stores `user:pass` as one secret handle
   (`http.basic` handle_kind) rather than growing a two-field secret shape.
3. **`full_response` lenient-JSON envelope shape** (`body` JsonValue-or-null
   + `body_text` excerpt) vs strict-JSON-else-error even in envelope mode.
   Recommendation: lenient as specced — the whole point of the mode is
   wire truth; erroring on a non-JSON 502 page defeats it.
4. **Reserve `Idempotency-Key` header injection for v2?** Recommendation:
   yes, reserve the option name now (`idempotency_header`), reject with a
   forward-pointing error like the schedule trigger's reserved fields, and
   gate building it on a template that needs it.
5. **Timeout override surface** — static IR field per node vs input field.
   Recommendation: static IR field (keeps runtime inputs pure data, and
   the planner can see worst-case budgets); default 10s.
6. **Where does the no-redirect knob live** — per-request field on
   `capabilities::http::HttpRequest` (additive, serde-default) vs
   provider-constructor option. Recommendation: per-request field, because
   the Workers guest serializes `HttpRequest` across the cap-call boundary
   and per-provider config would fork behavior between hosts.
   RESOLVED-BY-REVIEW (F2): per-request field, promoted to blocking
   packet H2a with both-provider 3xx tests in its definition of done.
7. **Tier 1 in v1?** The bounded-static-role-set mechanism (§2, F6) is a
   real change to how endpoint roles are declared, and no shortlist
   template has needed multi-origin selection yet. Recommendation: spec
   it (done), build it only when the first template needs it; ship v1 as
   Tier 0 + Tier 2.
8. **H2b sequencing** — host-wasmtime auth hardening is a pre-existing,
   all-connector hole that connector.http makes more valuable to attack.
   Recommendation: land H2b in the same wave as H2 (parallel packet), and
   until it merges, document host-wasmtime as honest-guest-only for ALL
   credentialed connectors, not just this one.

## 15. Adversarial review record (2026-07-11)

Verdict: **ACCEPT-WITH-REVISIONS** (revisions folded into this document).
Survived unchanged and should not be re-argued: connector-not-stdlib;
op-per-method effect honesty; lock-time origins + three-tier ladder;
CAP110 as the runtime backstop **across the wasm boundary** (per-node
`ScopedResources` is built host-side — kernel-exec — so a GET-declared
node issuing a POST gets `MissingHttpWrite` even from a guest); corpus
method/URL/auth distributions (node math verified exact: 2,061 workflows,
895 with httpRequest, 2,128 nodes); `required: bool` additivity;
`resource::http` → Ambient on Workers; the flat per-node override
mechanism; 10s timeout; `append_query_pair` encoding.

Residual risks accepted **consciously** (each named where it is incurred):

- **(a) DNS rebinding / resolve-time SSRF on native-host Tier 2.** No
  resolve-then-pin guard in v1; hostname-literal checks + redirects-off +
  renderer warning only. Tier 2 is a trusted-author mode (§10).
- **(b) Guest trust on host-wasmtime.** Credentialed request bytes enter
  guest memory and scope is guest-asserted, for ALL connectors today —
  pre-existing; fixed by H2b, and until then the §7 truth table is the
  honest statement of where confinement holds.
- **(c) Tier-2 data-channel secrets.** The no-lock-auth rule confines
  platform-managed credentials only; an author can still route a secret
  through header/query/body data on an any-origin op. Boundary stated in
  §2/§10; not preventable without forbidding dynamic data entirely.
- **(d) POST-as-read Effectful ceiling.** ~46 GraphQL-ish query POSTs
  (~2% of nodes) carry write-side obligations they do not semantically
  need — the accepted price of refusing effect-weakening attestations
  (§11 alternative 8).

## 16. v1.1 binary & artifact contract (H5) — the byte plane

Status of this section: **contract finalized + adversarially reviewed
2026-07-12** (supersedes the "reserved, unusable" placeholders in §0.4, §5,
§13-H5 and finalizes the `ops/clone-playbook.md` §3 binary-handoff decision).
ACCEPT-WITH-REVISIONS; revisions R1–R3 folded (§16.10). **Design-only — build
is H5a–H5d below (H5b is the load-bearing enforcement refactor).**

### 16.0 The decision, in one rule

**Bytes never enter the JSON data plane.** Node-to-node data is
`serde_json::Value` (`kernel-exec::NodeOutput::Value`); raw bytes can only
travel as base64-in-JSON, which inflates 33%, bloats every checkpoint,
forbids streaming, and offers no place to hang a size cap or a grant. So the
byte plane is separate: bytes live in the run-scoped `Workspace`
(`capabilities/src/workspace.rs`) or a `BlobStore` (`capabilities/src/lib.rs:2062`);
a small, self-describing **`Artifact`** flows the data plane; any node that
needs the bytes **dereferences a minted, attenuable handle** under a
`workspace::read`/`blob::read` grant. This is the same "authority is
lock/graph-visible, not runtime-smuggled" move the whole node makes for URLs
(§2), applied to bytes.

### 16.1 The three types

```rust
// A capability to reach bytes. Attenuable (narrow-only). Minted host-side.
// Scope is a phantom type param so a port's authority granularity is visible
// in its signature; a runtime witness rides inside for the deref check.
pub struct Handle<S: Scope = Exact> {
    store: StoreRef,        // binding name of the backing Workspace/BlobStore
    scope: HandleScope,     // runtime witness: Exact(path|key) | Prefix(path)
    mint: MintToken,        // unforgeable, bound to (store, scope); see 16.2
    _s: PhantomData<S>,
}
pub enum Scope {}           // sealed marker trait impls: Exact, Prefix
pub enum HandleScope { Exact(String), Prefix(String) }

// The data-plane value: a handle plus enough self-description to route it
// without re-fetching. content_hash is lifted straight from
// WorkspaceEntry.content_hash (workspace.rs:112) when the store provides it —
// this is the provenance-ledger seam (§16.7).
pub struct Artifact<S = Exact> {
    pub handle: Handle<S>,
    pub content_type: String,
    pub len: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
}

// The universal byte-input type. Everything that accepts "content bytes"
// (http request body, a multipart part, a future sheets/blob byte field)
// accepts THIS, so the effect rule (16.3) applies uniformly.
pub enum ByteSource {
    Inline(#[serde(with = "base64")] Vec<u8>),  // base64 is just its wire form
    Artifact(Artifact<Exact>),                   // full Artifact — carries
                                                 // content_type so `form!` needs
                                                 // no extra deref (review F7);
                                                 // deref under a read grant
}
// Authoring ergonomics only — the port/wire type stays the concrete enum:
impl From<&str> for ByteSource { /* Inline */ }
impl From<Vec<u8>> for ByteSource { /* Inline */ }
impl From<Artifact> for ByteSource { /* Artifact(a) — keeps content_type */ }
```

`Base64` is **not** a distinct variant: any bytes crossing a JSON port are
base64 on the wire already, so `Inline` subsumes it. Accepting an
already-encoded external base64 string without a decode round-trip is a v1.2
convenience at most.

### 16.2 Scope, attenuation, minting (the authority model)

- **One type, three granularities.** A whole-workspace handle is
  `Prefix("")`; a subtree is `Prefix("uploads/")`; a single file is
  `Exact("uploads/report.csv")`. A `FileHandle` is just a fully-narrowed
  workspace handle — no separate type.
- **Attenuation is narrow-only, guest-side, via a macaroon caveat chain**
  (revised per review F3 — a naive `child = H(parent_mint, sub_scope)` HMAC
  chain does NOT compose: `H(H(root,"uploads/"),"a/b")` ≠ `H(root,"uploads/a/b")`,
  so the host could not recompute a mint without knowing how the guest split
  the narrowing). The mint is a **macaroon**: `mint = { root_key_id, caveats:
  [scope-bound...], tag }` where `tag` starts as `HMAC(root_key, root_key_id)`
  and each narrowing appends a caveat and extends the tag
  `tag' = HMAC(tag, caveat)`. `Handle<Prefix>::narrow(sub) -> Handle<Prefix>`
  and `::file(leaf) -> Handle<Exact>` add a "scope ⊆ sub" caveat purely
  guest-side (no root key needed — this is the property macaroons exist for).
  Verification (gate 2, §16.4) folds the chain from the root key host-side and
  checks the final `scope` satisfies **every** caveat. **Widening is
  impossible**: a caveat can only restrict, and forging a tag for a broader
  scope needs the root key, which never leaves the host. A node handed the
  whole workspace can safely re-narrow; a node handed one file cannot climb
  out. **Residual (new, §16.10-R2):** the mint is a bearer token that rides
  the JSON data plane inside `Artifact`, so it enters node outputs,
  checkpoints, and logs — the byte-plane analogue of the §7 credential-bytes
  residual. It is scope-confined (a leaked leaf mint grants only that leaf)
  and run-scoped (workspace dies with the run), so the blast radius is one
  artifact; documented, not eliminated.
- **Type-level scope, enforced at the port (the chosen design; runtime-enum
  was the alternative).** `Handle<Exact>` vs `Handle<Prefix>` is
  serde-transparent (phantom, no wire change), but the handle's `Deserialize`
  **validates the wire `scope` variant against `S` and fails closed on
  mismatch**. Consequence: a node whose port is typed `Handle<Exact>`
  *cannot even deserialize* a directory handle — authority granularity is a
  compile-time property of the signature, checked fail-closed at the data
  boundary. This is the variant that stays legible to a future provenance
  signature (§16.7): authority you can read off the type is authority you can
  attest. Cost over the runtime enum: a sealed `Scope` trait + one validating
  `Deserialize`. Worth it.
- **Blob is `Exact`-only.** `BlobStore` has no `list` and no hierarchy
  (`get/put/delete` by flat key, `lib.rs:2062`), so a `Prefix` blob handle
  cannot enumerate — it degenerates to "a bag of exact keys you were already
  told." `Prefix` for blob is a **mint namespace** (bounds what sub-handles
  may be derived), never a browsable directory. The `Handle` type is unified;
  `Prefix`'s usefulness is a per-store capability. Spec this explicitly so no
  one expects to `list` a blob prefix.

### 16.3 Accepting an Artifact is an effect (the honesty rule)

Effect floors are static (`ConnectorOpMetadata`, `NodeIR.effect_hints`); they
cannot depend on which `ByteSource` variant shows up at runtime. So the rule
is: **an op whose input *can* carry `Artifact` bytes declares the store's
`read`/`write` hint on its static floor, unconditionally** — because it *may*
deref/stage. This is `min_effects`-is-a-floor (§4) applied to bytes.

Mechanically this reuses the op-per-method pattern (§0.2) as **op-per-effect-shape**:

| op | over the §4 floor | why |
| --- | --- | --- |
| `connector.http.get` / `.post` (JSON body) | unchanged | body is `Option<JsonValue>`; never touches the byte plane |
| `connector.http.get_binary` | **+ `resource::workspace::write`** | stages the 2xx body into a workspace artifact, returns `Artifact` |
| `connector.http.post_multipart` / `.put_multipart` | **+ `resource::workspace::read`** | a part may be `ByteSource::Artifact`, so it may deref |

The floor is visible in the `operation_id`, exactly like read-vs-write is
today. `ByteSource` is the *authoring* type (one surface everywhere bytes are
accepted); **op selection is what sets the static floor**. Want a node that
provably never touches the byte plane → use the JSON-body op, whose input
type cannot express `Artifact`. That is the "widen the input, pay the effect"
trade made concrete and compile-visible.

**Resolved `min_effects` per byte op (review F5).** `HINT_WORKSPACE_WRITE`
floors effects at `Effectful` (`workspace.rs` constraint), so **`get_binary`
is `Effectful`, not `ReadOnly`** — a binary GET both reads the remote and
mutates run state by staging a file. This is consistent with the §4
webhook-GET rule (a GET *can* be Effectful) and is stated, not hidden.
Crucially it does **not** pull in the §6 external-write idempotency ceremony:
the write target is **run-local, idempotent-by-path** storage (re-staging the
same artifact name overwrites; a replay re-downloads and re-stages to the same
path), so no exactly-once edge / dedupe key is required for the staging write.
`post_multipart` keeps its method's `Effectful` floor and adds
`workspace::read` (no determinism change). This resolution is a required
sentence in the H5c op metadata, not left implicit.

**Prerequisite for this floor to be enforceable, not asserted (review F1/F2 —
the load-bearing revision).** Today `ScopedResources::workspace()`
(`capabilities/src/scoped.rs:280`) returns the **whole raw `Workspace`** trait
(arbitrary-path `read/write/list/delete`) on *any* of
`Workspace|WorkspaceRead|WorkspaceWrite` — there is one accessor and one trait,
so a `workspace::read` grant confers write and delete, and a node can ignore
its handle and touch any path. The read/write split and handle-only surface of
§16.4 are therefore a **named enforcement refactor** (H5b), without which the
table above and every gate in §16.4 are decorative. H5b is why this contract
is not free.

v1.1 restricts `ByteSource::Artifact` and `get_binary` to **workspace-backed**
handles, so the added floor is exactly `resource::workspace::{read,write}`.
Blob-backed byte sources (adding `resource::blob::read`) are additive, built
when a template needs cross-run/content-addressed bytes.

### 16.4 The handle-only byte surface + three deref gates (revised per F1/F2)

The single most important correction from review: **the gates are only real if
the handle is the *only* way a node can reach bytes.** As the code stands, a
workspace-hinted node holds the raw `Workspace` trait and can read any path,
so mint/scope checks on a convenience wrapper protect nothing. So §16 mandates:

- **Nodes never receive the raw `Workspace` trait.** Byte-consuming nodes get a
  capability-narrowed view whose *only* methods are handle-scoped:
  - `WorkspaceWrite` view (granted by `resource::workspace::write`):
    `stage_artifact(name, bytes, content_type) -> Artifact` and `read(handle)`.
  - `WorkspaceRead` view (granted by `resource::workspace::read`):
    `read(handle) -> Bytes` only. No `stage`, no arbitrary-path read, no
    `list`/`delete`.
  The raw `Workspace` trait (arbitrary path) becomes **host-internal** —
  reachable only by the host fulfilling a handle-scoped op, never injected into
  a node. Any existing node that used raw-path workspace access migrates to the
  view (audited in H5b; the §16.5 example takes `ws: WorkspaceWrite`, not
  `Workspace`).
- **Read/write are distinct grants and distinct accessors.** `scoped.rs` gains
  `workspace_read()` / `workspace_write()` (mirroring `http_read()`/`http_write()`,
  scoped.rs:187–199), `GRANTS_WORKSPACE` splits so `read` does **not** confer
  `write`/`delete`, and the four host-wasmtime opcode handlers
  (`OP_WORKSPACE_READ`/`WRITE`/`LIST`/`DELETE`, host-wasmtime lib.rs:~1104–1123)
  gate per-opcode against the split grants and return a **structured**
  `MissingWorkspaceRead`/`MissingWorkspaceWrite` denial — today they route all
  four through one `resources.workspace()` and deny with an unstructured
  `"missing workspace provider"` backend string.

With that surface, a byte crossing passes only if **all three** hold:

1. **CAP110 grant** — the node holds the matching split grant
   (`workspace::read` to `read`, `workspace::write` to `stage`); a lie about
   the op floor yields a structured `MissingWorkspace*` denial **across the
   wasm boundary**, like `MissingHttpWrite` (§4).
2. **Mint verifies** — the macaroon chain folds from the host root key and the
   final `scope` satisfies every caveat (§16.2); guest cannot forge or widen.
3. **Scope contains target** — the requested path ⊆ `handle.scope` after
   `normalize_path` (traversal already rejected, `workspace.rs:177`).

Honesty test (mirrors the connector CAP110 denial tests, and is the H5b gate):
a node deref/stage with an empty grant set → structured `MissingWorkspace*`; a
`workspace::read`-only node attempting `stage`/`delete` → `MissingWorkspaceWrite`
(proves the split); a forged/widened macaroon → gate 2 failure; an out-of-scope
path on a valid handle → gate 3 failure. All four are load-bearing.

### 16.5 Response binary mode + request multipart surface

- **Ingress (`get_binary`) is a host-side composite op (revised per F4 — the
  original "streams, never materialized in guest memory" claim was false).**
  `HttpResponse.body` is a fully-buffered `Vec<u8>` (capabilities/src/lib.rs:774)
  and `Workspace::write_normalized` takes a complete `&[u8]` slice — there is no
  streaming path, and worse, routing the bytes guest→host through the current
  wasm workspace transport re-encodes them as a JSON *array of integers*
  (`serde_json::to_vec`, workspace.rs:~308/473), ~3.5× blowup — strictly worse
  than the base64 this whole section exists to avoid. So `get_binary` is
  specified as a **host-side composite**: the host performs the fetch **and**
  the stage, so the bytes never enter guest memory and never cross the cap
  boundary twice; the guest receives only the `Artifact` handle back. The op
  returns `Artifact { handle, content_type: <from response Content-Type>, len,
  content_hash }`. Size is bounded by `WorkspacePolicy.max_single_file_bytes`
  (over-limit → NodeError, no partial artifact); non-2xx / transport errors
  behave as §5 (NodeError). **H5a prerequisite:** fix the wasm workspace
  transport to binary length-prefixed framing (mirror the blob transport's
  `encode_put_request`, lib.rs:~2142, which already does this) so any
  guest-side `stage_artifact` on the egress path is not paying the JSON-int-array
  tax either.
- **Egress (`post_multipart`).** Body built via a `form!` macro over
  `(field, ByteSource)` pairs → `multipart/form-data`; each part's
  Content-Type comes from `Artifact.content_type` (now carried on
  `ByteSource::Artifact`, F7) or an explicit override. Raw single-body binary
  (`Content-Type: application/octet-stream` from one `ByteSource`) is the
  degenerate one-part case.
- **The CSV round-trip, end to end** (the maintainer's ergonomics test —
  two nodes, no base64, no checkpoint bloat):

  ```rust
  // Node 1: produce bytes + stage. Floor: resource::workspace::write.
  // Receives the WRITE view (§16.4), never the raw Workspace trait.
  #[def_node(effects = [workspace_write])]
  async fn build_report(rows: Vec<Row>, ws: WorkspaceWrite) -> NodeResult<Artifact> {
      let mut w = csv::Writer::from_writer(Vec::new());
      for r in &rows { w.serialize(r)?; }
      // stage_artifact hashes the bytes at stage time (F6), populating
      // Artifact.content_hash — the backends leave WorkspaceEntry.content_hash
      // None today, so the hash is computed here where the full bytes are held.
      ws.stage_artifact("report.csv", &w.into_inner()?, "text/csv").await
  }
  // Node 2: send it. Floor: resource::http::write + resource::workspace::read.
  connect!(build_report -> upload);
  http.post_multipart(url, form!{ "file" => artifact, "kind" => "daily" })
  ```

  CSV generation is plain Rust (`csv::Writer`) — the maintainer's "code stays
  in rust" position: serialization is a code node, not a Lattice primitive.
  Note this is distinct from "upload to Google Sheets," which is **not** a
  binary case — Sheets append is structured JSON rows through the typed
  connector path and never touches this section.

### 16.6 Workspace vs blob — pick workspace for transit

| | `Workspace` | `BlobStore` |
| --- | --- | --- |
| addressing | path (hierarchy, `list`) | flat key (no `list`) |
| lifecycle | **run-scoped, auto-GC'd** (`WorkspaceRunScope`, `WorkspacePolicy`) | caller-managed, cross-run |
| handle scopes | `Exact` + `Prefix` | `Exact` only (16.2) |
| use for | **transit files** (build-and-send, download-process-upload) | durable / content-addressed / cross-run artifacts |

Default is **workspace**: the file's lifetime is the run, so run-scoped
auto-cleanup prevents leaks, the sandbox blocks traversal, and
`WorkspacePolicy.max_single_file_bytes` gives the size cap for free. Blob is
the deliberate opt-in for durability.

### 16.7 Provenance forward-reference (do not design against this)

This section deliberately keeps two properties that the verifiable-compute
ladder (`ops/verifiable-compute-ladder.md`, tier 2 → provenance/integrity)
will consume: **artifacts stay content-addressed** (`Artifact.content_hash`,
computed at stage time by `stage_artifact` since the backends leave
`WorkspaceEntry.content_hash` `None` today — review F6) and **effect floors
stay static and op-visible** (§16.3). Together they mean an artifact transiting a flow becomes
a signable ledger line — `(op_id, workspace::{read,write}, artifact.content_hash)`
— rather than an opaque blob with an untraceable effect. No provenance
machinery is built here; the only ask is the negative one: **do not introduce
a byte-plane seam that is hard to attest later** (e.g. mutable-in-place handles
with no content hash, or a runtime-variant effect floor). The type-level scope
choice (16.2) is the more-attestable option for the same reason.

### 16.8 Packet decomposition (H5a–H5d, subagent-dispatchable — revised post-review)

- **H5a (M) — the byte-plane types + transport fix.** `Handle<S>` (sealed
  `Scope`, validating `Deserialize`, guest-side `narrow`/`file` **macaroon**
  attenuation — §16.2), `Artifact<S>`, `ByteSource` (carries `Artifact<Exact>`,
  F7) + `From` impls. Macaroon verification host-side (root key never in guest;
  fold chain, check caveats). **`stage_artifact` computes `content_hash` at
  stage time** (F6). **Fix the wasm workspace transport to binary
  length-prefixed framing** (mirror the blob transport `encode_put_request`;
  today it uses `serde_json::to_vec` → JSON-int-array blowup, F4). No http yet.
  Unit tests: macaroon narrows-not-widens and composes across split points,
  cross-scope deref fails, `Handle<Exact>` refuses a prefix wire value.
- **H5b (L — raised from M; the load-bearing enforcement refactor, F1/F2).**
  This is what makes "accepting an Artifact is an effect" *true* rather than
  asserted, and it is not free:
  1. **Handle-only node surface** — introduce `WorkspaceRead` / `WorkspaceWrite`
     views (handle-scoped methods only); stop injecting the raw `Workspace`
     trait into nodes (it becomes host-internal); migrate any existing
     raw-path node users (audit `scoped.rs` consumers).
  2. **Read/write grant + accessor split** — `scoped.rs` gains
     `workspace_read()`/`workspace_write()`, `GRANTS_WORKSPACE` splits so
     `read` does not confer `write`/`delete`.
  3. **Per-opcode host gating + structured denials** — the four host-wasmtime
     workspace opcode handlers gate against the split grants and emit
     structured `MissingWorkspaceRead`/`MissingWorkspaceWrite` across the wasm
     boundary (not the current unstructured `"missing workspace provider"`).
  4. **The four-case honesty test** (§16.4): empty-grant denial; read-only node
     denied `stage`/`delete`; forged/widened macaroon; out-of-scope path.
  Benefits every workspace consumer, not just connector.http — like H2b did for
  auth. H5c is gated on it.
- **H5c (M) — connector.http byte ops.** `get_binary` as a **host-side
  composite** (host fetches + stages; bytes never enter guest memory, F4;
  +workspace_write; **Effectful floor, run-local-idempotent so no dedupe edge**,
  F5) and `post_multipart`/`put_multipart` (+workspace_read, `form!` builder),
  §16.3 floors static in `ConnectorOpMetadata`, cross-mirror plumbing (same four
  mirror sites as Basic-auth, §7-F8). Runtime tests: binary download → artifact,
  multipart with inline + artifact parts, non-2xx unchanged. **Byte ops are
  Tier 0/1 only, both directions** — no `get_binary_any_origin` / multipart on
  Tier-2 (F8: a Tier-2 binary GET would stage attacker-chosen bytes under a
  valid handle; SSRF residual (a) must not upgrade to the byte plane).
- **H5d (S) — acceptance example + recipe.** Extend the s26 longtail example
  (or a new `sNN_binary`) with the CSV build-and-POST round-trip and a
  download→process→upload round-trip; CLI end-to-end + render clean; add the
  §16 normative recipe rules to `ops/clone-playbook.md` §3 (done: the
  binary-handoff row is flipped to RESOLVED).

Order: **H5a → H5b → (H5c ∥ H5d-scaffold) → H5d**. H5b gates H5c (no byte op
ships without its denial test). Blob-backed `ByteSource`, `Artifact<Prefix>`
(multi-file), and Tier-2 byte ops are explicitly out of H5 (additive
follow-ups).

### 16.9 Open questions (H5-specific)

1. **Macaroon vs host-call attenuation** — RESOLVED by review F3: macaroon
   caveat chain (guest-side narrowing composes correctly; host folds from root
   to verify). The naive HMAC-of-parent-mint chain is rejected (does not
   compose across narrowing split points).
2. **`get_binary` size ceiling** — cap via `WorkspacePolicy.max_single_file_bytes`
   (already exists) vs a per-op limit. Recommendation: reuse the policy cap;
   over-limit → NodeError, no partial artifact.
3. **`Artifact<Prefix>` (a directory as one value)** — needed for
   "download N files → hand the folder downstream." Recommendation: spec the
   type (it is free from `Handle<Prefix>`), build only when a template needs
   the multi-file case; v1.1 ships `Artifact<Exact>` (the `= Exact` default).
4. **Content-hash trust** — advisory in v1.1 (populate at stage, do not gate
   on it), promoted to load-bearing in the T-PROV provenance packet
   (`ops/verifiable-compute-ladder.md` §4) so the two land coherently.

### 16.10 Adversarial review record (2026-07-12)

Verdict: **ACCEPT-WITH-REVISIONS** (fable, all source verified). The core
decision — bytes off the JSON plane, `Artifact` indirection, static
op-shaped floors, type-level scope — survived and is consistent with §2/§4/§5.
The security narrative around it was *asserted, not achievable with the cited
machinery*; the load-bearing revisions are folded above:

- **R1 (F1+F2) — handle-only surface + workspace read/write split.** As-was,
  `ScopedResources::workspace()` returns the whole raw trait on any workspace
  hint, so all three deref gates and the read/write floor were decorative.
  Fixed: §16.4 mandates the views + accessor/grant split + per-opcode host
  gating; H5b raised to L and named as the reason this contract is not free.
- **R2 (F3) — macaroon mint, not HMAC-of-parent chain.** The naive chain does
  not compose; §16.2 now specifies a macaroon caveat chain (guest-side
  attenuation preserved, host-verifiable). Mint-token-is-a-bearer-secret-in-
  checkpoints recorded as an accepted, scope-confined, run-scoped residual.
- **R3 (F4+F5) — streaming claim corrected + effect floor stated.**
  `get_binary` is a host-side composite (no guest materialization; wasm
  workspace transport re-framed to binary in H5a), and is `Effectful`
  (workspace::write) but run-local-idempotent so it needs no dedupe edge.
- Also folded: F6 (hash at stage time), F7 (`ByteSource` carries `Artifact`),
  F8 (byte ops Tier 0/1 both directions). F9 (type-level scope soundness) held
  unchanged.

Residuals accepted consciously: (R2) scope-confined mint bytes in the data
plane / checkpoints; the pre-existing native-host single-trust-domain model
(a malicious Rust node can call the host-internal `Workspace` directly — same
posture as every capability, §7). These do not block H5.
