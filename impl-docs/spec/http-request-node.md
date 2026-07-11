Status: Draft (design only — no implementation; revised per adversarial review 2026-07-11, verdict ACCEPT-WITH-REVISIONS: F1 enforcement locus, F2 redirect precondition, F3 Tier-2 guarantee scope)
Purpose: spec
Owner: Core
Last reviewed: 2026-07-11

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
   Binary/file responses are **deferred to v1.1** behind the unresolved
   binary-handoff decision (workspace-handle recommendation).
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
| 2xx, binary content | as above — garbage in is an error, not silent bytes | same | `[HTTP104]` | v1: `body_text` lossy + flag. v1.1: `binary` mode returns a workspace artifact handle (`{ artifact: WorkspaceHandle, content_type, len }`), requires `resource::workspace::write` on the node, per the `ops/clone-playbook.md` §3 recommendation. Mode name reserved; **unusable until the binary-handoff decision is final.** |

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
- **H5 (S, after binary-handoff decision) — v1.1 binary mode.** `binary`
  response mode → workspace artifact handle (+`resource::workspace::write`
  hint), multipart/binary request bodies. Blocked on `clone-playbook` §3
  final call; do not start until it lands.

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
