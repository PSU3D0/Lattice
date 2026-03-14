Status: Draft
Purpose: spec
Owner: Runtime
Last reviewed: 2026-03-13

# Connector Manifest — Phase B Action-First Surface (0.1.x)

This document defines the **concrete Phase-B manifest surface** for
`LAT-000028`.

It turns the broader connector architecture into an implementation-oriented
contract for the first runnable connector-codegen landing.

## Scope of this document

This is the concrete manifest/codegen surface for the **action-first** phase.
It is intentionally narrower than the full long-horizon connector model, but it
must still be structurally compatible with later polling/webhook trigger work.

Related docs:
- `impl-docs/spec/connector-crate-surface.md`
- `impl-docs/spec/connector-trigger-runtime-contract.md`
- `impl-docs/spec/credential-provider.md`
- `impl-docs/spec/node-vs-capability-surface.md`

## Decision summary

Phase B should standardize:
- a canonical connector manifest file,
- a minimal typed manifest type system,
- semantic **action** surface generation,
- outbound-auth and endpoint-profile references,
- reserved but explicit trigger sections from day one.

Phase B should **not** require:
- runnable polling trigger runtime support,
- runnable webhook trigger runtime support,
- provisioning/inbound verifier execution,
- or plugin-inner support.

## Canonical manifest file

Recommended canonical manifest path:

```text
connector.yaml
```

Recommended Phase-B canonical format:
- **YAML**

Why YAML first:
- clearer for nested transport/profile examples,
- easy to author/read during farming research,
- aligns with other repo planning/config surfaces.

A later tool may accept JSON/TOML imports, but the first documented canonical
shape should be YAML.

## Phase-B tooling split

Phase B should assume three crates/components:

### 1. `crates/connector-spec`
Owns:
- manifest Rust models,
- schema validation,
- stable diagnostics,
- serialization helpers.

### 2. `crates/connectors-std`
Owns shared runtime helpers for generated connectors, for example:
- action transport execution,
- auth-profile application shims,
- simple pagination loops,
- response extraction/error mapping helpers.

### 3. `crates/connector-codegen` (new)
Owns:
- manifest loading,
- validation invocation,
- crate/file generation,
- deterministic output formatting.

## Generation model

Phase-B generation should be:
- **generation-time**, not build-time magic.

Recommended flow:
1. author `connector.yaml`
2. run `connector-codegen generate --manifest connector.yaml --out <crate_dir>`
3. commit the emitted Rust crate files
4. `cargo check` the generated crate

This avoids introducing build-script indirection for the first landing.

## Top-level manifest shape

```yaml
connector:
  id: connector.github.issues
  vendor: github
  family: issues
  version: 0.1.0
  crate: connector_github_issues
  summary: Semantic GitHub issues connector

profiles:
  outbound_auth:
    github_pat:
      kind: bearer
      handle_kind: http.bearer
  endpoint_profiles:
    github_default:
      base_url: https://api.github.com
      default_headers:
        Accept: application/json

  # reserved sections; Phase B documents these but does not need runnable impl
  provisioning_auth: {}
  inbound_verifiers: {}

types:
  ...

surfaces:
  ...
```

## Top-level sections

### `connector`
Required metadata about the connector family.

### `profiles`
Named references for auth, verifier, and endpoint/environment profiles.

### `types`
Typed input/output object/enum definitions used by surfaces.

### `surfaces`
A tagged list of connector semantic surfaces.

In Phase B:
- `action` is the runnable/generated surface,
- `polling_trigger` and `webhook_trigger` are documented and reserved,
  and may be parsed/validated or fail with a stable “not yet implemented”
  diagnostic.

## `connector` section

```yaml
connector:
  id: connector.github.issues
  vendor: github
  family: issues
  version: 0.1.0
  crate: connector_github_issues
  summary: Semantic GitHub issues connector
```

### Fields

- `id`: stable connector family identifier
- `vendor`: short vendor/group slug
- `family`: semantic family slug
- `version`: connector family version
- `crate`: emitted Rust crate name
- `summary`: human summary

### Rules

- `id` must be stable and portable.
- `crate` must be a valid Rust crate name.
- `vendor` and `family` should be deterministic enough to derive output paths.

## `profiles` section

### Phase-B supported runnable profile groups

#### `outbound_auth`
Used by action nodes for normal request execution.

#### `endpoint_profiles`
Used to select base URL and stable default request metadata.

### Reserved but not runnable in Phase B

#### `provisioning_auth`
For future activation/setup credentials.

#### `inbound_verifiers`
For future webhook verifier roles.

### Example

```yaml
profiles:
  outbound_auth:
    github_pat:
      kind: bearer
      handle_kind: http.bearer

    github_api_key:
      kind: api_key_header
      header_name: Authorization
      prefix: Bearer
      handle_kind: raw.secret

  endpoint_profiles:
    github_default:
      base_url: https://api.github.com
      default_headers:
        Accept: application/json
        X-GitHub-Api-Version: "2022-11-28"
```

## Phase-B outbound auth profile kinds

### `bearer`
Example:

```yaml
kind: bearer
handle_kind: http.bearer
```

### `api_key_header`
Example:

```yaml
kind: api_key_header
header_name: X-Api-Key
handle_kind: raw.secret
```

### `api_key_query`
Example:

```yaml
kind: api_key_query
query_name: api_key
handle_kind: raw.secret
```

## Design-complete but implementation-deferred auth kinds

These should be valid design targets but do not need runnable Phase-B support:
- `oauth2`
- `service_account_jwt`
- `session_bootstrap`
- `signed_request`

The manifest parser may:
- accept them with stable data models and clear “not yet implemented” codegen
  diagnostics, or
- reserve the enum variants and reject with a stable unsupported-surface
  diagnostic.

## `endpoint_profiles` section

Example:

```yaml
endpoint_profiles:
  github_default:
    base_url: https://api.github.com
    default_headers:
      Accept: application/json
      X-GitHub-Api-Version: "2022-11-28"
```

### Fields
- `base_url`: required
- `default_headers`: optional static header map

### Notes
- endpoint profiles are the Phase-B place for environment/server defaults
- tenant-specific runtime selection belongs in binding/deployment concerns, not
  flow payloads

## `types` section

Phase-B should use a small explicit type system.

### Supported root kinds
- `object`
- `enum`

### Supported field/container kinds
- `string`
- `bool`
- `u32`
- `u64`
- `i64`
- `f64`
- `bytes`
- `list`
- `object_ref`
- `enum_ref`
- `json` (escape hatch; discouraged)

### Example object type

```yaml
types:
  GithubIssuesListInput:
    kind: object
    fields:
      owner:
        type: string
      repo:
        type: string
      state:
        type: enum_ref
        target: GithubIssueState
        optional: true
      return_all:
        type: bool
        default: false
      limit:
        type: u32
        optional: true
```

### Example enum type

```yaml
  GithubIssueState:
    kind: enum
    variants:
      - open
      - closed
      - all
```

### Example nested object output

```yaml
  GithubIssueSummary:
    kind: object
    fields:
      number:
        type: u64
      title:
        type: string
      state:
        type: string
      html_url:
        type: string

  GithubIssuesListOutput:
    kind: object
    fields:
      items:
        type: list
        item:
          type: object_ref
          target: GithubIssueSummary
```

## `json` escape hatch

`json` should be available only as an explicit escape hatch.

Example:

```yaml
      raw_payload:
        type: json
        escape_hatch_reason: service returns unstable vendor-defined metadata
```

Phase-B rule:
- generated action connectors should prefer typed object/enum fields,
- `json` should be used sparingly and intentionally.

This aligns with:
- `impl-docs/spec/typed-boundary-policy.md`

## `surfaces` section

`surfaces` is a tagged list with `kind`.

### Phase-B supported runnable kind
- `action`

### Reserved kinds
- `polling_trigger`
- `webhook_trigger`

## `action` surface

### Example

```yaml
surfaces:
  - kind: action
    identifier: connector.github.issues.list
    name: GithubIssuesList
    summary: List issues for a repository
    input: GithubIssuesListInput
    output: GithubIssuesListOutput
    auth: github_pat
    endpoint: github_default
    effects: ReadOnly
    determinism: BestEffort
    resources:
      - http_read(capabilities::http::HttpRead)
    request:
      method: GET
      path_template: /repos/{owner}/{repo}/issues
      path_params:
        owner: owner
        repo: repo
      query:
        state: state
    pagination:
      kind: link_header_next
      enabled_from: return_all
      page_size_param: per_page
      page_size: 100
      max_items_from: limit
    response:
      kind: json_body
      root_path: body
```

### Required fields
- `kind`
- `identifier`
- `name`
- `summary`
- `input`
- `output`
- `endpoint`
- `effects`
- `determinism`
- `request`

### Optional fields
- `auth`
- `resources`
- `idempotency`
- `pagination`
- `response`

### Notes
- `resources` should drive effect/determinism/resource declarations truthfully
- effect/determinism must remain consistent with existing validation rules
- `auth` references a named outbound auth profile
- `endpoint` references a named endpoint profile

## Request mapping surface

### Minimal Phase-B request model

```yaml
request:
  method: GET | POST | PUT | PATCH | DELETE
  path_template: /repos/{owner}/{repo}/issues
  path_params:
    owner: owner
    repo: repo
  query:
    state: state
  body:
    title: title
    body: body
  headers:
    X-Custom-Header:
      const: static-value
```

### Mapping rules

#### `path_params`
Map template placeholders to input field names.

#### `query`
Map query parameters from optional input fields.
- absent optional fields are omitted

#### `body`
Map JSON body properties from input fields.
- absent optional fields are omitted

#### `headers`
Primarily for static or endpoint-level additions in Phase B.
Avoid overusing per-surface custom header mapping when auth profiles or endpoint
profiles are the better abstraction.

## Pagination surface

Phase-B should support a small set of reusable strategies.

### Required runnable strategy
- `link_header_next`

### Good follow-on strategies
- `cursor_token`
- `offset_page`
- `response_field_token`

### Example

```yaml
pagination:
  kind: link_header_next
  enabled_from: return_all
  page_size_param: per_page
  page_size: 100
  max_items_from: limit
```

### Semantics
- `enabled_from`: input field controlling whether pagination loop is used
- `page_size_param`: query parameter used to request page size
- `page_size`: static page size to request
- `max_items_from`: optional input field limiting collected results

## Response extraction surface

### Minimal Phase-B response model

```yaml
response:
  kind: json_body
  root_path: body
```

### Meaning
- decode response JSON into the declared output type
- `root_path` identifies where to decode from
- default may simply be `body`

A later phase may add richer response transformations, but Phase B should avoid
becoming a general transformation DSL.

## Reserved trigger kinds from day one

These must be representable in the documented surface even if codegen does not
emit runnable runtime support yet.

### `polling_trigger`

```yaml
  - kind: polling_trigger
    identifier: connector.google.sheets.rows_watch
    name: GoogleSheetsRowsWatch
    output: GoogleSheetRowChanged
    auth: google_oauth
    lifecycle: host_managed_poll
    poll:
      cursor_type: GoogleSheetRevisionCursor
      dedupe_key: revision_id
      minimum_interval: 60s
```

### `webhook_trigger`

```yaml
  - kind: webhook_trigger
    identifier: connector.slack.events
    name: SlackEvents
    output: SlackEventEnvelope
    provisioning_auth: slack_api
    verifier: slack_signing
    lifecycle: manual_external
    webhook:
      method: POST
      route_hint: slack/events
```

## Generated crate layout (Phase B)

Recommended output path:

```text
crates/connectors/<vendor>/<family>/
```

Recommended generated crate structure:

```text
crates/connectors/<vendor>/<family>/
  connector.yaml
  Cargo.toml
  src/
    lib.rs
    generated/
      mod.rs
      manifest.rs
      types.rs
      profiles.rs
      register.rs
      actions/
        mod.rs
        <action_name>.rs
    runtime/
      mod.rs
      transport.rs
      pagination.rs
      errors.rs
    ext.rs
  tests/
    manifest.rs
    contract.rs
```

## File responsibilities

### `src/lib.rs`
Thin stable exports.

### `src/generated/manifest.rs`
Generated constants and metadata.

### `src/generated/types.rs`
Generated input/output Rust structs and enums.

### `src/generated/profiles.rs`
Generated auth/endpoint profile constants and helpers.

### `src/generated/register.rs`
Generated `register_all(...)` surface.

### `src/generated/actions/*.rs`
Generated action-node implementations.

### `src/runtime/*`
Shared handwritten/runtime helper layer using `connectors-std`.

### `src/ext.rs`
Optional handwritten extension hooks / future escape hatch.

## Codegen output requirements

Phase-B codegen should produce:
- typed Rust structs/enums
- node specs
- register helpers
- action implementation modules
- deterministic formatting/order
- no required manual edits to compile the generated example crate

## First example connector recommendation

Recommended first example family:
- **GitHub issues**

Why:
- clear semantic actions (`get`, `list`, `create`)
- simple auth story for Phase B (`bearer`/PAT)
- useful pagination example (`link` header)
- good alignment with existing n8n declarative examples

Avoid as first runnable example:
- Slack webhook/event family
- Google Sheets polling trigger
- HubSpot/Jira managed webhook subscription families
- multi-step binary upload connectors

## Diagnostics expectations

Phase-B should emit stable diagnostics for:
- unknown profile references
- unknown type references
- unsupported `surfaces.kind`
- unsupported `profiles.outbound_auth.*.kind`
- invalid effect/determinism/resource declarations
- invalid `json` escape hatch usage without reason
- duplicate identifiers / duplicate generated module names

## Example manifest bundle

### GitHub issues — runnable Phase-B action slice

```yaml
connector:
  id: connector.github.issues
  vendor: github
  family: issues
  version: 0.1.0
  crate: connector_github_issues
  summary: Semantic GitHub issues connector

profiles:
  outbound_auth:
    github_pat:
      kind: bearer
      handle_kind: http.bearer
  endpoint_profiles:
    github_default:
      base_url: https://api.github.com
      default_headers:
        Accept: application/json
        X-GitHub-Api-Version: "2022-11-28"
  provisioning_auth: {}
  inbound_verifiers: {}

types:
  GithubIssueState:
    kind: enum
    variants: [open, closed, all]

  GithubIssuesListInput:
    kind: object
    fields:
      owner:
        type: string
      repo:
        type: string
      state:
        type: enum_ref
        target: GithubIssueState
        optional: true
      return_all:
        type: bool
        default: false
      limit:
        type: u32
        optional: true

  GithubIssueSummary:
    kind: object
    fields:
      number:
        type: u64
      title:
        type: string
      state:
        type: string
      html_url:
        type: string

  GithubIssuesListOutput:
    kind: object
    fields:
      items:
        type: list
        item:
          type: object_ref
          target: GithubIssueSummary

surfaces:
  - kind: action
    identifier: connector.github.issues.list
    name: GithubIssuesList
    summary: List issues for a repository
    input: GithubIssuesListInput
    output: GithubIssuesListOutput
    auth: github_pat
    endpoint: github_default
    effects: ReadOnly
    determinism: BestEffort
    resources:
      - http_read(capabilities::http::HttpRead)
    request:
      method: GET
      path_template: /repos/{owner}/{repo}/issues
      path_params:
        owner: owner
        repo: repo
      query:
        state: state
    pagination:
      kind: link_header_next
      enabled_from: return_all
      page_size_param: per_page
      page_size: 100
      max_items_from: limit
    response:
      kind: json_body
      root_path: body
```

### Slack events — reserved future trigger shape

```yaml
connector:
  id: connector.slack.events
  vendor: slack
  family: events
  version: 0.1.0
  crate: connector_slack_events
  summary: Slack event trigger family

profiles:
  outbound_auth:
    slack_api:
      kind: bearer
      handle_kind: http.bearer
  endpoint_profiles: {}
  provisioning_auth:
    slack_setup:
      kind: bearer
      handle_kind: http.bearer
  inbound_verifiers:
    slack_signing:
      kind: hmac_sha256
      handle_kind: raw.secret

types:
  SlackEventEnvelope:
    kind: object
    fields:
      type:
        type: string
      team_id:
        type: string
        optional: true
      event:
        type: json
        escape_hatch_reason: vendor event payload is variant-rich

surfaces:
  - kind: webhook_trigger
    identifier: connector.slack.events
    name: SlackEvents
    output: SlackEventEnvelope
    provisioning_auth: slack_setup
    verifier: slack_signing
    lifecycle: manual_external
    webhook:
      method: POST
      route_hint: slack/events
```

## Cross references

- `impl-docs/spec/connector-crate-surface.md`
- `impl-docs/spec/connector-trigger-runtime-contract.md`
- `impl-docs/spec/credential-provider.md`
- `ops/connector-crate-phasing-2026-03-13.md`
