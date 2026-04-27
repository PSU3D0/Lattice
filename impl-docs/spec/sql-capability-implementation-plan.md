Status: Draft
Purpose: implementation-plan
Owner: Core
Last reviewed: 2026-04-25

# SQL Capability Implementation Plan

This plan phases in `resource::sql` without forcing S14 or existing runtime hosts to jump directly from flow-local ledger traits to a full provider ecosystem.

Primary spec: `impl-docs/spec/sql-capability.md`

## Goals

- Add a low-level `resource::sql` capability family.
- Prefer a SQLx-backed provider implementation path where it fits:
  - SQLx SQLite for the first native/local provider,
  - SQLx Postgres/MySQL later,
  - `sqlx-d1` as the first candidate for Cloudflare D1 after the native path is proven.
- Let flows declare SQL read/write/admin resource hints.
- Keep higher-level ledgers/outboxes/checkpoints as libraries over SQL, not resource domains.
- Migrate S14 from a flow-local `TranscriptJobStore` abstraction toward a `TranscriptJobLedger` helper over generic SQL.
- Preserve wasm/dynamic-bundle host transport as a first-class path.
- Verify wasm compileability of the core SQL capability and any optional helper dependencies before they become part of wasm-targeted crates.

## Non-goals for the first implementation

- Full SQL dialect portability.
- A query optimizer, parser, or ORM.
- Long-lived transaction handles.
- A generalized migration engine.
- Postgres/MySQL providers in the first PR.
- Enforcing read/write/admin by parsing arbitrary SQL as the only security boundary.

## Execution model — worktree subagent driven

Implementation should be split into small, isolated branches/worktrees and delegated to subagents. The repo is currently carrying active S14 local-live changes, so create clean worktrees from the appropriate base branch before starting implementation PRs.

Recommended pattern:

```text
main checkout
  docs/sequencing update branch
  worker-defaults branch/worktree
  sql-core branch/worktree
  sqlx-sqlite-provider branch/worktree
  sqlx-d1-spike branch/worktree
```

Each worktree should have a narrow acceptance contract and return:

- changed files summary,
- test commands run,
- known gaps,
- follow-up tickets.

Do not mix the S14 local-live prototype changes, Worker runtime upgrades, SQL core capability work, and D1 exploration in the same worktree.

## Phase 0 — Decision record and naming cleanup

Deliverables:

1. Land the SQL capability spec and implementation plan.
2. Update `impl-docs/spec/capabilities-and-binding.md` to list SQL as the planned replacement for placeholder DB hints.
3. Update `impl-docs/spec/resource-catalog.md` examples from `resource::db` / `isolation.sql_schema_prefix (DB)` to `resource::sql`.
4. Decide deprecation posture for existing `resource::db::*`:
   - keep existing hint constants for compatibility,
   - document them as legacy/placeholder,
   - add tests that new SQL hints are preferred.

Acceptance:

- Docs make the resource/pattern/provider split clear:
  - `resource::sql` = low-level substrate,
  - D1/SQLite/Postgres = providers,
  - transcript ledger/outbox/etc. = helpers over SQL.

## Phase 1 — Workers defaults hygiene

Before SQL provider work, update Workers defaults as a hygiene task. This reduces version skew before evaluating `sqlx-d1`, whose current compatibility matrix tracks `worker` crate versions.

Deliverables:

1. Inventory all `worker` crate pins/usages across:
   - `crates/host-workers`,
   - `crates/cap-*-workers`,
   - `examples/*` Workers paths,
   - workerd tests.
2. Move default Worker runtime dependencies to the current intended baseline, preferably `worker 0.8.*` if compatible.
3. Update workerd/miniflare package fixtures if required.
4. Run existing Workers/workerd tests before changing SQL behavior.
5. Record any breaking Worker API changes that affect provider crates.

Acceptance:

```bash
cargo check --workspace --exclude <known-nonportable-if-any>
cd crates/host-workers/workerd-tests && npm test
```

If `worker 0.8.*` introduces unrelated breakage, land the smallest compatible Worker baseline update first and document the blocker before the D1 spike.

Worktree result (2026-04-25): `worker = 0.8.1` passed focused native/wasm checks for Worker crates, existing workerd tests, S14 wasm check, and a temp `sqlx-d1 0.4.1` compatibility probe. No Worker API source changes were required in existing provider crates.

## Phase 2 — Core capability types and hint registration

Files likely touched:

- `crates/capabilities/src/lib.rs`
- `crates/capabilities/src/hints.rs`
- `crates/dag-macros/src/lib.rs`
- `crates/dag-macros/tests/*`
- `crates/kernel-plan/src/lib.rs` if tests need explicit validation fixtures

Deliverables:

1. Add `capabilities::sql` module with:
   - `HINT_SQL`
   - `HINT_SQL_READ`
   - `HINT_SQL_WRITE`
   - `HINT_SQL_ADMIN`
   - `ensure_registered()` effect/determinism constraints
   - `SqlRead`, `SqlWrite`, `SqlAdmin` traits
   - statement/result/value/error/metadata structs

2. Add capability hint inference:
   - `sql_read` -> `resource::sql::read`
   - `sql_write` -> `resource::sql::write`
   - `sql_admin` -> `resource::sql::admin`
   - D1/SQLite/Postgres aliases infer SQL, not old DB.

3. Add `dag-macros` resource support:

```rust
resources(sql_read(capabilities::sql::SqlRead))
resources(sql_write(capabilities::sql::SqlWrite))
resources(sql_admin(capabilities::sql::SqlAdmin))
```

4. Add compile-time / macro tests:
   - read node emits `resource::sql::read` + `resource::sql` determinism hint,
   - write node emits `resource::sql::write`,
   - admin node emits `resource::sql::admin`,
   - `effects = Pure` with SQL read/write fails appropriately,
   - `determinism = Deterministic` with SQL access fails appropriately.

Acceptance commands:

```bash
cargo test -p capabilities sql
cargo test -p dag-macros sql
cargo test -p kernel-plan sql
```

## Phase 3 — ResourceAccess / ResourceBag / host preflight

Files likely touched:

- `crates/capabilities/src/lib.rs`
- `crates/host-inproc/src/lib.rs`
- `crates/kernel-exec/src/lib.rs` if dynamic/invoke resource access paths need passthrough
- worker host crates once remote transport is added

Deliverables:

1. Extend `ResourceAccess`:

```rust
fn sql_read(&self) -> Option<&dyn capabilities::sql::SqlRead> { None }
fn sql_write(&self) -> Option<&dyn capabilities::sql::SqlWrite> { None }
fn sql_admin(&self) -> Option<&dyn capabilities::sql::SqlAdmin> { None }
```

2. Extend `ResourceBag` with SQL storage and builders.

3. Extend host preflight:

```rust
resource::sql::read  -> resources.sql_read().is_some()
resource::sql::write -> resources.sql_write().is_some()
resource::sql::admin -> resources.sql_admin().is_some()
```

4. Add tests:
   - flow requiring SQL read fails without `SqlRead`,
   - flow requiring SQL write fails without `SqlWrite`,
   - read-only binding does not satisfy write,
   - admin binding is separate from write.

Acceptance:

```bash
cargo test -p host-inproc sql
cargo test -p kernel-exec sql
```

## Phase 4 — Native SQLite provider on SQLx

Suggested crate:

```text
crates/cap-sql-sqlx-sqlite
```

Initial backend:

- SQLx SQLite for native/local execution.
- This establishes the native SQLx provider pathway before adding Cloudflare/D1-specific complexity.
- This provider is not wasm-targeted in MVP; wasm guests should eventually use remote SQL capability transport instead of embedding SQLite.
- `rusqlite` remains a fallback if SQLx SQLite proves too heavy or awkward, but it is no longer the preferred first path.

Deliverables:

1. `SqlxSqlite` provider implementing:
   - `SqlRead`
   - `SqlWrite`
   - optionally `SqlAdmin`

2. Constructors:

```rust
SqlxSqlite::connect(path_or_url: impl AsRef<str>) -> Result<Self, SqlError>
SqlxSqlite::in_memory() -> Result<Self, SqlError>
```

3. Metadata:

```text
dialect = Sqlite
placeholder_styles = [? , :name]
consistency = Strong
transaction_support = ExplicitTransactions
features include:
  PositionalParams
  NamedParams
  Batch
  AtomicBatch
  ExplicitTransactions
  UpsertOnConflict
  Returning if supported by bundled SQLite version
  RowsAffected
  LastInsertId
  JsonFunctions if available/verified
  ForeignKeys
  Indexes
  UniqueConstraints
  Ddl
```

4. Error normalization:
   - unique constraint -> `ConstraintViolation { kind: Unique, ... }`
   - busy/locked -> `Busy`
   - invalid SQL -> `InvalidStatement` or `Provider`

5. Tests:
   - query basic rows,
   - execute insert/update/delete,
   - parameter binding for text/int/null/blob,
   - unique constraint violation normalization,
   - atomic batch rollback,
   - capability metadata contents.

6. Wasm compileability check:
   - confirm `capabilities::sql` builds for wasm,
   - confirm the SQLite provider is native-only and does not get pulled into wasm-targeted bundles accidentally,
   - record whether any SQLx core/helper types leak into wasm-targeted crates.

Acceptance:

```bash
cargo test -p cap-sql-sqlx-sqlite
cargo check -p capabilities --target wasm32-unknown-unknown
```

Worktree result (2026-04-25): `cap-sql-sqlx-sqlite` tests passed after aligning workspace `rusqlite` to `0.31` so SQLx SQLite and existing rusqlite users share `libsqlite3-sys 0.28`. `capabilities` wasm check passed. This confirmed the native SQLx path and exposed an integration invariant: adding SQLx SQLite may require keeping rusqlite/libsqlite3-sys versions aligned while older rusqlite users remain in the workspace.

## Phase 5 — SQLx/D1 feasibility spike

Do not start with a D1 provider. First prove the generic SQL capability and native SQLx SQLite path. Then evaluate whether Cloudflare can use the same SQLx-family approach via `sqlx-d1`.

Spike target:

```text
https://github.com/ohkami-rs/sqlx-d1
```

Known facts at planning time:

- `sqlx-d1` exposes SQLx-style `D1Connection`, `query`, `query_as`, and `query!` macros.
- It uses Miniflare's local D1 SQLite file or `.sqlx` offline cache for compile-time verification.
- Current compatibility table:
  - `sqlx-d1 0.3.*` -> `worker 0.7.*`,
  - `sqlx-d1 0.4.*` -> `worker 0.8.*`.
- Transactions and connection pools are not supported by D1/sqlx-d1.

Deliverables:

1. Confirm Worker baseline compatibility after Phase 1.
2. Test `sqlx-d1` in an isolated worktree against the selected Worker baseline.
3. Create a tiny prototype provider wrapper:

```rust
WorkersD1Sqlx {
    conn: sqlx_d1::D1Connection,
}
```

4. Verify D1 behavior in workerd/miniflare:
   - create table through fixture or admin/setup path,
   - insert/select/update/delete,
   - `RETURNING` if used by helpers,
   - unique constraint violation mapping,
   - rows affected / last insert id behavior,
   - query macro behavior with Miniflare and `.sqlx` cache.
5. Verify wasm build/runtime posture:
   - `sqlx-d1` compiles for the Workers target,
   - native SQLx SQLite provider does not get pulled into Workers bundles,
   - any shared SQLx helper crate remains wasm-compatible or is target-gated.
6. Decide one of:
   - proceed with `sqlx-d1` provider,
   - use raw `worker::D1Database` provider as fallback,
   - defer D1 provider until Worker/sqlx-d1 versions settle.

Acceptance:

```bash
cd crates/host-workers/workerd-tests && npm run test -- --run -t "sqlx d1"
```

Spike result (2026-04-25): conditional GO for a small `sqlx-d1` provider prototype. `sqlx-d1 0.4.1` with `worker 0.8.1` compiled to wasm, built with `worker-build`, and ran against local D1. Basic CRUD and `RETURNING` worked. Caveats: `execute()` metadata returned zeroed `rows_affected`/`last_insert_row_id`; database errors classify as `Other` with generic `message()`, so Lattice constraint normalization would need string parsing or upstream/provider patches.

## Phase 6 — Cloudflare D1 provider

Suggested crate:

```text
crates/cap-sql-workers-d1
```

Backend:

- Prefer `sqlx-d1` if Phase 5 passes.
- Fallback: raw `worker` crate D1 bindings, wasm32 only.
- Use workerd/miniflare tests for runtime proof.

Deliverables:

1. `WorkersD1Sql` provider implementing:   - `SqlRead`
   - `SqlWrite`
   - optionally `SqlAdmin` if schema execution is intentionally exposed for setup/test contexts.

2. Constructors:

```rust
WorkersD1Sql::from_env(env: &worker::Env, binding: &str) -> Result<Self, worker::Error>
WorkersD1Sql::from_database(database: worker::d1::D1Database) -> Self
```

3. D1 binding behavior:
   - translate `SqlValue` to SQLx/D1 supported bind values,
   - map SQLx/D1 result rows back to `SqlValue`,
   - normalize common D1/sqlx-d1 errors.

4. Metadata:

```text
dialect = CloudflareD1
placeholder_styles = provider-verified (? and/or named style)
consistency = ProviderDefined unless stronger semantics are confidently documented for the binding
transaction_support = AtomicBatch or ProviderDefined, only if verified
features include only verified D1 behavior
limits filled from known D1 limits where stable
```

5. Workerd tests:
   - provision test D1 binding,
   - create table through setup/admin path or test fixture,
   - read/write rows through capability,
   - verify ResourceBag binding and host preflight,
   - verify missing SQL binding fails before invocation.

Acceptance:

```bash
cd crates/host-workers/workerd-tests && npm run test -- --run -t "sql"
```

## Phase 7 — Remote wasm guest transport

Dynamic bundles need remote SQL capability transport, similar to blob/http remote capability support.

Files likely touched:

- `crates/capabilities/src/lib.rs` SQL module
- `crates/kernel-exec/src/lib.rs`
- `crates/host-workers/src/lib.rs` or host import wiring
- workerd tests

Deliverables:

1. Define SQL opcode family:

```rust
OP_FAMILY_SQL
OP_SQL_QUERY
OP_SQL_EXECUTE
OP_SQL_BATCH
OP_SQL_EXECUTE_DDL
```

2. Add wasm-side remote providers:

```rust
RemoteSqlRead
RemoteSqlWrite
RemoteSqlAdmin
```

3. Encode/decode request/response using `serde_json` first. Optimize later only if needed.

4. Host import dispatch routes SQL ops to the bound provider.

5. Tests:
   - wasm guest queries SQL through host-bound SQLite or D1 test provider,
   - missing provider maps to capability error,
   - error envelope preserves taxonomy.

Acceptance:

```bash
cargo test -p capabilities sql_remote
cd crates/host-workers/workerd-tests && npm run test -- --run -t "remote sql"
```

## Phase 8 — Binding catalog and requirement metadata

Files likely touched:

- `impl-docs/spec/resource-catalog.md`
- resource catalog schema/validator if present
- CLI binder/preflight if present

Deliverables:

1. Add provider registry entries:
   - `sql.sqlx_sqlite` (or final chosen `sql.sqlite` alias backed by SQLx),
   - `sql.cloudflare_d1` once D1 provider strategy is chosen.

2. Add `provides[]` validation for SQL hints.

3. Add portable SQL metadata fields to catalog/lock schema where needed.

4. Add MVP requirement evaluation for SQL:
   - presence: read/write/admin,
   - optional features: all-of feature set,
   - optional dialect allow-list,
   - optional transaction support at-least.

5. Unknown-handling:
   - if a requirement is set and provider reports unknown, fail preflight.
   - if no requirement is set, unknown metadata is allowed.

Acceptance:

- Catalog tests reject provider kinds that claim unsupported SQL hints.
- Preflight distinguishes:
  - missing SQL capability (`CAP101`),
  - present but incompatible SQL provider (new planned code, e.g. `CAP102`).

## Phase 9 — S14 migration to SQL helper

Files likely touched:

- `examples/s14_meeting_transcript_sync/src/*`
- existing `cloudflare/d1_store.rs`
- local SQLite adapter code

Deliverables:

1. Introduce `TranscriptJobLedger` helper over generic SQL:

```rust
pub struct TranscriptJobLedger {
    read: Arc<dyn SqlRead>,
    write: Arc<dyn SqlWrite>,
}
```

2. Move S14 schema SQL into helper modules with dialect-aware rendering where necessary.

3. Keep domain methods:
   - `upsert_discovered`
   - `due_jobs`
   - `save_job`
   - inspection/status helpers later

4. Replace direct `TranscriptJobStore` implementations with:
   - local provider binding: `cap-sql-sqlx-sqlite` + `TranscriptJobLedger`,
   - Workers provider binding: `cap-sql-workers-d1` + `TranscriptJobLedger` after the D1 strategy is proven.

5. Update S14 flow resource hints:

```rust
resources(
    http_read(capabilities::http::HttpRead),
    http_write(capabilities::http::HttpWrite),
    sql_read(capabilities::sql::SqlRead),
    sql_write(capabilities::sql::SqlWrite)
)
```

6. Tests:
   - existing S14 engine tests still pass with in-memory/fake path or SQLite `:memory:`.
   - local SQLite ledger tests prove idempotent upsert and due selection.
   - D1/workerd test proves equivalent behavior.

Acceptance:

```bash
cargo test -p example-s14-meeting-transcript-sync
cargo check -p example-s14-meeting-transcript-sync
```

## Phase 10 — Provider expansion and reusable helpers

Potential providers:

- `cap-sql-postgres`
- `cap-sql-mysql`
- `cap-sql-libsql` / Turso if useful

Potential helper crates:

- `lattice-sql-ledger`
  - reusable due-row/status/retry table pattern over SQL,
  - still not a `resource::*` domain.

- `lattice-sql-outbox`
  - outbox table pattern over SQL + queue/http dispatch.

- `lattice-sql-idempotency`
  - unique-key claim/check table pattern over SQL.

Promotion rule:

- Start helpers application-local.
- Extract only after two or more examples converge on the same pattern.

## Compatibility and migration from `resource::db`

Current code has placeholder DB hints. Suggested posture:

1. Leave constants for one release window:

```text
resource::db
resource::db::read
resource::db::write
```

2. Stop emitting them for new aliases.
3. Update docs to mark them legacy/placeholder.
4. Optionally map old DB hints to SQL in host preflight only if that avoids breaking existing examples.
5. Prefer explicit diagnostics:

```text
resource::db::* is deprecated; use resource::sql::* for relational SQL stores.
```

## Risk register

### Risk: pretending SQL is portable

Mitigation:
- Dialect/features/limits are first-class metadata.
- Helpers validate provider info.
- Provider-specific SQL remains allowed inside provider-aware helpers.

### Risk: read/write/admin enforcement by SQL parsing is weak

Mitigation:
- Split handles at the host/provider boundary.
- Provider APIs should expose read/write/admin handles separately when possible.
- Any parser-based classification is advisory, not the only control.

### Risk: D1 semantics are assumed but not verified

Mitigation:
- Advertise conservative metadata.
- Workerd tests verify behavior used by S14.
- Use `ProviderDefined`/`Unknown` where guarantees are not proven.

### Risk: remote wasm transport becomes too complex

Mitigation:
- Use JSON envelopes first.
- Keep MVP operations small: query, execute, batch.
- Defer streaming and transactions.

### Risk: helpers become hidden resources

Mitigation:
- Keep resource hints at SQL level.
- Domain helpers validate SQL provider compatibility but do not become `resource::*` domains.

## Suggested first PR breakdown

1. **Docs-only PR**
   - `sql-capability.md`
   - this plan
   - library survey updates
   - updates to capabilities/resource catalog docs

2. **Worker defaults PR**
   - update Worker runtime defaults/pins,
   - prove existing workerd tests still pass,
   - unblock current `sqlx-d1` compatibility where possible.

3. **Core capability PR**
   - `capabilities::sql`
   - hints/macros/tests
   - ResourceAccess/ResourceBag/preflight
   - wasm check for pure capability types.

4. **SQLx SQLite provider PR**
   - `cap-sql-sqlx-sqlite`
   - native tests
   - verify provider is target-gated out of wasm bundles.

5. **SQLx-D1 spike PR / report**
   - isolated worktree,
   - evaluate `sqlx-d1`,
   - produce go/no-go recommendation.

6. **D1 provider PR**
   - `cap-sql-workers-d1`
   - use `sqlx-d1` if accepted; raw worker D1 fallback otherwise,
   - workerd tests.

7. **S14 migration PR**
   - `TranscriptJobLedger` over SQL
   - local SQLite first,
   - D1 path after provider proof.

8. **Remote wasm SQL PR**
   - only if dynamic bundles need SQL before S14 migration is considered complete.

## Open implementation decisions

1. Should `SqlWrite::query_write` be in MVP, or should write-returning use `SqlRead + SqlWrite` plus provider policy?
2. Should `SqlAdmin` be compiled into all runtimes but disabled by binding policy, or feature-gated out of runtime hosts?
3. Should the SQLite provider crate be named `cap-sql-sqlx-sqlite` explicitly, or should provider implementation detail stay hidden behind `cap-sql-sqlite`?
4. Should D1 provider live in `cap-sql-workers-d1` or inside a broader `cap-sql-workers` crate?
5. Should `resource::db::*` preflight alias to SQL or fail with a deprecation diagnostic?
6. Should `sqlx-d1` become the default D1 backend, or remain optional behind a feature until it has more production proof?
