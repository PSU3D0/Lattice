Status: Draft
Purpose: research-note
Owner: Core
Last reviewed: 2026-04-25

# SQL Capability Library Survey

This note is a preliminary think-through of Rust libraries that may reduce the burden of implementing `resource::sql` providers, dialect-aware helpers, and SQL validation. It is not a final dependency decision.

Primary spec:
- `impl-docs/spec/sql-capability.md`

Implementation plan:
- `impl-docs/spec/sql-capability-implementation-plan.md`

## Summary

No single Rust library appears to remove the need for Lattice SQL provider adapters.

Provider adapters remain necessary because Lattice must align database access with:

- `resource::*` hint semantics,
- read/write/admin authority separation,
- `ResourceAccess` / `ResourceBag` binding,
- host preflight,
- wasm guest/host transport,
- provider metadata (`SqlCapabilityInfo`),
- normalized error taxonomy,
- Cloudflare Workers/D1 runtime constraints.

Libraries can still help in four places:

1. **SQLx-family provider implementation**
   - `sqlx` SQLite for the first native/local provider,
   - `sqlx` Postgres/MySQL later,
   - `sqlx-d1` as the first candidate for Cloudflare D1 after Worker defaults and native SQLx provider shape are proven.

2. **Dialect-aware SQL generation**
   - `sea-query` remains a candidate for generating helper-level statements, but it is secondary to settling the provider/capability invariants.

3. **SQL parsing/classification/guardrails**
   - `sqlparser-rs` is useful for parsing and analysis, but should not be the sole security boundary.

4. **Fallback execution clients**
   - `rusqlite`, raw `worker::D1Database`, and focused clients like `tokio-postgres` remain fallback/provider-specific options if SQLx-family paths do not fit.

The important conclusion: prefer **SQLx as the background implementation family where it fits**, while keeping Lattice's capability traits/types/policy independent of SQLx. Prefer **generation** of dialect-specific SQL for helpers over **parsing and rewriting** arbitrary SQL.

## Library categories

## Native execution libraries

### `sqlx`

Use case:
- Preferred native provider family for SQLite first, Postgres/MySQL later.

Pros:
- Async.
- Supports SQLite, Postgres, and MySQL.
- Rich ecosystem and pooling for native/server hosts.
- Optional compile-time query checking for static application/provider queries.
- Aligns local SQLite and future Postgres/MySQL providers around one mental model.

Cons:
- Native SQLx SQLite is not the Cloudflare D1 runtime path by itself.
- Compile-time query macros do not compose cleanly with generic `SqlStatement { sql: String, ... }` execution.
- Core SQLx database clients are not the wasm guest story; wasm guests should use Lattice remote SQL transport.
- Dependency weight is higher than `rusqlite`, so target-gating and build impact must be checked.

Recommendation:
- Use SQLx SQLite for the first native/local provider unless an implementation spike shows unacceptable weight or target leakage.
- Do not expose SQLx types from `capabilities::sql`; wrap SQLx behind Lattice provider adapters.

### `rusqlite`

Use case:
- Fallback local SQLite provider if SQLx SQLite proves too heavy or awkward.

Pros:
- Already in the workspace.
- Mature SQLite binding.
- Fine for `sqlite` files and `:memory:` tests.
- Can map SQLite errors to `SqlError` taxonomy.

Cons:
- Synchronous API.
- Native/C SQLite dependency; not a general wasm guest answer.
- Does not help with a unified SQLx provider family.

Recommendation:
- Keep as fallback only. The current preferred sequence starts with SQLx SQLite to establish the SQLx-native pathway before D1 exploration.

### `tokio-postgres` / `postgres`

Use case:
- Dedicated Postgres provider.

Pros:
- Mature, focused Postgres client.
- Better control over provider-specific error mapping than a generic layer.

Cons:
- Native/server runtime only.
- Not useful for D1/SQLite.

Recommendation:
- Candidate for later `cap-sql-postgres` if we want a focused provider.

### `mysql_async`

Use case:
- Dedicated MySQL provider.

Recommendation:
- Later only, if real flows need MySQL-compatible providers.

### `libsql` / Turso ecosystem

Use case:
- Potential provider for libSQL/Turso or local embedded replicas.

Pros:
- SQLite-like semantics.
- Potentially relevant if Lattice wants edge/local replicated SQL.

Cons:
- Not a substitute for D1.
- Runtime and wasm suitability need a focused spike.

Recommendation:
- Defer until a deployment wants libSQL/Turso.

### Diesel

Use case:
- ORM/query builder for Rust apps.

Pros:
- Mature native Rust ORM.

Cons:
- Heavy and application-model oriented.
- Not aligned with generic `resource::sql` provider transport.
- Not wasm/D1-friendly.

Recommendation:
- Do not use for capability core.

## Cloudflare Workers / D1

### `sqlx-d1`

Use case:
- First candidate backend for Cloudflare D1 provider (`cap-sql-workers-d1`).

Observed facts from `https://github.com/ohkami-rs/sqlx-d1`:
- MIT licensed, active as of 2026-04.
- Exposes SQLx-style `D1Connection`, `query`, `query_as`, `query_scalar`, and `query!` macros.
- Uses Miniflare's local D1 SQLite file or `.sqlx` offline cache for compile-time verification.
- Compatibility matrix tracks the `worker` crate:
  - `sqlx-d1 0.3.*` -> `worker 0.7.*`,
  - `sqlx-d1 0.4.*` -> `worker 0.8.*`.
- Transactions and connection pools are unsupported.

Pros:
- Makes D1 look much more like the SQLx ecosystem.
- May let Lattice use SQLx-family providers across SQLite, D1, Postgres, and MySQL.
- Gives D1 code access to SQLx-style query macros and compile-time verification in suitable setups.
- Better ergonomics than hand-wrapping raw D1 for every query path.

Cons:
- Still needs a Lattice provider adapter for resource hints, metadata, errors, and bindings.
- Current project Worker defaults must be updated/checked before adopting latest `sqlx-d1`.
- No transactions or pools.
- Compile-time verification depends on Miniflare state or `.sqlx` cache and may support only constrained D1 setups.
- Generic dynamic `SqlStatement` execution will not benefit from `query!` macro checking.
- Error normalization may still be thin; Lattice must map SQLx/D1 errors into `SqlError`.

Recommendation:
- Do not start implementation with D1.
- First update Worker defaults, implement core SQL capability, and build the SQLx SQLite provider.
- Then run an isolated `sqlx-d1` spike and choose `sqlx-d1` as D1 backend only if compatibility, wasm build, and workerd behavior check out.

Spike result (2026-04-25): conditional GO for a small `sqlx-d1`-backed provider prototype after Worker defaults move to `worker = 0.8.1`.
- `sqlx-d1 0.4.1` + `worker 0.8.1` compiled for `wasm32-unknown-unknown`.
- A scratch Worker built with `worker-build --release` and ran under `wrangler dev --local`.
- D1 create/insert/select/update/delete worked.
- `INSERT ... RETURNING`, `UPDATE ... RETURNING`, and `DELETE ... RETURNING` worked and should be preferred for mutation outputs.
- `execute()` returned zeroed `rows_affected` and `last_insert_row_id`; do not depend on those through `sqlx-d1` without patching/upstream changes.
- `sqlx_d1::Error::Database(...).kind()` was `Other` and `message()` was generic; unique constraint normalization requires string parsing of `D1_ERROR: ... SQLITE_CONSTRAINT` unless `sqlx-d1` improves classification.
- Macro verification requires exactly one Miniflare D1 SQLite file under `.wrangler/state/v3/d1/miniflare-D1DatabaseObject` or an `.sqlx` cache; multi-D1 local macro workflows may be awkward.

### `worker` crate D1 bindings

Use case:
- Fallback Cloudflare D1 provider backend if `sqlx-d1` does not fit.

Pros:
- Direct access to Workers runtime D1 binding.
- Already used in S14.
- Correct low-level API for wasm32 Workers.

Cons:
- Provider-specific API.
- Does not provide SQLx ergonomics or macro checking.
- Does not provide generic Lattice metadata/errors/preflight.
- Does not help native/local SQLite.

Recommendation:
- Keep as fallback. Prefer evaluating `sqlx-d1` after Worker defaults and SQLx SQLite provider work.

## Dialect-aware SQL generation

### `sea-query`

Use case:
- Helper/library layer SQL rendering for SQLite/Postgres/MySQL-like dialects.

Pros:
- Generates SQL for multiple dialects.
- Better than ad-hoc string concatenation for table/column names and placeholders.
- Core query builder is not inherently tied to a specific async runtime.
- Useful for helpers like transcript ledger, outbox, idempotency table.

Cons:
- Need to verify wasm compatibility and dependency footprint.
- D1 is SQLite-like but not necessarily identical; may need custom handling or use SQLite builder with D1 metadata checks.
- Does not execute SQL.
- Does not eliminate provider adapters.

Recommendation:
- Strong candidate for helper-level SQL generation.
- Spike with S14 ledger SQL generation for SQLite and D1.

### `sea-orm`

Use case:
- Full ORM built on SeaQuery.

Pros:
- Rich application ORM.

Cons:
- Too high-level for Lattice capability core.
- Entity model and runtime assumptions likely do not match generic provider transport.
- D1 integration uncertain.

Recommendation:
- Avoid for core; maybe useful in application code outside Lattice capability layer, but not here.

### `quaint`

Use case:
- Query abstraction used by Prisma ecosystem.

Pros:
- Multi-dialect ideas.

Cons:
- Dependency/runtime fit uncertain.
- Likely not ideal as a foundational Lattice dependency.

Recommendation:
- Not first choice; only investigate if SeaQuery fails.

## SQL parsing / classification

### `sqlparser-rs`

Use case:
- Parse SQL to classify statement kind, inspect tables, maybe validate limited policies.

Pros:
- Pure Rust parser.
- Supports multiple dialects.
- Likely wasm-friendly in principle because it is not a native DB client.
- Useful for tooling and guardrails.

Cons:
- SQL dialect parsing is not easy, and parser support will never perfectly match every provider.
- Parsing is not execution semantics.
- Rewriting arbitrary SQL safely is hard.
- Should not be the sole enforcement mechanism for read/write/admin authority.

Recommendation:
- Use optionally for diagnostics, linting, tests, or statement classification.
- Do not make it a hard dependency of the core capability initially unless there is a concrete validation need.
- Do not rely on parser classification as the security boundary.

### `sqlformat` / formatters

Use case:
- Developer ergonomics and snapshot readability.

Recommendation:
- Not relevant to MVP capability core.

## Migration libraries

### `refinery`

Use case:
- Native migration runner.

Pros:
- Mature-ish Rust migration tool.

Cons:
- Not clearly useful for D1/Workers.
- A full migration engine is out of MVP scope.

Recommendation:
- Defer. Prefer out-of-band migrations first.

### `barrel`

Use case:
- Schema generation/migrations.

Recommendation:
- Defer. Query/schema generation can be reconsidered after SQL providers exist.

## Wasm-friendliness notes

There are two different wasm questions:

1. **Wasm guest/dynamic bundle**
   - The guest should not embed native SQLite/Postgres clients.
   - It should call `RemoteSqlRead`/`RemoteSqlWrite` through Lattice host transport.
   - Core types must be serde-friendly and wasm-safe.

2. **Cloudflare Workers provider**
   - The provider runs in wasm32 Workers but talks to D1 through the `worker` crate.
   - It is provider-specific, not a generic SQL client.

Because of this split, wasm-friendliness matters most for:

- `capabilities::sql` data types,
- remote transport encoding,
- optional query builders/parsers used inside guest code.

It matters less for native providers like `cap-sql-sqlite` or `cap-sql-postgres`.

## Dialect parsing vs dialect generation

The safer posture is:

```text
Generate dialect-specific SQL from helper operations where possible.
Parse arbitrary SQL only for diagnostics/guardrails.
```

For S14 ledger, the helper already knows the operations:

- create table,
- upsert discovered meeting,
- select due jobs,
- update job status,
- record upload destination.

It should not accept arbitrary SQL and rewrite it. It can render a small family of known statements based on provider metadata:

```rust
match info.dialect {
    SqlDialect::Sqlite | SqlDialect::CloudflareD1 => render_sqlite_style_statement(...),
    SqlDialect::Postgres => render_postgres_style_statement(...),
    _ => return Err(SqlError::IncompatibleProvider(...)),
}
```

A builder like SeaQuery may reduce manual dialect rendering, but explicit snapshots/tests should still pin the SQL that S14 emits for each supported dialect.

## What libraries can save us from

Likely saves:

- Hand-writing every SQL string for every helper and dialect (`sea-query`).
- Implementing native SQLite directly over FFI (`rusqlite`).
- Implementing native Postgres wire protocol (`tokio-postgres` or `sqlx`).
- Writing a parser from scratch for diagnostics (`sqlparser-rs`).

Likely does not save:

- Lattice `SqlRead`/`SqlWrite`/`SqlAdmin` trait design.
- `ResourceAccess`/`ResourceBag` integration.
- Host preflight and resource hints.
- D1 provider wrapper.
- Wasm guest/host transport.
- Capability metadata normalization.
- Error taxonomy normalization.
- Provider binding/catalog semantics.
- Policy decisions around read/write/admin.

## Recommended dependency posture

MVP sequence:

- Core `capabilities::sql`: no external SQL execution/parser dependency.
- Worker defaults: update/check before D1 work so the `sqlx-d1` compatibility matrix is not blocked by stale pins.
- SQLite provider: SQLx SQLite.
- D1 provider: defer until after core + SQLx SQLite; then evaluate `sqlx-d1` first and raw `worker` D1 as fallback.
- S14 helper: manual SQL first or small SeaQuery spike after provider invariants are stable.

Near-term provider spike:

1. Build `cap-sql-sqlx-sqlite` and prove native local SQL behavior.
2. Verify `capabilities::sql` builds for `wasm32-unknown-unknown`.
3. Verify SQLx SQLite provider is native-only and not accidentally included in wasm bundles.
4. Evaluate `sqlx-d1` in an isolated worktree against the selected Worker baseline.
5. If `sqlx-d1` works, use it under `cap-sql-workers-d1`; otherwise fallback to raw `worker::D1Database`.

Near-term helper spike:

1. Add a small prototype using `sea-query` to render S14 ledger statements for SQLite/D1 style SQL.
2. Snapshot the generated SQL.
3. Verify it works against SQLx SQLite and, later, D1/workerd.
4. If dependency footprint and wasm compatibility are acceptable, use SeaQuery in helper crates.

Optional tooling spike:

1. Use `sqlparser-rs` to classify statement kind in tests/lints.
2. Confirm it parses our supported statement subset for SQLite/D1/Postgres dialects.
3. Keep classification advisory, not authoritative.

## Proposed library spike checklist

- [ ] Verify current latest versions and licenses for `sqlx`, `sqlx-d1`, `sea-query`, `sqlparser`, `rusqlite`, `tokio-postgres`, and `libsql`.
- [x] Update/check Worker defaults before selecting a `sqlx-d1` version (`worker 0.8.1` check passed in `sql-worker-defaults` worktree).
- [x] Build `capabilities::sql` for `wasm32-unknown-unknown` (check passed in SQL core/provider worktrees).
- [x] Confirm SQLx SQLite provider is target-gated out of wasm builds (native provider tests passed; wasm capabilities check passed without pulling provider).
- [ ] Check `wasm32-unknown-unknown` build for `sea-query` with default features disabled if needed.
- [ ] Check `wasm32-unknown-unknown` build for `sqlparser` with default features disabled if needed.
- [ ] Generate S14 ledger SQL with SeaQuery for SQLite style.
- [x] Execute baseline SQL provider operations against local SQLx SQLite (`cap-sql-sqlx-sqlite` tests passed in worktree).
- [x] Execute baseline D1 operations against D1/workerd in `sqlx-d1` spike scratch crate.
- [ ] Compare generated SQL readability and stability against hand-written SQL.
- [ ] Decide whether SeaQuery belongs in core helpers, S14 only, or not at all.

## Current recommendation

Use libraries, but do not let any one library define the Lattice SQL capability.

The likely architecture is:

```text
capabilities::sql
  pure Lattice traits/types/metadata/errors

cap-sql-sqlx-sqlite
  SQLx SQLite-backed native/local provider

cap-sql-workers-d1
  preferably sqlx-d1-backed Workers/D1 provider after spike
  raw worker::D1Database fallback if needed

future providers
  SQLx Postgres/MySQL where suitable

helper crates / examples
  optionally SeaQuery for SQL generation
  optionally sqlparser-rs for diagnostics/linting
```

This keeps the capability stable, wasm transportable, and provider-governable while still avoiding unnecessary hand-rolled database clients or parser code. SQLx is the preferred implementation family where it fits; it is not the Lattice resource API.
