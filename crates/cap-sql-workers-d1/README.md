# cap-sql-workers-d1

Prototype Cloudflare Workers D1 SQL capability provider for Lattice.

Implements `capabilities::sql::{SqlRead, SqlWrite, SqlAdmin}` against a
Cloudflare D1 binding when compiled for `wasm32` Workers. On native targets
the provider is intentionally inert and returns `IncompatibleProvider`.

## Surface

- Provider kind: `sql.cloudflare.d1.prototype`.
- Dialect: `CloudflareD1` (SQLite-compatible) with `?` positional placeholders.
- Transactions: `SingleStatementAtomic` only (no explicit transactions).
- `query_write` + `RETURNING` is the recommended path for mutation output;
  see "Caveats" below.

## Provenance

This adapter is built on top of the
[`sqlx-d1`](https://crates.io/crates/sqlx-d1) crate (currently pinned to
`=0.4.1`), which is a third-party `sqlx` driver for Cloudflare D1 layered on
top of `workers-rs` (`worker = 0.8.x`). At the time of writing `sqlx-d1` is the
only feasible bridge between the `sqlx` query model and the D1 `fetch` API
exposed inside a Worker, but it has known shortcomings that bleed into our
capability surface:

- `execute` metadata (`rows_affected`, `last_insert_id`) is often unavailable
  or zeroed because D1's wire format does not always carry it back through
  `sqlx-d1`. Callers needing mutation output should use `query_write` with a
  `RETURNING` clause.
- Database error classification frequently arrives as `sqlx_d1::Error::Other`
  with a generic message. We therefore normalize errors partly via string
  inspection (see `normalize_database_message` in `src/lib.rs`), which is
  fragile against upstream wording changes.
- `batch` is best-effort and explicitly rejects `SqlBatchAtomicity::RequireAtomic`
  until the adapter switches to the lower-level Workers `D1Database::batch`
  API directly.
- Result columns are inferred from the first returned row; queries that
  return zero rows currently report an empty column list.

These caveats are also surfaced at runtime through `SqlCapabilityInfo.extensions`
(`prototype: true`, `execute_metadata`, `error_normalization`, etc.).

### Vendoring `sqlx-d1` is an acceptable future path

If we need to patch `sqlx-d1` to:

- preserve D1 result metadata through to `SqlExecuteResult`,
- expose richer typed `DatabaseError` variants for SQLite constraint codes
  (so we can drop the message-string heuristics), or
- adopt a more direct `D1Database::batch` integration with atomic semantics,

then vendoring `sqlx-d1` (e.g. under `crates/vendor/sqlx-d1/`) and pointing the
`Cargo.toml` dependency at the local path is an explicitly acceptable next
step. The crate is small and has a permissive license; a vendored fork keeps
us unblocked on D1-specific behavior without waiting on upstream.

## Tests

Unit tests for validation/normalization live in `src/lib.rs`. End-to-end
coverage against a real D1 binding lives in `workerd-tests/` and exercises
the capability through Miniflare:

```bash
cd crates/cap-sql-workers-d1/workerd-tests
npm install
npm test    # builds the wasm worker, then runs vitest against Miniflare
```

The fixture worker exposes `/admin/ddl`, `/write/execute`, `/write/returning`,
`/read/query`, and `/capability_info` routes for asserting CRUD, RETURNING, and
normalized constraint errors against an actual D1 backing store.

## Status

Prototype. Not for production until the metadata and error-classification gaps
above are closed, either upstream in `sqlx-d1` or via a vendored fork.
