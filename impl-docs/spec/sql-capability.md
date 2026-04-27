Status: Draft
Purpose: spec
Owner: Core
Last reviewed: 2026-04-25

# SQL Capability (`resource::sql`)

This document proposes a first-class low-level SQL capability for Lattice. It is intended to replace the current placeholder `resource::db` hints with a narrower, infra-shaped resource surface that can be satisfied by concrete providers such as SQLite, Cloudflare D1, Postgres, and potentially MySQL-compatible services.

Related specs:
- `impl-docs/spec/capabilities-and-binding.md`
- `impl-docs/spec/resource-catalog.md`
- `impl-docs/spec/node-vs-capability-surface.md`

## Motivation

Several flows need durable, queryable state with indexes, unique constraints, and due-row selection. S14 meeting transcript sync is the immediate forcing example: it needs a durable table of discovered meetings, source-resolution state, retry state, upload results, and operator-inspectable failure status.

That application-level structure is a ledger, but **ledger is not the right `resource::*` abstraction**. Existing Lattice resources are low-level infrastructure affordances:

- `resource::http`
- `resource::kv`
- `resource::blob`
- `resource::queue`
- `resource::workspace`
- `resource::clock`
- `resource::dedupe`

A `resource::job_ledger` or `resource::reconcile_ledger` would be too workflow-specific. Conversely, `resource::db` is too broad: document stores, vector stores, embedded analytical engines, relational SQL stores, and caches can all be called databases while exposing very different semantics.

The proposed middle is:

```text
resource::sql
```

The resource means: this node may issue parameterized SQL statements against a bound SQL-capable provider, subject to declared access level and provider-advertised dialect/features/limits.

Higher-level patterns such as transcript ledgers, outboxes, inboxes, idempotency tables, and checkpoint tables should be implemented as libraries over `resource::sql`, not as resource domains themselves.

## Design principles

1. **Low-level, not domain-specific**
   - SQL is an infrastructure substrate.
   - Job ledgers and reconciliation state machines are libraries over SQL.

2. **Provider-bound, not provider-specific in flow logic**
   - A flow declares SQL read/write needs.
   - Deployment binds SQLite, D1, Postgres, etc.

3. **Honest portability**
   - SQL strings are not universally portable.
   - Providers expose dialect/features/limits.
   - Helpers validate required features and render dialect-appropriate statements where necessary.

4. **Read/write/admin separation**
   - `SELECT`-style reads are not equivalent to writes.
   - DDL/migration authority is stronger than normal data writes.

5. **Parameterized statements by default**
   - The core API should favor bound parameters and make string interpolation unnecessary for values.

6. **No ORM in the capability**
   - The capability executes SQL and returns rows.
   - Schema helpers, query builders, migrations, and domain ledgers live above it.

## Capability domains and hints

Canonical hint ids:

```text
resource::sql
resource::sql::read
resource::sql::write
resource::sql::admin
```

Effect/determinism constraints:

| Hint | Minimum effect | Determinism constraint | Meaning |
| --- | --- | --- | --- |
| `resource::sql::read` | `ReadOnly` | `resource::sql => BestEffort` | Node reads external SQL state. |
| `resource::sql::write` | `Effectful` | `resource::sql => BestEffort` | Node mutates SQL state. |
| `resource::sql::admin` | `Effectful` | `resource::sql => BestEffort` | Node/tool may execute schema/admin SQL. |

`resource::sql::admin` is intentionally separate from `resource::sql::write`. Normal application workflows should rarely declare admin access; setup/migration tools may.

## Capability traits

The first version should split read/write/admin capabilities. A provider can implement one or more traits, and a host can expose only the traits a node declared.

```rust
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait SqlRead: Capability {
    async fn query(&self, statement: SqlStatement) -> Result<SqlQueryResult, SqlError>;

    fn capability_info(&self) -> SqlCapabilityInfo;
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait SqlWrite: Capability {
    async fn execute(&self, statement: SqlStatement) -> Result<SqlExecuteResult, SqlError>;

    async fn query_write(&self, statement: SqlStatement) -> Result<SqlQueryResult, SqlError> {
        Err(SqlError::Unsupported(SqlFeature::WriteReturning))
    }

    async fn batch(&self, batch: SqlBatch) -> Result<SqlBatchResult, SqlError> {
        Err(SqlError::Unsupported(SqlFeature::Batch))
    }

    fn capability_info(&self) -> SqlCapabilityInfo;
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait SqlAdmin: Capability {
    async fn execute_ddl(&self, statement: SqlStatement) -> Result<SqlExecuteResult, SqlError>;

    async fn migrate(&self, migration: SqlMigration) -> Result<SqlMigrationResult, SqlError> {
        Err(SqlError::Unsupported(SqlFeature::Migrations))
    }

    fn capability_info(&self) -> SqlCapabilityInfo;
}
```

### Why not one `Sql` trait?

A single trait would be easier to wire, but it would blur authority. Lattice already distinguishes `HttpRead` and `HttpWrite`; SQL has the same need, with an additional schema-management tier.

A read-only node should not receive a handle that can write rows. A data-writing node should not automatically receive a handle that can alter schemas.

## Statement model

The core statement type should be intentionally simple:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlStatement {
    pub sql: String,
    pub params: Vec<SqlValue>,
    pub named_params: BTreeMap<String, SqlValue>,
    pub options: SqlStatementOptions,
}
```

`params` and `named_params` are mutually exclusive for a single statement in MVP. Providers should reject a statement that sets both unless they explicitly support mixing.

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SqlValue {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Text(String),
    Bytes(Vec<u8>),
}
```

Avoid first-class timestamp/decimal/json types in MVP. Those vary by provider and can be represented initially as text/blob with helper-level conventions. Type hints and feature flags can grow later.

```rust
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SqlStatementOptions {
    pub timeout_ms: Option<u64>,
    pub max_rows: Option<u32>,
    pub statement_kind: Option<SqlStatementKind>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlStatementKind {
    Read,
    Write,
    Ddl,
}
```

`statement_kind` is advisory and useful for early rejection. It is not a security boundary; providers and hosts must not rely on string classification alone to enforce authority.

## Query result model

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlQueryResult {
    pub columns: Vec<SqlColumn>,
    pub rows: Vec<SqlRow>,
    pub rows_returned: u64,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlColumn {
    pub name: String,
    pub type_hint: Option<SqlTypeHint>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlRow {
    pub values: Vec<SqlValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlTypeHint {
    Null,
    Bool,
    Integer,
    Real,
    Text,
    Blob,
    Json,
    Timestamp,
    Unknown,
}
```

Rows are vector-based rather than map-based to preserve column order and avoid duplicate-column-name ambiguity. Helpers can convert rows to structs or maps when appropriate.

`cursor` is reserved for providers that support cursor/pagination semantics. Most MVP providers may return `None`.

## Execute result model

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlExecuteResult {
    pub rows_affected: Option<u64>,
    pub last_insert_id: Option<SqlValue>,
}
```

These fields are optional because providers differ in what they can report reliably.

## Batch and atomicity

Long-lived transaction handles are awkward across trait objects, wasm guest/host transports, remote providers, and Cloudflare Workers-style bindings. MVP should prefer batch semantics:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlBatch {
    pub statements: Vec<SqlStatement>,
    pub atomicity: SqlBatchAtomicity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlBatchAtomicity {
    BestEffort,
    RequireAtomic,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlBatchResult {
    pub outcomes: Vec<SqlStatementOutcome>,
    pub atomic: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SqlStatementOutcome {
    Query(SqlQueryResult),
    Execute(SqlExecuteResult),
}
```

Provider behavior:
- If `RequireAtomic` is requested and the provider cannot guarantee atomic batch behavior, it must return `SqlError::Unsupported(SqlFeature::AtomicBatch)` or a more specific incompatibility error.
- If `BestEffort` is requested, partial execution may be possible and should be documented in provider metadata.

Explicit transaction handles can be added later if real examples need them.

## Migration/admin model

Schema changes should normally be out-of-band from ordinary flow execution.

MVP supports three modes:

1. **Out-of-band migrations**
   - Recommended first posture.
   - Wrangler/D1 migrations, local SQLite setup scripts, or deployment tools manage schema.

2. **Admin-capability setup tools**
   - A CLI/setup command can declare/use `resource::sql::admin` and call migration helpers.

3. **Self-migrating flows**
   - Allowed only if explicitly declared and accepted by policy.
   - Not recommended for normal scheduled/runtime flows.

Migration types can start minimal:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlMigration {
    pub id: String,
    pub statements: Vec<SqlStatement>,
    pub atomicity: SqlBatchAtomicity,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlMigrationResult {
    pub applied: bool,
    pub statements_executed: usize,
}
```

A full migration registry/checksum engine is not required for the first SQL capability.

## Provider metadata

Each provider must expose machine-readable metadata. This mirrors `KvCapabilityInfo` but accounts for SQL dialect and feature variability.

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlCapabilityInfo {
    pub dialect: SqlDialect,
    pub placeholder_styles: Vec<SqlPlaceholderStyle>,
    pub consistency: SqlConsistency,
    pub transaction_support: SqlTransactionSupport,
    pub features: BTreeSet<SqlFeature>,
    pub limits: SqlLimits,
    pub extensions: BTreeMap<String, serde_json::Value>,
}
```

### Dialect

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SqlDialect {
    Ansi,
    Sqlite,
    CloudflareD1,
    Postgres,
    MySql,
    Unknown,
}
```

D1 is SQLite-like but should be distinct. It has Cloudflare-specific APIs, operational limits, and runtime constraints.

### Placeholder styles

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SqlPlaceholderStyle {
    Question,        // ?
    NumberedDollar, // $1, $2
    NamedColon,     // :name
    NamedAt,        // @name
}
```

Helpers can render different statement strings based on supported placeholder styles.

### Consistency

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlConsistency {
    Strong,
    Eventual,
    ReadYourWrites,
    ProviderDefined,
    Unknown,
}
```

This should describe the bound provider instance, not the product in the abstract. If a provider cannot guarantee or detect semantics, it should report `Unknown` or `ProviderDefined` rather than overclaim.

### Transaction support

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SqlTransactionSupport {
    None,
    SingleStatementAtomic,
    AtomicBatch,
    ExplicitTransactions,
    Unknown,
}
```

Ordered comparisons are useful for requirements such as `at_least: AtomicBatch`, except `Unknown` must fail if a requirement is set.

### Features

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SqlFeature {
    PositionalParams,
    NamedParams,
    Batch,
    AtomicBatch,
    ExplicitTransactions,

    Returning,
    WriteReturning,
    UpsertOnConflict,
    LastInsertId,
    RowsAffected,

    JsonFunctions,
    ForeignKeys,
    Indexes,
    UniqueConstraints,

    Ddl,
    Migrations,

    PaginationCursor,
    StreamingRows,
    ReadReplicas,
}
```

The feature set should remain conservative. A provider should advertise a feature only when the bound instance can support it in the way the capability contract expects.

### Limits

```rust
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SqlLimits {
    pub max_statement_bytes: Option<u64>,
    pub max_params: Option<u32>,
    pub max_rows_returned: Option<u64>,
    pub max_batch_statements: Option<u32>,
    pub max_result_bytes: Option<u64>,
    pub timeout_ms: Option<u64>,
}
```

Unknown limit fields should be `None`. Providers should not guess.

## Requirements and preflight

MVP preflight can remain domain-level:

```text
node declares resource::sql::read -> ResourceAccess::sql_read().is_some()
node declares resource::sql::write -> ResourceAccess::sql_write().is_some()
node declares resource::sql::admin -> ResourceAccess::sql_admin().is_some()
```

A later requirement language should support provider compatibility checks, for example:

```json
{
  "sql": {
    "dialect": { "any_of": ["sqlite", "cloudflare_d1"] },
    "transaction_support": { "at_least": "atomic_batch" },
    "features": {
      "all_of": ["positional_params", "upsert_on_conflict", "rows_affected", "unique_constraints"]
    }
  }
}
```

Until that is lifted into Flow IR/binding preflight, helpers should perform runtime validation:

```rust
TranscriptJobLedger::validate_sql(read.capability_info(), write.capability_info())?;
```

## Error taxonomy

SQL needs a richer error taxonomy than blob/KV:

```rust
#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    #[error("unsupported feature: {0:?}")]
    Unsupported(SqlFeature),

    #[error("incompatible provider: {0}")]
    IncompatibleProvider(String),

    #[error("invalid statement: {0}")]
    InvalidStatement(String),

    #[error("invalid parameters: {0}")]
    InvalidParams(String),

    #[error("constraint violation: {kind:?}, constraint={constraint:?}, message={message}")]
    ConstraintViolation {
        kind: SqlConstraintKind,
        constraint: Option<String>,
        message: String,
    },

    #[error("transaction aborted: {0}")]
    TransactionAborted(String),

    #[error("database busy")]
    Busy,

    #[error("rate limited")]
    RateLimited,

    #[error("timeout after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },

    #[error("result too large: {0}")]
    ResultTooLarge(String),

    #[error("auth denied: {0}")]
    AuthDenied(String),

    #[error("provider error: {0}")]
    Provider(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlConstraintKind {
    Unique,
    ForeignKey,
    NotNull,
    Check,
    Unknown,
}
```

Provider adapters should normalize common vendor errors into these variants where practical. Raw provider messages can remain in `Provider` or `message` fields.

## `ResourceAccess` integration

`ResourceAccess` should gain optional SQL accessors:

```rust
fn sql_read(&self) -> Option<&dyn capabilities::sql::SqlRead> {
    None
}

fn sql_write(&self) -> Option<&dyn capabilities::sql::SqlWrite> {
    None
}

fn sql_admin(&self) -> Option<&dyn capabilities::sql::SqlAdmin> {
    None
}
```

`ResourceBag` should gain setters/builders for each trait. A single provider instance may be exposed as read/write/admin, or a host can expose different handles for different authorities.

Host preflight must map SQL hints to these accessors and treat unknown SQL hints as missing capabilities.

## Macro hint inference

`dag-macros` should support explicit resource annotations:

```rust
resources(sql_read(capabilities::sql::SqlRead))
resources(sql_write(capabilities::sql::SqlWrite))
resources(sql_admin(capabilities::sql::SqlAdmin))
```

Hint inference should map aliases/idents conservatively:

- `sql_read`, `sql_reader`, `sqlite_reader`, `postgres_read`, `d1_read` -> `resource::sql::read`
- `sql_write`, `sql_writer`, `sqlite_writer`, `postgres_write`, `d1_write` -> `resource::sql::write`
- `sql_admin`, `migration`, `schema` -> `resource::sql::admin`

Do not keep expanding the old `db_*` inference surface except for compatibility warnings/migration.

## Provider implementation strategy

The capability API should remain independent of any one SQL library. Provider crates may use SQLx or other clients internally, but `capabilities::sql` should expose only Lattice-owned traits/types/metadata/errors.

Preferred implementation sequence:

1. **SQLx SQLite native provider**
   - Establish a SQLx-backed local/native path first.
   - Verify the provider is target-gated out of wasm bundles.
   - Verify `capabilities::sql` itself builds for `wasm32-unknown-unknown`.

2. **Cloudflare D1 feasibility spike**
   - After Worker defaults are updated, evaluate `sqlx-d1` as the first D1 backend candidate.
   - `sqlx-d1` may let the D1 provider share SQLx ergonomics and query macro behavior with native providers.
   - If `sqlx-d1` is incompatible with the selected Worker baseline or Lattice runtime constraints, fallback to a raw `worker::D1Database` adapter.

3. **Future SQLx providers**
   - Postgres/MySQL providers should prefer SQLx unless a focused client provides materially better semantics/error mapping.

This means SQLx can be the preferred provider implementation family, but it must not become the resource boundary. Lattice still owns `SqlRead`, `SqlWrite`, `SqlAdmin`, `SqlCapabilityInfo`, `SqlError`, preflight, binding, and wasm transport semantics.

## Provider binding examples

Local SQLite:

```json
{
  "instances": {
    "transcript_state_local": {
      "provider_kind": "sql.sqlx_sqlite",
      "mode": "external",
      "provides": ["resource::sql::read", "resource::sql::write", "resource::sql::admin"],
      "connect": { "path": "scratch/s14.sqlite" },
      "config": { "journal_mode": "wal" }
    }
  }
}
```

Cloudflare D1, after provider strategy is proven:

```json
{
  "instances": {
    "transcript_state_d1": {
      "provider_kind": "sql.cloudflare_d1",
      "mode": "external",
      "provides": ["resource::sql::read", "resource::sql::write"],
      "connect": { "binding": "MEETING_TRANSCRIPT_DB" },
      "config": {}
    }
  }
}
```

Postgres:

```json
{
  "instances": {
    "transcript_state_pg": {
      "provider_kind": "sql.postgres",
      "mode": "external",
      "provides": ["resource::sql::read", "resource::sql::write", "resource::sql::admin"],
      "connect": { "database_url_secret": "TRANSCRIPT_DATABASE_URL" },
      "config": { "ssl_mode": "require" }
    }
  }
}
```

## Isolation wrappers

Existing resource-catalog language mentions `isolation.sql_schema_prefix` under DB. SQL should formalize wrappers such as:

- `isolation.sql_schema_prefix`
  - Applies cleanly to Postgres schemas.
  - Not portable to SQLite/D1 in the same way.

- `isolation.sql_table_prefix`
  - More portable across SQLite/D1/Postgres.
  - Requires helpers/migrations to render table names through a validated identifier mechanism.

- `isolation.sql_read_only`
  - Host-level read-only wrapper where provider supports it.
  - Otherwise expose only `SqlRead`.

- `isolation.sql_statement_policy`
  - Optional future wrapper for deny/allow lists, statement limits, or table allow-lists.
  - Must not rely solely on naive string parsing for security.

Identifier escaping and table/schema prefixing should be explicit helper APIs, not interpolated strings.

## S14 application shape

S14 should evolve from:

```rust
Arc<dyn TranscriptJobStore>
```

toward:

```rust
pub struct TranscriptJobLedger {
    read: Arc<dyn SqlRead>,
    write: Arc<dyn SqlWrite>,
}
```

The Flow IR declares low-level SQL resources:

```rust
resources(
    http_read(capabilities::http::HttpRead),
    http_write(capabilities::http::HttpWrite),
    sql_read(capabilities::sql::SqlRead),
    sql_write(capabilities::sql::SqlWrite)
)
```

The helper implements S14's schema and transitions:

```text
upsert_discovered
select_due
mark_waiting
mark_retryable
mark_manual_review
mark_permanent_failure
mark_uploaded
```

Those are not `resource::*` operations. They are S14/domain-library operations over `resource::sql`.

## Non-goals for MVP

- A complete SQL parser.
- A cross-dialect ORM.
- Compile-time query checking.
- Streaming result sets.
- Long-lived transaction handles.
- Automatic migrations for runtime flows.
- SQL injection prevention via parser magic.
- Universal SQL portability.

## Open questions

1. Should `SqlRead::query` accept multiple statements? MVP answer should be no.
2. Should `SqlWrite::execute` permit `RETURNING`? MVP answer: use `query_write` and require `SqlFeature::WriteReturning`.
3. Should `SqlAdmin` be available in dynamic wasm guests? Probably yes as a capability, but policy should block it by default for runtime flows.
4. Should old `resource::db::*` hints remain as aliases? Probably temporarily, with deprecation diagnostics toward `resource::sql::*`.
5. Should provider features be declared in Flow IR immediately? MVP can start with runtime helper validation and add IR requirements after one or two providers exist.
