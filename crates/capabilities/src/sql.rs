use super::*;

pub const HINT_SQL: &str = dag_core::EffectHint::Sql.as_str();
pub const HINT_SQL_READ: &str = dag_core::EffectHint::SqlRead.as_str();
pub const HINT_SQL_WRITE: &str = dag_core::EffectHint::SqlWrite.as_str();
pub const HINT_SQL_ADMIN: &str = dag_core::EffectHint::SqlAdmin.as_str();

/// Hint constraints are derived exhaustively from `dag_core::EffectHint`
/// (packet A1); no runtime registration required. Retained as a no-op for
/// API compatibility.
pub fn ensure_registered() {}

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

    async fn query_write(&self, _statement: SqlStatement) -> Result<SqlQueryResult, SqlError> {
        Err(SqlError::Unsupported(SqlFeature::WriteReturning))
    }

    async fn batch(&self, _batch: SqlBatch) -> Result<SqlBatchResult, SqlError> {
        Err(SqlError::Unsupported(SqlFeature::Batch))
    }

    fn capability_info(&self) -> SqlCapabilityInfo;
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait SqlAdmin: Capability {
    async fn execute_ddl(&self, statement: SqlStatement) -> Result<SqlExecuteResult, SqlError>;

    async fn migrate(&self, _migration: SqlMigration) -> Result<SqlMigrationResult, SqlError> {
        Err(SqlError::Unsupported(SqlFeature::Migrations))
    }

    fn capability_info(&self) -> SqlCapabilityInfo;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlStatement {
    pub sql: String,
    #[serde(default)]
    pub params: Vec<SqlValue>,
    #[serde(default)]
    pub named_params: BTreeMap<String, SqlValue>,
    #[serde(default)]
    pub options: SqlStatementOptions,
}

impl SqlStatement {
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
            named_params: BTreeMap::new(),
            options: SqlStatementOptions::default(),
        }
    }

    pub fn with_params(mut self, params: impl Into<Vec<SqlValue>>) -> Self {
        self.params = params.into();
        self
    }

    pub fn with_named_params(mut self, named_params: BTreeMap<String, SqlValue>) -> Self {
        self.named_params = named_params;
        self
    }

    pub fn with_options(mut self, options: SqlStatementOptions) -> Self {
        self.options = options;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SqlValue {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Text(String),
    Bytes(Vec<u8>),
}

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlExecuteResult {
    pub rows_affected: Option<u64>,
    pub last_insert_id: Option<SqlValue>,
}

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

impl Default for SqlCapabilityInfo {
    fn default() -> Self {
        Self {
            dialect: SqlDialect::Unknown,
            placeholder_styles: Vec::new(),
            consistency: SqlConsistency::Unknown,
            transaction_support: SqlTransactionSupport::Unknown,
            features: BTreeSet::new(),
            limits: SqlLimits::default(),
            extensions: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SqlDialect {
    Ansi,
    Sqlite,
    CloudflareD1,
    Postgres,
    MySql,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SqlPlaceholderStyle {
    Question,
    NumberedDollar,
    NamedColon,
    NamedAt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlConsistency {
    Strong,
    Eventual,
    ReadYourWrites,
    ProviderDefined,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SqlTransactionSupport {
    None,
    SingleStatementAtomic,
    AtomicBatch,
    ExplicitTransactions,
    Unknown,
}

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

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SqlLimits {
    pub max_statement_bytes: Option<u64>,
    pub max_params: Option<u32>,
    pub max_rows_returned: Option<u64>,
    pub max_batch_statements: Option<u32>,
    pub max_result_bytes: Option<u64>,
    pub timeout_ms: Option<u64>,
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_constraints_once() {
        ensure_registered();
        ensure_registered();

        let read = dag_core::effects_registry::constraint_for_hint(HINT_SQL_READ)
            .expect("sql read constraint");
        assert_eq!(read.minimum, dag_core::Effects::ReadOnly);

        let write = dag_core::effects_registry::constraint_for_hint(HINT_SQL_WRITE)
            .expect("sql write constraint");
        assert_eq!(write.minimum, dag_core::Effects::Effectful);

        let admin = dag_core::effects_registry::constraint_for_hint(HINT_SQL_ADMIN)
            .expect("sql admin constraint");
        assert_eq!(admin.minimum, dag_core::Effects::Effectful);

        let determinism = dag_core::determinism::constraint_for_hint(HINT_SQL)
            .expect("sql determinism");
        assert_eq!(determinism.minimum, dag_core::Determinism::BestEffort);
    }

    #[test]
    fn statement_defaults_to_no_params_or_options() {
        let statement = SqlStatement::new("select 1");

        assert_eq!(statement.sql, "select 1");
        assert!(statement.params.is_empty());
        assert!(statement.named_params.is_empty());
        assert_eq!(statement.options, SqlStatementOptions::default());
    }

    #[test]
    fn default_metadata_is_conservative() {
        let info = SqlCapabilityInfo::default();

        assert_eq!(info.dialect, SqlDialect::Unknown);
        assert_eq!(info.consistency, SqlConsistency::Unknown);
        assert_eq!(info.transaction_support, SqlTransactionSupport::Unknown);
        assert!(info.features.is_empty());
        assert_eq!(info.limits, SqlLimits::default());
    }
}
