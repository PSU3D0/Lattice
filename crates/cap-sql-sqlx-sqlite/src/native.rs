use async_trait::async_trait;
use capabilities::{
    Capability,
    sql::{
        self, SqlBatch, SqlBatchAtomicity, SqlBatchResult, SqlCapabilityInfo, SqlColumn,
        SqlConsistency, SqlConstraintKind, SqlDialect, SqlError, SqlExecuteResult, SqlFeature,
        SqlPlaceholderStyle, SqlQueryResult, SqlRow, SqlStatement, SqlStatementKind,
        SqlStatementOutcome, SqlTransactionSupport, SqlTypeHint, SqlValue,
    },
};
use sqlx::{
    Column, Row, Sqlite, SqlitePool, TypeInfo, ValueRef,
    query::Query,
    sqlite::{
        SqliteArguments, SqliteColumn, SqliteConnectOptions, SqlitePoolOptions, SqliteRow,
    },
};
use std::{borrow::Cow, path::Path, str::FromStr};

const PROVIDER_NAME: &str = "sql.sqlx.sqlite";

#[derive(Clone, Debug)]
pub struct SqlxSqlite {
    pool: SqlitePool,
}

impl SqlxSqlite {
    pub fn connect(path_or_url: impl AsRef<str>) -> Result<Self, SqlError> {
        let options = connect_options(path_or_url.as_ref())?;
        Ok(Self::from_options(options))
    }

    pub fn in_memory() -> Result<Self, SqlError> {
        Self::connect("sqlite::memory:")
    }

    pub fn from_pool(pool: SqlitePool) -> Self {
        sql::ensure_registered();
        Self { pool }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    fn from_options(options: SqliteConnectOptions) -> Self {
        sql::ensure_registered();
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_lazy_with(options);
        Self { pool }
    }

    async fn query_statement(&self, statement: SqlStatement) -> Result<SqlQueryResult, SqlError> {
        validate_statement(&statement)?;
        if matches!(
            statement.options.statement_kind,
            Some(SqlStatementKind::Write | SqlStatementKind::Ddl)
        ) {
            return Err(SqlError::InvalidStatement(
                "query received a write or DDL statement kind".to_string(),
            ));
        }

        fetch_query(&self.pool, &statement).await
    }

    async fn execute_statement(
        &self,
        statement: SqlStatement,
    ) -> Result<SqlExecuteResult, SqlError> {
        validate_statement(&statement)?;
        if matches!(statement.options.statement_kind, Some(SqlStatementKind::Read)) {
            return Err(SqlError::InvalidStatement(
                "execute received a read statement kind".to_string(),
            ));
        }

        execute_query(&self.pool, &statement).await
    }

    async fn execute_batch(&self, batch: SqlBatch) -> Result<SqlBatchResult, SqlError> {
        if batch.statements.is_empty() {
            return Ok(SqlBatchResult {
                outcomes: Vec::new(),
                atomic: matches!(batch.atomicity, SqlBatchAtomicity::RequireAtomic),
            });
        }

        match batch.atomicity {
            SqlBatchAtomicity::RequireAtomic => {
                let mut tx = self.pool.begin().await.map_err(map_sqlx_error)?;
                let mut outcomes = Vec::with_capacity(batch.statements.len());

                for statement in &batch.statements {
                    validate_statement(statement)?;
                    let outcome = if matches!(
                        statement.options.statement_kind,
                        Some(SqlStatementKind::Read)
                    ) {
                        fetch_query(&mut *tx, statement)
                            .await
                            .map(SqlStatementOutcome::Query)
                    } else {
                        execute_query(&mut *tx, statement)
                            .await
                            .map(SqlStatementOutcome::Execute)
                    };

                    match outcome {
                        Ok(outcome) => outcomes.push(outcome),
                        Err(err) => {
                            let _ = tx.rollback().await;
                            return Err(err);
                        }
                    }
                }

                tx.commit().await.map_err(map_sqlx_error)?;
                Ok(SqlBatchResult {
                    outcomes,
                    atomic: true,
                })
            }
            SqlBatchAtomicity::BestEffort => {
                let mut outcomes = Vec::with_capacity(batch.statements.len());
                for statement in &batch.statements {
                    validate_statement(statement)?;
                    let outcome = if matches!(
                        statement.options.statement_kind,
                        Some(SqlStatementKind::Read)
                    ) {
                        SqlStatementOutcome::Query(fetch_query(&self.pool, statement).await?)
                    } else {
                        SqlStatementOutcome::Execute(execute_query(&self.pool, statement).await?)
                    };
                    outcomes.push(outcome);
                }

                Ok(SqlBatchResult {
                    outcomes,
                    atomic: false,
                })
            }
        }
    }
}

impl Capability for SqlxSqlite {
    fn name(&self) -> &'static str {
        PROVIDER_NAME
    }
}

#[async_trait]
impl sql::SqlRead for SqlxSqlite {
    async fn query(&self, statement: SqlStatement) -> Result<SqlQueryResult, SqlError> {
        self.query_statement(statement).await
    }

    fn capability_info(&self) -> SqlCapabilityInfo {
        capability_info()
    }
}

#[async_trait]
impl sql::SqlWrite for SqlxSqlite {
    async fn execute(&self, statement: SqlStatement) -> Result<SqlExecuteResult, SqlError> {
        self.execute_statement(statement).await
    }

    async fn query_write(&self, statement: SqlStatement) -> Result<SqlQueryResult, SqlError> {
        validate_statement(&statement)?;
        fetch_query(&self.pool, &statement).await
    }

    async fn batch(&self, batch: SqlBatch) -> Result<SqlBatchResult, SqlError> {
        self.execute_batch(batch).await
    }

    fn capability_info(&self) -> SqlCapabilityInfo {
        capability_info()
    }
}

#[async_trait]
impl sql::SqlAdmin for SqlxSqlite {
    async fn execute_ddl(&self, statement: SqlStatement) -> Result<SqlExecuteResult, SqlError> {
        validate_statement(&statement)?;
        execute_query(&self.pool, &statement).await
    }

    fn capability_info(&self) -> SqlCapabilityInfo {
        capability_info()
    }
}

fn connect_options(input: &str) -> Result<SqliteConnectOptions, SqlError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(SqlError::IncompatibleProvider(
            "SQLite path or URL cannot be empty".to_string(),
        ));
    }

    let options = if trimmed == ":memory:" || trimmed.starts_with("sqlite:") {
        SqliteConnectOptions::from_str(trimmed).map_err(map_sqlx_error)?
    } else {
        SqliteConnectOptions::new()
            .filename(Path::new(trimmed))
            .create_if_missing(true)
    };

    Ok(options.foreign_keys(true))
}

fn validate_statement(statement: &SqlStatement) -> Result<(), SqlError> {
    if statement.sql.trim().is_empty() {
        return Err(SqlError::InvalidStatement(
            "SQL statement cannot be empty".to_string(),
        ));
    }

    if !statement.params.is_empty() && !statement.named_params.is_empty() {
        return Err(SqlError::InvalidParams(
            "positional and named parameters cannot be mixed".to_string(),
        ));
    }

    Ok(())
}

async fn execute_query<'q, E>(
    executor: E,
    statement: &'q SqlStatement,
) -> Result<SqlExecuteResult, SqlError>
where
    E: sqlx::Executor<'q, Database = Sqlite>,
{
    let (sql, params) = sql_and_params(statement)?;
    let mut query = sqlx::query(sql.as_ref());
    for value in params {
        query = bind_value(query, value);
    }

    let result = query.execute(executor).await.map_err(map_sqlx_error)?;

    Ok(SqlExecuteResult {
        rows_affected: Some(result.rows_affected()),
        last_insert_id: Some(SqlValue::I64(result.last_insert_rowid())),
    })
}

async fn fetch_query<'q, E>(
    executor: E,
    statement: &'q SqlStatement,
) -> Result<SqlQueryResult, SqlError>
where
    E: sqlx::Executor<'q, Database = Sqlite>,
{
    let (sql, params) = sql_and_params(statement)?;
    let mut query = sqlx::query(sql.as_ref());
    for value in params {
        query = bind_value(query, value);
    }

    let rows = query.fetch_all(executor).await.map_err(map_sqlx_error)?;

    let columns = rows
        .first()
        .map(|row| row.columns().iter().map(sql_column).collect())
        .unwrap_or_default();

    let mut result_rows = Vec::with_capacity(rows.len());
    let max_rows = statement.options.max_rows.map(|value| value as usize);
    for row in &rows {
        if max_rows.is_some_and(|max| result_rows.len() >= max) {
            break;
        }
        result_rows.push(sql_row(row)?);
    }

    Ok(SqlQueryResult {
        columns,
        rows_returned: result_rows.len() as u64,
        rows: result_rows,
        cursor: None,
    })
}

fn bind_value<'q>(
    query: Query<'q, Sqlite, SqliteArguments<'q>>,
    value: &'q SqlValue,
) -> Query<'q, Sqlite, SqliteArguments<'q>> {
    match value {
        SqlValue::Null => query.bind(Option::<i64>::None),
        SqlValue::Bool(value) => query.bind(*value),
        SqlValue::I64(value) => query.bind(*value),
        SqlValue::F64(value) => query.bind(*value),
        SqlValue::Text(value) => query.bind(value.as_str()),
        SqlValue::Bytes(value) => query.bind(value.as_slice()),
    }
}

fn sql_and_params<'a>(
    statement: &'a SqlStatement,
) -> Result<(Cow<'a, str>, Vec<&'a SqlValue>), SqlError> {
    if statement.named_params.is_empty() {
        return Ok((
            Cow::Borrowed(&statement.sql),
            statement.params.iter().collect(),
        ));
    }

    let placeholders = named_placeholders(&statement.sql);
    if placeholders.is_empty() {
        return Err(SqlError::InvalidParams(
            "named parameters supplied but no named placeholders were found".to_string(),
        ));
    }

    let mut rewritten_sql = String::with_capacity(statement.sql.len());
    let mut values = Vec::with_capacity(placeholders.len());
    let mut last = 0;
    for placeholder in placeholders {
        rewritten_sql.push_str(&statement.sql[last..placeholder.start]);
        rewritten_sql.push('?');
        last = placeholder.end;

        let Some(value) = statement
            .named_params
            .get(&placeholder.without_prefix)
            .or_else(|| statement.named_params.get(&placeholder.with_prefix))
        else {
            return Err(SqlError::InvalidParams(format!(
                "missing value for named parameter {}",
                placeholder.with_prefix
            )));
        };
        values.push(value);
    }
    rewritten_sql.push_str(&statement.sql[last..]);

    Ok((Cow::Owned(rewritten_sql), values))
}

#[derive(Debug, Clone)]
struct NamedPlaceholder {
    start: usize,
    end: usize,
    with_prefix: String,
    without_prefix: String,
}

fn named_placeholders(sql: &str) -> Vec<NamedPlaceholder> {
    let bytes = sql.as_bytes();
    let mut placeholders = Vec::new();
    let mut i = 0;
    let mut quote: Option<u8> = None;

    while i < bytes.len() {
        let byte = bytes[i];
        if let Some(end_quote) = quote {
            if byte == end_quote {
                quote = None;
            }
            i += 1;
            continue;
        }

        if matches!(byte, b'\'' | b'"' | b'`') {
            quote = Some(byte);
            i += 1;
            continue;
        }

        if byte == b'-' && bytes.get(i + 1) == Some(&b'-') {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        if byte == b'/' && bytes.get(i + 1) == Some(&b'*') {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(bytes.len());
            continue;
        }

        if matches!(byte, b':' | b'@' | b'$')
            && bytes
                .get(i + 1)
                .is_some_and(|next| is_name_start(*next))
        {
            let start = i;
            i += 2;
            while i < bytes.len() && is_name_continue(bytes[i]) {
                i += 1;
            }
            placeholders.push(NamedPlaceholder {
                start,
                end: i,
                with_prefix: sql[start..i].to_string(),
                without_prefix: sql[start + 1..i].to_string(),
            });
            continue;
        }

        i += 1;
    }

    placeholders
}

fn is_name_start(byte: u8) -> bool {
    byte == b'_' || byte.is_ascii_alphabetic()
}

fn is_name_continue(byte: u8) -> bool {
    byte == b'_' || byte.is_ascii_alphanumeric()
}

fn sql_column(column: &SqliteColumn) -> SqlColumn {
    SqlColumn {
        name: column.name().to_string(),
        type_hint: Some(type_hint(column.type_info().name())),
    }
}

fn sql_row(row: &SqliteRow) -> Result<SqlRow, SqlError> {
    let mut values = Vec::with_capacity(row.len());
    for index in 0..row.len() {
        values.push(sql_value(row, index)?);
    }
    Ok(SqlRow { values })
}

fn sql_value(row: &SqliteRow, index: usize) -> Result<SqlValue, SqlError> {
    let raw = row.try_get_raw(index).map_err(map_sqlx_error)?;
    if raw.is_null() {
        return Ok(SqlValue::Null);
    }

    match type_hint(raw.type_info().name()) {
        SqlTypeHint::Integer | SqlTypeHint::Bool => row
            .try_get::<i64, _>(index)
            .map(SqlValue::I64)
            .map_err(map_sqlx_error),
        SqlTypeHint::Real => row
            .try_get::<f64, _>(index)
            .map(SqlValue::F64)
            .map_err(map_sqlx_error),
        SqlTypeHint::Text | SqlTypeHint::Json | SqlTypeHint::Timestamp => row
            .try_get::<String, _>(index)
            .map(SqlValue::Text)
            .map_err(map_sqlx_error),
        SqlTypeHint::Blob => row
            .try_get::<Vec<u8>, _>(index)
            .map(SqlValue::Bytes)
            .map_err(map_sqlx_error),
        SqlTypeHint::Null => Ok(SqlValue::Null),
        SqlTypeHint::Unknown => row
            .try_get::<String, _>(index)
            .map(SqlValue::Text)
            .or_else(|_| row.try_get::<i64, _>(index).map(SqlValue::I64))
            .or_else(|_| row.try_get::<f64, _>(index).map(SqlValue::F64))
            .or_else(|_| row.try_get::<Vec<u8>, _>(index).map(SqlValue::Bytes))
            .map_err(map_sqlx_error),
    }
}

fn type_hint(type_name: &str) -> SqlTypeHint {
    match type_name.to_ascii_uppercase().as_str() {
        "NULL" => SqlTypeHint::Null,
        "BOOL" | "BOOLEAN" => SqlTypeHint::Bool,
        "INT" | "INTEGER" => SqlTypeHint::Integer,
        "REAL" | "FLOAT" | "DOUBLE" => SqlTypeHint::Real,
        "TEXT" | "VARCHAR" | "CHAR" | "CLOB" => SqlTypeHint::Text,
        "BLOB" => SqlTypeHint::Blob,
        "JSON" => SqlTypeHint::Json,
        "TIMESTAMP" | "DATETIME" | "DATE" => SqlTypeHint::Timestamp,
        _ => SqlTypeHint::Unknown,
    }
}

fn capability_info() -> SqlCapabilityInfo {
    let features = [
        SqlFeature::PositionalParams,
        SqlFeature::NamedParams,
        SqlFeature::Batch,
        SqlFeature::AtomicBatch,
        SqlFeature::ExplicitTransactions,
        SqlFeature::UpsertOnConflict,
        SqlFeature::Returning,
        SqlFeature::WriteReturning,
        SqlFeature::RowsAffected,
        SqlFeature::LastInsertId,
        SqlFeature::JsonFunctions,
        SqlFeature::ForeignKeys,
        SqlFeature::Indexes,
        SqlFeature::UniqueConstraints,
        SqlFeature::Ddl,
    ]
    .into_iter()
    .collect();

    let mut extensions = std::collections::BTreeMap::new();
    extensions.insert("provider".to_string(), serde_json::json!("sqlx"));
    extensions.insert("target".to_string(), serde_json::json!("native"));

    SqlCapabilityInfo {
        dialect: SqlDialect::Sqlite,
        placeholder_styles: vec![SqlPlaceholderStyle::Question, SqlPlaceholderStyle::NamedColon],
        consistency: SqlConsistency::Strong,
        transaction_support: SqlTransactionSupport::ExplicitTransactions,
        features,
        limits: sql::SqlLimits {
            max_params: Some(32_766),
            ..sql::SqlLimits::default()
        },
        extensions,
    }
}

fn map_sqlx_error(err: sqlx::Error) -> SqlError {
    match err {
        sqlx::Error::Database(db_err) => {
            let code = db_err.code().map(|code| code.to_string());
            let message = db_err.message().to_string();
            map_database_error(code.as_deref(), &message)
        }
        sqlx::Error::PoolTimedOut => SqlError::Busy,
        sqlx::Error::Configuration(err) => SqlError::IncompatibleProvider(err.to_string()),
        sqlx::Error::Io(err) => SqlError::Provider(err.to_string()),
        sqlx::Error::Protocol(message) => SqlError::Provider(message),
        sqlx::Error::ColumnDecode { source, .. } => SqlError::Provider(source.to_string()),
        other => SqlError::Provider(other.to_string()),
    }
}

fn map_database_error(code: Option<&str>, message: &str) -> SqlError {
    let lower = message.to_ascii_lowercase();
    match code {
        Some("1555" | "2067") if lower.contains("unique") => SqlError::ConstraintViolation {
            kind: SqlConstraintKind::Unique,
            constraint: constraint_name(message),
            message: message.to_string(),
        },
        Some("1299") if lower.contains("not null") => SqlError::ConstraintViolation {
            kind: SqlConstraintKind::NotNull,
            constraint: constraint_name(message),
            message: message.to_string(),
        },
        Some("787") if lower.contains("foreign key") => SqlError::ConstraintViolation {
            kind: SqlConstraintKind::ForeignKey,
            constraint: constraint_name(message),
            message: message.to_string(),
        },
        Some("275") if lower.contains("check") => SqlError::ConstraintViolation {
            kind: SqlConstraintKind::Check,
            constraint: constraint_name(message),
            message: message.to_string(),
        },
        Some("5" | "6" | "261" | "262") => SqlError::Busy,
        _ if lower.contains("unique constraint failed") => SqlError::ConstraintViolation {
            kind: SqlConstraintKind::Unique,
            constraint: constraint_name(message),
            message: message.to_string(),
        },
        _ if lower.contains("database is locked") || lower.contains("database is busy") => {
            SqlError::Busy
        }
        _ if lower.contains("syntax error") || lower.contains("incomplete input") => {
            SqlError::InvalidStatement(message.to_string())
        }
        _ => SqlError::Provider(message.to_string()),
    }
}

fn constraint_name(message: &str) -> Option<String> {
    message
        .split_once(':')
        .map(|(_, rest)| rest.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use capabilities::sql::{SqlAdmin, SqlRead, SqlWrite};
    use std::collections::BTreeMap;

    fn statement(sql: impl Into<String>) -> SqlStatement {
        SqlStatement::new(sql)
    }

    async fn create_items(db: &SqlxSqlite) {
        SqlAdmin::execute_ddl(
            db,
            statement(
                "create table items (\
                 id integer primary key, \
                 name text not null unique, \
                 value integer)",
            ),
        )
        .await
        .expect("create table");
    }

    #[tokio::test]
    async fn basic_create_insert_select_update_delete() {
        let db = SqlxSqlite::in_memory().expect("sqlite provider");
        create_items(&db).await;

        let inserted = SqlWrite::execute(
            &db,
            statement("insert into items (name, value) values (?, ?)")
                .with_params(vec![SqlValue::Text("alpha".to_string()), SqlValue::I64(10)]),
        )
        .await
        .expect("insert");
        assert_eq!(inserted.rows_affected, Some(1));
        assert_eq!(inserted.last_insert_id, Some(SqlValue::I64(1)));

        let selected = SqlRead::query(
            &db,
            statement("select name, value from items where name = ?")
                .with_params(vec![SqlValue::Text("alpha".to_string())]),
        )
        .await
        .expect("select");
        assert_eq!(selected.rows_returned, 1);
        assert_eq!(selected.columns[0].name, "name");
        assert_eq!(
            selected.rows[0].values,
            vec![SqlValue::Text("alpha".to_string()), SqlValue::I64(10)]
        );

        let updated = SqlWrite::execute(
            &db,
            statement("update items set value = ? where name = ?")
                .with_params(vec![SqlValue::I64(20), SqlValue::Text("alpha".to_string())]),
        )
        .await
        .expect("update");
        assert_eq!(updated.rows_affected, Some(1));

        let deleted = SqlWrite::execute(
            &db,
            statement("delete from items where name = ?")
                .with_params(vec![SqlValue::Text("alpha".to_string())]),
        )
        .await
        .expect("delete");
        assert_eq!(deleted.rows_affected, Some(1));

        let count = SqlRead::query(&db, statement("select count(*) from items"))
            .await
            .expect("count");
        assert_eq!(count.rows[0].values, vec![SqlValue::I64(0)]);
    }

    #[tokio::test]
    async fn binds_text_int_null_blob_and_named_parameters() {
        let db = SqlxSqlite::in_memory().expect("sqlite provider");
        SqlAdmin::execute_ddl(
            &db,
            statement("create table params (t text, n integer, z text null, b blob)"),
        )
        .await
        .expect("create table");

        SqlWrite::execute(
            &db,
            statement("insert into params (t, n, z, b) values (?, ?, ?, ?)").with_params(vec![
                SqlValue::Text("hello".to_string()),
                SqlValue::I64(42),
                SqlValue::Null,
                SqlValue::Bytes(vec![0, 1, 2, 255]),
            ]),
        )
        .await
        .expect("insert positional");

        let mut named = BTreeMap::new();
        named.insert("t".to_string(), SqlValue::Text("named".to_string()));
        named.insert("n".to_string(), SqlValue::I64(7));
        named.insert("z".to_string(), SqlValue::Null);
        named.insert("b".to_string(), SqlValue::Bytes(vec![9, 8, 7]));
        SqlWrite::execute(
            &db,
            statement("insert into params (t, n, z, b) values (:t, :n, :z, :b)")
                .with_named_params(named),
        )
        .await
        .expect("insert named");

        let rows = SqlRead::query(&db, statement("select t, n, z, b from params order by n desc"))
            .await
            .expect("select");
        assert_eq!(rows.rows_returned, 2);
        assert_eq!(
            rows.rows[0].values,
            vec![
                SqlValue::Text("hello".to_string()),
                SqlValue::I64(42),
                SqlValue::Null,
                SqlValue::Bytes(vec![0, 1, 2, 255])
            ]
        );
        assert_eq!(
            rows.rows[1].values,
            vec![
                SqlValue::Text("named".to_string()),
                SqlValue::I64(7),
                SqlValue::Null,
                SqlValue::Bytes(vec![9, 8, 7])
            ]
        );
    }

    #[tokio::test]
    async fn unique_constraint_is_normalized() {
        let db = SqlxSqlite::in_memory().expect("sqlite provider");
        create_items(&db).await;

        let insert = statement("insert into items (name, value) values (?, ?)")
            .with_params(vec![SqlValue::Text("dupe".to_string()), SqlValue::I64(1)]);
        SqlWrite::execute(&db, insert.clone())
            .await
            .expect("first insert");

        let err = SqlWrite::execute(&db, insert)
            .await
            .expect_err("duplicate insert should fail");
        assert!(matches!(
            err,
            SqlError::ConstraintViolation {
                kind: SqlConstraintKind::Unique,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn invalid_sql_is_normalized() {
        let db = SqlxSqlite::in_memory().expect("sqlite provider");
        let err = SqlRead::query(&db, statement("select from"))
            .await
            .expect_err("invalid SQL should fail");
        assert!(matches!(err, SqlError::InvalidStatement(_)));
    }

    #[tokio::test]
    async fn atomic_batch_rolls_back_on_error() {
        let db = SqlxSqlite::in_memory().expect("sqlite provider");
        create_items(&db).await;

        let batch = SqlBatch {
            atomicity: SqlBatchAtomicity::RequireAtomic,
            statements: vec![
                statement("insert into items (name, value) values (?, ?)")
                    .with_params(vec![SqlValue::Text("dupe".to_string()), SqlValue::I64(1)]),
                statement("insert into items (name, value) values (?, ?)")
                    .with_params(vec![SqlValue::Text("dupe".to_string()), SqlValue::I64(2)]),
            ],
        };

        let err = SqlWrite::batch(&db, batch)
            .await
            .expect_err("batch should fail");
        assert!(matches!(
            err,
            SqlError::ConstraintViolation {
                kind: SqlConstraintKind::Unique,
                ..
            }
        ));

        let count = SqlRead::query(&db, statement("select count(*) from items"))
            .await
            .expect("count");
        assert_eq!(count.rows[0].values, vec![SqlValue::I64(0)]);
    }

    #[tokio::test]
    async fn metadata_describes_sqlite_provider() {
        let db = SqlxSqlite::in_memory().expect("sqlite provider");
        let info = SqlRead::capability_info(&db);

        assert_eq!(info.dialect, SqlDialect::Sqlite);
        assert_eq!(
            info.placeholder_styles,
            vec![SqlPlaceholderStyle::Question, SqlPlaceholderStyle::NamedColon]
        );
        assert_eq!(info.consistency, SqlConsistency::Strong);
        assert_eq!(
            info.transaction_support,
            SqlTransactionSupport::ExplicitTransactions
        );
        for feature in [
            SqlFeature::PositionalParams,
            SqlFeature::NamedParams,
            SqlFeature::Batch,
            SqlFeature::AtomicBatch,
            SqlFeature::RowsAffected,
            SqlFeature::LastInsertId,
            SqlFeature::UniqueConstraints,
            SqlFeature::Ddl,
        ] {
            assert!(info.features.contains(&feature), "missing {feature:?}");
        }
        assert_eq!(
            info.extensions.get("target"),
            Some(&serde_json::json!("native"))
        );
    }

    #[tokio::test]
    async fn connects_to_file_path() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let path = tempdir.path().join("test.sqlite");
        let db = SqlxSqlite::connect(path.to_str().expect("utf-8 path")).expect("file provider");

        SqlAdmin::execute_ddl(&db, statement("create table file_test (id integer primary key)"))
            .await
            .expect("create table");
        SqlWrite::execute(&db, statement("insert into file_test default values"))
            .await
            .expect("insert");
        let rows = SqlRead::query(&db, statement("select count(*) from file_test"))
            .await
            .expect("select");
        assert_eq!(rows.rows[0].values, vec![SqlValue::I64(1)]);
    }
}
