use super::*;
use async_trait::async_trait;
use capabilities::sql::{SqlColumn, SqlRow};
use sqlx_d1::sqlx_core::{column::Column, row::Row, type_info::TypeInfo, value::ValueRef};

#[derive(Clone, Debug)]
pub struct WorkersD1Sql {
    conn: sqlx_d1::D1Connection,
}

impl WorkersD1Sql {
    pub fn from_env(env: &worker::Env, binding: &str) -> Result<Self, worker::Error> {
        let database = env.d1(binding)?;
        Ok(Self::from_database(database))
    }

    pub fn from_database(database: worker::D1Database) -> Self {
        sql::ensure_registered();
        Self {
            conn: sqlx_d1::D1Connection::new(database),
        }
    }

    pub fn from_connection(conn: sqlx_d1::D1Connection) -> Self {
        sql::ensure_registered();
        Self { conn }
    }

    pub fn connection(&self) -> &sqlx_d1::D1Connection {
        &self.conn
    }

    async fn query_statement(&self, statement: SqlStatement) -> Result<SqlQueryResult, SqlError> {
        validate_read_statement(&statement)?;
        fetch_query(&self.conn, &statement).await
    }

    async fn execute_statement(
        &self,
        statement: SqlStatement,
    ) -> Result<SqlExecuteResult, SqlError> {
        validate_write_statement(&statement)?;
        execute_query(&self.conn, &statement).await
    }

    async fn execute_batch(&self, batch: SqlBatch) -> Result<SqlBatchResult, SqlError> {
        if batch.statements.is_empty() {
            return Ok(SqlBatchResult {
                outcomes: Vec::new(),
                atomic: false,
            });
        }

        if matches!(batch.atomicity, SqlBatchAtomicity::RequireAtomic) {
            return Err(SqlError::Unsupported(SqlFeature::AtomicBatch));
        }

        let mut outcomes = Vec::with_capacity(batch.statements.len());
        for statement in &batch.statements {
            validate_statement(statement)?;
            let outcome = if matches!(
                statement.options.statement_kind,
                Some(SqlStatementKind::Read)
            ) {
                SqlStatementOutcome::Query(fetch_query(&self.conn, statement).await?)
            } else {
                SqlStatementOutcome::Execute(execute_query(&self.conn, statement).await?)
            };
            outcomes.push(outcome);
        }

        Ok(SqlBatchResult {
            outcomes,
            atomic: false,
        })
    }
}

impl Capability for WorkersD1Sql {
    fn name(&self) -> &'static str {
        PROVIDER_NAME
    }
}

#[async_trait(?Send)]
impl sql::SqlRead for WorkersD1Sql {
    async fn query(&self, statement: SqlStatement) -> Result<SqlQueryResult, SqlError> {
        self.query_statement(statement).await
    }

    fn capability_info(&self) -> SqlCapabilityInfo {
        capability_info()
    }
}

#[async_trait(?Send)]
impl sql::SqlWrite for WorkersD1Sql {
    async fn execute(&self, statement: SqlStatement) -> Result<SqlExecuteResult, SqlError> {
        self.execute_statement(statement).await
    }

    async fn query_write(&self, statement: SqlStatement) -> Result<SqlQueryResult, SqlError> {
        validate_statement(&statement)?;
        fetch_query(&self.conn, &statement).await
    }

    async fn batch(&self, batch: SqlBatch) -> Result<SqlBatchResult, SqlError> {
        self.execute_batch(batch).await
    }

    fn capability_info(&self) -> SqlCapabilityInfo {
        capability_info()
    }
}

#[async_trait(?Send)]
impl sql::SqlAdmin for WorkersD1Sql {
    async fn execute_ddl(&self, statement: SqlStatement) -> Result<SqlExecuteResult, SqlError> {
        validate_write_statement(&statement)?;
        execute_query(&self.conn, &statement).await
    }

    fn capability_info(&self) -> SqlCapabilityInfo {
        capability_info()
    }
}

async fn execute_query(
    conn: &sqlx_d1::D1Connection,
    statement: &SqlStatement,
) -> Result<SqlExecuteResult, SqlError> {
    let mut query = sqlx_d1::query(statement.sql.as_str());
    for value in &statement.params {
        query = bind_value(query, value);
    }

    // sqlx-d1 currently exposes a D1QueryResult shape, but runtime probing showed
    // D1 execute metadata may be unavailable/zeroed. Keep the Lattice result
    // conservative and direct callers to `query_write` + `RETURNING` when they
    // need mutation output.
    let _result = query.execute(conn).await.map_err(map_sqlx_error)?;
    Ok(SqlExecuteResult {
        rows_affected: None,
        last_insert_id: None,
    })
}

async fn fetch_query(
    conn: &sqlx_d1::D1Connection,
    statement: &SqlStatement,
) -> Result<SqlQueryResult, SqlError> {
    let mut query = sqlx_d1::query(statement.sql.as_str());
    for value in &statement.params {
        query = bind_value(query, value);
    }

    let rows = query.fetch_all(conn).await.map_err(map_sqlx_error)?;
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
    query: sqlx_d1::query::Query<
        'q,
        sqlx_d1::D1,
        <sqlx_d1::D1 as sqlx_d1::sqlx_core::database::Database>::Arguments<'q>,
    >,
    value: &'q SqlValue,
) -> sqlx_d1::query::Query<
    'q,
    sqlx_d1::D1,
    <sqlx_d1::D1 as sqlx_d1::sqlx_core::database::Database>::Arguments<'q>,
> {
    match value {
        SqlValue::Null => query.bind(Option::<i64>::None),
        SqlValue::Bool(value) => query.bind(*value),
        SqlValue::I64(value) => query.bind(*value),
        SqlValue::F64(value) => query.bind(*value),
        SqlValue::Text(value) => query.bind(value.as_str()),
        SqlValue::Bytes(value) => query.bind(value.as_slice()),
    }
}

type D1Column = <sqlx_d1::D1 as sqlx_d1::sqlx_core::database::Database>::Column;
type D1Row = <sqlx_d1::D1 as sqlx_d1::sqlx_core::database::Database>::Row;

fn sql_column(column: &D1Column) -> SqlColumn {
    SqlColumn {
        name: column.name().to_string(),
        type_hint: Some(type_hint(column.type_info().name())),
    }
}

fn sql_row(row: &D1Row) -> Result<SqlRow, SqlError> {
    let mut values = Vec::with_capacity(row.columns().len());
    for index in 0..row.columns().len() {
        values.push(sql_value(row, index)?);
    }
    Ok(SqlRow { values })
}

fn sql_value(row: &D1Row, index: usize) -> Result<SqlValue, SqlError> {
    let raw = row.try_get_raw(index).map_err(map_sqlx_error)?;
    if raw.is_null() {
        return Ok(SqlValue::Null);
    }

    match type_hint(raw.type_info().name()) {
        SqlTypeHint::Integer => row
            .try_get::<i64, _>(index)
            .map(SqlValue::I64)
            .map_err(map_sqlx_error),
        SqlTypeHint::Bool => row
            .try_get::<bool, _>(index)
            .map(SqlValue::Bool)
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

fn map_sqlx_error(err: sqlx_d1::Error) -> SqlError {
    match err {
        sqlx_d1::Error::Database(db_err) => {
            let message = db_err.message().to_string();
            let source = db_err.as_error().to_string();
            let normalized = if source.is_empty() || source == message {
                message
            } else if message == "Error from D1" {
                source
            } else {
                format!("{message}: {source}")
            };
            normalize_database_message(&normalized)
        }
        sqlx_d1::Error::Configuration(err) => SqlError::IncompatibleProvider(err.to_string()),
        sqlx_d1::Error::Io(err) => SqlError::Provider(err.to_string()),
        sqlx_d1::Error::Protocol(message) => SqlError::Provider(message),
        sqlx_d1::Error::ColumnDecode { source, .. } => SqlError::Provider(source.to_string()),
        sqlx_d1::Error::Encode(err) => SqlError::InvalidParams(err.to_string()),
        sqlx_d1::Error::Decode(err) => SqlError::Provider(err.to_string()),
        sqlx_d1::Error::ColumnNotFound(name) => {
            SqlError::InvalidStatement(format!("column not found: {name}"))
        }
        sqlx_d1::Error::ColumnIndexOutOfBounds { index, len } => SqlError::Provider(format!(
            "column index {index} out of bounds for row with {len} columns"
        )),
        sqlx_d1::Error::RowNotFound => SqlError::Provider("row not found".to_string()),
        other => normalize_database_message(&other.to_string()),
    }
}
