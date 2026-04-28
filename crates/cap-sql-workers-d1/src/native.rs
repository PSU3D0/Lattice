use super::*;
use async_trait::async_trait;

#[derive(Clone, Debug, Default)]
pub struct WorkersD1Sql;

impl WorkersD1Sql {
    pub fn new_unavailable() -> Self {
        sql::ensure_registered();
        Self
    }
}

impl Capability for WorkersD1Sql {
    fn name(&self) -> &'static str {
        PROVIDER_NAME
    }
}

#[async_trait]
impl sql::SqlRead for WorkersD1Sql {
    async fn query(&self, _statement: SqlStatement) -> Result<SqlQueryResult, SqlError> {
        Err(SqlError::IncompatibleProvider(WASM_REQUIRED.to_string()))
    }

    fn capability_info(&self) -> SqlCapabilityInfo {
        capability_info()
    }
}

#[async_trait]
impl sql::SqlWrite for WorkersD1Sql {
    async fn execute(&self, _statement: SqlStatement) -> Result<SqlExecuteResult, SqlError> {
        Err(SqlError::IncompatibleProvider(WASM_REQUIRED.to_string()))
    }

    async fn query_write(&self, _statement: SqlStatement) -> Result<SqlQueryResult, SqlError> {
        Err(SqlError::IncompatibleProvider(WASM_REQUIRED.to_string()))
    }

    async fn batch(&self, _batch: SqlBatch) -> Result<SqlBatchResult, SqlError> {
        Err(SqlError::IncompatibleProvider(WASM_REQUIRED.to_string()))
    }

    fn capability_info(&self) -> SqlCapabilityInfo {
        capability_info()
    }
}

#[async_trait]
impl sql::SqlAdmin for WorkersD1Sql {
    async fn execute_ddl(&self, _statement: SqlStatement) -> Result<SqlExecuteResult, SqlError> {
        Err(SqlError::IncompatibleProvider(WASM_REQUIRED.to_string()))
    }

    fn capability_info(&self) -> SqlCapabilityInfo {
        capability_info()
    }
}
