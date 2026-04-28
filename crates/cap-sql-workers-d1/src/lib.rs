#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code, unused_imports))]

//! Prototype Cloudflare Workers D1 SQL capability provider.
//!
//! This crate is an initial `sqlx-d1`-backed provider for Cloudflare Workers D1.
//! It intentionally reports conservative metadata and keeps known D1/sqlx-d1
//! caveats visible:
//!
//! - `execute` metadata may be unavailable or zeroed by `sqlx-d1`; callers should
//!   prefer `query_write` with `RETURNING` when mutation output matters.
//! - Error normalization relies partly on provider error strings because current
//!   D1/sqlx-d1 database errors may classify as `Other` with generic messages.
//! - `batch` is best-effort in this prototype and does not claim D1 atomic batch
//!   semantics until the adapter uses the lower-level Workers batch API directly.
//! - Result columns are inferred from returned rows; queries returning zero rows
//!   may report an empty column list.

use capabilities::{
    Capability,
    sql::{
        self, SqlBatch, SqlBatchAtomicity, SqlBatchResult, SqlCapabilityInfo, SqlConsistency,
        SqlConstraintKind, SqlDialect, SqlError, SqlExecuteResult, SqlFeature, SqlPlaceholderStyle,
        SqlQueryResult, SqlStatement, SqlStatementKind, SqlStatementOutcome, SqlTransactionSupport,
        SqlTypeHint, SqlValue,
    },
};

const PROVIDER_NAME: &str = "sql.cloudflare.d1.prototype";
#[cfg(not(target_arch = "wasm32"))]
const WASM_REQUIRED: &str = "cap-sql-workers-d1 requires wasm32 Cloudflare Workers runtime";

#[cfg(target_arch = "wasm32")]
mod wasm;

#[cfg(not(target_arch = "wasm32"))]
mod native;

#[cfg(not(target_arch = "wasm32"))]
pub use native::WorkersD1Sql;
#[cfg(target_arch = "wasm32")]
pub use wasm::WorkersD1Sql;

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

    if !statement.named_params.is_empty() {
        return Err(SqlError::Unsupported(SqlFeature::NamedParams));
    }

    Ok(())
}

fn validate_read_statement(statement: &SqlStatement) -> Result<(), SqlError> {
    validate_statement(statement)?;
    if matches!(
        statement.options.statement_kind,
        Some(SqlStatementKind::Write | SqlStatementKind::Ddl)
    ) {
        return Err(SqlError::InvalidStatement(
            "query received a write or DDL statement kind".to_string(),
        ));
    }
    Ok(())
}

fn validate_write_statement(statement: &SqlStatement) -> Result<(), SqlError> {
    validate_statement(statement)?;
    if matches!(
        statement.options.statement_kind,
        Some(SqlStatementKind::Read)
    ) {
        return Err(SqlError::InvalidStatement(
            "execute received a read statement kind".to_string(),
        ));
    }
    Ok(())
}

fn capability_info() -> SqlCapabilityInfo {
    let features = [
        SqlFeature::PositionalParams,
        SqlFeature::Batch,
        SqlFeature::UpsertOnConflict,
        SqlFeature::Returning,
        SqlFeature::WriteReturning,
        SqlFeature::JsonFunctions,
        SqlFeature::ForeignKeys,
        SqlFeature::Indexes,
        SqlFeature::UniqueConstraints,
        SqlFeature::Ddl,
    ]
    .into_iter()
    .collect();

    let mut extensions = std::collections::BTreeMap::new();
    extensions.insert("provider".to_string(), serde_json::json!("sqlx-d1"));
    extensions.insert("provider_version".to_string(), serde_json::json!("0.4.1"));
    extensions.insert("worker_version".to_string(), serde_json::json!("0.8.1"));
    extensions.insert("prototype".to_string(), serde_json::json!(true));
    extensions.insert(
        "execute_metadata".to_string(),
        serde_json::json!("may be unavailable or zeroed by sqlx-d1"),
    );
    extensions.insert(
        "error_normalization".to_string(),
        serde_json::json!("partly string-based"),
    );

    SqlCapabilityInfo {
        dialect: SqlDialect::CloudflareD1,
        placeholder_styles: vec![SqlPlaceholderStyle::Question],
        consistency: SqlConsistency::ProviderDefined,
        transaction_support: SqlTransactionSupport::SingleStatementAtomic,
        features,
        limits: sql::SqlLimits {
            max_statement_bytes: Some(100_000),
            max_params: Some(100),
            ..sql::SqlLimits::default()
        },
        extensions,
    }
}

fn type_hint(type_name: &str) -> SqlTypeHint {
    match type_name.to_ascii_uppercase().as_str() {
        "NULL" => SqlTypeHint::Null,
        "BOOL" | "BOOLEAN" => SqlTypeHint::Bool,
        "INT" | "INTEGER" | "NUMERIC" => SqlTypeHint::Integer,
        "REAL" | "FLOAT" | "DOUBLE" => SqlTypeHint::Real,
        "TEXT" | "VARCHAR" | "CHAR" | "CLOB" => SqlTypeHint::Text,
        "BLOB" => SqlTypeHint::Blob,
        "JSON" => SqlTypeHint::Json,
        "TIMESTAMP" | "DATETIME" | "DATE" => SqlTypeHint::Timestamp,
        _ => SqlTypeHint::Unknown,
    }
}

fn normalize_database_message(message: &str) -> SqlError {
    let lower = message.to_ascii_lowercase();

    if lower.contains("unique constraint")
        || lower.contains("sqlite_constraint_unique")
        || (lower.contains("sqlite_constraint") && lower.contains("unique"))
    {
        return SqlError::ConstraintViolation {
            kind: SqlConstraintKind::Unique,
            constraint: constraint_name(message),
            message: message.to_string(),
        };
    }

    if lower.contains("not null constraint")
        || lower.contains("sqlite_constraint_notnull")
        || lower.contains("not null constraint failed")
    {
        return SqlError::ConstraintViolation {
            kind: SqlConstraintKind::NotNull,
            constraint: constraint_name(message),
            message: message.to_string(),
        };
    }

    if lower.contains("foreign key constraint")
        || lower.contains("sqlite_constraint_foreignkey")
        || lower.contains("foreign key constraint failed")
    {
        return SqlError::ConstraintViolation {
            kind: SqlConstraintKind::ForeignKey,
            constraint: constraint_name(message),
            message: message.to_string(),
        };
    }

    if lower.contains("check constraint") || lower.contains("sqlite_constraint_check") {
        return SqlError::ConstraintViolation {
            kind: SqlConstraintKind::Check,
            constraint: constraint_name(message),
            message: message.to_string(),
        };
    }

    if lower.contains("sqlite_constraint") || lower.contains("constraint failed") {
        return SqlError::ConstraintViolation {
            kind: SqlConstraintKind::Unknown,
            constraint: constraint_name(message),
            message: message.to_string(),
        };
    }

    if lower.contains("database is locked") || lower.contains("database is busy") {
        return SqlError::Busy;
    }

    if lower.contains("rate limit") || lower.contains("too many requests") {
        return SqlError::RateLimited;
    }

    if lower.contains("timeout") || lower.contains("timed out") {
        return SqlError::Provider(message.to_string());
    }

    if lower.contains("syntax error")
        || lower.contains("incomplete input")
        || lower.contains("no such table")
        || lower.contains("no such column")
    {
        return SqlError::InvalidStatement(message.to_string());
    }

    SqlError::Provider(message.to_string())
}

fn constraint_name(message: &str) -> Option<String> {
    let lower = message.to_ascii_lowercase();
    let markers = [
        "unique constraint failed:",
        "not null constraint failed:",
        "foreign key constraint failed:",
        "check constraint failed:",
        "constraint failed:",
        "failed:",
    ];

    for marker in markers {
        if let Some(pos) = lower.find(marker) {
            let start = pos + marker.len();
            let candidate = message[start..]
                .split(": SQLITE_")
                .next()
                .unwrap_or_default()
                .trim()
                .trim_matches('`')
                .trim_matches('"')
                .to_string();
            if !candidate.is_empty() {
                return Some(candidate);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_is_conservative_for_prototype() {
        let info = capability_info();

        assert_eq!(info.dialect, SqlDialect::CloudflareD1);
        assert_eq!(info.placeholder_styles, vec![SqlPlaceholderStyle::Question]);
        assert_eq!(info.consistency, SqlConsistency::ProviderDefined);
        assert_eq!(
            info.transaction_support,
            SqlTransactionSupport::SingleStatementAtomic
        );
        assert!(info.features.contains(&SqlFeature::PositionalParams));
        assert!(info.features.contains(&SqlFeature::Batch));
        assert!(!info.features.contains(&SqlFeature::AtomicBatch));
        assert!(!info.features.contains(&SqlFeature::RowsAffected));
        assert!(!info.features.contains(&SqlFeature::LastInsertId));
        assert_eq!(
            info.extensions.get("prototype"),
            Some(&serde_json::json!(true))
        );
    }

    #[test]
    fn rejects_named_parameters_for_sqlx_d1_path() {
        let mut named = std::collections::BTreeMap::new();
        named.insert("name".to_string(), SqlValue::Text("alpha".to_string()));
        let statement =
            SqlStatement::new("select * from items where name = :name").with_named_params(named);

        assert!(matches!(
            validate_statement(&statement),
            Err(SqlError::Unsupported(SqlFeature::NamedParams))
        ));
    }

    #[test]
    fn parses_d1_unique_constraint_message() {
        let err = normalize_database_message(
            "D1_ERROR: UNIQUE constraint failed: items.name: SQLITE_CONSTRAINT",
        );

        match err {
            SqlError::ConstraintViolation {
                kind,
                constraint,
                message,
            } => {
                assert_eq!(kind, SqlConstraintKind::Unique);
                assert_eq!(constraint.as_deref(), Some("items.name"));
                assert!(message.contains("UNIQUE constraint failed"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn parses_other_constraint_messages() {
        assert!(matches!(
            normalize_database_message(
                "D1_ERROR: NOT NULL constraint failed: items.name: SQLITE_CONSTRAINT_NOTNULL"
            ),
            SqlError::ConstraintViolation {
                kind: SqlConstraintKind::NotNull,
                ..
            }
        ));
        assert!(matches!(
            normalize_database_message(
                "D1_ERROR: FOREIGN KEY constraint failed: SQLITE_CONSTRAINT_FOREIGNKEY"
            ),
            SqlError::ConstraintViolation {
                kind: SqlConstraintKind::ForeignKey,
                ..
            }
        ));
        assert!(matches!(
            normalize_database_message(
                "D1_ERROR: CHECK constraint failed: amount_positive: SQLITE_CONSTRAINT_CHECK"
            ),
            SqlError::ConstraintViolation {
                kind: SqlConstraintKind::Check,
                ..
            }
        ));
    }

    #[test]
    fn normalizes_busy_rate_limit_and_invalid_statement_strings() {
        assert!(matches!(
            normalize_database_message("database is locked"),
            SqlError::Busy
        ));
        assert!(matches!(
            normalize_database_message("too many requests for D1 database"),
            SqlError::RateLimited
        ));
        assert!(matches!(
            normalize_database_message("D1_ERROR: near \"from\": syntax error"),
            SqlError::InvalidStatement(_)
        ));
    }

    #[test]
    fn type_hint_maps_d1_names() {
        assert_eq!(type_hint("INTEGER"), SqlTypeHint::Integer);
        assert_eq!(type_hint("REAL"), SqlTypeHint::Real);
        assert_eq!(type_hint("TEXT"), SqlTypeHint::Text);
        assert_eq!(type_hint("BLOB"), SqlTypeHint::Blob);
        assert_eq!(type_hint("NULL"), SqlTypeHint::Null);
    }
}
