//! Test worker for cap-sql-workers-d1 E2E tests against a real D1 binding.
//!
//! Exposes HTTP endpoints to exercise SqlAdmin / SqlWrite / SqlRead through
//! `cap_sql_workers_d1::WorkersD1Sql`:
//!
//! - POST   /admin/ddl       - SqlAdmin::execute_ddl
//! - POST   /write/execute   - SqlWrite::execute
//! - POST   /write/returning - SqlWrite::query_write
//! - POST   /read/query      - SqlRead::query
//! - GET    /capability_info - SqlRead::capability_info
//! - GET    /health
//!
//! The body of each endpoint accepts a JSON SqlStatement; the response carries
//! the corresponding capability result or a normalized SqlError shape.

use cap_sql_workers_d1::WorkersD1Sql;
use capabilities::sql::{
    SqlAdmin, SqlConstraintKind, SqlError, SqlExecuteResult, SqlQueryResult, SqlRead, SqlStatement,
    SqlStatementKind, SqlStatementOptions, SqlValue, SqlWrite,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use std::collections::BTreeMap;
use worker::{Context, Env, Request, Response, Result, RouteContext, Router, event};

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    Router::new()
        .get_async("/health", handle_health)
        .get_async("/capability_info", handle_capability_info)
        .post_async("/admin/ddl", handle_admin_ddl)
        .post_async("/write/execute", handle_write_execute)
        .post_async("/write/returning", handle_write_returning)
        .post_async("/read/query", handle_read_query)
        .run(req, env)
        .await
}

async fn handle_health(_req: Request, _ctx: RouteContext<()>) -> Result<Response> {
    Response::from_json(&json!({ "status": "ok" }))
}

fn client(env: &Env) -> Result<WorkersD1Sql> {
    WorkersD1Sql::from_env(env, "DB").map_err(|err| worker::Error::RustError(err.to_string()))
}

fn json_error(message: impl Into<String>, status: u16) -> Result<Response> {
    Response::from_json(&json!({ "error": message.into() })).map(|r| r.with_status(status))
}

/// JSON wire format for incoming statements. Uses ergonomic `params` of
/// untagged JSON values that we map into `SqlValue` ourselves so tests can
/// write `params: ["alpha", 1]` instead of the externally-tagged serde shape.
#[derive(Deserialize)]
struct StatementWire {
    sql: String,
    #[serde(default)]
    params: Vec<JsonValue>,
    #[serde(default)]
    named_params: BTreeMap<String, JsonValue>,
    #[serde(default)]
    options: Option<StatementOptionsWire>,
}

#[derive(Deserialize, Default)]
struct StatementOptionsWire {
    timeout_ms: Option<u64>,
    max_rows: Option<u32>,
    statement_kind: Option<String>,
}

fn json_to_sql_value(value: JsonValue) -> std::result::Result<SqlValue, String> {
    match value {
        JsonValue::Null => Ok(SqlValue::Null),
        JsonValue::Bool(b) => Ok(SqlValue::Bool(b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(SqlValue::I64(i))
            } else if let Some(f) = n.as_f64() {
                Ok(SqlValue::F64(f))
            } else {
                Err(format!("unsupported number: {n}"))
            }
        }
        JsonValue::String(s) => Ok(SqlValue::Text(s)),
        // Allow `{"bytes_base64": "..."}` for blobs round-trip; otherwise reject.
        JsonValue::Object(mut map) => {
            if let Some(JsonValue::String(s)) = map.remove("bytes_base64") {
                base64_decode(&s)
                    .map(SqlValue::Bytes)
                    .map_err(|e| format!("invalid base64: {e}"))
            } else {
                Err("objects must use {\"bytes_base64\": \"...\"} envelope".to_string())
            }
        }
        JsonValue::Array(_) => Err("arrays are not valid SqlValue".to_string()),
    }
}

fn parse_statement_kind(name: &str) -> std::result::Result<SqlStatementKind, String> {
    match name {
        "Read" => Ok(SqlStatementKind::Read),
        "Write" => Ok(SqlStatementKind::Write),
        "Ddl" => Ok(SqlStatementKind::Ddl),
        other => Err(format!("invalid statement_kind: {other}")),
    }
}

fn statement_from_wire(wire: StatementWire) -> std::result::Result<SqlStatement, String> {
    let mut params = Vec::with_capacity(wire.params.len());
    for v in wire.params {
        params.push(json_to_sql_value(v)?);
    }
    let mut named = BTreeMap::new();
    for (k, v) in wire.named_params {
        named.insert(k, json_to_sql_value(v)?);
    }
    let mut options = SqlStatementOptions::default();
    if let Some(opts) = wire.options {
        options.timeout_ms = opts.timeout_ms;
        options.max_rows = opts.max_rows;
        if let Some(kind) = opts.statement_kind {
            options.statement_kind = Some(parse_statement_kind(&kind)?);
        }
    }
    Ok(SqlStatement {
        sql: wire.sql,
        params,
        named_params: named,
        options,
    })
}

/// Parse a SqlStatement from request body JSON using the ergonomic wire format.
async fn parse_statement(req: &mut Request) -> std::result::Result<SqlStatement, String> {
    let wire: StatementWire = req
        .json()
        .await
        .map_err(|e| format!("invalid JSON: {e}"))?;
    statement_from_wire(wire)
}

async fn handle_capability_info(_req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let cap = match client(&ctx.env) {
        Ok(c) => c,
        Err(err) => return json_error(err.to_string(), 500),
    };
    Response::from_json(&<WorkersD1Sql as SqlRead>::capability_info(&cap))
}

async fn handle_admin_ddl(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let stmt = match parse_statement(&mut req).await {
        Ok(s) => s,
        Err(err) => return json_error(err, 400),
    };
    let cap = match client(&ctx.env) {
        Ok(c) => c,
        Err(err) => return json_error(err.to_string(), 500),
    };
    match cap.execute_ddl(stmt).await {
        Ok(result) => Response::from_json(&execute_result_json(&result)),
        Err(err) => Response::from_json(&error_json(&err)).map(|r| r.with_status(200)),
    }
}

async fn handle_write_execute(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let stmt = match parse_statement(&mut req).await {
        Ok(s) => s,
        Err(err) => return json_error(err, 400),
    };
    let cap = match client(&ctx.env) {
        Ok(c) => c,
        Err(err) => return json_error(err.to_string(), 500),
    };
    match cap.execute(stmt).await {
        Ok(result) => Response::from_json(&execute_result_json(&result)),
        Err(err) => Response::from_json(&error_json(&err)).map(|r| r.with_status(200)),
    }
}

async fn handle_write_returning(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let stmt = match parse_statement(&mut req).await {
        Ok(s) => s,
        Err(err) => return json_error(err, 400),
    };
    let cap = match client(&ctx.env) {
        Ok(c) => c,
        Err(err) => return json_error(err.to_string(), 500),
    };
    match cap.query_write(stmt).await {
        Ok(result) => Response::from_json(&query_result_json(&result)),
        Err(err) => Response::from_json(&error_json(&err)).map(|r| r.with_status(200)),
    }
}

async fn handle_read_query(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let stmt = match parse_statement(&mut req).await {
        Ok(s) => s,
        Err(err) => return json_error(err, 400),
    };
    let cap = match client(&ctx.env) {
        Ok(c) => c,
        Err(err) => return json_error(err.to_string(), 500),
    };
    match cap.query(stmt).await {
        Ok(result) => Response::from_json(&query_result_json(&result)),
        Err(err) => Response::from_json(&error_json(&err)).map(|r| r.with_status(200)),
    }
}

#[derive(Serialize)]
struct ExecuteResultJson {
    ok: bool,
    rows_affected: Option<u64>,
    last_insert_id: Option<JsonValue>,
}

fn execute_result_json(result: &SqlExecuteResult) -> ExecuteResultJson {
    ExecuteResultJson {
        ok: true,
        rows_affected: result.rows_affected,
        last_insert_id: result.last_insert_id.as_ref().map(sql_value_to_json),
    }
}

#[derive(Serialize)]
struct QueryResultJson {
    ok: bool,
    columns: Vec<ColumnJson>,
    rows_returned: u64,
    rows: Vec<Vec<JsonValue>>,
    cursor: Option<String>,
}

#[derive(Serialize)]
struct ColumnJson {
    name: String,
    type_hint: Option<String>,
}

fn query_result_json(result: &SqlQueryResult) -> QueryResultJson {
    QueryResultJson {
        ok: true,
        columns: result
            .columns
            .iter()
            .map(|c| ColumnJson {
                name: c.name.clone(),
                type_hint: c.type_hint.map(|hint| format!("{hint:?}")),
            })
            .collect(),
        rows_returned: result.rows_returned,
        rows: result
            .rows
            .iter()
            .map(|r| r.values.iter().map(sql_value_to_json).collect())
            .collect(),
        cursor: result.cursor.clone(),
    }
}

fn sql_value_to_json(value: &SqlValue) -> JsonValue {
    match value {
        SqlValue::Null => JsonValue::Null,
        SqlValue::Bool(b) => json!(*b),
        SqlValue::I64(i) => json!(*i),
        SqlValue::F64(f) => json!(*f),
        SqlValue::Text(t) => json!(t),
        SqlValue::Bytes(b) => json!({ "bytes_base64": base64_encode(b) }),
    }
}

fn error_json(err: &SqlError) -> JsonValue {
    match err {
        SqlError::Unsupported(feature) => json!({
            "ok": false,
            "kind": "Unsupported",
            "feature": format!("{feature:?}"),
            "message": err.to_string(),
        }),
        SqlError::IncompatibleProvider(msg) => json!({
            "ok": false,
            "kind": "IncompatibleProvider",
            "message": msg,
        }),
        SqlError::InvalidStatement(msg) => json!({
            "ok": false,
            "kind": "InvalidStatement",
            "message": msg,
        }),
        SqlError::InvalidParams(msg) => json!({
            "ok": false,
            "kind": "InvalidParams",
            "message": msg,
        }),
        SqlError::ConstraintViolation {
            kind,
            constraint,
            message,
        } => json!({
            "ok": false,
            "kind": "ConstraintViolation",
            "constraint_kind": constraint_kind_name(*kind),
            "constraint": constraint,
            "message": message,
        }),
        SqlError::TransactionAborted(msg) => json!({
            "ok": false,
            "kind": "TransactionAborted",
            "message": msg,
        }),
        SqlError::Busy => json!({ "ok": false, "kind": "Busy" }),
        SqlError::RateLimited => json!({ "ok": false, "kind": "RateLimited" }),
        SqlError::Timeout { timeout_ms } => json!({
            "ok": false,
            "kind": "Timeout",
            "timeout_ms": timeout_ms,
        }),
        SqlError::ResultTooLarge(msg) => json!({
            "ok": false,
            "kind": "ResultTooLarge",
            "message": msg,
        }),
        SqlError::AuthDenied(msg) => json!({
            "ok": false,
            "kind": "AuthDenied",
            "message": msg,
        }),
        SqlError::Provider(msg) => json!({
            "ok": false,
            "kind": "Provider",
            "message": msg,
        }),
    }
}

fn constraint_kind_name(kind: SqlConstraintKind) -> &'static str {
    match kind {
        SqlConstraintKind::Unique => "Unique",
        SqlConstraintKind::ForeignKey => "ForeignKey",
        SqlConstraintKind::NotNull => "NotNull",
        SqlConstraintKind::Check => "Check",
        SqlConstraintKind::Unknown => "Unknown",
    }
}

fn base64_decode(input: &str) -> std::result::Result<Vec<u8>, String> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut lookup = [255u8; 256];
    for (i, &b) in ALPHABET.iter().enumerate() {
        lookup[b as usize] = i as u8;
    }
    let bytes: Vec<u8> = input.bytes().filter(|&b| b != b'\n' && b != b'\r').collect();
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks(4) {
        if chunk.len() < 2 {
            return Err("truncated base64 chunk".to_string());
        }
        let v0 = lookup[chunk[0] as usize];
        let v1 = lookup[chunk[1] as usize];
        if v0 == 255 || v1 == 255 {
            return Err("invalid base64 char".to_string());
        }
        out.push((v0 << 2) | (v1 >> 4));
        if chunk.len() >= 3 && chunk[2] != b'=' {
            let v2 = lookup[chunk[2] as usize];
            if v2 == 255 {
                return Err("invalid base64 char".to_string());
            }
            out.push((v1 << 4) | (v2 >> 2));
            if chunk.len() == 4 && chunk[3] != b'=' {
                let v3 = lookup[chunk[3] as usize];
                if v3 == 255 {
                    return Err("invalid base64 char".to_string());
                }
                out.push((v2 << 6) | v3);
            }
        }
    }
    Ok(out)
}

fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;
        result.push(ALPHABET[b0 >> 2] as char);
        result.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);
        if chunk.len() > 1 {
            result.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(ALPHABET[b2 & 0x3f] as char);
        } else {
            result.push('=');
        }
    }
    result
}
