//! Test worker for cap-do-workers Durable Objects E2E tests.
//!
//! Exposes HTTP endpoints for:
//! - Dedupe put_if_absent/forget
//! - DO storage get/put/delete/list
//! - Alarms get/set/delete
//! - SQLite exec (json/raw)

use cap_do_workers::{SqlExecMode, SqlValue, StorageListOptions, StorageValue, WorkersDurableObject};
use capabilities::dedupe::DedupeStore;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use worker::{Context, Env, Request, Response, Result, RouteContext, Router, event};

pub use cap_do_workers::FlowDurableObject;

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    Router::new()
        .get_async("/health", handle_health)
        .post_async("/dedupe/put_if_absent", handle_dedupe_put)
        .post_async("/dedupe/forget", handle_dedupe_forget)
        .post_async("/storage/put", handle_storage_put)
        .get_async("/storage/get", handle_storage_get)
        .delete_async("/storage/delete", handle_storage_delete)
        .get_async("/storage/list", handle_storage_list)
        .get_async("/alarm/get", handle_alarm_get)
        .post_async("/alarm/set", handle_alarm_set)
        .delete_async("/alarm/delete", handle_alarm_delete)
        .post_async("/sql/exec", handle_sql_exec)
        .run(req, env)
        .await
}

async fn handle_health(_req: Request, _ctx: RouteContext<()>) -> Result<Response> {
    Response::from_json(&json!({ "status": "ok" }))
}

fn client(env: &Env) -> Result<WorkersDurableObject> {
    WorkersDurableObject::from_env(env, "FLOW_DO", Some("test-scope".to_string()))
        .map_err(|err| worker::Error::RustError(err.to_string()))
}

fn json_error(message: impl Into<String>, status: u16) -> Result<Response> {
    Response::from_json(&json!({ "error": message.into() })).map(|r| r.with_status(status))
}

#[derive(Deserialize)]
struct DedupePutRequest {
    key: String,
    #[serde(default)]
    ttl_seconds: Option<u64>,
}

async fn handle_dedupe_put(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let body: DedupePutRequest = match req.json().await {
        Ok(value) => value,
        Err(err) => return json_error(format!("invalid request body: {err}"), 400),
    };

    let dedupe = client(&ctx.env)?;
    let ttl = Duration::from_secs(body.ttl_seconds.unwrap_or(60));
    match dedupe.put_if_absent(body.key.as_bytes(), ttl).await {
        Ok(inserted) => Response::from_json(&json!({ "inserted": inserted })),
        Err(err) => json_error(err.to_string(), 500),
    }
}

#[derive(Deserialize)]
struct DedupeForgetRequest {
    key: String,
}

async fn handle_dedupe_forget(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let body: DedupeForgetRequest = match req.json().await {
        Ok(value) => value,
        Err(err) => return json_error(format!("invalid request body: {err}"), 400),
    };

    let dedupe = client(&ctx.env)?;
    match dedupe.forget(body.key.as_bytes()).await {
        Ok(()) => Response::from_json(&json!({ "success": true })),
        Err(err) => json_error(err.to_string(), 500),
    }
}

#[derive(Deserialize)]
struct StoragePutRequest {
    key: String,
    value: JsonValue,
    #[serde(default)]
    ttl_seconds: Option<u64>,
}

async fn handle_storage_put(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let body: StoragePutRequest = match req.json().await {
        Ok(value) => value,
        Err(err) => return json_error(format!("invalid request body: {err}"), 400),
    };

    let storage = client(&ctx.env)?;
    let ttl = body.ttl_seconds.map(Duration::from_secs);
    match storage
        .storage_put(&body.key, StorageValue::Json(body.value), ttl)
        .await
    {
        Ok(()) => Response::from_json(&json!({ "success": true })),
        Err(err) => json_error(err.to_string(), 500),
    }
}

async fn handle_storage_get(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let url = req.url()?;
    let key = url
        .query_pairs()
        .find(|(k, _)| k == "key")
        .map(|(_, v)| v.to_string());

    let key = match key {
        Some(value) => value,
        None => return json_error("missing 'key' query parameter", 400),
    };

    let storage = client(&ctx.env)?;
    match storage.storage_get(&key).await {
        Ok(Some(StorageValue::Json(value))) => {
            Response::from_json(&json!({ "found": true, "value": value }))
        }
        Ok(Some(StorageValue::Bytes(value))) => Response::from_json(&json!({
            "found": true,
            "value": value,
            "encoding": "base64"
        })),
        Ok(None) => Response::from_json(&json!({ "found": false, "value": null })),
        Err(err) => json_error(err.to_string(), 500),
    }
}

async fn handle_storage_delete(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let url = req.url()?;
    let key = url
        .query_pairs()
        .find(|(k, _)| k == "key")
        .map(|(_, v)| v.to_string());

    let key = match key {
        Some(value) => value,
        None => return json_error("missing 'key' query parameter", 400),
    };

    let storage = client(&ctx.env)?;
    match storage.storage_delete(&key).await {
        Ok(deleted) => Response::from_json(&json!({ "deleted": deleted })),
        Err(err) => json_error(err.to_string(), 500),
    }
}

async fn handle_storage_list(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let url = req.url()?;
    let prefix = url
        .query_pairs()
        .find(|(k, _)| k == "prefix")
        .map(|(_, v)| v.to_string());
    let start = url
        .query_pairs()
        .find(|(k, _)| k == "start")
        .map(|(_, v)| v.to_string());
    let limit = url
        .query_pairs()
        .find(|(k, _)| k == "limit")
        .and_then(|(_, v)| v.parse::<usize>().ok());

    let mut options = StorageListOptions::default();
    if let Some(prefix) = prefix {
        options = options.with_prefix(prefix);
    }
    if let Some(start) = start {
        options = options.with_start(start);
    }
    if let Some(limit) = limit {
        options = options.with_limit(limit);
    }

    let storage = client(&ctx.env)?;
    match storage.storage_list(options).await {
        Ok(keys) => Response::from_json(&json!({ "keys": keys })),
        Err(err) => json_error(err.to_string(), 500),
    }
}

async fn handle_alarm_get(_req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let storage = client(&ctx.env)?;
    match storage.alarm_get().await {
        Ok(alarm_ms) => Response::from_json(&json!({ "alarm_ms": alarm_ms })),
        Err(err) => json_error(err.to_string(), 500),
    }
}

#[derive(Deserialize)]
struct AlarmSetRequest {
    scheduled_ms: i64,
}

async fn handle_alarm_set(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let body: AlarmSetRequest = match req.json().await {
        Ok(value) => value,
        Err(err) => return json_error(format!("invalid request body: {err}"), 400),
    };

    let storage = client(&ctx.env)?;
    match storage.alarm_set(body.scheduled_ms).await {
        Ok(()) => Response::from_json(&json!({ "success": true })),
        Err(err) => json_error(err.to_string(), 500),
    }
}

async fn handle_alarm_delete(_req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let storage = client(&ctx.env)?;
    match storage.alarm_delete().await {
        Ok(()) => Response::from_json(&json!({ "success": true })),
        Err(err) => json_error(err.to_string(), 500),
    }
}

#[derive(Deserialize)]
struct SqlExecRequest {
    query: String,
    #[serde(default)]
    bindings: Vec<SqlValue>,
    #[serde(default)]
    mode: SqlExecMode,
}

#[derive(Serialize)]
struct SqlExecResponse<T> {
    rows: T,
}

async fn handle_sql_exec(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let body: SqlExecRequest = match req.json().await {
        Ok(value) => value,
        Err(err) => return json_error(format!("invalid request body: {err}"), 400),
    };

    let storage = client(&ctx.env)?;
    match body.mode {
        SqlExecMode::Json => match storage.sql_exec_json(body.query, body.bindings).await {
            Ok(rows) => Response::from_json(&SqlExecResponse { rows }),
            Err(err) => json_error(err.to_string(), 500),
        },
        SqlExecMode::Raw => match storage.sql_exec_raw(body.query, body.bindings).await {
            Ok(rows) => Response::from_json(&SqlExecResponse { rows }),
            Err(err) => json_error(err.to_string(), 500),
        },
    }
}

#[allow(dead_code)]
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
