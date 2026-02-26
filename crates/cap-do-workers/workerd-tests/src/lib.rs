//! Test worker for cap-do-workers Durable Objects E2E tests.
//!
//! Exposes HTTP endpoints for:
//! - Dedupe put_if_absent/forget
//! - DO storage get/put/delete/list
//! - Alarms get/set/delete
//! - SQLite exec (json/raw)

use cap_do_workers::{
    SqlExecMode, SqlValue, StorageListOptions, StorageValue, WorkersDurableObject,
};
use capabilities::dedupe::DedupeStore;
use capabilities::durability::{
    CheckpointFilter, CheckpointHandle, CheckpointRecord, CheckpointStore, FlowFrontier,
    IdempotencyState, ResumeScheduler, ResumeSignalSource, SerializedState, TokenConfig,
};
use dag_core::FlowId;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use std::time::Duration;
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
        .post_async("/checkpoint/put", handle_checkpoint_put)
        .get_async("/checkpoint/get", handle_checkpoint_get)
        .delete_async("/checkpoint/ack", handle_checkpoint_ack)
        .post_async("/checkpoint/lease", handle_checkpoint_lease)
        .post_async("/checkpoint/release", handle_checkpoint_release)
        .get_async("/checkpoint/list", handle_checkpoint_list)
        .post_async("/resume/schedule_after", handle_resume_schedule_after)
        .post_async("/resume/schedule_status", handle_resume_schedule_status)
        .post_async("/resume/schedule_cancel", handle_resume_schedule_cancel)
        .post_async("/resume/token_create", handle_resume_token_create)
        .post_async("/resume/token_resolve", handle_resume_token_resolve)
        .post_async("/resume/token_revoke", handle_resume_token_revoke)
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

fn parse_checkpoint_handle(url: &worker::Url) -> Option<CheckpointHandle> {
    let checkpoint_id = url
        .query_pairs()
        .find(|(k, _)| k == "checkpoint_id")
        .map(|(_, v)| v.to_string())?;
    let flow_id = url
        .query_pairs()
        .find(|(k, _)| k == "flow_id")
        .map(|(_, v)| v.to_string())?;
    let run_id = url
        .query_pairs()
        .find(|(k, _)| k == "run_id")
        .map(|(_, v)| v.to_string())?;
    Some(CheckpointHandle {
        checkpoint_id,
        flow_id: FlowId(flow_id),
        run_id,
    })
}

#[derive(Deserialize)]
struct CheckpointPutRequest {
    checkpoint_id: String,
    flow_id: String,
    run_id: String,
    #[serde(default)]
    ttl_ms: Option<u64>,
    #[serde(default)]
    created_at_ms: Option<u64>,
}

async fn handle_checkpoint_put(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let body: CheckpointPutRequest = match req.json().await {
        Ok(value) => value,
        Err(err) => return json_error(format!("invalid request body: {err}"), 400),
    };

    let store = client(&ctx.env)?;
    let record = CheckpointRecord {
        checkpoint_id: body.checkpoint_id.clone(),
        flow_id: FlowId(body.flow_id.clone()),
        flow_version: "1.0.0".to_string(),
        run_id: body.run_id.clone(),
        parent_run_id: None,
        frontier: FlowFrontier {
            completed: Vec::new(),
            pending: Vec::new(),
        },
        state: SerializedState {
            data: json!({"ok": true}),
            blobs: Vec::new(),
        },
        idempotency: IdempotencyState::default(),
        created_at_ms: body.created_at_ms.unwrap_or(0),
        resume_after_ms: None,
        ttl_ms: body.ttl_ms,
        version: 1,
    };

    match store.put(record).await {
        Ok(handle) => Response::from_json(&json!({
            "checkpoint_id": handle.checkpoint_id,
            "flow_id": handle.flow_id.0,
            "run_id": handle.run_id,
        })),
        Err(err) => json_error(err.to_string(), 500),
    }
}

async fn handle_checkpoint_get(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let url = req.url()?;
    let Some(handle) = parse_checkpoint_handle(&url) else {
        return json_error("missing checkpoint_id/flow_id/run_id query parameters", 400);
    };
    let store = client(&ctx.env)?;
    match store.get(&handle).await {
        Ok(record) => Response::from_json(&json!({
            "checkpoint_id": record.checkpoint_id,
            "flow_id": record.flow_id.0,
            "run_id": record.run_id,
            "ttl_ms": record.ttl_ms,
        })),
        Err(err) => json_error(err.to_string(), 500),
    }
}

async fn handle_checkpoint_ack(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let url = req.url()?;
    let Some(handle) = parse_checkpoint_handle(&url) else {
        return json_error("missing checkpoint_id/flow_id/run_id query parameters", 400);
    };
    let store = client(&ctx.env)?;
    match store.ack(&handle).await {
        Ok(()) => Response::from_json(&json!({ "success": true })),
        Err(err) => json_error(err.to_string(), 500),
    }
}

#[derive(Deserialize)]
struct CheckpointLeaseRequest {
    checkpoint_id: String,
    flow_id: String,
    run_id: String,
    ttl_seconds: u64,
}

async fn handle_checkpoint_lease(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let body: CheckpointLeaseRequest = match req.json().await {
        Ok(value) => value,
        Err(err) => return json_error(format!("invalid request body: {err}"), 400),
    };

    let store = client(&ctx.env)?;
    let handle = CheckpointHandle {
        checkpoint_id: body.checkpoint_id,
        flow_id: FlowId(body.flow_id),
        run_id: body.run_id,
    };
    match store
        .lease(&handle, Duration::from_secs(body.ttl_seconds.max(1)))
        .await
    {
        Ok(lease) => Response::from_json(&json!({
            "lease_id": lease.lease_id,
            "expires_at_ms": lease.expires_at_ms,
        })),
        Err(err) => json_error(err.to_string(), 500),
    }
}

#[derive(Deserialize)]
struct CheckpointReleaseRequest {
    lease_id: String,
    expires_at_ms: u64,
}

async fn handle_checkpoint_release(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let body: CheckpointReleaseRequest = match req.json().await {
        Ok(value) => value,
        Err(err) => return json_error(format!("invalid request body: {err}"), 400),
    };

    let store = client(&ctx.env)?;
    match store
        .release_lease(capabilities::durability::Lease {
            lease_id: body.lease_id,
            expires_at_ms: body.expires_at_ms,
        })
        .await
    {
        Ok(()) => Response::from_json(&json!({ "success": true })),
        Err(err) => json_error(err.to_string(), 500),
    }
}

async fn handle_checkpoint_list(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let url = req.url()?;
    let flow_id = url
        .query_pairs()
        .find(|(k, _)| k == "flow_id")
        .map(|(_, v)| FlowId(v.to_string()));
    let run_id = url
        .query_pairs()
        .find(|(k, _)| k == "run_id")
        .map(|(_, v)| v.to_string());
    let status = url
        .query_pairs()
        .find(|(k, _)| k == "status")
        .and_then(|(_, v)| match v.as_ref() {
            "active" => Some(capabilities::durability::CheckpointStatus::Active),
            "expired" => Some(capabilities::durability::CheckpointStatus::Expired),
            "completed" => Some(capabilities::durability::CheckpointStatus::Completed),
            _ => None,
        });

    let filter = CheckpointFilter {
        flow_id,
        run_id,
        status,
    };

    let store = client(&ctx.env)?;
    match store.list(filter).await {
        Ok(handles) => Response::from_json(&json!({
            "checkpoints": handles
                .into_iter()
                .map(|h| json!({
                    "checkpoint_id": h.checkpoint_id,
                    "flow_id": h.flow_id.0,
                    "run_id": h.run_id,
                }))
                .collect::<Vec<_>>()
        })),
        Err(err) => json_error(err.to_string(), 500),
    }
}

#[derive(Deserialize)]
struct ResumeScheduleAfterRequest {
    checkpoint_id: String,
    flow_id: String,
    run_id: String,
    delay_ms: u64,
}

async fn handle_resume_schedule_after(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let body: ResumeScheduleAfterRequest = match req.json().await {
        Ok(value) => value,
        Err(err) => return json_error(format!("invalid request body: {err}"), 400),
    };
    let scheduler = client(&ctx.env)?;
    let handle = CheckpointHandle {
        checkpoint_id: body.checkpoint_id,
        flow_id: FlowId(body.flow_id),
        run_id: body.run_id,
    };
    match scheduler
        .schedule_after(handle, Duration::from_millis(body.delay_ms.max(1)))
        .await
    {
        Ok(schedule_id) => Response::from_json(&json!({ "schedule_id": schedule_id.0 })),
        Err(err) => json_error(err.to_string(), 500),
    }
}

#[derive(Deserialize)]
struct ResumeScheduleStatusRequest {
    schedule_id: String,
}

async fn handle_resume_schedule_status(
    mut req: Request,
    ctx: RouteContext<()>,
) -> Result<Response> {
    let body: ResumeScheduleStatusRequest = match req.json().await {
        Ok(value) => value,
        Err(err) => return json_error(format!("invalid request body: {err}"), 400),
    };
    let scheduler = client(&ctx.env)?;
    match scheduler
        .status(capabilities::durability::ScheduleId(body.schedule_id))
        .await
    {
        Ok(status) => Response::from_json(&json!({ "status": format!("{status:?}") })),
        Err(err) => json_error(err.to_string(), 500),
    }
}

async fn handle_resume_schedule_cancel(
    mut req: Request,
    ctx: RouteContext<()>,
) -> Result<Response> {
    let body: ResumeScheduleStatusRequest = match req.json().await {
        Ok(value) => value,
        Err(err) => return json_error(format!("invalid request body: {err}"), 400),
    };
    let scheduler = client(&ctx.env)?;
    match scheduler
        .cancel(capabilities::durability::ScheduleId(body.schedule_id))
        .await
    {
        Ok(()) => Response::from_json(&json!({ "success": true })),
        Err(err) => json_error(err.to_string(), 500),
    }
}

#[derive(Deserialize)]
struct ResumeTokenCreateRequest {
    checkpoint_id: String,
    flow_id: String,
    run_id: String,
    #[serde(default)]
    ttl_seconds: Option<u64>,
    #[serde(default)]
    single_use: Option<bool>,
}

async fn handle_resume_token_create(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let body: ResumeTokenCreateRequest = match req.json().await {
        Ok(value) => value,
        Err(err) => return json_error(format!("invalid request body: {err}"), 400),
    };
    let signals = client(&ctx.env)?;
    let handle = CheckpointHandle {
        checkpoint_id: body.checkpoint_id,
        flow_id: FlowId(body.flow_id),
        run_id: body.run_id,
    };
    let config = TokenConfig {
        ttl: body.ttl_seconds.map(Duration::from_secs),
        single_use: body.single_use.unwrap_or(true),
        metadata: None,
    };
    match signals.create_token(&handle, config).await {
        Ok(token) => Response::from_json(&json!({ "token": token.0 })),
        Err(err) => json_error(err.to_string(), 500),
    }
}

#[derive(Deserialize)]
struct ResumeTokenResolveRequest {
    token: String,
}

async fn handle_resume_token_resolve(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let body: ResumeTokenResolveRequest = match req.json().await {
        Ok(value) => value,
        Err(err) => return json_error(format!("invalid request body: {err}"), 400),
    };
    let signals = client(&ctx.env)?;
    match signals
        .resolve_token(&capabilities::durability::ResumeToken(body.token))
        .await
    {
        Ok(handle) => Response::from_json(&json!({
            "checkpoint_id": handle.checkpoint_id,
            "flow_id": handle.flow_id.0,
            "run_id": handle.run_id,
        })),
        Err(err) => json_error(err.to_string(), 500),
    }
}

async fn handle_resume_token_revoke(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let body: ResumeTokenResolveRequest = match req.json().await {
        Ok(value) => value,
        Err(err) => return json_error(format!("invalid request body: {err}"), 400),
    };
    let signals = client(&ctx.env)?;
    match signals
        .revoke_token(&capabilities::durability::ResumeToken(body.token))
        .await
    {
        Ok(()) => Response::from_json(&json!({ "success": true })),
        Err(err) => json_error(err.to_string(), 500),
    }
}
