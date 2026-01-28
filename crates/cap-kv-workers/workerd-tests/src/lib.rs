//! Test worker for cap-kv-workers E2E tests.
//!
//! This worker exposes HTTP endpoints to exercise Workers KV operations:
//! - POST /kv/put - put a key-value pair
//! - GET /kv/get - get a value by key
//! - DELETE /kv/delete - delete a key
//! - GET /kv/list - list keys with prefix/limit/cursor

use cap_kv_workers::WorkersKv;
use capabilities::kv::{KeyValue, KvGetOptions, KvListOptions, KvPutOptions};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use std::time::Duration;
use worker::{Context, Env, Request, Response, Result, RouteContext, Router, event, kv::KvStore};

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    Router::new()
        .get_async("/health", handle_health)
        .post_async("/kv/put", handle_put)
        .get_async("/kv/get", handle_get)
        .delete_async("/kv/delete", handle_delete)
        .get_async("/kv/list", handle_list)
        .get_async("/kv/get-with-metadata", handle_get_with_metadata)
        .run(req, env)
        .await
}

/// GET /health - Returns 200 OK with status JSON
async fn handle_health(_req: Request, _ctx: RouteContext<()>) -> Result<Response> {
    Response::from_json(&json!({ "status": "ok" }))
}

/// Request body for PUT operations
#[derive(Deserialize)]
struct PutRequest {
    key: String,
    value: String,
    #[serde(default)]
    options: Option<PutOptionsRequest>,
}

#[derive(Deserialize, Default)]
struct PutOptionsRequest {
    /// TTL in seconds
    ttl_seconds: Option<u64>,
    /// Metadata to store with the key
    metadata: Option<JsonValue>,
}

/// POST /kv/put - Put a key-value pair
/// Body: { "key": "...", "value": "...", "options": { "ttl_seconds": N, "metadata": {...} } }
async fn handle_put(mut req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let body: PutRequest = match req.json().await {
        Ok(b) => b,
        Err(e) => {
            return Response::from_json(&json!({
                "error": format!("invalid request body: {}", e)
            }))
            .map(|r| r.with_status(400));
        }
    };

    let kv_store: KvStore = ctx.env.kv("TEST_KV")?;
    let kv = WorkersKv::new(kv_store);

    let mut options = KvPutOptions::default();
    if let Some(opts) = body.options {
        if let Some(ttl) = opts.ttl_seconds {
            options = options.with_ttl(Duration::from_secs(ttl));
        }
        if let Some(metadata) = opts.metadata {
            options = options.with_metadata(metadata);
        }
    }

    match kv
        .put_with_options(&body.key, body.value.as_bytes(), options)
        .await
    {
        Ok(()) => Response::from_json(&json!({ "success": true })),
        Err(e) => Response::from_json(&json!({
            "error": format!("{}", e)
        }))
        .map(|r| r.with_status(500)),
    }
}

/// GET /kv/get?key=X - Get a value by key
async fn handle_get(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let url = req.url()?;
    let key = url
        .query_pairs()
        .find(|(k, _)| k == "key")
        .map(|(_, v)| v.to_string());

    let cache_ttl = url
        .query_pairs()
        .find(|(k, _)| k == "cache_ttl")
        .and_then(|(_, v)| v.parse::<u64>().ok());

    let key = match key {
        Some(k) => k,
        None => {
            return Response::from_json(&json!({ "error": "missing 'key' query parameter" }))
                .map(|r| r.with_status(400));
        }
    };

    let kv_store: KvStore = ctx.env.kv("TEST_KV")?;
    let kv = WorkersKv::new(kv_store);

    let mut options = KvGetOptions::default();
    if let Some(ttl) = cache_ttl {
        options = options.with_cache_ttl(Duration::from_secs(ttl));
    }

    match kv.get_with_options(&key, options).await {
        Ok(Some(value)) => {
            // Try to decode as UTF-8, otherwise base64 encode
            let value_str =
                String::from_utf8(value.clone()).unwrap_or_else(|_| base64_encode(&value));
            Response::from_json(&json!({
                "found": true,
                "value": value_str
            }))
        }
        Ok(None) => Response::from_json(&json!({
            "found": false,
            "value": null
        })),
        Err(e) => Response::from_json(&json!({
            "error": format!("{}", e)
        }))
        .map(|r| r.with_status(500)),
    }
}

/// GET /kv/get-with-metadata?key=X - Get a value with metadata
async fn handle_get_with_metadata(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let url = req.url()?;
    let key = url
        .query_pairs()
        .find(|(k, _)| k == "key")
        .map(|(_, v)| v.to_string());

    let cache_ttl = url
        .query_pairs()
        .find(|(k, _)| k == "cache_ttl")
        .and_then(|(_, v)| v.parse::<u64>().ok());

    let key = match key {
        Some(k) => k,
        None => {
            return Response::from_json(&json!({ "error": "missing 'key' query parameter" }))
                .map(|r| r.with_status(400));
        }
    };

    let kv_store: KvStore = ctx.env.kv("TEST_KV")?;
    let kv = WorkersKv::new(kv_store);

    let mut options = KvGetOptions::default();
    if let Some(ttl) = cache_ttl {
        options = options.with_cache_ttl(Duration::from_secs(ttl));
    }

    match kv.get_with_metadata(&key, options).await {
        Ok(Some(kv_value)) => {
            let value_str = String::from_utf8(kv_value.value.clone())
                .unwrap_or_else(|_| base64_encode(&kv_value.value));
            Response::from_json(&json!({
                "found": true,
                "value": value_str,
                "metadata": kv_value.metadata
            }))
        }
        Ok(None) => Response::from_json(&json!({
            "found": false,
            "value": null,
            "metadata": null
        })),
        Err(e) => Response::from_json(&json!({
            "error": format!("{}", e)
        }))
        .map(|r| r.with_status(500)),
    }
}

/// DELETE /kv/delete?key=X - Delete a key
async fn handle_delete(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let url = req.url()?;
    let key = url
        .query_pairs()
        .find(|(k, _)| k == "key")
        .map(|(_, v)| v.to_string());

    let key = match key {
        Some(k) => k,
        None => {
            return Response::from_json(&json!({ "error": "missing 'key' query parameter" }))
                .map(|r| r.with_status(400));
        }
    };

    let kv_store: KvStore = ctx.env.kv("TEST_KV")?;
    let kv = WorkersKv::new(kv_store);

    match kv.delete(&key).await {
        Ok(()) => Response::from_json(&json!({ "success": true })),
        Err(e) => Response::from_json(&json!({
            "error": format!("{}", e)
        }))
        .map(|r| r.with_status(500)),
    }
}

/// Response for list operations
#[derive(Serialize)]
struct ListResponse {
    keys: Vec<ListKeyEntry>,
    list_complete: bool,
    cursor: Option<String>,
}

#[derive(Serialize)]
struct ListKeyEntry {
    key: String,
    metadata: Option<JsonValue>,
    expires_at: Option<u64>,
}

/// GET /kv/list?prefix=X&limit=N&cursor=C - List keys
async fn handle_list(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let url = req.url()?;

    let prefix = url
        .query_pairs()
        .find(|(k, _)| k == "prefix")
        .map(|(_, v)| v.to_string());

    let limit = url
        .query_pairs()
        .find(|(k, _)| k == "limit")
        .and_then(|(_, v)| v.parse::<usize>().ok());

    let cursor = url
        .query_pairs()
        .find(|(k, _)| k == "cursor")
        .map(|(_, v)| v.to_string());

    let include_metadata = url
        .query_pairs()
        .find(|(k, _)| k == "include_metadata")
        .and_then(|(_, v)| parse_bool(&v));

    let include_expiration = url
        .query_pairs()
        .find(|(k, _)| k == "include_expiration")
        .and_then(|(_, v)| parse_bool(&v));

    let kv_store: KvStore = ctx.env.kv("TEST_KV")?;
    let kv = WorkersKv::new(kv_store);

    let mut options = KvListOptions::default();
    if let Some(p) = prefix {
        options = options.with_prefix(p);
    }
    if let Some(l) = limit {
        options = options.with_limit(l);
    }
    if let Some(c) = cursor {
        options = options.with_cursor(c);
    }
    if include_metadata == Some(true) {
        options = options.include_metadata();
    }
    if include_expiration == Some(true) {
        options = options.include_expiration();
    }

    match kv.list(options).await {
        Ok(result) => {
            let keys: Vec<ListKeyEntry> = result
                .keys
                .into_iter()
                .map(|entry| ListKeyEntry {
                    key: entry.key,
                    metadata: entry.metadata,
                    expires_at: entry
                        .expires_at
                        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|duration| duration.as_secs()),
                })
                .collect();

            Response::from_json(&ListResponse {
                keys,
                list_complete: result.list_complete,
                cursor: result.cursor,
            })
        }
        Err(e) => Response::from_json(&json!({
            "error": format!("{}", e)
        }))
        .map(|r| r.with_status(500)),
    }
}

/// Simple base64 encoding for binary data
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

fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}
