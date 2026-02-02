use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;
#[cfg(not(target_arch = "wasm32"))]
use std::time::{SystemTime, UNIX_EPOCH};

use async_stream::stream;
use cap_do_workers::{SNAPSHOT_SEQ_KEY, StorageValue, WorkersDurableObject};
#[cfg(target_arch = "wasm32")]
use cap_do_workers::DurableObjectBinding;
#[cfg(not(target_arch = "wasm32"))]
type DurableObjectBinding = ();
use capabilities::context;
use capabilities::http::{HttpHeaders, HttpMethod, HttpRequest, HttpWrite};
use capabilities::kv::{KeyValue, KvGetOptions, KvListOptions, KvPutOptions};
use dag_core::{FlowIR, IdempotencyScope, NodeError, NodeResult};
use dag_macros::{def_node, node};
use futures::Stream;
use host_inproc::{FlowBundle, FlowEntrypoint, NodeContract, NodeSource};
use kernel_exec::{NodeRegistry, RegistryError};
use kernel_plan::{ValidatedIR, validate};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const IDEM_TTL_SECS: u64 = 24 * 60 * 60;
const EVENT_TTL_SECS: u64 = 7 * 24 * 60 * 60;
const SNAPSHOT_TTL_SECS: u64 = 30 * 24 * 60 * 60;
const DEFAULT_SNAPSHOT_INTERVAL: u64 = 10;
const DEFAULT_LIST_LIMIT: u32 = 100;
const DEFAULT_STREAM_PAGE_LIMIT: u32 = 100;
const DEFAULT_STREAM_MAX_PAGES: u32 = 5;
const DEFAULT_STREAM_DELAY_MS: u64 = 200;
const OTEL_PAYLOAD_LIMIT_BYTES: usize = 4096;
const OTEL_TIMEOUT_MS: u64 = 2500;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub idem_ttl_secs: u64,
    pub event_ttl_secs: u64,
    pub snapshot_ttl_secs: u64,
    pub snapshot_interval: u64,
    pub list_default_limit: u32,
    pub stream_page_limit: u32,
    pub stream_max_pages: u32,
    pub stream_page_delay_ms: u64,
    pub otel_endpoint: Option<String>,
    pub otel_headers: Option<String>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            idem_ttl_secs: IDEM_TTL_SECS,
            event_ttl_secs: EVENT_TTL_SECS,
            snapshot_ttl_secs: SNAPSHOT_TTL_SECS,
            snapshot_interval: DEFAULT_SNAPSHOT_INTERVAL,
            list_default_limit: DEFAULT_LIST_LIMIT,
            stream_page_limit: DEFAULT_STREAM_PAGE_LIMIT,
            stream_max_pages: DEFAULT_STREAM_MAX_PAGES,
            stream_page_delay_ms: DEFAULT_STREAM_DELAY_MS,
            otel_endpoint: None,
            otel_headers: None,
        }
    }
}

pub fn config_from_env<F>(get: F) -> WorkerConfig
where
    F: Fn(&str) -> Option<String>,
{
    let mut config = WorkerConfig::default();
    if let Some(value) = get("IDEM_TTL_SECS") {
        if let Ok(parsed) = value.parse::<u64>() {
            config.idem_ttl_secs = parsed;
        }
    }
    if let Some(value) = get("EVENT_TTL_SECS") {
        if let Ok(parsed) = value.parse::<u64>() {
            config.event_ttl_secs = parsed;
        }
    }
    if let Some(value) = get("SNAPSHOT_TTL_SECS") {
        if let Ok(parsed) = value.parse::<u64>() {
            config.snapshot_ttl_secs = parsed;
        }
    }
    if let Some(value) = get("SNAPSHOT_INTERVAL") {
        if let Ok(parsed) = value.parse::<u64>() {
            config.snapshot_interval = parsed.max(1);
        }
    }
    if let Some(value) = get("LIST_LIMIT") {
        if let Ok(parsed) = value.parse::<u32>() {
            config.list_default_limit = parsed.max(1);
        }
    }
    if let Some(value) = get("STREAM_PAGE_LIMIT") {
        if let Ok(parsed) = value.parse::<u32>() {
            config.stream_page_limit = parsed.max(1);
        }
    }
    if let Some(value) = get("STREAM_MAX_PAGES") {
        if let Ok(parsed) = value.parse::<u32>() {
            config.stream_max_pages = parsed.max(1);
        }
    }
    if let Some(value) = get("STREAM_PAGE_DELAY_MS") {
        if let Ok(parsed) = value.parse::<u64>() {
            config.stream_page_delay_ms = parsed;
        }
    }
    config.otel_endpoint = get("OTEL_EXPORTER_OTLP_ENDPOINT");
    config.otel_headers = get("OTEL_EXPORTER_OTLP_HEADERS");
    config
}

mod worker_context {
    use super::{DurableObjectBinding, WorkerConfig};
    use std::cell::RefCell;

    #[derive(Clone)]
    pub struct WorkerContext {
        pub binding: DurableObjectBinding,
        pub config: WorkerConfig,
    }

    thread_local! {
        static CONTEXT: RefCell<Option<WorkerContext>> = const { RefCell::new(None) };
    }

    pub fn set(binding: DurableObjectBinding, config: WorkerConfig) {
        CONTEXT.with(|slot| {
            *slot.borrow_mut() = Some(WorkerContext { binding, config });
        });
    }

    pub fn clear() {
        CONTEXT.with(|slot| {
            *slot.borrow_mut() = None;
        });
    }

    pub fn binding() -> Option<DurableObjectBinding> {
        CONTEXT.with(|slot| slot.borrow().as_ref().map(|ctx| ctx.binding.clone()))
    }

    pub fn config() -> WorkerConfig {
        CONTEXT
            .with(|slot| slot.borrow().as_ref().map(|ctx| ctx.config.clone()))
            .unwrap_or_default()
    }
}

pub fn set_worker_context(binding: DurableObjectBinding, config: WorkerConfig) {
    worker_context::set(binding, config);
}

pub fn clear_worker_context() {
    worker_context::clear();
}

#[cfg(target_arch = "wasm32")]
fn now_ms() -> i64 {
    js_sys::Date::now() as i64
}

#[cfg(not(target_arch = "wasm32"))]
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(target_arch = "wasm32")]
async fn sleep_delay(duration: Duration) {
    gloo_timers::future::sleep(duration).await;
}

#[cfg(not(target_arch = "wasm32"))]
async fn sleep_delay(duration: Duration) {
    tokio::time::sleep(duration).await;
}

fn durable_object_for_scope(scope: &str) -> Result<WorkersDurableObject, NodeError> {
    #[cfg(target_arch = "wasm32")]
    {
        let binding = worker_context::binding().ok_or_else(|| {
            NodeError::new("durable object binding missing from worker context")
        })?;
        return Ok(WorkersDurableObject::from_binding(
            binding,
            Some(scope.to_string()),
        ));
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        Ok(WorkersDurableObject::new_stub(scope.to_string()))
    }
}

fn payload_hash(payload: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    let digest = hasher.finalize();
    hex::encode(digest)
}

fn event_key(tenant: &str, stream: &str, seq: u64) -> String {
    format!("evt:{tenant}:{stream}:{seq:020}")
}

fn snapshot_key(tenant: &str, stream: &str) -> String {
    format!("snap:{tenant}:{stream}")
}

fn parse_headers(raw: &str) -> HttpHeaders {
    let mut headers = HttpHeaders::default();
    for pair in raw.split(',') {
        let trimmed = pair.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut parts = trimmed.splitn(2, '=');
        if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
            headers.insert(key.trim(), value.trim());
        }
    }
    headers
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IngestRequest {
    pub tenant: String,
    pub stream: String,
    pub idempotency_key: String,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonValue>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidatedIngest {
    pub tenant: String,
    pub stream: String,
    pub idempotency_key: String,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonValue>,
    pub payload_hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DedupeResult {
    pub inserted: bool,
    pub seq: u64,
    pub first_seen_ts: i64,
    pub tenant: String,
    pub stream: String,
    pub idempotency_key: String,
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonValue>,
    pub payload_hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KvWriteResult {
    pub stored: bool,
    pub key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotResult {
    pub snapshot_written: bool,
    pub snapshot_seq: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OtelDispatchResult {
    pub status: i32,
    pub ok: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListRequest {
    pub tenant: String,
    pub stream: String,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_u32")]
    pub limit: Option<u32>,
    #[serde(default)]
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListEvent {
    pub seq: u64,
    #[serde(with = "serde_bytes")]
    pub payload_bytes: Vec<u8>,
    pub metadata: JsonValue,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListResponse {
    pub tenant: String,
    pub stream: String,
    pub idempotency_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    pub events: Vec<ListEvent>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventsResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    pub events: Vec<ListEvent>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotRequest {
    pub tenant: String,
    pub stream: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotResponse {
    pub seq: u64,
    #[serde(with = "serde_bytes")]
    pub payload_bytes: Vec<u8>,
    pub metadata: JsonValue,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OtelAttrValue {
    String(String),
    Bool(bool),
    Number(serde_json::Number),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OtelEvent {
    pub event_name: String,
    pub idempotency_key: String,
    pub attrs: BTreeMap<String, OtelAttrValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_bytes: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IngestResponse {
    pub seq: u64,
    pub inserted: bool,
    pub hash: String,
}

#[def_node(trigger, name = "trigger_http_ingest", summary = "Ingress trigger for ingest")]
async fn trigger_http_ingest(request: IngestRequest) -> NodeResult<IngestRequest> {
    Ok(request)
}

fn build_list_idempotency_key(request: &ListRequest) -> String {
    let mut key = format!("{}:{}", request.tenant, request.stream);
    if let Some(cursor) = request.cursor.as_ref() {
        key.push(':');
        key.push_str(cursor);
    }
    key
}

#[def_node(trigger, name = "trigger_http_events", summary = "Ingress trigger for event listing")]
async fn trigger_http_events(mut request: ListRequest) -> NodeResult<ListRequest> {
    if request.idempotency_key.is_empty() {
        request.idempotency_key = build_list_idempotency_key(&request);
    }
    Ok(request)
}

#[def_node(trigger, name = "trigger_http_snapshot", summary = "Ingress trigger for snapshot reads")]
async fn trigger_http_snapshot(request: SnapshotRequest) -> NodeResult<SnapshotRequest> {
    Ok(request)
}

#[def_node(trigger, name = "trigger_http_stream", summary = "Ingress trigger for stream")]
async fn trigger_http_stream(mut request: ListRequest) -> NodeResult<ListRequest> {
    if request.idempotency_key.is_empty() {
        request.idempotency_key = build_list_idempotency_key(&request);
    }
    Ok(request)
}

#[def_node(
    name = "ingest_validate",
    summary = "Validate and hash ingest payload",
    effects = "Pure",
    determinism = "Strict"
)]
async fn ingest_validate(request: IngestRequest) -> NodeResult<ValidatedIngest> {
    let hash = payload_hash(&request.payload);
    Ok(ValidatedIngest {
        tenant: request.tenant,
        stream: request.stream,
        idempotency_key: request.idempotency_key,
        payload: request.payload,
        metadata: request.metadata,
        payload_hash: hash,
    })
}

#[def_node(
    name = "dedupe_sequence",
    summary = "Reserve a per-stream sequence with DO idempotency",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(dedupe_write(capabilities::dedupe::DedupeStore))
)]
async fn dedupe_sequence(input: ValidatedIngest) -> NodeResult<DedupeResult> {
    let scope = format!("idem:{}:{}", input.tenant, input.stream);
    let client = durable_object_for_scope(&scope)?;
    let config = worker_context::config();
    let seq = client
        .sequence(
            input.idempotency_key.as_str(),
            input.payload_hash.as_str(),
            Duration::from_secs(config.idem_ttl_secs),
            Some(now_ms()),
        )
        .await
        .map_err(|err| NodeError::new(format!("durable object sequence failed: {err}")))?;
    Ok(DedupeResult {
        inserted: seq.inserted,
        seq: seq.seq,
        first_seen_ts: seq.first_seen_ts,
        tenant: input.tenant,
        stream: input.stream,
        idempotency_key: input.idempotency_key,
        payload: input.payload,
        metadata: input.metadata,
        payload_hash: input.payload_hash,
    })
}

#[def_node(
    name = "kv_put_event",
    summary = "Store event payload + metadata in Workers KV",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(kv_write(capabilities::kv::KeyValue))
)]
async fn kv_put_event(input: DedupeResult) -> NodeResult<KvWriteResult> {
    let key = event_key(&input.tenant, &input.stream, input.seq);
    let key_for_put = key.clone();
    if !input.inserted {
        return Ok(KvWriteResult { stored: false, key });
    }

    let config = worker_context::config();
    let timestamp = now_ms();
    let mut meta = serde_json::Map::new();
    meta.insert("idempotency_key".to_string(), json!(input.idempotency_key));
    meta.insert("payload_hash".to_string(), json!(input.payload_hash));
    meta.insert("ts".to_string(), json!(timestamp));
    meta.insert("size".to_string(), json!(input.payload.len() as u64));
    meta.insert("seq".to_string(), json!(input.seq));
    if let Some(user_meta) = input.metadata.clone() {
        meta.insert("user".to_string(), user_meta);
    }

    let metadata = JsonValue::Object(meta);
    let ttl = Duration::from_secs(config.event_ttl_secs.max(60));

    context::with_current_async(|resources| async move {
        let kv = resources
            .kv()
            .ok_or_else(|| NodeError::new("kv capability missing"))?;
        kv.put_with_options(
            &key_for_put,
            &input.payload,
            KvPutOptions::default().with_ttl(ttl).with_metadata(metadata),
        )
        .await
        .map_err(|err| NodeError::new(format!("kv put failed: {err}")))?;
        Ok::<_, NodeError>(())
    })
    .await
    .ok_or_else(|| NodeError::new("resource context missing"))??;

    Ok(KvWriteResult { stored: true, key })
}

#[def_node(
    name = "snapshot_maybe",
    summary = "Persist snapshot every N events",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(kv_write(capabilities::kv::KeyValue))
)]
async fn snapshot_maybe(input: DedupeResult) -> NodeResult<SnapshotResult> {
    let config = worker_context::config();
    if !input.inserted {
        return Ok(SnapshotResult {
            snapshot_written: false,
            snapshot_seq: input.seq,
        });
    }

    if config.snapshot_interval == 0 || input.seq % config.snapshot_interval != 0 {
        return Ok(SnapshotResult {
            snapshot_written: false,
            snapshot_seq: input.seq,
        });
    }

    let timestamp = now_ms();
    let mut meta = serde_json::Map::new();
    meta.insert("seq".to_string(), json!(input.seq));
    meta.insert("ts".to_string(), json!(timestamp));
    meta.insert("hash".to_string(), json!(input.payload_hash));
    let metadata = JsonValue::Object(meta);
    let key = snapshot_key(&input.tenant, &input.stream);
    let ttl = Duration::from_secs(config.snapshot_ttl_secs.max(60));

    context::with_current_async(|resources| async move {
        let kv = resources
            .kv()
            .ok_or_else(|| NodeError::new("kv capability missing"))?;
        kv.put_with_options(
            &key,
            &input.payload,
            KvPutOptions::default().with_ttl(ttl).with_metadata(metadata.clone()),
        )
        .await
        .map_err(|err| NodeError::new(format!("snapshot kv put failed: {err}")))?;
        Ok::<_, NodeError>(())
    })
    .await
    .ok_or_else(|| NodeError::new("resource context missing"))??;

    #[cfg(target_arch = "wasm32")]
    if let Some(binding) = worker_context::binding() {
        let scope = format!("idem:{}:{}", input.tenant, input.stream);
        let client = WorkersDurableObject::from_binding(binding, Some(scope));
        let value = json!({
            "seq": input.seq,
            "ts": timestamp,
            "hash": input.payload_hash,
        });
        let _ = client
            .storage_put(SNAPSHOT_SEQ_KEY, StorageValue::Json(value), None)
            .await;
    }

    Ok(SnapshotResult {
        snapshot_written: true,
        snapshot_seq: input.seq,
    })
}

#[def_node(
    name = "otel_dispatch",
    summary = "Dispatch OTLP/HTTP payload",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(http_write(capabilities::http::HttpWrite))
)]
async fn otel_dispatch(event: OtelEvent) -> NodeResult<OtelDispatchResult> {
    if should_skip_otel(&event) {
        return Ok(OtelDispatchResult { status: 0, ok: true });
    }

    let config = worker_context::config();
    let endpoint = match config.otel_endpoint.as_ref() {
        Some(endpoint) => endpoint.clone(),
        None => {
            return Ok(OtelDispatchResult { status: 0, ok: false });
        }
    };

    let payload = build_otlp_payload(&event);
    let mut request = HttpRequest::new(HttpMethod::Post, endpoint);
    request.body = Some(payload);
    request.timeout_ms = Some(OTEL_TIMEOUT_MS);
    request.headers.insert("content-type".to_string(), "application/json".to_string());

    if let Some(raw) = config.otel_headers.as_ref() {
        let extra = parse_headers(raw);
        for (key, value) in extra.iter() {
            request.headers.insert(key.clone(), value.clone());
        }
    }

    let response = context::with_current_async(|resources| async move {
        let http = resources
            .http_write()
            .ok_or_else(|| NodeError::new("http write capability missing"))?;
        http.send(request)
            .await
            .map_err(|err| NodeError::new(format!("otel dispatch failed: {err}")))
    })
    .await
    .ok_or_else(|| NodeError::new("resource context missing"));

    match response {
        Ok(Ok(response)) => Ok(OtelDispatchResult {
            status: response.status as i32,
            ok: response.is_success(),
        }),
        _ => Ok(OtelDispatchResult { status: 0, ok: false }),
    }
}

#[def_node(
    name = "events_list",
    summary = "List stored events from KV",
    effects = "ReadOnly",
    determinism = "BestEffort",
    resources(kv_read(capabilities::kv::KeyValue))
)]
async fn events_list(request: ListRequest) -> NodeResult<ListResponse> {
    let prefix = format!("evt:{}:{}:", request.tenant, request.stream);
    let limit = request
        .limit
        .unwrap_or(worker_context::config().list_default_limit)
        .max(1) as usize;
    let cursor = request.cursor.clone();
    let tenant = request.tenant.clone();
    let stream_name = request.stream.clone();
    let idempotency_key = request.idempotency_key.clone();

    context::with_current_async(|resources| async move {
        let kv = resources
            .kv()
            .ok_or_else(|| NodeError::new("kv capability missing"))?;
        let mut options = KvListOptions::default()
            .with_prefix(prefix)
            .with_limit(limit)
            .include_metadata();
        if let Some(cursor) = cursor {
            options = options.with_cursor(cursor);
        }

        let listed = kv
            .list(options)
            .await
            .map_err(|err| NodeError::new(format!("kv list failed: {err}")))?;

        let mut events = Vec::with_capacity(listed.keys.len());
        for entry in listed.keys {
            if let Some(value) = kv
                .get_with_metadata(&entry.key, KvGetOptions::default())
                .await
                .map_err(|err| NodeError::new(format!("kv get failed: {err}")))?
            {
                let seq = value
                    .metadata
                    .as_ref()
                    .and_then(|meta| meta.get("seq"))
                    .and_then(|value| value.as_u64())
                    .unwrap_or_else(|| parse_seq_from_key(&entry.key));
                let metadata = value.metadata.unwrap_or_else(|| json!({}));
                events.push(ListEvent {
                    seq,
                    payload_bytes: value.value,
                    metadata,
                });
            }
        }

        Ok::<_, NodeError>(ListResponse {
            tenant,
            stream: stream_name,
            idempotency_key,
            cursor: listed.cursor,
            events,
        })
    })
    .await
    .ok_or_else(|| NodeError::new("resource context missing"))?
}

#[def_node(
    name = "snapshot_get",
    summary = "Read latest snapshot from KV",
    effects = "ReadOnly",
    determinism = "BestEffort",
    resources(kv_read(capabilities::kv::KeyValue))
)]
async fn snapshot_get(request: SnapshotRequest) -> NodeResult<SnapshotResponse> {
    let key = snapshot_key(&request.tenant, &request.stream);

    context::with_current_async(|resources| async move {
        let kv = resources
            .kv()
            .ok_or_else(|| NodeError::new("kv capability missing"))?;
        let value = kv
            .get_with_metadata(&key, KvGetOptions::default())
            .await
            .map_err(|err| NodeError::new(format!("kv get failed: {err}")))?;

        let value = value.ok_or_else(|| NodeError::new("snapshot not found"))?;
        let seq = value
            .metadata
            .as_ref()
            .and_then(|meta| meta.get("seq"))
            .and_then(|value| value.as_u64())
            .unwrap_or(0);
        let metadata = value.metadata.unwrap_or_else(|| json!({}));
        Ok::<_, NodeError>(SnapshotResponse {
            seq,
            payload_bytes: value.value,
            metadata,
        })
    })
    .await
    .ok_or_else(|| NodeError::new("resource context missing"))?
}

#[def_node(
    name = "build_otel_ingest",
    summary = "Build ingest OTLP payload",
    effects = "Pure",
    determinism = "BestEffort"
)]
async fn build_otel_ingest(input: DedupeResult) -> NodeResult<OtelEvent> {
    let mut attrs = BTreeMap::new();
    attrs.insert("tenant".to_string(), OtelAttrValue::String(input.tenant.clone()));
    attrs.insert("stream".to_string(), OtelAttrValue::String(input.stream.clone()));
    attrs.insert(
        "seq".to_string(),
        OtelAttrValue::Number(serde_json::Number::from(input.seq)),
    );
    attrs.insert("inserted".to_string(), OtelAttrValue::Bool(input.inserted));
    attrs.insert(
        "idempotency_key".to_string(),
        OtelAttrValue::String(input.idempotency_key.clone()),
    );
    attrs.insert(
        "payload_hash".to_string(),
        OtelAttrValue::String(input.payload_hash.clone()),
    );
    attrs.insert(
        "payload_size".to_string(),
        OtelAttrValue::Number(serde_json::Number::from(input.payload.len() as u64)),
    );
    attrs.insert("event_id".to_string(), OtelAttrValue::String(Uuid::new_v4().to_string()));
    attrs.insert("run_id".to_string(), OtelAttrValue::String(Uuid::new_v4().to_string()));

    let payload_bytes = if input.inserted {
        Some(truncate_bytes(&input.payload))
    } else {
        None
    };

    Ok(OtelEvent {
        event_name: "ingest".to_string(),
        idempotency_key: input.idempotency_key.clone(),
        attrs,
        payload_bytes,
    })
}

#[def_node(
    name = "build_otel_replay",
    summary = "Build replay OTLP payload",
    effects = "Pure",
    determinism = "BestEffort"
)]
async fn build_otel_replay(input: ListResponse) -> NodeResult<OtelEvent> {
    let mut attrs = BTreeMap::new();
    attrs.insert("tenant".to_string(), OtelAttrValue::String(input.tenant.clone()));
    attrs.insert("stream".to_string(), OtelAttrValue::String(input.stream.clone()));
    attrs.insert(
        "idempotency_key".to_string(),
        OtelAttrValue::String(input.idempotency_key.clone()),
    );
    attrs.insert("event_id".to_string(), OtelAttrValue::String(Uuid::new_v4().to_string()));
    attrs.insert("run_id".to_string(), OtelAttrValue::String(Uuid::new_v4().to_string()));
    attrs.insert(
        "count".to_string(),
        OtelAttrValue::Number(serde_json::Number::from(input.events.len() as u64)),
    );
    if let Some(cursor) = input.cursor.as_ref() {
        attrs.insert("cursor".to_string(), OtelAttrValue::String(cursor.clone()));
    }

    Ok(OtelEvent {
        event_name: "replay_page".to_string(),
        idempotency_key: input.idempotency_key.clone(),
        attrs,
        payload_bytes: None,
    })
}

#[def_node(
    name = "ingest_response",
    summary = "Format ingest response",
    effects = "Pure",
    determinism = "Strict"
)]
async fn ingest_response(input: DedupeResult) -> NodeResult<IngestResponse> {
    Ok(IngestResponse {
        seq: input.seq,
        inserted: input.inserted,
        hash: input.payload_hash,
    })
}

#[def_node(
    name = "events_response",
    summary = "Format events response",
    effects = "Pure",
    determinism = "Strict"
)]
async fn events_response(input: ListResponse) -> NodeResult<EventsResponse> {
    Ok(EventsResponse {
        cursor: input.cursor,
        events: input.events,
    })
}

#[cfg(target_arch = "wasm32")]
type ListEventStreamInner = dyn Stream<Item = NodeResult<ListEvent>>;

#[cfg(not(target_arch = "wasm32"))]
type ListEventStreamInner = dyn Stream<Item = NodeResult<ListEvent>> + Send;

struct ListEventStream {
    inner: Mutex<Pin<Box<ListEventStreamInner>>>,
}

impl ListEventStream {
    #[cfg(target_arch = "wasm32")]
    fn new(stream: impl Stream<Item = NodeResult<ListEvent>> + 'static) -> Self {
        Self {
            inner: Mutex::new(Box::pin(stream)),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn new(stream: impl Stream<Item = NodeResult<ListEvent>> + Send + 'static) -> Self {
        Self {
            inner: Mutex::new(Box::pin(stream)),
        }
    }
}

impl Stream for ListEventStream {
    type Item = NodeResult<ListEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let mut guard = self.inner.lock().expect("stream lock poisoned");
        guard.as_mut().poll_next(cx)
    }
}

impl serde::Serialize for ListEventStream {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_unit()
    }
}

#[def_node(
    name = "stream_events",
    summary = "Stream events over SSE",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(kv_read(capabilities::kv::KeyValue), http_write(capabilities::http::HttpWrite)),
    out = "ListEvent"
)]
async fn stream_events(request: ListRequest) -> NodeResult<ListEventStream> {
    let resources = context::current_handle().ok_or_else(|| {
        NodeError::new("resource context missing")
    })?;
    let config = worker_context::config();
    let tenant = request.tenant.clone();
    let stream_name = request.stream.clone();
    let mut cursor = request.cursor.clone();
    let page_limit = request.limit.unwrap_or(config.stream_page_limit).max(1);
    let max_pages = config.stream_max_pages.max(1);
    let delay = Duration::from_millis(config.stream_page_delay_ms);

    let open_event = build_stream_event("stream_open", &tenant, &stream_name, cursor.as_deref());
    let close_event = build_stream_event("stream_close", &tenant, &stream_name, cursor.as_deref());

    let stream = stream! {
        let _ = dispatch_otel(resources.clone(), open_event).await;
        let mut pages = 0u32;
        loop {
            if pages >= max_pages {
                break;
            }

            let page_cursor = cursor.clone();
            let tenant = tenant.clone();
            let stream_name = stream_name.clone();
            let resources = resources.clone();
            let page = context::with_resources(resources.clone(), async move {
                let kv = resources
                    .kv()
                    .ok_or_else(|| NodeError::new("kv capability missing"))?;
                let prefix = format!("evt:{}:{}:", tenant, stream_name);
                let options = KvListOptions::default()
                    .with_prefix(prefix)
                    .with_limit(page_limit as usize)
                    .include_metadata();
                let options = if let Some(cursor) = page_cursor {
                    options.with_cursor(cursor)
                } else {
                    options
                };
                let listed = kv
                    .list(options)
                    .await
                    .map_err(|err| NodeError::new(format!("kv list failed: {err}")))?;

                let mut events = Vec::new();
                for entry in listed.keys {
                    if let Some(value) = kv
                        .get_with_metadata(&entry.key, KvGetOptions::default())
                        .await
                        .map_err(|err| NodeError::new(format!("kv get failed: {err}")))?
                    {
                        let seq = value
                            .metadata
                            .as_ref()
                            .and_then(|meta| meta.get("seq"))
                            .and_then(|value| value.as_u64())
                            .unwrap_or_else(|| parse_seq_from_key(&entry.key));
                        let metadata = value.metadata.unwrap_or_else(|| json!({}));
                        events.push(ListEvent {
                            seq,
                            payload_bytes: value.value,
                            metadata,
                        });
                    }
                }
                Ok::<_, NodeError>((listed.cursor, events, listed.list_complete))
            })
            .await;

            match page {
                Ok((next_cursor, events, list_complete)) => {
                    for event in events {
                        yield Ok(event);
                    }
                    cursor = next_cursor;
                    pages += 1;
                    if list_complete || cursor.is_none() {
                        break;
                    }
                }
                Err(err) => {
                    yield Err(err);
                    break;
                }
            }

            if delay.as_millis() > 0 {
                sleep_delay(delay).await;
            }
        }
        let _ = dispatch_otel(resources.clone(), close_event).await;
    };

    Ok(ListEventStream::new(stream))
}

fn build_stream_event(name: &str, tenant: &str, stream: &str, cursor: Option<&str>) -> OtelEvent {
    let mut attrs = BTreeMap::new();
    attrs.insert("tenant".to_string(), OtelAttrValue::String(tenant.to_string()));
    attrs.insert("stream".to_string(), OtelAttrValue::String(stream.to_string()));
    attrs.insert("event_id".to_string(), OtelAttrValue::String(Uuid::new_v4().to_string()));
    attrs.insert("run_id".to_string(), OtelAttrValue::String(Uuid::new_v4().to_string()));
    if let Some(cursor) = cursor {
        attrs.insert("cursor".to_string(), OtelAttrValue::String(cursor.to_string()));
    }

    let mut idempotency_key = format!("{}:{}", tenant, stream);
    if let Some(cursor) = cursor {
        idempotency_key.push(':');
        idempotency_key.push_str(cursor);
    }

    OtelEvent {
        event_name: name.to_string(),
        idempotency_key,
        attrs,
        payload_bytes: None,
    }
}

async fn dispatch_otel(
    resources: Arc<dyn capabilities::ResourceAccess>,
    event: OtelEvent,
) -> Result<OtelDispatchResult, NodeError> {
    if should_skip_otel(&event) {
        return Ok(OtelDispatchResult { status: 0, ok: true });
    }
    let config = worker_context::config();
    let endpoint = match config.otel_endpoint.as_ref() {
        Some(endpoint) => endpoint.clone(),
        None => return Ok(OtelDispatchResult { status: 0, ok: false }),
    };

    let payload = build_otlp_payload(&event);
    let mut request = HttpRequest::new(HttpMethod::Post, endpoint);
    request.body = Some(payload);
    request.timeout_ms = Some(OTEL_TIMEOUT_MS);
    request.headers.insert("content-type".to_string(), "application/json".to_string());
    if let Some(raw) = config.otel_headers.as_ref() {
        let extra = parse_headers(raw);
        for (key, value) in extra.iter() {
            request.headers.insert(key.clone(), value.clone());
        }
    }

    let http = match resources.http_write() {
        Some(http) => http,
        None => return Ok(OtelDispatchResult { status: 0, ok: false }),
    };
    let response = http.send(request).await;
    match response {
        Ok(response) => Ok(OtelDispatchResult {
            status: response.status as i32,
            ok: response.is_success(),
        }),
        Err(_) => Ok(OtelDispatchResult { status: 0, ok: false }),
    }
}

fn should_skip_otel(event: &OtelEvent) -> bool {
    if event.event_name == "ingest" {
        if let Some(OtelAttrValue::Bool(inserted)) = event.attrs.get("inserted") {
            return !*inserted;
        }
    }
    false
}

fn truncate_bytes(payload: &[u8]) -> Vec<u8> {
    if payload.len() <= OTEL_PAYLOAD_LIMIT_BYTES {
        payload.to_vec()
    } else {
        payload[..OTEL_PAYLOAD_LIMIT_BYTES].to_vec()
    }
}

fn build_otlp_payload(event: &OtelEvent) -> Vec<u8> {
    let trace_id = Uuid::new_v4().simple().to_string();
    let span_id = Uuid::new_v4().simple().to_string()[..16].to_string();
    let now_ns = (now_ms() as u64) * 1_000_000;

    let mut attributes = Vec::with_capacity(event.attrs.len());
    for (key, value) in event.attrs.iter() {
        let value = match value {
            OtelAttrValue::String(value) => json!({ "stringValue": value }),
            OtelAttrValue::Bool(value) => json!({ "boolValue": value }),
            OtelAttrValue::Number(value) => json!({ "intValue": value }),
        };
        attributes.push(json!({ "key": key, "value": value }));
    }

    if let Some(payload) = event.payload_bytes.as_ref() {
        let truncated = truncate_bytes(payload);
        attributes.push(json!({
            "key": "payload_b64",
            "value": { "stringValue": base64_encode(&truncated) }
        }));
    }

    let payload = json!({
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        { "key": "service.name", "value": { "stringValue": "lattice-flow" } }
                    ]
                },
                "scopeSpans": [
                    {
                        "scope": { "name": "idem-ingest" },
                        "spans": [
                            {
                                "traceId": trace_id,
                                "spanId": span_id,
                                "name": event.event_name,
                                "kind": "SPAN_KIND_INTERNAL",
                                "startTimeUnixNano": now_ns.to_string(),
                                "endTimeUnixNano": now_ns.to_string(),
                                "attributes": attributes
                            }
                        ]
                    }
                ]
            }
        ]
    });

    serde_json::to_vec(&payload).unwrap_or_default()
}

fn parse_seq_from_key(key: &str) -> u64 {
    key.rsplit(':')
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0)
}

fn deserialize_optional_u32<'de, D>(deserializer: D) -> Result<Option<u32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<JsonValue>::deserialize(deserializer)?;
    match value {
        None => Ok(None),
        Some(JsonValue::Number(num)) => num
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| serde::de::Error::custom("invalid limit value"))
            .map(Some),
        Some(JsonValue::String(raw)) => raw
            .parse::<u32>()
            .map(Some)
            .map_err(|_| serde::de::Error::custom("invalid limit value")),
        Some(_) => Err(serde::de::Error::custom("invalid limit value")),
    }
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

fn stream_events_stream_register(registry: &mut NodeRegistry) -> Result<(), RegistryError> {
    registry.register_stream_fn(
        concat!(module_path!(), "::", stringify!(stream_events)),
        stream_events,
    )
}

mod bundle_def {
    use super::*;
    use dag_macros::node;

    dag_macros::flow! {
        name: s7_cloudflare_idem_flow,
        version: "1.0.0",
        profile: Web,
        summary: "Cloudflare idempotent ingest + replay example";

        let trigger_http_ingest = node!(trigger_http_ingest);
        let trigger_http_events = node!(trigger_http_events);
        let trigger_http_snapshot = node!(trigger_http_snapshot);
        let trigger_http_stream = node!(trigger_http_stream);

        let ingest_validate = node!(ingest_validate);
        let dedupe_sequence = node!(dedupe_sequence);
        let kv_put_event = node!(kv_put_event);
        let snapshot_maybe = node!(snapshot_maybe);
        let build_otel_ingest = node!(build_otel_ingest);
        let otel_dispatch = node!(otel_dispatch);
        let ingest_response = node!(ingest_response);

        let events_list = node!(events_list);
        let build_otel_replay = node!(build_otel_replay);
        let events_response = node!(events_response);

        let snapshot_get = node!(snapshot_get);

        let stream_events = node!(stream_events);

        connect!(trigger_http_ingest -> ingest_validate);
        connect!(ingest_validate -> dedupe_sequence);
        connect!(dedupe_sequence -> kv_put_event);
        connect!(dedupe_sequence -> snapshot_maybe);
        connect!(dedupe_sequence -> build_otel_ingest);
        connect!(build_otel_ingest -> otel_dispatch);
        connect!(dedupe_sequence -> ingest_response);

        connect!(trigger_http_events -> events_list);
        connect!(events_list -> build_otel_replay);
        connect!(build_otel_replay -> otel_dispatch);
        connect!(events_list -> events_response);

        connect!(trigger_http_snapshot -> snapshot_get);

        connect!(trigger_http_stream -> stream_events);

        entrypoint!({
            trigger: "trigger_http_ingest",
            capture: "ingest_response",
            route_aliases: ["/ingest"],
            method: "POST",
            deadline_ms: 2000,
        });

        entrypoint!({
            trigger: "trigger_http_events",
            capture: "events_response",
            route_aliases: ["/events"],
            method: "GET",
            deadline_ms: 2000,
        });

        entrypoint!({
            trigger: "trigger_http_snapshot",
            capture: "snapshot_get",
            route_aliases: ["/snapshot"],
            method: "GET",
            deadline_ms: 2000,
        });

        entrypoint!({
            trigger: "trigger_http_stream",
            capture: "stream_events",
            route_aliases: ["/stream"],
            method: "GET",
            deadline_ms: 10000,
        });
    }
}

pub fn flow() -> FlowIR {
    let mut flow = bundle_def::flow();
    flow.policies.lint.allow_multiple_triggers = Some(true);
    let ttl_ms = IDEM_TTL_SECS.saturating_mul(1000);
    ensure_idempotency(&mut flow, "dedupe_sequence", "idempotency_key", ttl_ms);
    ensure_idempotency(&mut flow, "kv_put_event", "idempotency_key", ttl_ms);
    ensure_idempotency(&mut flow, "snapshot_maybe", "idempotency_key", ttl_ms);
    ensure_idempotency(&mut flow, "otel_dispatch", "idempotency_key", ttl_ms);
    ensure_idempotency(&mut flow, "stream_events", "idempotency_key", ttl_ms);
    flow
}

fn ensure_idempotency(flow: &mut FlowIR, alias: &str, key: &str, ttl_ms: u64) {
    if let Some(node) = flow.nodes.iter_mut().find(|node| node.alias == alias) {
        node.idempotency.key = Some(key.to_string());
        node.idempotency.scope = Some(IdempotencyScope::Node);
        node.idempotency.ttl_ms = Some(ttl_ms);
    }
}

pub fn validated_ir() -> ValidatedIR {
    validate(&flow()).expect("s7 flow should validate")
}

pub fn bundle() -> FlowBundle {
    let validated_ir = validated_ir();
    let mut registry = NodeRegistry::new();
    trigger_http_ingest_register(&mut registry).expect("register ingest trigger");
    trigger_http_events_register(&mut registry).expect("register events trigger");
    trigger_http_snapshot_register(&mut registry).expect("register snapshot trigger");
    trigger_http_stream_register(&mut registry).expect("register stream trigger");
    ingest_validate_register(&mut registry).expect("register ingest_validate");
    dedupe_sequence_register(&mut registry).expect("register dedupe_sequence");
    kv_put_event_register(&mut registry).expect("register kv_put_event");
    snapshot_maybe_register(&mut registry).expect("register snapshot_maybe");
    build_otel_ingest_register(&mut registry).expect("register build_otel_ingest");
    otel_dispatch_register(&mut registry).expect("register otel_dispatch");
    ingest_response_register(&mut registry).expect("register ingest_response");
    events_list_register(&mut registry).expect("register events_list");
    build_otel_replay_register(&mut registry).expect("register build_otel_replay");
    events_response_register(&mut registry).expect("register events_response");
    snapshot_get_register(&mut registry).expect("register snapshot_get");
    stream_events_stream_register(&mut registry).expect("register stream_events");

    let resolver: Arc<dyn host_inproc::NodeResolver> = Arc::new(registry);
    let entrypoints = vec![
        FlowEntrypoint {
            trigger_alias: "trigger_http_ingest".to_string(),
            capture_alias: "ingest_response".to_string(),
            route_path: Some("/ingest".to_string()),
            method: Some("POST".to_string()),
            deadline: Some(Duration::from_millis(2000)),
            route_aliases: vec!["/ingest".to_string()],
        },
        FlowEntrypoint {
            trigger_alias: "trigger_http_events".to_string(),
            capture_alias: "events_response".to_string(),
            route_path: Some("/events".to_string()),
            method: Some("GET".to_string()),
            deadline: Some(Duration::from_millis(2000)),
            route_aliases: vec!["/events".to_string()],
        },
        FlowEntrypoint {
            trigger_alias: "trigger_http_snapshot".to_string(),
            capture_alias: "snapshot_get".to_string(),
            route_path: Some("/snapshot".to_string()),
            method: Some("GET".to_string()),
            deadline: Some(Duration::from_millis(2000)),
            route_aliases: vec!["/snapshot".to_string()],
        },
        FlowEntrypoint {
            trigger_alias: "trigger_http_stream".to_string(),
            capture_alias: "stream_events".to_string(),
            route_path: Some("/stream".to_string()),
            method: Some("GET".to_string()),
            deadline: Some(Duration::from_millis(10000)),
            route_aliases: vec!["/stream".to_string()],
        },
    ];

    let node_contracts = vec![
        node!(trigger_http_ingest),
        node!(trigger_http_events),
        node!(trigger_http_snapshot),
        node!(trigger_http_stream),
        node!(ingest_validate),
        node!(dedupe_sequence),
        node!(kv_put_event),
        node!(snapshot_maybe),
        node!(build_otel_ingest),
        node!(build_otel_replay),
        node!(otel_dispatch),
        node!(ingest_response),
        node!(events_list),
        node!(events_response),
        node!(snapshot_get),
        node!(stream_events),
    ]
    .into_iter()
    .map(|spec| NodeContract {
        identifier: spec.identifier.to_string(),
        contract_hash: None,
        source: NodeSource::Local,
    })
    .collect();

    FlowBundle {
        validated_ir,
        entrypoints,
        resolver,
        node_contracts,
        environment_plugins: Vec::new(),
    }
}
