use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use capabilities::ResourceAccess;
use capabilities::connector::{
    ConnectorBindingScope, OP_CONNECTOR_APPLY_OUTBOUND_AUTH, OP_CONNECTOR_GET_SCOPE,
    OP_CONNECTOR_RESOLVE_ENDPOINT_PROFILE,
};
use capabilities::http::{
    HttpError, HttpRequest, OP_HTTP_READ_SEND, OP_HTTP_WRITE_SEND, RemoteHttpErrorEnvelope,
};
use dag_core::{FlowIR, NodeError, NodeResult};
use flow_bundle::{
    ExecPolicy, FlowEntry, FlowIrRef, Manifest, expand_subflow_ir, select_artifact,
    sha256_prefixed, validate_manifest,
};
use host_inproc::{FlowBundle, FlowEntrypoint, NodeContract, NodeSource};
use kernel_exec::{NodeHandler, NodeOutput, NodeResolver};
use kernel_plan::validate;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use wasmtime::{Caller, Engine, Linker, Memory, Module, Store, TypedFunc};

use capabilities::blob::{BlobError, OP_BLOB_DELETE, OP_BLOB_GET, OP_BLOB_PUT};
use capabilities::kv::{
    OP_KV_DELETE, OP_KV_GET, OP_KV_LIST, OP_KV_PUT,
    KvDeleteRequest, KvGetRequest, KvListRequest, KvListResponseTransport,
    KvListEntryTransport, KvPutOptions, KvPutRequest,
};
use capabilities::workspace::{
    OP_WORKSPACE_DELETE, OP_WORKSPACE_LIST, OP_WORKSPACE_READ, OP_WORKSPACE_WRITE,
    WorkspaceDeleteRequest, WorkspaceErrorEnvelope, WorkspaceListRequest,
    WorkspaceReadRequest, WorkspaceWriteRequest,
};
use capabilities::durability::{
    CancelScheduleRequest, CheckpointHandle, CreateTokenRequest, ResolveTokenRequest,
    RevokeTokenRequest, ScheduleAfterRequest, ScheduleAtRequest, ScheduleStatusRequest,
    ScheduleStatusTransport, TokenConfig, OP_DURABILITY_GET_CHECKPOINT_HANDLE,
    OP_RESUME_CANCEL, OP_RESUME_SCHEDULE_AFTER, OP_RESUME_SCHEDULE_AT, OP_RESUME_STATUS,
    OP_TOKEN_CREATE, OP_TOKEN_RESOLVE, OP_TOKEN_REVOKE,
};

/// Drive an async future to completion from inside a synchronous wasmtime import handler.
///
/// Wasmtime import functions are synchronous, but host capability providers
/// (reqwest HTTP, connector runtime token exchange, etc.) are async and often
/// depend on Tokio I/O. We cannot call `block_on` when there is already an
/// ambient Tokio runtime on the current thread (Tokio panics).
///
/// Solution: use `std::thread::scope` to run the future on a short-lived OS
/// thread that has no ambient Tokio context. The scoped thread creates a fresh
/// single-thread Tokio runtime, drives the future, and returns the result.
/// `std::thread::scope` blocks the caller until the thread finishes, so borrows
/// from the caller's stack are safe.
fn host_block_on<F: std::future::Future + Send>(fut: F) -> F::Output
where
    F::Output: Send,
{
    std::thread::scope(|s| {
        s.spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("host_block_on: failed to create import runtime");
            rt.block_on(fut)
        })
        .join()
        .expect("host_block_on: import thread panicked")
    })
}

const ERRNO_ENOBUFS: i32 = -12;
const ERRNO_EFAULT: i32 = -14;
const ERRNO_EUNSUPPORTED: i32 = -95;

const RESP_OK: u8 = 0;
const RESP_NOT_FOUND: u8 = 1;
const RESP_ERR: u8 = 2;

const INVOKE_OK: u8 = 0;
const INVOKE_ERR: u8 = 2;

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum TransportOutboundAuthKind {
    Bearer {
        handle_kind: String,
    },
    ApiKeyHeader {
        header_name: String,
        prefix: Option<String>,
        handle_kind: String,
    },
    ApiKeyQuery {
        query_name: String,
        handle_kind: String,
    },
    Unsupported {
        kind_name: String,
        handle_kind: String,
    },
}

#[derive(Debug, Deserialize)]
struct TransportOutboundAuthProfileDescriptor {
    connector_id: String,
    name: String,
    kind: TransportOutboundAuthKind,
}

#[derive(Debug, Deserialize)]
struct TransportEndpointProfileDescriptor {
    connector_id: String,
    name: String,
    base_url: String,
    default_headers: Vec<(String, String)>,
}

#[derive(Debug, Deserialize)]
struct ApplyOutboundAuthRequest {
    scope: ConnectorBindingScope,
    profile: TransportOutboundAuthProfileDescriptor,
    request: HttpRequest,
}

#[derive(Debug, Deserialize)]
struct ResolveEndpointProfileRequest {
    scope: ConnectorBindingScope,
    profile: TransportEndpointProfileDescriptor,
}

struct HostState {
    resources: Arc<dyn ResourceAccess>,
    /// Checkpoint handle for the current invocation, set by the host before
    /// invoking a halt-capable node. The guest reads it via
    /// `OP_DURABILITY_GET_CHECKPOINT_HANDLE`.
    checkpoint_handle: Option<CheckpointHandle>,
}

pub struct WasmRuntime {
    engine: Engine,
    module: Module,
}

impl WasmRuntime {
    pub fn new(wasm_bytes: &[u8]) -> Result<Self> {
        let engine = Engine::default();
        let module =
            Module::from_binary(&engine, wasm_bytes).context("failed to load guest wasm module")?;
        Ok(Self { engine, module })
    }

    pub fn invoke_value(
        &self,
        identifier: &str,
        input: &JsonValue,
        resources: Arc<dyn ResourceAccess>,
    ) -> NodeResult<JsonValue> {
        let input_bytes = serde_json::to_vec(input)
            .map_err(|err| NodeError::new(format!("failed to serialize node input: {err}")))?;
        let payload = self
            .invoke_raw(identifier.as_bytes(), &input_bytes, resources)
            .map_err(NodeError::from)?;
        if payload.is_empty() {
            return Err(NodeError::new("empty invoke response"));
        }
        match payload[0] {
            INVOKE_OK => {
                let json: JsonValue = serde_json::from_slice(&payload[1..]).map_err(|err| {
                    NodeError::new(format!("failed to decode node output: {err}"))
                })?;
                Ok(json)
            }
            INVOKE_ERR => {
                let message = std::str::from_utf8(&payload[1..]).unwrap_or("(non-utf8 error)");
                Err(NodeError::new(message))
            }
            other => Err(NodeError::new(format!(
                "unknown invoke status byte {other}"
            ))),
        }
    }

    fn invoke_raw(
        &self,
        id_bytes: &[u8],
        input_bytes: &[u8],
        resources: Arc<dyn ResourceAccess>,
    ) -> Result<Vec<u8>> {
        let state = HostState {
            resources,
            checkpoint_handle: None,
        };
        let mut store = Store::new(&self.engine, state);
        let mut linker = Linker::new(&self.engine);

        linker.func_wrap(
            "lattice",
            "lf_cap_call",
            |mut caller: Caller<'_, HostState>,
             op: u32,
             in_ptr: u32,
             in_len: u32,
             out_ptr: u32,
             out_cap: u32|
             -> i32 {
                let memory = match caller.get_export("memory") {
                    Some(wasmtime::Extern::Memory(mem)) => mem,
                    _ => return ERRNO_EUNSUPPORTED,
                };

                let in_ptr = in_ptr as usize;
                let in_len = in_len as usize;
                let data = memory.data(&caller);
                if in_ptr.saturating_add(in_len) > data.len() {
                    return ERRNO_EFAULT;
                }
                let req = &data[in_ptr..in_ptr + in_len];

                let response = match op {
                    OP_BLOB_GET => handle_blob_get(caller.data().resources.as_ref(), req),
                    OP_BLOB_PUT => handle_blob_put(caller.data().resources.as_ref(), req),
                    OP_BLOB_DELETE => handle_blob_delete(caller.data().resources.as_ref(), req),
                    OP_HTTP_READ_SEND => {
                        handle_http_send(caller.data().resources.as_ref(), req, true)
                    }
                    OP_HTTP_WRITE_SEND => {
                        handle_http_send(caller.data().resources.as_ref(), req, false)
                    }
                    OP_CONNECTOR_GET_SCOPE => {
                        handle_connector_get_scope(caller.data().resources.as_ref())
                    }
                    OP_CONNECTOR_APPLY_OUTBOUND_AUTH => {
                        handle_connector_apply_outbound_auth(caller.data().resources.as_ref(), req)
                    }
                    OP_CONNECTOR_RESOLVE_ENDPOINT_PROFILE => {
                        handle_connector_resolve_endpoint_profile(
                            caller.data().resources.as_ref(),
                            req,
                        )
                    }
                    OP_KV_GET => handle_kv_get(caller.data().resources.as_ref(), req),
                    OP_KV_PUT => handle_kv_put(caller.data().resources.as_ref(), req),
                    OP_KV_DELETE => handle_kv_delete(caller.data().resources.as_ref(), req),
                    OP_KV_LIST => handle_kv_list(caller.data().resources.as_ref(), req),
                    OP_WORKSPACE_READ => {
                        handle_workspace_read(caller.data().resources.as_ref(), req)
                    }
                    OP_WORKSPACE_WRITE => {
                        handle_workspace_write(caller.data().resources.as_ref(), req)
                    }
                    OP_WORKSPACE_LIST => {
                        handle_workspace_list(caller.data().resources.as_ref(), req)
                    }
                    OP_WORKSPACE_DELETE => {
                        handle_workspace_delete(caller.data().resources.as_ref(), req)
                    }
                    OP_RESUME_SCHEDULE_AT => {
                        handle_resume_schedule_at(caller.data().resources.as_ref(), req)
                    }
                    OP_RESUME_SCHEDULE_AFTER => {
                        handle_resume_schedule_after(caller.data().resources.as_ref(), req)
                    }
                    OP_RESUME_CANCEL => {
                        handle_resume_cancel(caller.data().resources.as_ref(), req)
                    }
                    OP_RESUME_STATUS => {
                        handle_resume_status(caller.data().resources.as_ref(), req)
                    }
                    OP_TOKEN_CREATE => {
                        handle_token_create(caller.data().resources.as_ref(), req)
                    }
                    OP_TOKEN_RESOLVE => {
                        handle_token_resolve(caller.data().resources.as_ref(), req)
                    }
                    OP_TOKEN_REVOKE => {
                        handle_token_revoke(caller.data().resources.as_ref(), req)
                    }
                    OP_DURABILITY_GET_CHECKPOINT_HANDLE => {
                        handle_get_checkpoint_handle(caller.data())
                    }
                    _ => encode_err("unsupported opcode"),
                };

                write_response(&mut caller, &memory, out_ptr, out_cap, &response)
            },
        )?;

        let instance = linker.instantiate(&mut store, &self.module)?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .context("guest wasm does not export memory")?;

        let alloc: TypedFunc<u32, u32> = instance
            .get_typed_func(&mut store, "lf_guest_alloc")
            .context("guest wasm missing export lf_guest_alloc")?;
        let free: TypedFunc<(u32, u32), ()> = instance
            .get_typed_func(&mut store, "lf_guest_free")
            .context("guest wasm missing export lf_guest_free")?;
        let invoke: TypedFunc<(u32, u32, u32, u32), u64> = instance
            .get_typed_func(&mut store, "lf_invoke_node")
            .context("guest wasm missing export lf_invoke_node")?;

        let id_ptr = alloc.call(&mut store, id_bytes.len() as u32)?;
        memory.write(&mut store, id_ptr as usize, id_bytes)?;
        let input_ptr = alloc.call(&mut store, input_bytes.len() as u32)?;
        memory.write(&mut store, input_ptr as usize, input_bytes)?;

        let packed = invoke.call(
            &mut store,
            (
                id_ptr,
                id_bytes.len() as u32,
                input_ptr,
                input_bytes.len() as u32,
            ),
        )?;
        let (out_ptr, out_len) = parse_packed_ptr(packed);
        let mut out = vec![0u8; out_len as usize];
        memory.read(&mut store, out_ptr as usize, &mut out)?;
        free.call(&mut store, (out_ptr, out_len))?;
        free.call(&mut store, (id_ptr, id_bytes.len() as u32))?;
        free.call(&mut store, (input_ptr, input_bytes.len() as u32))?;

        Ok(out)
    }
}

pub struct WasmResolver {
    runtime: Arc<WasmRuntime>,
    allowlist: Arc<BTreeSet<String>>,
}

impl WasmResolver {
    pub fn new(runtime: Arc<WasmRuntime>, allowlist: BTreeSet<String>) -> Self {
        Self {
            runtime,
            allowlist: Arc::new(allowlist),
        }
    }
}

impl NodeResolver for WasmResolver {
    fn resolve(&self, identifier: &str) -> Option<Arc<dyn NodeHandler>> {
        if self.allowlist.contains(identifier) {
            Some(Arc::new(WasmNodeHandler {
                runtime: Arc::clone(&self.runtime),
                identifier: identifier.to_string(),
            }))
        } else {
            None
        }
    }
}

struct WasmNodeHandler {
    runtime: Arc<WasmRuntime>,
    identifier: String,
}

#[async_trait]
impl NodeHandler for WasmNodeHandler {
    async fn invoke(
        &self,
        input: JsonValue,
        _ctx: &kernel_exec::NodeContext,
    ) -> NodeResult<NodeOutput> {
        let json = self
            .runtime
            .invoke_value(&self.identifier, &input, _ctx.resource_handle())?;
        Ok(NodeOutput::Value(json))
    }
}

pub fn load_flow_bundle(
    bundle_dir: &Path,
    policy: ExecPolicy,
    flow_id: Option<&str>,
    _resources: Arc<dyn ResourceAccess>,
) -> Result<FlowBundle> {
    let manifest = read_manifest(bundle_dir)?;
    let flow = select_flow(&manifest, flow_id)?;

    let wasm_artifact = select_artifact(&manifest, policy, "native")?;
    let wasm_path = bundle_dir.join(&wasm_artifact.file);
    let wasm_bytes = read_and_verify(&wasm_path, &wasm_artifact.hash)?;
    let runtime = Arc::new(WasmRuntime::new(&wasm_bytes)?);

    let flow_ir = load_flow_ir(bundle_dir, &manifest, flow)?;
    let validated =
        validate(&flow_ir).map_err(|diags| anyhow!("flow IR validation failed: {diags:?}"))?;

    let entrypoints = flow
        .entrypoints
        .iter()
        .map(|entry| FlowEntrypoint {
            trigger_alias: entry.trigger.clone(),
            capture_alias: entry.capture.clone(),
            route_path: entry.route_aliases.first().cloned(),
            method: entry.method.clone(),
            deadline: entry.deadline_ms.map(Duration::from_millis),
            route_aliases: entry.route_aliases.clone(),
        })
        .collect::<Vec<_>>();

    let mut allowlist = BTreeSet::new();
    let node_contracts = flow_ir
        .nodes
        .iter()
        .map(|node| {
            allowlist.insert(node.identifier.clone());
            NodeContract {
                identifier: node.identifier.clone(),
                contract_hash: None,
                source: NodeSource::Remote,
            }
        })
        .collect::<Vec<_>>();

    let resolver = Arc::new(WasmResolver::new(runtime, allowlist));

    Ok(FlowBundle {
        validated_ir: validated,
        entrypoints,
        resolver,
        node_contracts,
        environment_plugins: Vec::new(),
    })
}

fn read_manifest(bundle_dir: &Path) -> Result<Manifest> {
    let manifest_path = bundle_dir.join("manifest.json");
    let data = fs::read(&manifest_path)
        .with_context(|| format!("failed to read {}", manifest_path.display()))?;
    let manifest: Manifest =
        serde_json::from_slice(&data).context("manifest.json is not valid bundle manifest JSON")?;
    validate_manifest(&manifest)?;
    Ok(manifest)
}

fn select_flow<'a>(manifest: &'a Manifest, flow_id: Option<&str>) -> Result<&'a FlowEntry> {
    if let Some(id) = flow_id {
        return manifest
            .flows
            .iter()
            .find(|flow| flow.id == id)
            .with_context(|| format!("bundle missing flow id {id}"));
    }
    if let Some(default_flow) = manifest.default_flow.as_ref() {
        if let Some(flow) = manifest.flows.iter().find(|flow| flow.id == *default_flow) {
            return Ok(flow);
        }
    }
    manifest.flows.first().context("bundle has no flow entries")
}

fn load_flow_ir(bundle_dir: &Path, manifest: &Manifest, flow: &FlowEntry) -> Result<FlowIR> {
    let flow_ir_ref = flow
        .flow_ir_expanded
        .as_ref()
        .or(flow.flow_ir.as_ref())
        .context("flow entry missing flow_ir reference")?;
    let flow_ir = read_flow_ir(bundle_dir, flow_ir_ref)?;

    if flow.flow_ir_expanded.is_some() || manifest.subflows.is_empty() {
        return Ok(flow_ir);
    }

    let subflows = load_subflows(bundle_dir, manifest)?;
    expand_subflow_ir(&flow_ir, &subflows).map_err(|err| anyhow!(err))
}

fn load_subflows(bundle_dir: &Path, manifest: &Manifest) -> Result<BTreeMap<String, FlowIR>> {
    let mut out = BTreeMap::new();
    for entry in &manifest.subflows {
        let Some(flow_ir_ref) = entry.flow_ir.as_ref() else {
            continue;
        };
        let flow_ir = read_flow_ir(bundle_dir, flow_ir_ref)?;
        out.insert(entry.id.clone(), flow_ir);
    }
    Ok(out)
}

fn read_flow_ir(bundle_dir: &Path, flow_ir_ref: &FlowIrRef) -> Result<FlowIR> {
    let path = bundle_dir.join(&flow_ir_ref.artifact);
    let bytes = read_and_verify(&path, &flow_ir_ref.hash)?;
    serde_json::from_slice(&bytes).context("flow_ir artifact is not valid JSON")
}

fn read_and_verify(path: &Path, expected_hash: &str) -> Result<Vec<u8>> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let actual = sha256_prefixed(&bytes);
    if actual != expected_hash {
        return Err(anyhow!(
            "hash mismatch for {} (expected {expected_hash}, got {actual})",
            path.display()
        ));
    }
    Ok(bytes)
}

fn decode_key_prefix(req: &[u8]) -> Result<(String, &[u8])> {
    if req.len() < 4 {
        return Err(anyhow!("invalid request: missing key length"));
    }
    let len = u32::from_le_bytes([req[0], req[1], req[2], req[3]]) as usize;
    if req.len() < 4 + len {
        return Err(anyhow!("invalid request: truncated key"));
    }
    let key_bytes = &req[4..4 + len];
    let key = std::str::from_utf8(key_bytes)
        .context("invalid request: key is not utf-8")?
        .to_string();
    Ok((key, &req[4 + len..]))
}

fn decode_json_request<T>(req: &[u8], label: &str) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_slice(req).with_context(|| format!("invalid {label} request payload"))
}

fn leak_string(value: String) -> &'static str {
    Box::leak(value.into_boxed_str())
}

fn transport_auth_profile_to_descriptor(
    profile: TransportOutboundAuthProfileDescriptor,
) -> capabilities::connector::OutboundAuthProfileDescriptor {
    capabilities::connector::OutboundAuthProfileDescriptor {
        connector_id: leak_string(profile.connector_id),
        name: leak_string(profile.name),
        env_var: "",
        kind: match profile.kind {
            TransportOutboundAuthKind::Bearer { handle_kind } => {
                capabilities::connector::OutboundAuthKind::Bearer {
                    handle_kind: leak_string(handle_kind),
                }
            }
            TransportOutboundAuthKind::ApiKeyHeader {
                header_name,
                prefix,
                handle_kind,
            } => capabilities::connector::OutboundAuthKind::ApiKeyHeader {
                header_name: leak_string(header_name),
                prefix: prefix.map(leak_string),
                handle_kind: leak_string(handle_kind),
            },
            TransportOutboundAuthKind::ApiKeyQuery {
                query_name,
                handle_kind,
            } => capabilities::connector::OutboundAuthKind::ApiKeyQuery {
                query_name: leak_string(query_name),
                handle_kind: leak_string(handle_kind),
            },
            TransportOutboundAuthKind::Unsupported {
                kind_name,
                handle_kind,
            } => capabilities::connector::OutboundAuthKind::Unsupported {
                kind_name: leak_string(kind_name),
                handle_kind: leak_string(handle_kind),
            },
        },
    }
}

fn transport_endpoint_profile_to_descriptor(
    profile: TransportEndpointProfileDescriptor,
) -> capabilities::connector::EndpointProfileDescriptor {
    let headers = profile
        .default_headers
        .into_iter()
        .map(|(name, value)| (leak_string(name), leak_string(value)))
        .collect::<Vec<_>>();
    let headers: &'static [(&'static str, &'static str)] = Box::leak(headers.into_boxed_slice());
    capabilities::connector::EndpointProfileDescriptor {
        connector_id: leak_string(profile.connector_id),
        name: leak_string(profile.name),
        env_base_url_var: "",
        base_url: leak_string(profile.base_url),
        default_headers: headers,
    }
}

fn encode_json_ok<T>(value: &T) -> Vec<u8>
where
    T: serde::Serialize,
{
    match serde_json::to_vec(value) {
        Ok(payload) => encode_ok(&payload),
        Err(err) => encode_err(format!("failed to serialize host response: {err}")),
    }
}

fn encode_http_err(err: HttpError) -> Vec<u8> {
    let envelope = RemoteHttpErrorEnvelope::from_http_error(err);
    match serde_json::to_vec(&envelope) {
        Ok(payload) => {
            let mut out = Vec::with_capacity(1 + payload.len());
            out.push(RESP_ERR);
            out.extend_from_slice(&payload);
            out
        }
        Err(err) => encode_err(format!("failed to serialize http error: {err}")),
    }
}

fn handle_blob_get(resources: &dyn ResourceAccess, req: &[u8]) -> Vec<u8> {
    let (key, _rest) = match decode_key_prefix(req) {
        Ok(value) => value,
        Err(err) => return encode_err(err),
    };
    let blob = match resources.blob() {
        Some(blob) => blob,
        None => return encode_err("missing blob provider"),
    };
    let result = host_block_on(blob.get(&key));
    match result {
        Ok(Some(bytes)) => encode_ok(&bytes),
        Ok(None) => encode_not_found(),
        Err(err) => encode_err(err),
    }
}

fn handle_blob_put(resources: &dyn ResourceAccess, req: &[u8]) -> Vec<u8> {
    let (key, rest) = match decode_key_prefix(req) {
        Ok(value) => value,
        Err(err) => return encode_err(err),
    };
    let blob = match resources.blob() {
        Some(blob) => blob,
        None => return encode_err("missing blob provider"),
    };
    let result = host_block_on(blob.put(&key, rest));
    match result {
        Ok(()) => encode_ok(&[]),
        Err(err) => encode_err(err),
    }
}

fn handle_blob_delete(resources: &dyn ResourceAccess, req: &[u8]) -> Vec<u8> {
    let (key, _rest) = match decode_key_prefix(req) {
        Ok(value) => value,
        Err(err) => return encode_err(err),
    };
    let blob = match resources.blob() {
        Some(blob) => blob,
        None => return encode_err("missing blob provider"),
    };
    let result = host_block_on(blob.delete(&key));
    match result {
        Ok(()) => encode_ok(&[]),
        Err(BlobError::NotFound) => encode_not_found(),
        Err(err) => encode_err(err),
    }
}

fn handle_http_send(resources: &dyn ResourceAccess, req: &[u8], read_only: bool) -> Vec<u8> {
    let request: HttpRequest = match decode_json_request(req, "http") {
        Ok(request) => request,
        Err(err) => return encode_http_err(HttpError::InvalidResponse(err.to_string())),
    };

    let result = if read_only {
        let client = match resources.http_read() {
            Some(client) => client,
            None => {
                return encode_http_err(HttpError::Transport(anyhow!(
                    "missing http_read provider"
                )));
            }
        };
        host_block_on(client.send(request))
    } else {
        let client = match resources.http_write() {
            Some(client) => client,
            None => {
                return encode_http_err(HttpError::Transport(anyhow!(
                    "missing http_write provider"
                )));
            }
        };
        host_block_on(client.send(request))
    };

    match result {
        Ok(response) => encode_json_ok(&response),
        Err(err) => encode_http_err(err),
    }
}

fn handle_connector_get_scope(resources: &dyn ResourceAccess) -> Vec<u8> {
    match resources.connector_scope() {
        Some(scope) => encode_json_ok(&scope),
        None => encode_not_found(),
    }
}

fn handle_connector_apply_outbound_auth(resources: &dyn ResourceAccess, req: &[u8]) -> Vec<u8> {
    let request: ApplyOutboundAuthRequest = match decode_json_request(req, "connector auth") {
        Ok(request) => request,
        Err(err) => return encode_err(err),
    };

    let runtime = match resources.connector_runtime() {
        Some(runtime) => runtime,
        None => return encode_err("missing connector runtime"),
    };

    let mut http_request = request.request;
    let profile = transport_auth_profile_to_descriptor(request.profile);
    let result =
        host_block_on(runtime.apply_outbound_auth(&request.scope, &profile, &mut http_request));
    match result {
        Ok(()) => encode_json_ok(&http_request),
        Err(err) => encode_err(err),
    }
}

fn handle_connector_resolve_endpoint_profile(
    resources: &dyn ResourceAccess,
    req: &[u8],
) -> Vec<u8> {
    let request: ResolveEndpointProfileRequest =
        match decode_json_request(req, "connector endpoint") {
            Ok(request) => request,
            Err(err) => return encode_err(err),
        };

    let runtime = match resources.connector_runtime() {
        Some(runtime) => runtime,
        None => return encode_err("missing connector runtime"),
    };

    let profile = transport_endpoint_profile_to_descriptor(request.profile);
    let result = host_block_on(runtime.resolve_endpoint_profile(&request.scope, &profile));
    match result {
        Ok(profile) => encode_json_ok(&profile),
        Err(err) => encode_err(err),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// KV host handlers
// ─────────────────────────────────────────────────────────────────────────

fn handle_kv_get(resources: &dyn ResourceAccess, req: &[u8]) -> Vec<u8> {
    let request: KvGetRequest = match decode_json_request(req, "kv get") {
        Ok(request) => request,
        Err(err) => return encode_err(err),
    };
    let kv = match resources.kv() {
        Some(kv) => kv,
        None => return encode_err("missing kv provider"),
    };
    let options = capabilities::kv::KvGetOptions {
        cache_ttl: request
            .cache_ttl_ms
            .map(std::time::Duration::from_millis),
    };
    let result = host_block_on(kv.get_with_options(&request.key, options));
    match result {
        Ok(Some(bytes)) => encode_ok(&bytes),
        Ok(None) => encode_not_found(),
        Err(err) => encode_err(err),
    }
}

fn handle_kv_put(resources: &dyn ResourceAccess, req: &[u8]) -> Vec<u8> {
    let request: KvPutRequest = match decode_json_request(req, "kv put") {
        Ok(request) => request,
        Err(err) => return encode_err(err),
    };
    let kv = match resources.kv() {
        Some(kv) => kv,
        None => return encode_err("missing kv provider"),
    };
    let options = KvPutOptions {
        ttl: request.ttl_ms.map(std::time::Duration::from_millis),
        expires_at: None,
        metadata: request.metadata,
    };
    let result = host_block_on(kv.put_with_options(&request.key, &request.value, options));
    match result {
        Ok(()) => encode_ok(&[]),
        Err(err) => encode_err(err),
    }
}

fn handle_kv_delete(resources: &dyn ResourceAccess, req: &[u8]) -> Vec<u8> {
    let request: KvDeleteRequest = match decode_json_request(req, "kv delete") {
        Ok(request) => request,
        Err(err) => return encode_err(err),
    };
    let kv = match resources.kv() {
        Some(kv) => kv,
        None => return encode_err("missing kv provider"),
    };
    let result = host_block_on(kv.delete(&request.key));
    match result {
        Ok(()) => encode_ok(&[]),
        Err(capabilities::kv::KvError::NotFound) => encode_not_found(),
        Err(err) => encode_err(err),
    }
}

fn handle_kv_list(resources: &dyn ResourceAccess, req: &[u8]) -> Vec<u8> {
    let request: KvListRequest = match decode_json_request(req, "kv list") {
        Ok(request) => request,
        Err(err) => return encode_err(err),
    };
    let kv = match resources.kv() {
        Some(kv) => kv,
        None => return encode_err("missing kv provider"),
    };
    let options = capabilities::kv::KvListOptions {
        prefix: request.prefix,
        cursor: request.cursor,
        limit: request.limit,
        include_metadata: request.include_metadata,
        include_expiration: request.include_expiration,
    };
    let result = host_block_on(kv.list(options));
    match result {
        Ok(response) => {
            let transport = KvListResponseTransport {
                keys: response
                    .keys
                    .into_iter()
                    .map(|entry| KvListEntryTransport {
                        key: entry.key,
                        expires_at_ms: entry.expires_at.and_then(|at| {
                            at.duration_since(std::time::SystemTime::UNIX_EPOCH)
                                .ok()
                                .map(|d| d.as_millis() as u64)
                        }),
                        metadata: entry.metadata,
                    })
                    .collect(),
                list_complete: response.list_complete,
                cursor: response.cursor,
            };
            encode_json_ok(&transport)
        }
        Err(err) => encode_err(err),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Workspace host handlers
// ─────────────────────────────────────────────────────────────────────────

fn handle_workspace_read(resources: &dyn ResourceAccess, req: &[u8]) -> Vec<u8> {
    let request: WorkspaceReadRequest = match decode_json_request(req, "workspace read") {
        Ok(request) => request,
        Err(err) => return encode_err(err),
    };
    let workspace = match resources.workspace() {
        Some(ws) => ws,
        None => return encode_err("missing workspace provider"),
    };
    let result = host_block_on(workspace.read_normalized(&request.path));
    match result {
        Ok(Some(read_result)) => encode_json_ok(&read_result),
        Ok(None) => encode_not_found(),
        Err(err) => encode_workspace_err(err),
    }
}

fn handle_workspace_write(resources: &dyn ResourceAccess, req: &[u8]) -> Vec<u8> {
    let request: WorkspaceWriteRequest = match decode_json_request(req, "workspace write") {
        Ok(request) => request,
        Err(err) => return encode_err(err),
    };
    let workspace = match resources.workspace() {
        Some(ws) => ws,
        None => return encode_err("missing workspace provider"),
    };
    let result = host_block_on(workspace.write_normalized(&request.path, &request.data, request.options));
    match result {
        Ok(write_result) => encode_json_ok(&write_result),
        Err(err) => encode_workspace_err(err),
    }
}

fn handle_workspace_list(resources: &dyn ResourceAccess, req: &[u8]) -> Vec<u8> {
    let request: WorkspaceListRequest = match decode_json_request(req, "workspace list") {
        Ok(request) => request,
        Err(err) => return encode_err(err),
    };
    let workspace = match resources.workspace() {
        Some(ws) => ws,
        None => return encode_err("missing workspace provider"),
    };
    let options = capabilities::workspace::WorkspaceListOptions {
        prefix: request.prefix,
    };
    let result = host_block_on(workspace.list_normalized(options));
    match result {
        Ok(entries) => encode_json_ok(&entries),
        Err(err) => encode_workspace_err(err),
    }
}

fn handle_workspace_delete(resources: &dyn ResourceAccess, req: &[u8]) -> Vec<u8> {
    let request: WorkspaceDeleteRequest = match decode_json_request(req, "workspace delete") {
        Ok(request) => request,
        Err(err) => return encode_err(err),
    };
    let workspace = match resources.workspace() {
        Some(ws) => ws,
        None => return encode_err("missing workspace provider"),
    };
    let result = host_block_on(workspace.delete_normalized(&request.path));
    match result {
        Ok(delete_result) => encode_json_ok(&delete_result),
        Err(err) => encode_workspace_err(err),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Durability host handlers
// ─────────────────────────────────────────────────────────────────────────

fn handle_resume_schedule_at(resources: &dyn ResourceAccess, req: &[u8]) -> Vec<u8> {
    let request: ScheduleAtRequest = match decode_json_request(req, "schedule_at") {
        Ok(r) => r,
        Err(err) => return encode_err(err),
    };
    let scheduler = match resources.resume_scheduler() {
        Some(s) => s,
        None => return encode_err("missing resume_scheduler provider"),
    };
    match host_block_on(scheduler.schedule_at(request.handle, request.at_ms)) {
        Ok(id) => encode_ok(id.0.as_bytes()),
        Err(err) => encode_err(err),
    }
}

fn handle_resume_schedule_after(resources: &dyn ResourceAccess, req: &[u8]) -> Vec<u8> {
    let request: ScheduleAfterRequest = match decode_json_request(req, "schedule_after") {
        Ok(r) => r,
        Err(err) => return encode_err(err),
    };
    let scheduler = match resources.resume_scheduler() {
        Some(s) => s,
        None => return encode_err("missing resume_scheduler provider"),
    };
    let delay = std::time::Duration::from_millis(request.delay_ms);
    match host_block_on(scheduler.schedule_after(request.handle, delay)) {
        Ok(id) => encode_ok(id.0.as_bytes()),
        Err(err) => encode_err(err),
    }
}

fn handle_resume_cancel(resources: &dyn ResourceAccess, req: &[u8]) -> Vec<u8> {
    let request: CancelScheduleRequest = match decode_json_request(req, "cancel_schedule") {
        Ok(r) => r,
        Err(err) => return encode_err(err),
    };
    let scheduler = match resources.resume_scheduler() {
        Some(s) => s,
        None => return encode_err("missing resume_scheduler provider"),
    };
    match host_block_on(scheduler.cancel(capabilities::durability::ScheduleId(
        request.schedule_id,
    ))) {
        Ok(()) => encode_ok(&[]),
        Err(capabilities::durability::ScheduleError::NotFound) => encode_not_found(),
        Err(err) => encode_err(err),
    }
}

fn handle_resume_status(resources: &dyn ResourceAccess, req: &[u8]) -> Vec<u8> {
    let request: ScheduleStatusRequest = match decode_json_request(req, "schedule_status") {
        Ok(r) => r,
        Err(err) => return encode_err(err),
    };
    let scheduler = match resources.resume_scheduler() {
        Some(s) => s,
        None => return encode_err("missing resume_scheduler provider"),
    };
    match host_block_on(scheduler.status(capabilities::durability::ScheduleId(
        request.schedule_id,
    ))) {
        Ok(status) => encode_json_ok(&ScheduleStatusTransport::from(status)),
        Err(capabilities::durability::ScheduleError::NotFound) => encode_not_found(),
        Err(err) => encode_err(err),
    }
}

fn handle_token_create(resources: &dyn ResourceAccess, req: &[u8]) -> Vec<u8> {
    let request: CreateTokenRequest = match decode_json_request(req, "create_token") {
        Ok(r) => r,
        Err(err) => return encode_err(err),
    };
    let source = match resources.resume_signal_source() {
        Some(s) => s,
        None => return encode_err("missing resume_signal_source provider"),
    };
    let config = TokenConfig {
        ttl: request.ttl_ms.map(std::time::Duration::from_millis),
        single_use: request.single_use,
        metadata: request.metadata,
    };
    match host_block_on(source.create_token(&request.handle, config)) {
        Ok(token) => encode_ok(token.0.as_bytes()),
        Err(err) => encode_err(err),
    }
}

fn handle_token_resolve(resources: &dyn ResourceAccess, req: &[u8]) -> Vec<u8> {
    let request: ResolveTokenRequest = match decode_json_request(req, "resolve_token") {
        Ok(r) => r,
        Err(err) => return encode_err(err),
    };
    let source = match resources.resume_signal_source() {
        Some(s) => s,
        None => return encode_err("missing resume_signal_source provider"),
    };
    match host_block_on(source.resolve_token(&capabilities::durability::ResumeToken(
        request.token,
    ))) {
        Ok(handle) => encode_json_ok(&handle),
        Err(capabilities::durability::TokenError::NotFound) => encode_not_found(),
        Err(err) => encode_err(err),
    }
}

fn handle_token_revoke(resources: &dyn ResourceAccess, req: &[u8]) -> Vec<u8> {
    let request: RevokeTokenRequest = match decode_json_request(req, "revoke_token") {
        Ok(r) => r,
        Err(err) => return encode_err(err),
    };
    let source = match resources.resume_signal_source() {
        Some(s) => s,
        None => return encode_err("missing resume_signal_source provider"),
    };
    match host_block_on(source.revoke_token(&capabilities::durability::ResumeToken(
        request.token,
    ))) {
        Ok(()) => encode_ok(&[]),
        Err(capabilities::durability::TokenError::NotFound) => encode_not_found(),
        Err(err) => encode_err(err),
    }
}

fn handle_get_checkpoint_handle(state: &HostState) -> Vec<u8> {
    match &state.checkpoint_handle {
        Some(handle) => encode_json_ok(handle),
        None => encode_not_found(),
    }
}

fn encode_workspace_err(err: capabilities::workspace::WorkspaceError) -> Vec<u8> {
    let envelope = WorkspaceErrorEnvelope::from_workspace_error(&err);
    match serde_json::to_vec(&envelope) {
        Ok(payload) => {
            let mut out = Vec::with_capacity(1 + payload.len());
            out.push(RESP_ERR);
            out.extend_from_slice(&payload);
            out
        }
        Err(ser_err) => encode_err(format!("failed to serialize workspace error: {ser_err}")),
    }
}

fn encode_ok(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + payload.len());
    out.push(RESP_OK);
    out.extend_from_slice(payload);
    out
}

fn encode_not_found() -> Vec<u8> {
    vec![RESP_NOT_FOUND]
}

fn encode_err(message: impl ToString) -> Vec<u8> {
    let msg = message.to_string();
    let bytes = msg.as_bytes();
    let mut out = Vec::with_capacity(1 + bytes.len());
    out.push(RESP_ERR);
    out.extend_from_slice(bytes);
    out
}

fn parse_packed_ptr(value: u64) -> (u32, u32) {
    let ptr = (value & 0xffff_ffff) as u32;
    let len = (value >> 32) as u32;
    (ptr, len)
}

fn write_response(
    caller: &mut Caller<'_, HostState>,
    memory: &Memory,
    out_ptr: u32,
    out_cap: u32,
    response: &[u8],
) -> i32 {
    if response.len() > out_cap as usize {
        return ERRNO_ENOBUFS;
    }
    if memory.write(caller, out_ptr as usize, response).is_err() {
        return ERRNO_EFAULT;
    }
    response.len() as i32
}
