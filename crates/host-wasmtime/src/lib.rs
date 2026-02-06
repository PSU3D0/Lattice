use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use capabilities::ResourceAccess;
use dag_core::{FlowIR, NodeError, NodeResult};
use flow_bundle::{
    expand_subflow_ir, select_artifact, sha256_prefixed, validate_manifest, ExecPolicy, FlowEntry,
    FlowIrRef, Manifest,
};
use host_inproc::{FlowBundle, FlowEntrypoint, NodeContract, NodeSource};
use kernel_exec::{NodeHandler, NodeOutput, NodeResolver};
use kernel_plan::validate;
use serde_json::Value as JsonValue;
use wasmtime::{Caller, Engine, Linker, Memory, Module, Store, TypedFunc};

use capabilities::blob::{BlobError, OP_BLOB_DELETE, OP_BLOB_GET, OP_BLOB_PUT};

const ERRNO_ENOBUFS: i32 = -12;
const ERRNO_EFAULT: i32 = -14;
const ERRNO_EUNSUPPORTED: i32 = -95;

const RESP_OK: u8 = 0;
const RESP_NOT_FOUND: u8 = 1;
const RESP_ERR: u8 = 2;

const INVOKE_OK: u8 = 0;
const INVOKE_ERR: u8 = 2;

struct HostState {
    resources: Arc<dyn ResourceAccess>,
}

pub struct WasmRuntime {
    engine: Engine,
    module: Module,
    resources: Arc<dyn ResourceAccess>,
}

impl WasmRuntime {
    pub fn new(wasm_bytes: &[u8], resources: Arc<dyn ResourceAccess>) -> Result<Self> {
        let engine = Engine::default();
        let module = Module::from_binary(&engine, wasm_bytes)
            .context("failed to load guest wasm module")?;
        Ok(Self {
            engine,
            module,
            resources,
        })
    }

    pub fn invoke_value(&self, identifier: &str, input: &JsonValue) -> NodeResult<JsonValue> {
        let input_bytes = serde_json::to_vec(input)
            .map_err(|err| NodeError::new(format!("failed to serialize node input: {err}")))?;
        let payload = self
            .invoke_raw(identifier.as_bytes(), &input_bytes)
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

    fn invoke_raw(&self, id_bytes: &[u8], input_bytes: &[u8]) -> Result<Vec<u8>> {
        let state = HostState {
            resources: Arc::clone(&self.resources),
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
                    OP_BLOB_GET => {
                        let (key, _rest) = match decode_key_prefix(req) {
                            Ok(value) => value,
                            Err(err) => {
                                return write_response(
                                    &mut caller,
                                    &memory,
                                    out_ptr,
                                    out_cap,
                                    &encode_err(err),
                                )
                            }
                        };
                        let blob = match caller.data().resources.blob() {
                            Some(blob) => blob,
                            None => {
                                return write_response(
                                    &mut caller,
                                    &memory,
                                    out_ptr,
                                    out_cap,
                                    &encode_err("missing blob provider"),
                                )
                            }
                        };
                        let result = futures::executor::block_on(blob.get(&key));
                        match result {
                            Ok(Some(bytes)) => encode_ok(&bytes),
                            Ok(None) => encode_not_found(),
                            Err(err) => encode_err(err),
                        }
                    }
                    OP_BLOB_PUT => {
                        let (key, rest) = match decode_key_prefix(req) {
                            Ok(value) => value,
                            Err(err) => {
                                return write_response(
                                    &mut caller,
                                    &memory,
                                    out_ptr,
                                    out_cap,
                                    &encode_err(err),
                                )
                            }
                        };
                        let blob = match caller.data().resources.blob() {
                            Some(blob) => blob,
                            None => {
                                return write_response(
                                    &mut caller,
                                    &memory,
                                    out_ptr,
                                    out_cap,
                                    &encode_err("missing blob provider"),
                                )
                            }
                        };
                        let result = futures::executor::block_on(blob.put(&key, rest));
                        match result {
                            Ok(()) => encode_ok(&[]),
                            Err(err) => encode_err(err),
                        }
                    }
                    OP_BLOB_DELETE => {
                        let (key, _rest) = match decode_key_prefix(req) {
                            Ok(value) => value,
                            Err(err) => {
                                return write_response(
                                    &mut caller,
                                    &memory,
                                    out_ptr,
                                    out_cap,
                                    &encode_err(err),
                                )
                            }
                        };
                        let blob = match caller.data().resources.blob() {
                            Some(blob) => blob,
                            None => {
                                return write_response(
                                    &mut caller,
                                    &memory,
                                    out_ptr,
                                    out_cap,
                                    &encode_err("missing blob provider"),
                                )
                            }
                        };
                        let result = futures::executor::block_on(blob.delete(&key));
                        match result {
                            Ok(()) => encode_ok(&[]),
                            Err(BlobError::NotFound) => encode_not_found(),
                            Err(err) => encode_err(err),
                        }
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
    async fn invoke(&self, input: JsonValue, _ctx: &kernel_exec::NodeContext) -> NodeResult<NodeOutput> {
        let json = self.runtime.invoke_value(&self.identifier, &input)?;
        Ok(NodeOutput::Value(json))
    }
}

pub fn load_flow_bundle(
    bundle_dir: &Path,
    policy: ExecPolicy,
    flow_id: Option<&str>,
    resources: Arc<dyn ResourceAccess>,
) -> Result<FlowBundle> {
    let manifest = read_manifest(bundle_dir)?;
    let flow = select_flow(&manifest, flow_id)?;

    let wasm_artifact = select_artifact(&manifest, policy, "native")?;
    let wasm_path = bundle_dir.join(&wasm_artifact.file);
    let wasm_bytes = read_and_verify(&wasm_path, &wasm_artifact.hash)?;
    let runtime = Arc::new(WasmRuntime::new(&wasm_bytes, resources)?);

    let flow_ir = load_flow_ir(bundle_dir, &manifest, flow)?;
    let validated = validate(&flow_ir)
        .map_err(|diags| anyhow!("flow IR validation failed: {diags:?}"))?;

    let entrypoints = flow
        .entrypoints
        .iter()
        .map(|entry| FlowEntrypoint {
            trigger_alias: entry.trigger.clone(),
            capture_alias: entry.capture.clone(),
            route_path: None,
            method: None,
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
    let manifest: Manifest = serde_json::from_slice(&data)
        .context("manifest.json is not valid bundle manifest JSON")?;
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
    manifest
        .flows
        .first()
        .context("bundle has no flow entries")
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
    if memory
        .write(caller, out_ptr as usize, response)
        .is_err()
    {
        return ERRNO_EFAULT;
    }
    response.len() as i32
}
