use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};
use dag_core::flow_registry::FlowRegistration;
use dag_core::{Determinism, Effects, FlowIR, Profile, SchemaRef};
use flow_bundle::{
    AbiRef, BUNDLE_VERSION, Capabilities, CodeDescriptor, DEFAULT_ABI_NAME, DEFAULT_ABI_VERSION,
    DurabilityProfile, Entrypoint, FlowEntry, FlowIrRef, Manifest, NodeDeterminism, NodeEffects,
    NodeSpec, compute_bundle_id, sha256_prefixed, wasm_guest_exports_for_flow_name,
};

#[derive(Debug, Clone, Default)]
pub struct BundleConfig {
    pub default_flow: Option<String>,
    pub flows: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct ExportBundle {
    pub manifest: Manifest,
    pub ir_files: BTreeMap<PathBuf, Vec<u8>>,
}

pub fn build_manifest_from_registry(config: &BundleConfig) -> Result<ExportBundle> {
    let mut regs: Vec<&FlowRegistration> = dag_core::flow_registry::iter().collect();
    regs.sort_by(|left, right| left.name.cmp(right.name));

    if let Some(flow_names) = config.flows.as_ref() {
        let allowlist: BTreeSet<_> = flow_names.iter().map(|name| name.as_str()).collect();
        regs.retain(|reg| allowlist.contains(reg.name));

        if regs.len() != allowlist.len() {
            let missing: Vec<_> = flow_names
                .iter()
                .filter(|name| !regs.iter().any(|reg| reg.name == name.as_str()))
                .cloned()
                .collect();
            return Err(anyhow!("missing flows in registry: {}", missing.join(", ")));
        }
    }

    if regs.is_empty() {
        return Err(anyhow!("no flows registered"));
    }

    let mut ir_files = BTreeMap::new();
    let mut flows = Vec::new();
    let mut flow_ids_by_name = BTreeMap::new();

    for reg in regs {
        reg.validate().map_err(|err| anyhow!("{err:?}"))?;
        let flow = (reg.flow_ir)();
        let flow_ir_bytes = serde_json::to_vec_pretty(&flow)?;
        let flow_ir_hash = sha256_prefixed(&flow_ir_bytes);
        let artifact_path = PathBuf::from(format!("flows/{}/flow_ir.json", reg.name));

        ir_files.insert(artifact_path.clone(), flow_ir_bytes);

        let requirements = derive_flow_requirements(reg, &flow, &flow_ir_hash)?;

        let flow_entry = FlowEntry {
            id: flow.id.as_str().to_string(),
            version: flow.version.to_string(),
            profile: profile_to_string(flow.profile),
            flow_ir: Some(FlowIrRef {
                artifact: artifact_path.to_string_lossy().to_string(),
                hash: flow_ir_hash,
            }),
            flow_ir_expanded: None,
            entrypoints: reg.entrypoints.iter().map(entrypoint_from_spec).collect(),
            nodes: nodes_from_flow_ir(&flow),
            capabilities: Capabilities::default(),
            subflows: Vec::new(),
            wasm_guest_exports: Some(wasm_guest_exports_for_flow_name(reg.name)),
            requirements: Some(requirements),
        };

        flow_ids_by_name.insert(reg.name.to_string(), flow_entry.id.clone());
        flows.push(flow_entry);
    }

    let default_flow = if let Some(name) = config.default_flow.as_ref() {
        flow_ids_by_name
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow!("default_flow not found in registry: {name}"))?
    } else {
        flows
            .first()
            .map(|flow| flow.id.clone())
            .ok_or_else(|| anyhow!("no flows registered"))?
    };

    let mut manifest = Manifest {
        bundle_version: BUNDLE_VERSION.to_string(),
        abi: AbiRef {
            name: DEFAULT_ABI_NAME.to_string(),
            version: DEFAULT_ABI_VERSION.to_string(),
        },
        bundle_id: String::new(),
        code: CodeDescriptor {
            target: "wasm32-unknown-unknown".to_string(),
            file: "module.wasm".to_string(),
            hash: sha256_prefixed(&[]),
            size_bytes: 0,
        },
        artifacts: Vec::new(),
        flows,
        subflows: Vec::new(),
        default_flow: Some(default_flow),
        signing: None,
    };

    manifest.bundle_id = compute_bundle_id(&manifest)?;

    Ok(ExportBundle { manifest, ir_files })
}

pub fn emit_bundle(out_dir: &Path, export: &ExportBundle) -> Result<()> {
    fs::create_dir_all(out_dir)
        .map_err(|err| anyhow!("failed to create {}: {err}", out_dir.display()))?;

    let manifest_path = out_dir.join("manifest.json");
    let manifest_bytes = serde_json::to_vec_pretty(&export.manifest)?;
    fs::write(&manifest_path, manifest_bytes)
        .map_err(|err| anyhow!("failed to write {}: {err}", manifest_path.display()))?;

    for (relative, bytes) in &export.ir_files {
        let path = out_dir.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| anyhow!("failed to create {}: {err}", parent.display()))?;
        }
        fs::write(&path, bytes)
            .map_err(|err| anyhow!("failed to write {}: {err}", path.display()))?;
    }

    Ok(())
}

/// Derive the static FlowRequirements manifest for a registered flow
/// (packet C1).
///
/// Pure data derivation — nothing is executed. Two enrichments only the
/// bundle assembler can perform are applied here:
/// - `flow_ir_hash` pins the manifest to the exact serialized Flow IR;
/// - entrypoint `deadline_ms` is copied from the flow registry's entrypoint
///   specs (Flow IR metadata does not carry deadlines today).
fn derive_flow_requirements(
    reg: &FlowRegistration,
    flow: &FlowIR,
    flow_ir_hash: &str,
) -> Result<dag_core::FlowRequirements> {
    let mut requirements = dag_core::FlowRequirements::derive(flow)
        .map_err(|err| anyhow!("failed to derive flow requirements for `{}`: {err}", reg.name))?
        .with_flow_ir_hash(flow_ir_hash);

    for entrypoint in &mut requirements.entrypoints {
        if let Some(spec) = reg.entrypoints.iter().find(|spec| {
            spec.trigger == entrypoint.trigger_alias && spec.capture == entrypoint.capture_alias
        }) {
            entrypoint.deadline_ms = spec.deadline_ms;
        }
    }

    Ok(requirements)
}

fn profile_to_string(profile: Profile) -> String {
    serde_json::to_value(profile)
        .ok()
        .and_then(|value| value.as_str().map(|s| s.to_string()))
        .unwrap_or_else(|| "dev".to_string())
}

fn entrypoint_from_spec(spec: &dag_core::flow_registry::EntrypointSpec) -> Entrypoint {
    Entrypoint {
        trigger: spec.trigger.to_string(),
        capture: spec.capture.to_string(),
        route_aliases: spec
            .route_aliases
            .iter()
            .map(|alias| alias.to_string())
            .collect(),
        method: spec.method.map(str::to_string),
        deadline_ms: spec.deadline_ms,
    }
}

fn nodes_from_flow_ir(flow: &FlowIR) -> BTreeMap<String, NodeSpec> {
    let mut nodes = BTreeMap::new();
    for node in &flow.nodes {
        let entry = NodeSpec {
            id: node.id.0.clone(),
            effects: effects_from_ir(node.effects),
            determinism: determinism_from_ir(node.determinism),
            durability: DurabilityProfile {
                checkpointable: node.durability.checkpointable,
                replayable: node.durability.replayable,
                halts: node.durability.halts,
            },
            bindings: Vec::new(),
            input_schema: schema_ref_to_string(&node.in_schema),
            output_schema: schema_ref_to_string(&node.out_schema),
        };
        nodes.insert(node.alias.clone(), entry);
    }
    nodes
}

fn schema_ref_to_string(schema: &SchemaRef) -> String {
    match schema {
        SchemaRef::Opaque => "schema://opaque".to_string(),
        SchemaRef::Named { name } => format!("schema://{name}"),
    }
}

fn effects_from_ir(effects: Effects) -> NodeEffects {
    match effects {
        Effects::Pure => NodeEffects::Pure,
        Effects::ReadOnly => NodeEffects::ReadOnly,
        Effects::Effectful => NodeEffects::Effectful,
    }
}

fn determinism_from_ir(determinism: Determinism) -> NodeDeterminism {
    match determinism {
        Determinism::Strict => NodeDeterminism::Strict,
        Determinism::Stable => NodeDeterminism::Stable,
        Determinism::BestEffort => NodeDeterminism::BestEffort,
        Determinism::Nondeterministic => NodeDeterminism::Nondeterministic,
    }
}
