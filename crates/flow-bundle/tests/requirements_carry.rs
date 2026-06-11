//! Tests for carrying the static FlowRequirements manifest in bundle
//! manifests (packet C1).

use std::collections::BTreeMap;

use dag_core::prelude::{Determinism, Effects, FlowBuilder, NodeSpec, Profile, SchemaSpec, Version};
use dag_core::{EffectHint, FlowRequirements};
use flow_bundle::{
    AbiRef, Capabilities, CodeDescriptor, FlowEntry, Manifest, compute_bundle_id,
};
use jsonschema::{Draft, JSONSchema};
use serde_json::json;

const FLOW_BUNDLE_SCHEMA: &str = include_str!("../../../schemas/flow_bundle.schema.json");
const HTTP_READ_HINTS: &[&str] = &[EffectHint::HttpRead.as_str()];

fn sample_requirements() -> FlowRequirements {
    let mut builder = FlowBuilder::new("bundle_reqs_demo", Version::new(1, 0, 0), Profile::Web);
    let spec = NodeSpec::inline_with_hints(
        "tests::fetch",
        "Fetch",
        SchemaSpec::Opaque,
        SchemaSpec::Opaque,
        Effects::ReadOnly,
        Determinism::BestEffort,
        None,
        &[],
        HTTP_READ_HINTS,
    );
    builder.add_node("fetch", &spec).expect("add node");
    let flow = builder.build();
    FlowRequirements::derive(&flow)
        .expect("derive requirements")
        .with_flow_ir_hash(
            "sha256:1111111111111111111111111111111111111111111111111111111111111111",
        )
}

fn manifest_with_requirements(requirements: Option<FlowRequirements>) -> Manifest {
    let mut manifest = Manifest {
        bundle_version: "0.1".to_string(),
        abi: AbiRef {
            name: "latticeflow.wit".to_string(),
            version: "0.1".to_string(),
        },
        bundle_id: String::new(),
        code: CodeDescriptor {
            target: "wasm32-unknown-unknown".to_string(),
            file: "flow.wasm".to_string(),
            hash: "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                .to_string(),
            size_bytes: 4,
        },
        artifacts: Vec::new(),
        flows: vec![FlowEntry {
            id: "flow://demo".to_string(),
            version: "v0.1.0".to_string(),
            profile: "wasm".to_string(),
            flow_ir: None,
            flow_ir_expanded: None,
            entrypoints: Vec::new(),
            nodes: BTreeMap::new(),
            capabilities: Capabilities::default(),
            subflows: Vec::new(),
            wasm_guest_exports: None,
            requirements,
        }],
        subflows: Vec::new(),
        default_flow: None,
        signing: None,
    };
    manifest.bundle_id = compute_bundle_id(&manifest).expect("bundle id");
    manifest
}

#[test]
fn manifest_round_trips_flow_requirements() {
    let manifest = manifest_with_requirements(Some(sample_requirements()));
    let value = serde_json::to_value(&manifest).expect("manifest json");

    let parsed: Manifest = serde_json::from_value(value).expect("parse manifest");
    let requirements = parsed.flows[0]
        .requirements
        .as_ref()
        .expect("requirements carried");
    assert_eq!(requirements, &sample_requirements());
    assert_eq!(requirements.effects.union, vec![EffectHint::HttpRead]);
}

#[test]
fn manifest_without_requirements_still_parses() {
    // Back-compat: manifests produced before the requirements field existed
    // must deserialize with `requirements: None`.
    let manifest_json = json!({
        "bundle_version": "0.1",
        "abi": { "name": "latticeflow.wit", "version": "0.1" },
        "bundle_id": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "code": {
            "target": "wasm32-unknown-unknown",
            "file": "flow.wasm",
            "hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "size_bytes": 4
        },
        "flows": [
            { "id": "flow://demo", "version": "v0.1.0", "profile": "wasm" }
        ]
    });

    let manifest: Manifest = serde_json::from_value(manifest_json).expect("manifest");
    assert!(manifest.flows[0].requirements.is_none());
}

#[test]
fn absent_requirements_are_omitted_from_serialization() {
    // bundle_id hashing must be unaffected for manifests without the field.
    let manifest = manifest_with_requirements(None);
    let value = serde_json::to_value(&manifest).expect("manifest json");
    assert!(value["flows"][0].get("requirements").is_none());
}

#[test]
fn manifest_deserialize_rejects_null_requirements() {
    let manifest_json = json!({
        "bundle_version": "0.1",
        "abi": { "name": "latticeflow.wit", "version": "0.1" },
        "bundle_id": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "code": {
            "target": "wasm32-unknown-unknown",
            "file": "flow.wasm",
            "hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "size_bytes": 4
        },
        "flows": [
            {
                "id": "flow://demo",
                "version": "v0.1.0",
                "profile": "wasm",
                "requirements": null
            }
        ]
    });

    let manifest = serde_json::from_value::<Manifest>(manifest_json);
    assert!(
        manifest.is_err(),
        "manifest deserialization accepted null flows[].requirements"
    );
}

#[test]
fn bundle_schema_accepts_manifest_with_requirements() {
    let manifest = manifest_with_requirements(Some(sample_requirements()));
    let instance = serde_json::to_value(&manifest).expect("manifest json");

    let schema: serde_json::Value = serde_json::from_str(FLOW_BUNDLE_SCHEMA).expect("schema");
    let compiled = JSONSchema::options()
        .with_draft(Draft::Draft202012)
        .compile(&schema)
        .expect("compile schema");
    if let Err(errors) = compiled.validate(&instance) {
        let messages: Vec<String> = errors.map(|error| error.to_string()).collect();
        panic!("schema validation failed: {}", messages.join("; "));
    }
}

#[test]
fn requirements_payload_validates_against_flow_requirements_schema() {
    let requirements = sample_requirements();
    let instance = serde_json::to_value(&requirements).expect("requirements json");

    let schema_json = dag_core::schema::schema_json_for_file("flow_requirements.schema.json")
        .expect("flow_requirements schema emitted");
    let compiled = JSONSchema::options()
        .with_draft(Draft::Draft202012)
        .compile(&schema_json)
        .expect("compile schema");
    if let Err(errors) = compiled.validate(&instance) {
        let messages: Vec<String> = errors.map(|error| error.to_string()).collect();
        panic!(
            "requirements schema validation failed: {}",
            messages.join("; ")
        );
    }
}
