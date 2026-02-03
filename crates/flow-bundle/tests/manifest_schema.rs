use flow_bundle::Manifest;
use jsonschema::{Draft, JSONSchema};
use serde_json::json;

const FLOW_BUNDLE_SCHEMA: &str = include_str!("../../../schemas/flow_bundle.schema.json");

#[test]
fn manifest_schema_accepts_multitarget_artifacts_and_constraints() {
    let manifest_json = json!({
        "bundle_version": "0.1",
        "abi": {
            "name": "latticeflow.wit",
            "version": "0.1"
        },
        "bundle_id": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "code": {
            "target": "wasm32-unknown-unknown",
            "file": "flow.wasm",
            "hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "size_bytes": 48
        },
        "artifacts": [
            {
                "target": "x86_64-unknown-linux-gnu",
                "file": "flow",
                "hash": "sha256:2222222222222222222222222222222222222222222222222222222222222222"
            }
        ],
        "capabilities": {
            "required": [
                {
                    "name": "runtime",
                    "kind": "scheduler",
                    "constraints": { "resolution_ms": 1000 }
                }
            ],
            "optional": []
        },
        "flow": {
            "id": "flow://demo",
            "version": "v0.1.0",
            "profile": "wasm"
        }
    });

    let schema_json = serde_json::from_str(FLOW_BUNDLE_SCHEMA).expect("schema json");
    let validator = JSONSchema::options()
        .with_draft(Draft::Draft202012)
        .compile(&schema_json)
        .expect("schema validator");
    if let Err(mut errors) = validator.validate(&manifest_json) {
        let first = errors.next().expect("schema error");
        panic!("manifest schema validation failed: {first}");
    }

    let manifest: Manifest = serde_json::from_value(manifest_json).expect("manifest");
    let round_trip = serde_json::to_value(&manifest).expect("manifest json");

    assert_eq!(round_trip["code"]["target"], "wasm32-unknown-unknown");
    assert_eq!(round_trip["code"]["file"], "flow.wasm");
    assert_eq!(
        round_trip["code"]["hash"],
        "sha256:1111111111111111111111111111111111111111111111111111111111111111"
    );
    assert_eq!(round_trip["code"]["size_bytes"], 48);
    assert_eq!(
        round_trip["artifacts"][0]["target"],
        "x86_64-unknown-linux-gnu"
    );
    assert_eq!(round_trip["artifacts"][0]["file"], "flow");
    assert_eq!(
        round_trip["artifacts"][0]["hash"],
        "sha256:2222222222222222222222222222222222222222222222222222222222222222"
    );
}

#[test]
fn manifest_schema_rejects_string_capability_constraints() {
    let manifest_json = json!({
        "bundle_version": "0.1",
        "abi": {
            "name": "latticeflow.wit",
            "version": "0.1"
        },
        "bundle_id": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "code": {
            "target": "wasm32-unknown-unknown",
            "file": "flow.wasm",
            "hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "size_bytes": 48
        },
        "capabilities": {
            "required": [
                {
                    "name": "runtime",
                    "kind": "scheduler",
                    "constraints": "per-second"
                }
            ],
            "optional": []
        },
        "flow": {
            "id": "flow://demo",
            "version": "v0.1.0",
            "profile": "wasm"
        }
    });

    let schema_json = serde_json::from_str(FLOW_BUNDLE_SCHEMA).expect("schema json");
    let validator = JSONSchema::options()
        .with_draft(Draft::Draft202012)
        .compile(&schema_json)
        .expect("schema validator");
    assert!(
        validator.validate(&manifest_json).is_err(),
        "schema accepted string constraints"
    );
}

#[test]
fn manifest_schema_rejects_uppercase_hashes() {
    let manifest_json = json!({
        "bundle_version": "0.1",
        "abi": {
            "name": "latticeflow.wit",
            "version": "0.1"
        },
        "bundle_id": "sha256:0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF",
        "code": {
            "target": "wasm32-unknown-unknown",
            "file": "flow.wasm",
            "hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "size_bytes": 48
        },
        "flow": {
            "id": "flow://demo",
            "version": "v0.1.0",
            "profile": "wasm"
        }
    });

    let schema_json = serde_json::from_str(FLOW_BUNDLE_SCHEMA).expect("schema json");
    let validator = JSONSchema::options()
        .with_draft(Draft::Draft202012)
        .compile(&schema_json)
        .expect("schema validator");
    assert!(
        validator.validate(&manifest_json).is_err(),
        "schema accepted uppercase hashes"
    );
}

#[test]
fn manifest_deserialize_rejects_null_capability_constraints() {
    let manifest_json = json!({
        "bundle_version": "0.1",
        "abi": {
            "name": "latticeflow.wit",
            "version": "0.1"
        },
        "bundle_id": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "code": {
            "target": "wasm32-unknown-unknown",
            "file": "flow.wasm",
            "hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "size_bytes": 48
        },
        "capabilities": {
            "required": [
                {
                    "name": "runtime",
                    "kind": "scheduler",
                    "constraints": null
                }
            ],
            "optional": []
        },
        "flow": {
            "id": "flow://demo",
            "version": "v0.1.0",
            "profile": "wasm"
        }
    });

    let manifest = serde_json::from_value::<Manifest>(manifest_json);
    assert!(
        manifest.is_err(),
        "manifest deserialization accepted null capability constraints"
    );
}

#[test]
fn manifest_deserialize_rejects_null_flow_ir() {
    let manifest_json = json!({
        "bundle_version": "0.1",
        "abi": {
            "name": "latticeflow.wit",
            "version": "0.1"
        },
        "bundle_id": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "code": {
            "target": "wasm32-unknown-unknown",
            "file": "flow.wasm",
            "hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "size_bytes": 48
        },
        "flow": {
            "id": "flow://demo",
            "version": "v0.1.0",
            "profile": "wasm"
        },
        "flow_ir": null
    });

    let manifest = serde_json::from_value::<Manifest>(manifest_json);
    assert!(
        manifest.is_err(),
        "manifest deserialization accepted null flow_ir"
    );
}

#[test]
fn manifest_deserialize_rejects_null_subflows() {
    let manifest_json = json!({
        "bundle_version": "0.1",
        "abi": {
            "name": "latticeflow.wit",
            "version": "0.1"
        },
        "bundle_id": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "code": {
            "target": "wasm32-unknown-unknown",
            "file": "flow.wasm",
            "hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "size_bytes": 48
        },
        "flow": {
            "id": "flow://demo",
            "version": "v0.1.0",
            "profile": "wasm"
        },
        "subflows": null
    });

    let manifest = serde_json::from_value::<Manifest>(manifest_json);
    assert!(
        manifest.is_err(),
        "manifest deserialization accepted null subflows"
    );
}

#[test]
fn manifest_deserialize_rejects_null_signing() {
    let manifest_json = json!({
        "bundle_version": "0.1",
        "abi": {
            "name": "latticeflow.wit",
            "version": "0.1"
        },
        "bundle_id": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "code": {
            "target": "wasm32-unknown-unknown",
            "file": "flow.wasm",
            "hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "size_bytes": 48
        },
        "flow": {
            "id": "flow://demo",
            "version": "v0.1.0",
            "profile": "wasm"
        },
        "signing": null
    });

    let manifest = serde_json::from_value::<Manifest>(manifest_json);
    assert!(
        manifest.is_err(),
        "manifest deserialization accepted null signing"
    );
}

#[test]
fn manifest_deserialize_rejects_null_entrypoint_deadline_ms() {
    let manifest_json = json!({
        "bundle_version": "0.1",
        "abi": {
            "name": "latticeflow.wit",
            "version": "0.1"
        },
        "bundle_id": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "code": {
            "target": "wasm32-unknown-unknown",
            "file": "flow.wasm",
            "hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "size_bytes": 48
        },
        "flow": {
            "id": "flow://demo",
            "version": "v0.1.0",
            "profile": "wasm"
        },
        "entrypoints": [
            {
                "trigger": "ingress",
                "capture": "out",
                "route_aliases": [],
                "deadline_ms": null
            }
        ]
    });

    let manifest = serde_json::from_value::<Manifest>(manifest_json);
    assert!(
        manifest.is_err(),
        "manifest deserialization accepted null entrypoints[].deadline_ms"
    );
}

#[test]
fn manifest_deserialize_rejects_subflows_without_entries() {
    let manifest_json = json!({
        "bundle_version": "0.1",
        "abi": {
            "name": "latticeflow.wit",
            "version": "0.1"
        },
        "bundle_id": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "code": {
            "target": "wasm32-unknown-unknown",
            "file": "flow.wasm",
            "hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "size_bytes": 48
        },
        "flow": {
            "id": "flow://demo",
            "version": "v0.1.0",
            "profile": "wasm"
        },
        "subflows": {
            "mode": "embedded"
        }
    });

    let manifest = serde_json::from_value::<Manifest>(manifest_json);
    assert!(
        manifest.is_err(),
        "manifest deserialization accepted subflows without entries"
    );
}

#[test]
fn manifest_schema_rejects_invalid_node_effects_determinism_and_subflows_mode() {
    let manifest_json = json!({
        "bundle_version": "0.1",
        "abi": {
            "name": "latticeflow.wit",
            "version": "0.1"
        },
        "bundle_id": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "code": {
            "target": "wasm32-unknown-unknown",
            "file": "flow.wasm",
            "hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "size_bytes": 48
        },
        "flow": {
            "id": "flow://demo",
            "version": "v0.1.0",
            "profile": "wasm"
        },
        "nodes": {
            "std.timer.wait": {
                "id": "node://std.timer.wait",
                "effects": "side_effects",
                "determinism": "chaotic",
                "durability": {
                    "checkpointable": true,
                    "replayable": true,
                    "halts": true
                },
                "input_schema": "schema://input",
                "output_schema": "schema://output"
            }
        },
        "subflows": {
            "mode": "dynamic",
            "entries": []
        }
    });

    let schema_json = serde_json::from_str(FLOW_BUNDLE_SCHEMA).expect("schema json");
    let validator = JSONSchema::options()
        .with_draft(Draft::Draft202012)
        .compile(&schema_json)
        .expect("schema validator");
    assert!(
        validator.validate(&manifest_json).is_err(),
        "schema accepted invalid node or subflow values"
    );
}

#[test]
fn manifest_schema_rejects_unknown_entrypoint_fields() {
    let manifest_json = json!({
        "bundle_version": "0.1",
        "abi": {
            "name": "latticeflow.wit",
            "version": "0.1"
        },
        "bundle_id": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "code": {
            "target": "wasm32-unknown-unknown",
            "file": "flow.wasm",
            "hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "size_bytes": 48
        },
        "flow": {
            "id": "flow://demo",
            "version": "v0.1.0",
            "profile": "wasm"
        },
        "entrypoints": [
            {
                "trigger": "webhook",
                "capture": "responder",
                "route_alias": "/echo"
            }
        ]
    });

    let schema_json = serde_json::from_str(FLOW_BUNDLE_SCHEMA).expect("schema json");
    let validator = JSONSchema::options()
        .with_draft(Draft::Draft202012)
        .compile(&schema_json)
        .expect("schema validator");
    assert!(
        validator.validate(&manifest_json).is_err(),
        "schema accepted unknown entrypoint fields"
    );
}

#[test]
fn manifest_schema_allows_missing_capability_lists_and_node_bindings() {
    let manifest_json = json!({
        "bundle_version": "0.1",
        "abi": {
            "name": "latticeflow.wit",
            "version": "0.1"
        },
        "bundle_id": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "code": {
            "target": "wasm32-unknown-unknown",
            "file": "flow.wasm",
            "hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "size_bytes": 48
        },
        "flow": {
            "id": "flow://demo",
            "version": "v0.1.0",
            "profile": "wasm"
        },
        "capabilities": {},
        "nodes": {
            "std.timer.wait": {
                "id": "node://std.timer.wait",
                "effects": "effectful",
                "determinism": "nondeterministic",
                "durability": {
                    "checkpointable": true,
                    "replayable": true,
                    "halts": true
                },
                "input_schema": "schema://input",
                "output_schema": "schema://output"
            }
        }
    });

    let schema_json = serde_json::from_str(FLOW_BUNDLE_SCHEMA).expect("schema json");
    let validator = JSONSchema::options()
        .with_draft(Draft::Draft202012)
        .compile(&schema_json)
        .expect("schema validator");
    if let Err(mut errors) = validator.validate(&manifest_json) {
        let first = errors.next().expect("schema error");
        panic!("manifest schema validation failed: {first}");
    }

    let manifest: Manifest = serde_json::from_value(manifest_json).expect("manifest");
    assert!(manifest.capabilities.required.is_empty());
    assert!(manifest.capabilities.optional.is_empty());
    let node = manifest.nodes.get("std.timer.wait").expect("node spec");
    assert!(node.bindings.is_empty());
}

#[test]
fn manifest_schema_defaults_optional_arrays_when_omitted() {
    let manifest_json = json!({
        "bundle_version": "0.1",
        "abi": {
            "name": "latticeflow.wit",
            "version": "0.1"
        },
        "bundle_id": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "code": {
            "target": "wasm32-unknown-unknown",
            "file": "flow.wasm",
            "hash": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            "size_bytes": 48
        },
        "flow": {
            "id": "flow://demo",
            "version": "v0.1.0",
            "profile": "wasm"
        }
    });

    let schema_json = serde_json::from_str(FLOW_BUNDLE_SCHEMA).expect("schema json");
    let validator = JSONSchema::options()
        .with_draft(Draft::Draft202012)
        .compile(&schema_json)
        .expect("schema validator");
    if let Err(mut errors) = validator.validate(&manifest_json) {
        let first = errors.next().expect("schema error");
        panic!("manifest schema validation failed: {first}");
    }

    let manifest: Manifest = serde_json::from_value(manifest_json).expect("manifest");
    assert!(manifest.entrypoints.is_empty());
    assert!(manifest.nodes.is_empty());
    assert!(manifest.capabilities.required.is_empty());
    assert!(manifest.capabilities.optional.is_empty());
}
