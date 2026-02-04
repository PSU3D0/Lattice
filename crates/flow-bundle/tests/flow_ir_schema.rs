use jsonschema::{Draft, JSONSchema};
use serde_json::json;

const FLOW_IR_SCHEMA: &str = include_str!("../../../schemas/flow_ir.schema.json");

#[test]
fn flow_ir_schema_rejects_invalid_subflow_ir() {
    let ir = json!({
        "id": "flow://demo",
        "name": "Demo",
        "profile": "dev",
        "version": "0.1.0",
        "nodes": [
            {
                "alias": "call_subflow",
                "id": "node://subflow.alpha",
                "name": "Call subflow",
                "kind": "subflow",
                "identifier": "subflow::subflow.alpha::entry",
                "effects": "effectful",
                "determinism": "nondeterministic",
                "in_schema": { "kind": "opaque" },
                "out_schema": { "kind": "opaque" },
                "subflow_ir": {
                    "name": "Invalid"
                }
            }
        ]
    });

    let schema_json = serde_json::from_str(FLOW_IR_SCHEMA).expect("schema json");
    let validator = JSONSchema::options()
        .with_draft(Draft::Draft202012)
        .compile(&schema_json)
        .expect("schema validator");
    assert!(
        validator.validate(&ir).is_err(),
        "schema accepted invalid subflow_ir payload"
    );
}
