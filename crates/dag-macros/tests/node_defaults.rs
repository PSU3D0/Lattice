#![allow(dead_code)]

use dag_core::{Determinism, Effects, NodeResult};
use dag_macros::{def_node, node};
use serde_json::Value as JsonValue;

#[def_node(summary = "Normalize input", effects = "Pure", determinism = "Strict")]
async fn normalize_input(input: String) -> NodeResult<String> {
    Ok(input)
}

#[def_node(
    summary = "Boundary JSON bridge",
    effects = "Pure",
    determinism = "Strict",
    json_boundary = true
)]
async fn boundary_bridge(input: JsonValue) -> NodeResult<JsonValue> {
    Ok(input)
}

#[test]
fn def_node_name_defaults_from_function() {
    let spec = node!(normalize_input);
    assert_eq!(spec.name, "NormalizeInput");
    assert_eq!(spec.effects, Effects::Pure);
    assert_eq!(spec.determinism, Determinism::Strict);
}

#[test]
fn def_node_json_boundary_annotation_emits_policy_hint() {
    let spec = node!(boundary_bridge);
    assert!(
        spec.effect_hints.contains(&"policy::json_boundary"),
        "expected policy::json_boundary hint, got {:?}",
        spec.effect_hints
    );
}
