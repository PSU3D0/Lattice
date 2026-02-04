#![allow(dead_code)]

use dag_core::{Determinism, Effects, NodeResult};
use dag_macros::{def_node, node};

#[def_node(summary = "Normalize input", effects = "Pure", determinism = "Strict")]
async fn normalize_input(input: String) -> NodeResult<String> {
    Ok(input)
}

#[test]
fn def_node_name_defaults_from_function() {
    let spec = node!(normalize_input);
    assert_eq!(spec.name, "NormalizeInput");
    assert_eq!(spec.effects, Effects::Pure);
    assert_eq!(spec.determinism, Determinism::Strict);
}
