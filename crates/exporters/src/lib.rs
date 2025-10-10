// TODO
use dag_core::FlowIR;
use serde_json::Value;

/// Serialise a Flow IR into a `serde_json::Value` helper.
pub fn to_json_value(flow: &FlowIR) -> Value {
    serde_json::to_value(flow).expect("Flow IR serialisation should not fail")
}

/// Emit a Graphviz DOT representation of the Flow IR.
pub fn to_dot(flow: &FlowIR) -> String {
    let mut buffer = String::new();
    buffer.push_str("digraph flow {\n");

    for node in &flow.nodes {
        buffer.push_str(&format!(
            "    \"{}\" [label=\"{}\"];\n",
            node.alias, node.name
        ));
    }

    for edge in &flow.edges {
        buffer.push_str(&format!("    \"{}\" -> \"{}\";\n", edge.from, edge.to));
    }

    buffer.push('}');
    buffer.push('\n');
    buffer
}

#[cfg(test)]
mod tests {
    use dag_core::prelude::*;

    use super::*;

    #[test]
    fn dot_contains_nodes_and_edges() {
        let mut builder = FlowBuilder::new("export_test", Version::new(1, 0, 0), Profile::Web);
        let a = builder
            .add_node(
                "a",
                &NodeSpec::inline(
                    "tests::a",
                    "A",
                    SchemaSpec::Opaque,
                    SchemaSpec::Named("String"),
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let b = builder
            .add_node(
                "b",
                &NodeSpec::inline(
                    "tests::b",
                    "B",
                    SchemaSpec::Named("String"),
                    SchemaSpec::Opaque,
                    Effects::ReadOnly,
                    Determinism::Stable,
                    None,
                ),
            )
            .unwrap();
        builder.connect(&a, &b);
        let flow = builder.build();

        let dot = to_dot(&flow);
        assert!(dot.contains("\"a\""));
        assert!(dot.contains("\"b\""));
        assert!(dot.contains("\"a\" -> \"b\""));
    }
}
