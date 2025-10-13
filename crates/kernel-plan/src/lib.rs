use std::collections::{HashMap, HashSet};

use dag_core::{Diagnostic, Effects, FlowIR, SchemaRef, diagnostic_codes};

/// Result of a successful validation run.
#[derive(Debug, Clone)]
pub struct ValidatedIR {
    flow: FlowIR,
}

impl ValidatedIR {
    /// Access the validated Flow IR.
    pub fn flow(&self) -> &FlowIR {
        &self.flow
    }

    /// Consume the validated wrapper and return the underlying Flow IR.
    pub fn into_inner(self) -> FlowIR {
        self.flow
    }
}

/// Validate a flow and return diagnostics if issues are discovered.
pub fn validate(flow: &FlowIR) -> Result<ValidatedIR, Vec<Diagnostic>> {
    let mut diagnostics = Vec::new();

    check_duplicate_aliases(flow, &mut diagnostics);
    check_edge_references(flow, &mut diagnostics);
    check_cycles(flow, &mut diagnostics);
    check_port_compatibility(flow, &mut diagnostics);
    check_effectful_idempotency(flow, &mut diagnostics);
    check_effect_conflicts(flow, &mut diagnostics);
    check_determinism_conflicts(flow, &mut diagnostics);

    if diagnostics.is_empty() {
        Ok(ValidatedIR { flow: flow.clone() })
    } else {
        Err(diagnostics)
    }
}

fn check_duplicate_aliases(flow: &FlowIR, diagnostics: &mut Vec<Diagnostic>) {
    let mut seen = HashSet::new();
    for node in &flow.nodes {
        if !seen.insert(&node.alias) {
            diagnostics.push(diagnostic(
                "DAG205",
                format!("duplicate node alias `{}`", node.alias),
            ));
        }
    }
}

fn check_edge_references(flow: &FlowIR, diagnostics: &mut Vec<Diagnostic>) {
    let aliases: HashSet<_> = flow.nodes.iter().map(|node| node.alias.as_str()).collect();
    for edge in &flow.edges {
        if !aliases.contains(edge.from.as_str()) {
            diagnostics.push(diagnostic(
                "DAG202",
                format!("unknown node alias `{}` referenced as source", edge.from),
            ));
        }
        if !aliases.contains(edge.to.as_str()) {
            diagnostics.push(diagnostic(
                "DAG202",
                format!("unknown node alias `{}` referenced as target", edge.to),
            ));
        }
        if !aliases.contains(edge.from.as_str()) || !aliases.contains(edge.to.as_str()) {
            continue;
        }
    }
}

fn check_cycles(flow: &FlowIR, diagnostics: &mut Vec<Diagnostic>) {
    let mut adjacency: HashMap<&str, Vec<&str>> = HashMap::new();
    for edge in &flow.edges {
        adjacency
            .entry(edge.from.as_str())
            .or_default()
            .push(edge.to.as_str());
    }

    let mut visiting = HashSet::new();
    let mut visited = HashSet::new();

    for node in &flow.nodes {
        if dfs_cycle(node.alias.as_str(), &adjacency, &mut visiting, &mut visited) {
            diagnostics.push(diagnostic("DAG200", "cycle detected in workflow"));
            break;
        }
    }
}

fn dfs_cycle<'a>(
    node: &'a str,
    adjacency: &HashMap<&'a str, Vec<&'a str>>,
    visiting: &mut HashSet<&'a str>,
    visited: &mut HashSet<&'a str>,
) -> bool {
    if visiting.contains(node) {
        return true;
    }
    if visited.contains(node) {
        return false;
    }

    visiting.insert(node);
    if let Some(neighbours) = adjacency.get(node) {
        for &next in neighbours {
            if dfs_cycle(next, adjacency, visiting, visited) {
                return true;
            }
        }
    }
    visiting.remove(node);
    visited.insert(node);
    false
}

fn check_port_compatibility(flow: &FlowIR, diagnostics: &mut Vec<Diagnostic>) {
    let nodes: HashMap<_, _> = flow
        .nodes
        .iter()
        .map(|node| (node.alias.as_str(), node))
        .collect();
    for edge in &flow.edges {
        let source = match nodes.get(edge.from.as_str()) {
            Some(node) => node,
            None => continue,
        };
        let target = match nodes.get(edge.to.as_str()) {
            Some(node) => node,
            None => continue,
        };
        if !schemas_compatible(&source.out_schema, &target.in_schema) {
            diagnostics.push(diagnostic(
                "DAG201",
                format!(
                    "port type mismatch: `{}` -> `{}` ({:?} -> {:?})",
                    edge.from, edge.to, source.out_schema, target.in_schema
                ),
            ));
        }
    }
}

fn schemas_compatible(source: &SchemaRef, target: &SchemaRef) -> bool {
    match (source, target) {
        (SchemaRef::Named { name: a }, SchemaRef::Named { name: b }) => a == b,
        _ => true,
    }
}

fn check_effectful_idempotency(flow: &FlowIR, diagnostics: &mut Vec<Diagnostic>) {
    for node in &flow.nodes {
        if node.effects == Effects::Effectful && node.idempotency.key.is_none() {
            diagnostics.push(diagnostic(
                "DAG004",
                format!("effectful node `{}` missing idempotency key", node.alias),
            ));
        }
    }
}

fn check_effect_conflicts(flow: &FlowIR, diagnostics: &mut Vec<Diagnostic>) {
    for node in &flow.nodes {
        for hint in &node.effect_hints {
            if let Some(conflict) = dag_core::effects_registry::constraint_for_hint(hint) {
                if !node.effects.is_at_least(conflict.minimum) {
                    diagnostics.push(diagnostic(
                        "EFFECT201",
                        format!(
                            "node `{}` declares effects {} but resource `{}` requires at least {}: {}",
                            node.alias,
                            node.effects.as_str(),
                            hint,
                            conflict.minimum.as_str(),
                            conflict.guidance
                        ),
                    ));
                }
            }
        }
    }
}

fn check_determinism_conflicts(flow: &FlowIR, diagnostics: &mut Vec<Diagnostic>) {
    for node in &flow.nodes {
        for hint in &node.determinism_hints {
            if let Some(conflict) = dag_core::determinism::constraint_for_hint(hint) {
                if !node.determinism.is_at_least(conflict.minimum) {
                    diagnostics.push(diagnostic(
                        "DET302",
                        format!(
                            "node `{}` declares determinism {} but resource `{}` requires at least {}: {}",
                            node.alias,
                            node.determinism.as_str(),
                            hint,
                            conflict.minimum.as_str(),
                            conflict.guidance
                        ),
                    ));
                }
            }
        }
    }
}

fn diagnostic(code: &str, message: impl Into<String>) -> Diagnostic {
    let entry = diagnostic_codes()
        .iter()
        .find(|item| item.code == code)
        .unwrap_or_else(|| panic!("unknown diagnostic code `{code}`"));
    Diagnostic::new(entry, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dag_core::prelude::*;

    fn build_sample_flow() -> FlowIR {
        let mut builder = FlowBuilder::new("sample", Version::new(1, 0, 0), Profile::Web);

        let producer_spec = NodeSpec::inline(
            "tests::producer",
            "Producer",
            SchemaSpec::Opaque,
            SchemaSpec::Named("String"),
            Effects::Pure,
            Determinism::Strict,
            None,
        );
        let consumer_spec = NodeSpec::inline(
            "tests::consumer",
            "Consumer",
            SchemaSpec::Named("String"),
            SchemaSpec::Opaque,
            Effects::ReadOnly,
            Determinism::Stable,
            None,
        );

        let producer = builder
            .add_node("producer", &producer_spec)
            .expect("add producer");
        let consumer = builder
            .add_node("consumer", &consumer_spec)
            .expect("add consumer");
        builder.connect(&producer, &consumer);

        builder.build()
    }

    #[test]
    fn validate_accepts_well_formed_flow() {
        let flow = build_sample_flow();
        let result = validate(&flow);
        assert!(result.is_ok(), "unexpected diagnostics: {result:?}");
    }

    #[test]
    fn detect_type_mismatch() {
        let mut flow = build_sample_flow();
        if let Some(node) = flow.nodes.iter_mut().find(|n| n.alias == "consumer") {
            node.in_schema = SchemaRef::Named {
                name: "Other".to_string(),
            };
        }
        let result = validate(&flow);
        assert!(result.is_err());
        let diagnostics = result.err().unwrap();
        assert!(diagnostics.iter().any(|d| d.code.code == "DAG201"));
    }

    #[test]
    fn detect_cycles() {
        let mut flow = build_sample_flow();
        flow.edges.push(dag_core::EdgeIR {
            from: "consumer".to_string(),
            to: "producer".to_string(),
            ..dag_core::EdgeIR::default()
        });
        let diagnostics = validate(&flow).err().expect("expected cycle diagnostic");
        assert!(diagnostics.iter().any(|d| d.code.code == "DAG200"));
    }

    #[test]
    fn detect_unknown_aliases() {
        let mut flow = build_sample_flow();
        flow.edges.push(dag_core::EdgeIR {
            from: "missing".to_string(),
            to: "consumer".to_string(),
            ..dag_core::EdgeIR::default()
        });
        flow.edges.push(dag_core::EdgeIR {
            from: "producer".to_string(),
            to: "absent".to_string(),
            ..dag_core::EdgeIR::default()
        });
        let diagnostics = validate(&flow).err().expect("expected alias diagnostics");
        let mut seen_source = false;
        let mut seen_target = false;
        for diag in diagnostics {
            if diag.code.code == "DAG202" && diag.message.contains("source") {
                seen_source = true;
            }
            if diag.code.code == "DAG202" && diag.message.contains("target") {
                seen_target = true;
            }
        }
        assert!(seen_source, "missing source alias diagnostic not emitted");
        assert!(seen_target, "missing target alias diagnostic not emitted");
    }

    #[test]
    fn detect_effect_conflicts() {
        let mut builder = FlowBuilder::new("effect_conflict", Version::new(1, 0, 0), Profile::Web);
        let writer = builder
            .add_node(
                "writer",
                &NodeSpec::inline_with_hints(
                    "tests::writer",
                    "Writer",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                    &[],
                    &["resource::http::write"],
                ),
            )
            .expect("add writer node");
        let sink = builder
            .add_node(
                "sink",
                &NodeSpec::inline(
                    "tests::sink",
                    "Sink",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::ReadOnly,
                    Determinism::BestEffort,
                    None,
                ),
            )
            .expect("add sink node");
        builder.connect(&writer, &sink);
        let flow = builder.build();

        let diagnostics = validate(&flow).err().expect("expected effect diagnostic");
        assert!(diagnostics.iter().any(|d| d.code.code == "EFFECT201"));
    }

    #[test]
    fn detect_missing_idempotency() {
        let mut flow = build_sample_flow();
        if let Some(node) = flow.nodes.iter_mut().find(|n| n.alias == "consumer") {
            node.effects = Effects::Effectful;
            node.idempotency.key = None;
        }
        let diagnostics = validate(&flow)
            .err()
            .expect("expected idempotency diagnostic");
        assert!(diagnostics.iter().any(|d| d.code.code == "DAG004"));
    }

    #[test]
    fn detect_determinism_conflicts() {
        let mut builder = FlowBuilder::new("det_conflict", Version::new(1, 0, 0), Profile::Web);
        let clock = builder
            .add_node(
                "clock",
                &NodeSpec::inline_with_hints(
                    "tests::clock",
                    "Clock",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::ReadOnly,
                    Determinism::Strict,
                    None,
                    &["resource::clock"],
                    &[],
                ),
            )
            .expect("add clock node");
        let sink = builder
            .add_node(
                "sink",
                &NodeSpec::inline(
                    "tests::sink",
                    "Sink",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::ReadOnly,
                    Determinism::BestEffort,
                    None,
                ),
            )
            .expect("add sink node");
        builder.connect(&clock, &sink);
        let flow = builder.build();

        let diagnostics = validate(&flow)
            .err()
            .expect("expected determinism diagnostic");
        assert!(diagnostics.iter().any(|d| d.code.code == "DET302"));
    }

    #[test]
    fn respect_registered_custom_conflicts() {
        const HINT: &str = "test::custom";
        dag_core::determinism::register_determinism_constraint(
            dag_core::determinism::DeterminismConstraint::new(
                HINT,
                Determinism::Stable,
                "Custom resource requires Stable determinism",
            ),
        );

        let mut builder = FlowBuilder::new("custom_conflict", Version::new(1, 0, 0), Profile::Web);
        let source = builder
            .add_node(
                "source",
                &NodeSpec::inline_with_hints(
                    "tests::source",
                    "Source",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::ReadOnly,
                    Determinism::Strict,
                    None,
                    &[HINT],
                    &[],
                ),
            )
            .expect("add source node");
        let sink = builder
            .add_node(
                "sink",
                &NodeSpec::inline(
                    "tests::sink",
                    "Sink",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::ReadOnly,
                    Determinism::BestEffort,
                    None,
                ),
            )
            .expect("add sink node");
        builder.connect(&source, &sink);
        let flow = builder.build();

        let diagnostics = validate(&flow)
            .err()
            .expect("expected determinism diagnostic");
        assert!(diagnostics.iter().any(|d| d.code.code == "DET302"));
    }
}
