use std::collections::{HashMap, HashSet};

use dag_core::{Delivery, Diagnostic, Effects, FlowIR, SchemaRef, diagnostic_codes};

const MIN_EXACTLY_ONCE_TTL_MS: u64 = 300_000;
const DEDUPE_HINT_PREFIX: &str = "resource::dedupe";

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
    check_exactly_once_requirements(flow, &mut diagnostics);
    check_edge_timeout_requirements(flow, &mut diagnostics);
    check_spill_requirements(flow, &mut diagnostics);

    if diagnostics.is_empty() {
        Ok(ValidatedIR { flow: flow.clone() })
    } else {
        Err(diagnostics)
    }
}

fn check_exactly_once_requirements(flow: &FlowIR, diagnostics: &mut Vec<Diagnostic>) {
    let nodes: HashMap<_, _> = flow
        .nodes
        .iter()
        .map(|node| (node.alias.as_str(), node))
        .collect();

    for edge in &flow.edges {
        if edge.delivery == Delivery::ExactlyOnce {
            let Some(target) = nodes.get(edge.to.as_str()) else {
                continue;
            };

            if !has_dedupe_binding(target) {
                diagnostics.push(diagnostic(
                    "EXACT001",
                    format!(
                        "edge `{}` -> `{}` requests Delivery::ExactlyOnce but node `{}` does not bind a dedupe capability (hint `{}` expected)",
                        edge.from,
                        edge.to,
                        target.alias,
                        DEDUPE_HINT_PREFIX
                    ),
                ));
            }

            if target.idempotency.key.is_none() {
                diagnostics.push(diagnostic(
                    "EXACT002",
                    format!(
                        "edge `{}` -> `{}` requests Delivery::ExactlyOnce but node `{}` has no idempotency key",
                        edge.from, edge.to, target.alias
                    ),
                ));
            }

            match target.idempotency.ttl_ms {
                Some(ttl) if ttl >= MIN_EXACTLY_ONCE_TTL_MS => {}
                Some(ttl) => diagnostics.push(diagnostic(
                    "EXACT003",
                    format!(
                        "edge `{}` -> `{}` requests Delivery::ExactlyOnce but node `{}` declares dedupe TTL {}ms (minimum {}ms)",
                        edge.from,
                        edge.to,
                        target.alias,
                        ttl,
                        MIN_EXACTLY_ONCE_TTL_MS
                    ),
                )),
                None => diagnostics.push(diagnostic(
                    "EXACT003",
                    format!(
                        "edge `{}` -> `{}` requests Delivery::ExactlyOnce but node `{}` does not declare a dedupe TTL (minimum {}ms)",
                        edge.from,
                        edge.to,
                        target.alias,
                        MIN_EXACTLY_ONCE_TTL_MS
                    ),
                )),
            }
        }
    }
}

fn has_dedupe_binding(node: &dag_core::NodeIR) -> bool {
    node.effect_hints
        .iter()
        .any(|hint| hint.starts_with(DEDUPE_HINT_PREFIX))
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
            if let Some(conflict) = dag_core::effects_registry::constraint_for_hint(hint)
                && !node.effects.is_at_least(conflict.minimum)
            {
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

fn check_determinism_conflicts(flow: &FlowIR, diagnostics: &mut Vec<Diagnostic>) {
    for node in &flow.nodes {
        for hint in &node.determinism_hints {
            if let Some(conflict) = dag_core::determinism::constraint_for_hint(hint)
                && !node.determinism.is_at_least(conflict.minimum)
            {
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

fn check_edge_timeout_requirements(flow: &FlowIR, diagnostics: &mut Vec<Diagnostic>) {
    for edge in &flow.edges {
        if edge.timeout_ms == Some(0) {
            diagnostics.push(diagnostic(
                "CTRL101",
                format!(
                    "edge `{}` -> `{}` configures `timeout_ms = 0`; timeout budgets must be positive",
                    edge.from, edge.to
                ),
            ));
        }
    }
}

fn check_spill_requirements(flow: &FlowIR, diagnostics: &mut Vec<Diagnostic>) {
    let has_blob_hint = flow.nodes.iter().any(|node| {
        node.effect_hints
            .iter()
            .any(|hint| hint.starts_with("resource::blob::"))
    });

    let mut emitted_blob_diagnostic = false;

    for edge in &flow.edges {
        if let Some(tier) = &edge.buffer.spill_tier {
            if edge.buffer.max_items.is_none() {
                diagnostics.push(diagnostic(
                    "SPILL001",
                    format!(
                        "edge `{}` -> `{}` configures `spill_tier = {tier}` without bounding `max_items`",
                        edge.from, edge.to
                    ),
                ));
            }

            if !has_blob_hint && !emitted_blob_diagnostic {
                diagnostics.push(diagnostic(
                    "SPILL002",
                    format!(
                        "edge `{}` -> `{}` configures `spill_tier = {tier}` but no node declares a blob capability hint",
                        edge.from, edge.to
                    ),
                ));
                emitted_blob_diagnostic = true;
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
    use capabilities::{blob, db, http, kv, queue};
    use dag_core::IdempotencyScope;
    use dag_core::NodeResult;
    use dag_core::prelude::*;
    use dag_macros::node;
    use proptest::prelude::*;
    use proptest::sample::select;
    use std::collections::BTreeSet;

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

    fn downgrade_effect(level: Effects) -> Option<Effects> {
        match level {
            Effects::Effectful => Some(Effects::ReadOnly),
            Effects::ReadOnly => Some(Effects::Pure),
            Effects::Pure => None,
        }
    }

    fn downgrade_determinism(level: Determinism) -> Option<Determinism> {
        match level {
            Determinism::Nondeterministic => Some(Determinism::BestEffort),
            Determinism::BestEffort => Some(Determinism::Stable),
            Determinism::Stable => Some(Determinism::Strict),
            Determinism::Strict => None,
        }
    }

    const DEDUPE_HINT_WRITE: &str = "resource::dedupe::write";

    fn set_idempotency(flow: &mut FlowIR, alias: &str) {
        if let Some(node) = flow.nodes.iter_mut().find(|n| n.alias == alias) {
            node.idempotency.key = Some("prop.case".to_string());
            node.idempotency.scope = Some(IdempotencyScope::Node);
            node.idempotency.ttl_ms = Some(MIN_EXACTLY_ONCE_TTL_MS);
        }
    }

    fn ensure_dedupe_hint(flow: &mut FlowIR, alias: &str) {
        if let Some(node) = flow.nodes.iter_mut().find(|n| n.alias == alias)
            && !node
                .effect_hints
                .iter()
                .any(|hint| hint == DEDUPE_HINT_WRITE)
        {
            node.effect_hints.push(DEDUPE_HINT_WRITE.to_string());
        }
    }

    fn register_all_hints() {
        http::ensure_registered();
        db::ensure_registered();
        kv::ensure_registered();
        blob::ensure_registered();
        queue::ensure_registered();
        capabilities::clock::ensure_registered();
        capabilities::rng::ensure_registered();
    }

    #[test]
    fn exactly_once_requires_dedupe_binding() {
        let mut flow = build_sample_flow();
        if let Some(edge) = flow.edges.first_mut() {
            edge.delivery = Delivery::ExactlyOnce;
        }
        set_idempotency(&mut flow, "consumer");

        let diagnostics = validate(&flow).expect_err("expected validation errors");
        assert!(diagnostics.iter().any(|d| d.code.code == "EXACT001"));
    }

    #[test]
    fn exactly_once_requires_idempotency_key() {
        let mut flow = build_sample_flow();
        if let Some(edge) = flow.edges.first_mut() {
            edge.delivery = Delivery::ExactlyOnce;
        }
        set_idempotency(&mut flow, "consumer");
        ensure_dedupe_hint(&mut flow, "consumer");
        if let Some(node) = flow.nodes.iter_mut().find(|n| n.alias == "consumer") {
            node.idempotency.key = None;
        }

        let diagnostics = validate(&flow).expect_err("expected validation errors");
        assert!(diagnostics.iter().any(|d| d.code.code == "EXACT002"));
        // Without a key TTL is irrelevant; ensure no panic when missing.
    }

    #[test]
    fn exactly_once_requires_ttl() {
        let mut flow = build_sample_flow();
        if let Some(edge) = flow.edges.first_mut() {
            edge.delivery = Delivery::ExactlyOnce;
        }
        set_idempotency(&mut flow, "consumer");
        ensure_dedupe_hint(&mut flow, "consumer");
        if let Some(node) = flow.nodes.iter_mut().find(|n| n.alias == "consumer") {
            node.idempotency.ttl_ms = None;
        }

        let diagnostics = validate(&flow).expect_err("expected validation errors");
        assert!(diagnostics.iter().any(|d| d.code.code == "EXACT003"));
    }

    #[test]
    fn exactly_once_requires_minimum_ttl() {
        let mut flow = build_sample_flow();
        if let Some(edge) = flow.edges.first_mut() {
            edge.delivery = Delivery::ExactlyOnce;
        }
        set_idempotency(&mut flow, "consumer");
        ensure_dedupe_hint(&mut flow, "consumer");
        if let Some(node) = flow.nodes.iter_mut().find(|n| n.alias == "consumer") {
            node.idempotency.ttl_ms = Some(MIN_EXACTLY_ONCE_TTL_MS - 1);
        }

        let diagnostics = validate(&flow).expect_err("expected validation errors");
        assert!(diagnostics.iter().any(|d| d.code.code == "EXACT003"));
    }

    #[test]
    fn edge_timeout_requires_positive_budget() {
        let mut flow = build_sample_flow();
        if let Some(edge) = flow.edges.first_mut() {
            edge.timeout_ms = Some(0);
        }

        let diagnostics = validate(&flow).expect_err("expected validation errors");
        assert!(diagnostics.iter().any(|d| d.code.code == "CTRL101"));
    }

    #[test]
    fn exactly_once_succeeds_when_prerequisites_met() {
        let mut flow = build_sample_flow();
        if let Some(edge) = flow.edges.first_mut() {
            edge.delivery = Delivery::ExactlyOnce;
        }
        set_idempotency(&mut flow, "consumer");
        ensure_dedupe_hint(&mut flow, "consumer");
        if let Some(node) = flow.nodes.iter_mut().find(|n| n.alias == "consumer") {
            node.idempotency.ttl_ms = Some(MIN_EXACTLY_ONCE_TTL_MS);
        }

        let result = validate(&flow);
        assert!(result.is_ok(), "unexpected diagnostics: {result:?}");
    }

    fn dedup_hints(hints: Vec<&'static str>) -> Vec<&'static str> {
        let mut set = BTreeSet::new();
        for hint in hints {
            set.insert(hint);
        }
        set.into_iter().collect()
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
        let diagnostics = validate(&flow).expect_err("expected cycle diagnostic");
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
        let diagnostics = validate(&flow).expect_err("expected alias diagnostics");
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
    fn fuzz_registry_hint_enforcement() {
        register_all_hints();
        let effect_universe: Vec<&'static str> = dag_core::effects_registry::all_constraints()
            .into_iter()
            .map(|c| c.hint)
            .collect();
        let determinism_universe: Vec<&'static str> = dag_core::determinism::all_constraints()
            .into_iter()
            .map(|c| c.hint)
            .collect();

        let effect_levels = vec![Effects::Pure, Effects::ReadOnly, Effects::Effectful];
        let determinism_levels = vec![
            Determinism::Strict,
            Determinism::Stable,
            Determinism::BestEffort,
            Determinism::Nondeterministic,
        ];

        let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
            cases: 64,
            ..ProptestConfig::default()
        });

        let strategy = (
            proptest::collection::vec(select(effect_universe.clone()), 0..=3),
            proptest::collection::vec(select(determinism_universe.clone()), 0..=3),
            select(effect_levels.clone()),
            select(determinism_levels.clone()),
            proptest::bool::ANY,
            proptest::bool::ANY,
        );

        runner
            .run(
                &strategy,
                |(
                    effect_hints_case,
                    det_hints_case,
                    base_effect,
                    base_det,
                    degrade_effect_flag,
                    degrade_det_flag,
                )| {
                    let effect_vec = dedup_hints(effect_hints_case);
                    let det_vec = dedup_hints(det_hints_case);

                    let required_effect = effect_vec
                        .iter()
                        .filter_map(|hint| {
                            dag_core::effects_registry::constraint_for_hint(hint).map(|c| c.minimum)
                        })
                        .fold(None::<Effects>, |acc, next| match acc {
                            Some(current) if current.rank() >= next.rank() => Some(current),
                            Some(_) => Some(next),
                            None => Some(next),
                        });

                    let required_det = det_vec
                        .iter()
                        .filter_map(|hint| {
                            dag_core::determinism::constraint_for_hint(hint).map(|c| c.minimum)
                        })
                        .fold(None::<Determinism>, |acc, next| match acc {
                            Some(current) if current.rank() >= next.rank() => Some(current),
                            Some(_) => Some(next),
                            None => Some(next),
                        });

                    let declared_effect = if degrade_effect_flag {
                        downgrade_effect(base_effect).unwrap_or(base_effect)
                    } else {
                        base_effect
                    };
                    let declared_det = if degrade_det_flag {
                        downgrade_determinism(base_det).unwrap_or(base_det)
                    } else {
                        base_det
                    };

                    let effect_slice: &'static [&'static str] =
                        Box::leak(effect_vec.clone().into_boxed_slice());
                    let det_slice: &'static [&'static str] =
                        Box::leak(det_vec.clone().into_boxed_slice());

                    let spec_box = Box::new(NodeSpec::inline_with_hints(
                        "tests::prop_validator",
                        "PropValidator",
                        SchemaSpec::Opaque,
                        SchemaSpec::Opaque,
                        declared_effect,
                        declared_det,
                        None,
                        det_slice,
                        effect_slice,
                    ));
                    let spec: &'static NodeSpec = Box::leak(spec_box);

                    let mut builder =
                        FlowBuilder::new("prop_validation", Version::new(0, 0, 1), Profile::Web);
                    builder.add_node("entry", spec).expect("add node");
                    let mut flow = builder.build();
                    if declared_effect == Effects::Effectful {
                        set_idempotency(&mut flow, "entry");
                    }

                    let result = validate(&flow);

                    let violates_effect = required_effect
                        .map(|req| !declared_effect.is_at_least(req))
                        .unwrap_or(false);
                    let violates_det = required_det
                        .map(|req| !declared_det.is_at_least(req))
                        .unwrap_or(false);

                    if violates_effect || violates_det {
                        let diagnostics = result.expect_err("expected validation errors");
                        if violates_effect {
                            prop_assert!(
                                diagnostics.iter().any(|d| d.code.code == "EFFECT201"),
                                "expected EFFECT201 diagnostic"
                            );
                        }
                        if violates_det {
                            prop_assert!(
                                diagnostics.iter().any(|d| d.code.code == "DET302"),
                                "expected DET302 diagnostic"
                            );
                        }
                    } else {
                        prop_assert!(
                            result.is_ok(),
                            "expected validation success when declared policies satisfy hints"
                        );
                    }
                    Ok(())
                },
            )
            .unwrap();
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

        let diagnostics = validate(&flow).expect_err("expected effect diagnostic");
        assert!(diagnostics.iter().any(|d| d.code.code == "EFFECT201"));
    }

    #[test]
    fn detect_missing_idempotency() {
        let mut flow = build_sample_flow();
        if let Some(node) = flow.nodes.iter_mut().find(|n| n.alias == "consumer") {
            node.effects = Effects::Effectful;
            node.idempotency.key = None;
        }
        let diagnostics = validate(&flow).expect_err("expected idempotency diagnostic");
        assert!(diagnostics.iter().any(|d| d.code.code == "DAG004"));
    }

    #[test]
    fn spill_requires_max_items_bound() {
        let mut builder = FlowBuilder::new("spill_no_bound", Version::new(1, 0, 0), Profile::Queue);
        let trigger = builder
            .add_node(
                "trigger",
                &NodeSpec::inline(
                    "tests::trigger",
                    "Trigger",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let worker = builder
            .add_node(
                "worker",
                &NodeSpec::inline(
                    "tests::worker",
                    "Worker",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &worker);

        let mut flow = builder.build();
        for edge in &mut flow.edges {
            edge.buffer.spill_tier = Some("local".into());
            edge.buffer.max_items = None;
        }

        let diagnostics = validate(&flow).expect_err("expected spill diagnostic");
        assert!(diagnostics.iter().any(|d| d.code.code == "SPILL001"));
    }

    #[test]
    fn spill_requires_blob_hint() {
        let mut builder =
            FlowBuilder::new("spill_blob_hint", Version::new(1, 0, 0), Profile::Queue);
        let trigger = builder
            .add_node(
                "trigger",
                &NodeSpec::inline(
                    "tests::trigger",
                    "Trigger",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let worker = builder
            .add_node(
                "worker",
                &NodeSpec::inline(
                    "tests::worker",
                    "Worker",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        builder.connect(&trigger, &worker);

        let mut flow = builder.build();
        for edge in &mut flow.edges {
            edge.buffer.spill_tier = Some("local".into());
            edge.buffer.max_items = Some(1);
        }

        let diagnostics = validate(&flow).expect_err("expected blob hint diagnostic");
        assert!(diagnostics.iter().any(|d| d.code.code == "SPILL002"));
    }

    #[test]
    fn spill_passes_when_blob_hint_declared() {
        let mut builder = FlowBuilder::new("spill_blob_ok", Version::new(1, 0, 0), Profile::Queue);
        let trigger = builder
            .add_node(
                "trigger",
                &NodeSpec::inline(
                    "tests::trigger",
                    "Trigger",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .unwrap();
        let worker_spec = NodeSpec::inline_with_hints(
            "tests::worker",
            "Worker",
            SchemaSpec::Opaque,
            SchemaSpec::Opaque,
            Effects::Effectful,
            Determinism::BestEffort,
            None,
            &[],
            &["resource::blob::write"],
        );
        let worker = builder.add_node("worker", &worker_spec).unwrap();
        builder.connect(&trigger, &worker);

        let mut flow = builder.build();
        for edge in &mut flow.edges {
            edge.buffer.spill_tier = Some("local".into());
            edge.buffer.max_items = Some(1);
        }
        set_idempotency(&mut flow, "worker");

        let result = validate(&flow);
        if let Err(diags) = &result {
            let codes: Vec<&str> = diags.iter().map(|d| d.code.code).collect();
            panic!(
                "spill validation should succeed when blob hints are present, diagnostics: {:?}",
                codes
            );
        }
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

        let diagnostics = validate(&flow).expect_err("expected determinism diagnostic");
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

        let diagnostics = validate(&flow).expect_err("expected determinism diagnostic");
        assert!(diagnostics.iter().any(|d| d.code.code == "DET302"));
    }

    mod auto_hint_validation {
        use super::*;
        use dag_core::IdempotencyScope;

        #[allow(dead_code)]
        struct HttpWrite;
        #[allow(dead_code)]
        struct TestClock;
        #[allow(dead_code)]
        struct DbHandle;
        #[allow(dead_code)]
        struct Noop;

        #[allow(dead_code)]
        #[node(
            name = "HttpWriter",
            effects = "Effectful",
            determinism = "BestEffort",
            resources(http(HttpWrite))
        )]
        async fn http_writer(_: ()) -> NodeResult<()> {
            Ok(())
        }

        #[allow(dead_code)]
        #[node(
            name = "ClockBestEffort",
            effects = "ReadOnly",
            determinism = "BestEffort",
            resources(clock(TestClock))
        )]
        async fn clock_best_effort(_: ()) -> NodeResult<()> {
            Ok(())
        }

        #[allow(dead_code)]
        #[node(
            name = "DbWriter",
            effects = "Effectful",
            determinism = "BestEffort",
            resources(db_writer(DbHandle))
        )]
        async fn db_writer(_: ()) -> NodeResult<()> {
            Ok(())
        }

        #[allow(dead_code)]
        #[node(name = "NoResources", effects = "Pure", determinism = "Strict")]
        async fn no_resources(_: ()) -> NodeResult<()> {
            Ok(())
        }

        fn single_node_flow(alias: &str, spec: &'static NodeSpec) -> FlowIR {
            let mut builder = FlowBuilder::new(alias, Version::new(1, 0, 0), Profile::Web);
            builder.add_node(alias, spec).expect("add node");
            builder.build()
        }

        fn ensure_idempotency(flow: &mut FlowIR, alias: &str) {
            if let Some(node) = flow.nodes.iter_mut().find(|n| n.alias == alias) {
                node.idempotency.key = Some("request.id".to_string());
                node.idempotency.scope = Some(IdempotencyScope::Node);
            }
        }

        #[test]
        fn validator_flags_effect_conflict_from_registry_hint() {
            capabilities::http::ensure_registered();
            let mut flow = single_node_flow("writer", http_writer_node_spec());
            ensure_idempotency(&mut flow, "writer");
            if let Some(node) = flow.nodes.iter_mut().find(|n| n.alias == "writer") {
                node.effects = Effects::Pure;
            }
            let diagnostics = validate(&flow).expect_err("expected effect mismatch diagnostic");
            assert!(
                diagnostics.iter().any(|d| d.code.code == "EFFECT201"),
                "expected EFFECT201, got: {:?}",
                diagnostics
            );
        }

        #[test]
        fn validator_accepts_effectful_node_with_registry_hint() {
            capabilities::http::ensure_registered();
            let flow = single_node_flow("writer_ok", http_writer_node_spec());
            // effectful nodes require idempotency; clone and set before validation
            let mut flow = flow;
            ensure_idempotency(&mut flow, "writer_ok");
            assert!(
                validate(&flow).is_ok(),
                "effectful http writer should validate cleanly"
            );
        }

        #[test]
        fn validator_flags_determinism_conflict_from_registry_hint() {
            capabilities::clock::ensure_registered();
            let mut flow = single_node_flow("clock", clock_best_effort_node_spec());
            if let Some(node) = flow.nodes.iter_mut().find(|n| n.alias == "clock") {
                node.determinism = Determinism::Strict;
            }
            let diagnostics =
                validate(&flow).expect_err("expected determinism mismatch diagnostic");
            assert!(
                diagnostics.iter().any(|d| d.code.code == "DET302"),
                "expected DET302, got: {:?}",
                diagnostics
            );
        }

        #[test]
        fn fallback_hints_still_trigger_conflicts() {
            let mut flow = single_node_flow("db_writer", db_writer_node_spec());
            ensure_idempotency(&mut flow, "db_writer");
            if let Some(node) = flow.nodes.iter_mut().find(|n| n.alias == "db_writer") {
                node.effects = Effects::Pure;
            }
            let diagnostics =
                validate(&flow).expect_err("expected effect conflict from fallback hints");
            assert!(
                diagnostics.iter().any(|d| d.code.code == "EFFECT201"),
                "expected EFFECT201 from fallback hints, got: {:?}",
                diagnostics
            );
        }

        #[test]
        fn nodes_without_resources_remain_hint_free() {
            let spec = no_resources_node_spec();
            assert!(
                spec.effect_hints.is_empty() && spec.determinism_hints.is_empty(),
                "expected no hints for resource-free node"
            );
            let flow = single_node_flow("noop", spec);
            assert!(
                validate(&flow).is_ok(),
                "resource-free nodes with pure/strict defaults should validate"
            );
        }
    }
}
