use dag_core::ControlSurfaceKind;
use dag_core::prelude::*;
use dag_macros::workflow;

const TRIGGER_SPEC: NodeSpec = NodeSpec {
    identifier: "tests::trigger",
    name: "Trigger",
    kind: NodeKind::Trigger,
    summary: None,
    in_schema: SchemaSpec::Opaque,
    out_schema: SchemaSpec::Opaque,
    effects: Effects::Pure,
    determinism: Determinism::Strict,
    determinism_hints: &[],
    effect_hints: &[],
};

const ROUTE_SPEC: NodeSpec = NodeSpec::inline(
    "tests::route",
    "Route",
    SchemaSpec::Opaque,
    SchemaSpec::Opaque,
    Effects::Pure,
    Determinism::Strict,
    None,
);

const BRANCH_SPEC: NodeSpec = NodeSpec::inline(
    "tests::branch",
    "Branch",
    SchemaSpec::Opaque,
    SchemaSpec::Opaque,
    Effects::Pure,
    Determinism::Strict,
    None,
);

const CAPTURE_SPEC: NodeSpec = NodeSpec::inline(
    "tests::capture",
    "Capture",
    SchemaSpec::Opaque,
    SchemaSpec::Opaque,
    Effects::Pure,
    Determinism::Strict,
    None,
);

workflow! {
    name: switch_flow,
    version: "1.0.0",
    profile: Dev;

    let trigger = &TRIGGER_SPEC;
    let route = &ROUTE_SPEC;
    let a = &BRANCH_SPEC;
    let b = &BRANCH_SPEC;
    let capture = &CAPTURE_SPEC;

    connect!(trigger -> route);
    connect!(route -> a);
    connect!(route -> b);
    connect!(a -> capture);
    connect!(b -> capture);

    switch!(
        source = route,
        selector_pointer = "/type",
        cases = { "a" => a, "b" => b },
        default = a
    );
}

#[test]
fn workflow_switch_emits_control_surface_ir() {
    let flow = switch_flow();
    assert_eq!(flow.control_surfaces.len(), 1);

    let surface = &flow.control_surfaces[0];
    assert_eq!(surface.id, "switch:route:0");
    assert_eq!(surface.kind, ControlSurfaceKind::Switch);
    assert!(surface.targets.contains(&"route".to_string()));
    assert!(surface.targets.contains(&"a".to_string()));
    assert!(surface.targets.contains(&"b".to_string()));

    let config = surface.config.as_object().expect("config object");
    assert_eq!(config["v"].as_u64(), Some(1));
    assert_eq!(config["source"].as_str(), Some("route"));
    assert_eq!(config["selector_pointer"].as_str(), Some("/type"));

    let cases = config["cases"].as_object().expect("cases object");
    assert_eq!(cases["a"].as_str(), Some("a"));
    assert_eq!(cases["b"].as_str(), Some("b"));
    assert_eq!(config["default"].as_str(), Some("a"));
}
