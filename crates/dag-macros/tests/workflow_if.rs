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
    name: if_flow,
    version: "1.0.0",
    profile: Dev;

    let trigger = &TRIGGER_SPEC;
    let route = &ROUTE_SPEC;
    let then_branch = &BRANCH_SPEC;
    let else_branch = &BRANCH_SPEC;
    let capture = &CAPTURE_SPEC;

    connect!(trigger -> route);
    connect!(route -> then_branch);
    connect!(route -> else_branch);
    connect!(then_branch -> capture);
    connect!(else_branch -> capture);

    if_!(
        source = route,
        selector_pointer = "/ok",
        then = then_branch,
        else = else_branch
    );
}

#[test]
fn workflow_if_emits_control_surface_ir() {
    let flow = if_flow();
    assert_eq!(flow.control_surfaces.len(), 1);

    let surface = &flow.control_surfaces[0];
    assert_eq!(surface.id, "if:route:0");
    assert_eq!(surface.kind, ControlSurfaceKind::If);
    assert!(surface.targets.contains(&"route".to_string()));
    assert!(surface.targets.contains(&"then_branch".to_string()));
    assert!(surface.targets.contains(&"else_branch".to_string()));

    let config = surface.config.as_object().expect("config object");
    assert_eq!(config["v"].as_u64(), Some(1));
    assert_eq!(config["source"].as_str(), Some("route"));
    assert_eq!(config["selector_pointer"].as_str(), Some("/ok"));
    assert_eq!(config["then"].as_str(), Some("then_branch"));
    assert_eq!(config["else"].as_str(), Some("else_branch"));
}
