use dag_core::prelude::*;
use dag_core::ControlSurfaceKind;
use dag_macros::{def_node, node, workflow};

#[def_node(trigger, name = "Trigger")]
async fn trigger(_input: ()) -> NodeResult<()> {
    Ok(())
}

#[def_node(name = "Route")]
async fn route(_input: ()) -> NodeResult<()> {
    Ok(())
}

#[def_node(name = "Branch")]
async fn branch(_input: ()) -> NodeResult<()> {
    Ok(())
}

#[def_node(name = "Capture")]
async fn capture(_input: ()) -> NodeResult<()> {
    Ok(())
}

workflow! {
    name: if_flow,
    version: "1.0.0",
    profile: Dev;

    let trigger = node!(trigger);
    let route = node!(route);
    let then_branch = node!(branch);
    let else_branch = node!(branch);
    let capture = node!(capture);

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
