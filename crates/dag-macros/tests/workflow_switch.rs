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
    name: switch_flow,
    version: "1.0.0",
    profile: Dev;

    let trigger = node!(trigger);
    let route = node!(route);
    let a = node!(branch);
    let b = node!(branch);
    let capture = node!(capture);

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
