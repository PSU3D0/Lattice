#![cfg(feature = "flow-registry")]

use dag_macros::{def_node, flow, node};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct RegistryInput;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct RegistryOutput;

#[def_node(
    trigger,
    name = "RegistryTrigger",
    summary = "Registry trigger",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn registry_trigger(input: RegistryInput) -> dag_core::NodeResult<RegistryInput> {
    Ok(input)
}

#[def_node(
    name = "RegistryCapture",
    summary = "Registry capture",
    effects = "Pure",
    determinism = "Strict"
)]
async fn registry_capture(_input: RegistryInput) -> dag_core::NodeResult<RegistryOutput> {
    Ok(RegistryOutput)
}

flow! {
    name: registry_flow,
    version: "0.1.0",
    profile: Web;
    let trigger = node!(crate::registry_trigger);
    let capture = node!(crate::registry_capture);
    connect!(trigger -> capture);
    entrypoint!({
        trigger: "trigger",
        capture: "capture",
        route_aliases: ["/registry"],
        method: "POST",
        deadline_ms: 1500,
    });
}

#[test]
fn flow_macro_registers_flow() {
    let registrations: Vec<_> = dag_core::flow_registry::iter().collect();

    assert!(
        registrations.iter().any(|registration| {
            registration.name == "registry_flow" && registration.version == "0.1.0"
        }),
        "expected registry_flow to be registered"
    );
}
