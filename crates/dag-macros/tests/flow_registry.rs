#![cfg(feature = "flow-registry")]

use dag_macros::{def_node, flow, node};

// When `host-bundle` is also enabled (e.g. `clippy --all-features`), `flow!`
// additionally emits a `bundle()` referencing `::host_inproc::*`. Satisfy
// those references with the same self-alias mock used by tests/flow_macro.rs.
#[cfg(feature = "host-bundle")]
extern crate self as host_inproc;

#[cfg(feature = "host-bundle")]
mod host_bundle_mock_types {
    use std::sync::Arc;
    use std::time::Duration;

    pub trait EnvironmentPlugin {}

    #[derive(Clone, Debug)]
    pub enum NodeSource {
        Local,
    }

    #[derive(Clone, Debug)]
    pub struct NodeContract {
        pub identifier: String,
        pub contract_hash: Option<String>,
        pub source: NodeSource,
    }

    #[derive(Clone, Debug)]
    pub struct FlowEntrypoint {
        pub trigger_alias: String,
        pub capture_alias: String,
        pub route_path: Option<String>,
        pub method: Option<String>,
        pub deadline: Option<Duration>,
        pub route_aliases: Vec<String>,
    }

    pub struct FlowBundle {
        pub validated_ir: kernel_plan::ValidatedIR,
        pub entrypoints: Vec<FlowEntrypoint>,
        pub resolver: Arc<dyn kernel_exec::NodeResolver>,
        pub node_contracts: Vec<NodeContract>,
        pub environment_plugins: Vec<Arc<dyn EnvironmentPlugin>>,
    }
}

#[cfg(feature = "host-bundle")]
pub use host_bundle_mock_types::*;

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
