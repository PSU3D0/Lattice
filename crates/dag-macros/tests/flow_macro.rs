use dag_macros::flow;

extern crate self as host_inproc;

use std::sync::Arc;
use std::time::Duration;

pub trait EnvironmentPlugin {}

pub trait NodeResolver {}

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
}

pub struct FlowBundle {
    pub validated_ir: kernel_plan::ValidatedIR,
    pub entrypoints: Vec<FlowEntrypoint>,
    pub resolver: Arc<dyn NodeResolver>,
    pub node_contracts: Vec<NodeContract>,
    pub environment_plugins: Vec<Arc<dyn EnvironmentPlugin>>,
}

impl NodeResolver for kernel_exec::NodeRegistry {}

flow! {
    name: s1_echo,
    version: "1.0.0",
    profile: Web;
}

#[test]
fn flow_macro_builds_bundle() {
    let ir = flow();
    assert_eq!(ir.name, "s1_echo");
}
