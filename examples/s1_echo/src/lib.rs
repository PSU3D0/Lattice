use std::sync::Arc;
use std::time::Duration;

use dag_core::{FlowIR, NodeResult};
use dag_macros::{node, trigger};
use host_inproc::EnvironmentPlugin;
use kernel_exec::{FlowExecutor, NodeRegistry};
use kernel_plan::{ValidatedIR, validate};
use serde::{Deserialize, Serialize};

mod auth;
use auth::AuthUser;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EchoRequest {
    pub value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EchoResponse {
    pub value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<AuthUser>,
}

#[trigger(
    name = "HttpTrigger",
    summary = "Ingress HTTP trigger for the echo route"
)]
async fn http_trigger(request: EchoRequest) -> NodeResult<EchoRequest> {
    Ok(request)
}

#[node(
    name = "Normalize",
    summary = "Trim whitespace and lowercase the payload",
    effects = "Pure",
    determinism = "Strict"
)]
async fn normalize(input: EchoRequest) -> NodeResult<EchoResponse> {
    let normalized = EchoResponse {
        value: input.value.trim().to_lowercase(),
        user: None,
    };
    Ok(normalized)
}

#[node(
    name = "Responder",
    summary = "Finalize the HTTP response",
    effects = "Pure",
    determinism = "Strict"
)]
async fn responder(mut payload: EchoResponse) -> NodeResult<EchoResponse> {
    payload.user = auth::current_user();
    Ok(payload)
}

dag_macros::workflow! {
    name: s1_echo_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Implements the S1 webhook echo example";
    let trigger = http_trigger_node_spec();
    let normalize = normalize_node_spec();
    let responder = responder_node_spec();
    connect!(trigger -> normalize);
    connect!(normalize -> responder);
}

/// Convenience helper to fetch the Flow IR for the example workflow.
pub fn flow() -> FlowIR {
    s1_echo_flow()
}

pub const TRIGGER_ALIAS: &str = "trigger";
pub const CAPTURE_ALIAS: &str = "responder";
pub const ROUTE_PATH: &str = "/echo";
pub const DEADLINE: Duration = Duration::from_millis(250);

/// Construct a node registry with the example node implementations registered.
pub fn executor() -> FlowExecutor {
    let mut registry = NodeRegistry::new();
    registry
        .register_fn("example_s1_echo::http_trigger", http_trigger)
        .expect("register http_trigger");
    registry
        .register_fn("example_s1_echo::normalize", normalize)
        .expect("register normalize");
    registry
        .register_fn("example_s1_echo::responder", responder)
        .expect("register responder");
    FlowExecutor::new(Arc::new(registry))
}

/// Validated Flow IR for the example workflow.
pub fn validated_ir() -> ValidatedIR {
    validate(&flow()).expect("S1 flow should validate")
}

pub fn environment_plugins() -> Vec<Arc<dyn EnvironmentPlugin>> {
    auth::environment_plugins()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flow_contains_expected_nodes() {
        let ir = flow();
        let aliases: Vec<_> = ir.nodes.iter().map(|node| node.alias.as_str()).collect();
        assert_eq!(aliases, vec!["trigger", "normalize", "responder"]);
        assert_eq!(ir.edges.len(), 2);
    }
}
