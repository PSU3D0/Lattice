use std::sync::Arc;

use dag_core::NodeResult;
use dag_macros::{node, trigger};
use host_inproc::EnvironmentPlugin;
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

dag_macros::workflow_bundle! {
    name: s1_echo_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Implements the S1 webhook echo example";
    let trigger = http_trigger_node_spec();
    let normalize = normalize_node_spec();
    let responder = responder_node_spec();
    connect!(trigger -> normalize);
    connect!(normalize -> responder);
    entrypoint!({
        trigger: "trigger",
        capture: "responder",
        route: "/echo",
        method: "POST",
        deadline_ms: 250,
    });
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
