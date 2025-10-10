use dag_core::{FlowIR, NodeResult};
use dag_macros::{node, trigger};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EchoRequest {
    pub value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EchoResponse {
    pub value: String,
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
    };
    Ok(normalized)
}

#[node(
    name = "Responder",
    summary = "Finalize the HTTP response",
    effects = "Effectful",
    determinism = "BestEffort"
)]
async fn responder(payload: EchoResponse) -> NodeResult<EchoResponse> {
    Ok(payload)
}

dag_macros::workflow! {
    name: s1_echo_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Implements the S1 webhook echo example",
    nodes: {
        trigger => http_trigger_node_spec(),
        normalize => normalize_node_spec(),
        responder => responder_node_spec(),
    },
    edges: [
        trigger => normalize,
        normalize => responder,
    ],
}

/// Convenience helper to fetch the Flow IR for the example workflow.
pub fn flow() -> FlowIR {
    s1_echo_flow()
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
