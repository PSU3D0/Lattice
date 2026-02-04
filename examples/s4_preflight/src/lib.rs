use dag_core::NodeResult;
use dag_macros::{def_node, node};
use serde_json::Value as JsonValue;

#[def_node(trigger,
    name = "HttpTrigger",
    summary = "Ingress trigger for preflight example"
)]
async fn http_trigger(payload: JsonValue) -> NodeResult<JsonValue> {
    Ok(payload)
}

#[def_node(
    name = "KvRead",
    summary = "Declares a KV read requirement to trigger CAP101 when unbound",
    effects = "ReadOnly",
    determinism = "BestEffort",
    resources(kv_read(capabilities::kv::KeyValue))
)]
async fn kv_read(payload: JsonValue) -> NodeResult<JsonValue> {
    Ok(payload)
}

#[def_node(
    name = "Capture",
    summary = "Capture terminal output",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture(payload: JsonValue) -> NodeResult<JsonValue> {
    Ok(payload)
}

dag_macros::flow! {
    name: s4_preflight_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Demonstrates CAP101 preflight failures when required capabilities are missing";

    let trigger = node!(http_trigger);
    let kv_read = node!(kv_read);
    let capture = node!(capture);

    connect!(trigger -> kv_read);
    connect!(kv_read -> capture);

    entrypoint!({
        trigger: "trigger",
        capture: "capture",
        route_aliases: ["/preflight"],
        method: "POST",
        deadline_ms: 250,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_serialises_to_json() {
        let ir = flow();
        let json = serde_json::to_value(&ir).expect("serialise flow");
        assert_eq!(json["profile"], serde_json::json!("web"));
    }
}
