use std::sync::Arc;
use std::time::Duration;

use dag_core::{FlowIR, NodeResult};
use dag_macros::{node, trigger};
use kernel_exec::{FlowExecutor, NodeRegistry};
use kernel_plan::{ValidatedIR, validate};
use serde_json::Value as JsonValue;

#[trigger(
    name = "HttpTrigger",
    summary = "Ingress trigger for preflight example"
)]
async fn http_trigger(payload: JsonValue) -> NodeResult<JsonValue> {
    Ok(payload)
}

#[node(
    name = "KvRead",
    summary = "Declares a KV read requirement to trigger CAP101 when unbound",
    effects = "ReadOnly",
    determinism = "BestEffort",
    resources(kv_read(capabilities::kv::KeyValue))
)]
async fn kv_read(payload: JsonValue) -> NodeResult<JsonValue> {
    Ok(payload)
}

#[node(
    name = "Capture",
    summary = "Capture terminal output",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture(payload: JsonValue) -> NodeResult<JsonValue> {
    Ok(payload)
}

dag_macros::workflow! {
    name: s4_preflight_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Demonstrates CAP101 preflight failures when required capabilities are missing";

    let trigger = http_trigger_node_spec();
    let kv_read = kv_read_node_spec();
    let capture = capture_node_spec();

    connect!(trigger -> kv_read);
    connect!(kv_read -> capture);
}

pub fn flow() -> FlowIR {
    s4_preflight_flow()
}

pub const TRIGGER_ALIAS: &str = "trigger";
pub const CAPTURE_ALIAS: &str = "capture";
pub const ROUTE_PATH: &str = "/preflight";
pub const DEADLINE: Duration = Duration::from_millis(250);

pub fn executor() -> FlowExecutor {
    let mut registry = NodeRegistry::new();
    registry
        .register_fn("example_s4_preflight::http_trigger", http_trigger)
        .expect("register http_trigger");
    registry
        .register_fn("example_s4_preflight::kv_read", kv_read)
        .expect("register kv_read");
    registry
        .register_fn("example_s4_preflight::capture", capture)
        .expect("register capture");
    FlowExecutor::new(Arc::new(registry))
}

pub fn validated_ir() -> ValidatedIR {
    validate(&flow()).expect("S4 flow should validate")
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
