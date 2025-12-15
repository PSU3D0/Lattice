use std::sync::Arc;
use std::time::Duration;

use dag_core::{ControlSurfaceIR, ControlSurfaceKind, FlowIR, NodeResult};
use dag_macros::{node, trigger};
use kernel_exec::{FlowExecutor, NodeRegistry};
use kernel_plan::{ValidatedIR, validate};
use serde_json::{Value as JsonValue, json};

#[trigger(
    name = "HttpTrigger",
    summary = "Ingress trigger for unsupported surface example"
)]
async fn http_trigger(payload: JsonValue) -> NodeResult<JsonValue> {
    Ok(payload)
}

#[node(
    name = "PassThrough",
    summary = "Pass through value to capture",
    effects = "Pure",
    determinism = "Strict"
)]
async fn passthrough(payload: JsonValue) -> NodeResult<JsonValue> {
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
    name: s5_unsupported_surface_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Demonstrates CTRL901 when a reserved surface is present but unimplemented";

    let trigger = http_trigger_node_spec();
    let passthrough = passthrough_node_spec();
    let capture = capture_node_spec();

    connect!(trigger -> passthrough);
    connect!(passthrough -> capture);
}

pub fn flow() -> FlowIR {
    let mut flow = s5_unsupported_surface_flow();
    flow.control_surfaces.push(ControlSurfaceIR {
        id: "rate_limit:0".to_string(),
        kind: ControlSurfaceKind::RateLimit,
        targets: vec![],
        config: json!({"v": 1, "target": "trigger", "qps": 1, "burst": 1}),
    });
    flow
}

pub const TRIGGER_ALIAS: &str = "trigger";
pub const CAPTURE_ALIAS: &str = "capture";
pub const ROUTE_PATH: &str = "/unsupported";
pub const DEADLINE: Duration = Duration::from_millis(250);

pub fn executor() -> FlowExecutor {
    let mut registry = NodeRegistry::new();
    registry
        .register_fn("example_s5_unsupported_surface::http_trigger", http_trigger)
        .expect("register http_trigger");
    registry
        .register_fn("example_s5_unsupported_surface::passthrough", passthrough)
        .expect("register passthrough");
    registry
        .register_fn("example_s5_unsupported_surface::capture", capture)
        .expect("register capture");
    FlowExecutor::new(Arc::new(registry))
}

pub fn validated_ir() -> ValidatedIR {
    validate(&flow()).expect("S5 flow should validate")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_contains_unsupported_surface() {
        let ir = flow();
        assert_eq!(ir.control_surfaces.len(), 1);
        assert_eq!(ir.control_surfaces[0].kind.as_str(), "rate_limit");
    }
}
