use dag_core::{ControlSurfaceIR, ControlSurfaceKind, FlowIR, NodeResult};
use dag_macros::{node, trigger};
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

mod bundle_def {
    use super::{
        capture_node_spec, capture_register, http_trigger_node_spec, http_trigger_register,
        passthrough_node_spec, passthrough_register,
    };

    dag_macros::workflow_bundle! {
        name: s5_unsupported_surface_flow,
        version: "1.0.0",
        profile: Web,
        summary: "Demonstrates CTRL901 when a reserved surface is present but unimplemented";

        let trigger = http_trigger_node_spec();
        let passthrough = passthrough_node_spec();
        let capture = capture_node_spec();

        connect!(trigger -> passthrough);
        connect!(passthrough -> capture);

        entrypoint!({
            trigger: "trigger",
            capture: "capture",
            route: "/unsupported",
            method: "POST",
            deadline_ms: 250,
        });
    }
}

pub fn flow() -> FlowIR {
    let mut flow = bundle_def::flow();
    flow.control_surfaces.push(ControlSurfaceIR {
        id: "rate_limit:0".to_string(),
        kind: ControlSurfaceKind::RateLimit,
        targets: vec![],
        config: json!({"v": 1, "target": "trigger", "qps": 1, "burst": 1}),
    });
    flow
}

pub fn validated_ir() -> ValidatedIR {
    validate(&flow()).expect("S5 flow should validate")
}

pub fn bundle() -> host_inproc::FlowBundle {
    let mut bundle = bundle_def::bundle();
    bundle.validated_ir = validated_ir();
    bundle
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
