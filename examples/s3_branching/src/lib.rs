use dag_core::NodeResult;
use dag_macros::{node, trigger};
use serde_json::{Value as JsonValue, json};

#[trigger(
    name = "HttpTrigger",
    summary = "Ingress trigger for branching example"
)]
async fn http_trigger(payload: JsonValue) -> NodeResult<JsonValue> {
    Ok(payload)
}

#[node(
    name = "Route",
    summary = "Normalize input into a routing object",
    effects = "Pure",
    determinism = "Strict"
)]
async fn route(payload: JsonValue) -> NodeResult<JsonValue> {
    let flag = payload
        .get("flag")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);
    let mode = payload
        .get("mode")
        .and_then(JsonValue::as_str)
        .unwrap_or("default")
        .to_string();

    Ok(json!({
        "flag": flag,
        "mode": mode,
        "input": payload
    }))
}

#[node(
    name = "ThenBranch",
    summary = "Annotate then branch prior to switch",
    effects = "Pure",
    determinism = "Strict"
)]
async fn then_branch(payload: JsonValue) -> NodeResult<JsonValue> {
    let mut obj = payload.as_object().cloned().unwrap_or_default();
    obj.insert("branch".to_string(), json!("then"));
    Ok(JsonValue::Object(obj))
}

#[node(
    name = "ElseBranch",
    summary = "Annotate else branch and return directly",
    effects = "Pure",
    determinism = "Strict"
)]
async fn else_branch(payload: JsonValue) -> NodeResult<JsonValue> {
    let mut obj = payload.as_object().cloned().unwrap_or_default();
    obj.insert("branch".to_string(), json!("else"));
    Ok(JsonValue::Object(obj))
}

#[node(
    name = "ModeRouter",
    summary = "Prepare value for switch routing",
    effects = "Pure",
    determinism = "Strict"
)]
async fn mode_router(payload: JsonValue) -> NodeResult<JsonValue> {
    Ok(payload)
}

#[node(
    name = "BranchA",
    summary = "Handle mode a",
    effects = "Pure",
    determinism = "Strict"
)]
async fn branch_a(payload: JsonValue) -> NodeResult<JsonValue> {
    let mut obj = payload.as_object().cloned().unwrap_or_default();
    obj.insert("mode_selected".to_string(), json!("a"));
    Ok(JsonValue::Object(obj))
}

#[node(
    name = "BranchB",
    summary = "Handle mode b",
    effects = "Pure",
    determinism = "Strict"
)]
async fn branch_b(payload: JsonValue) -> NodeResult<JsonValue> {
    let mut obj = payload.as_object().cloned().unwrap_or_default();
    obj.insert("mode_selected".to_string(), json!("b"));
    Ok(JsonValue::Object(obj))
}

#[node(
    name = "BranchDefault",
    summary = "Handle default mode",
    effects = "Pure",
    determinism = "Strict"
)]
async fn branch_default(payload: JsonValue) -> NodeResult<JsonValue> {
    let mut obj = payload.as_object().cloned().unwrap_or_default();
    obj.insert("mode_selected".to_string(), json!("default"));
    Ok(JsonValue::Object(obj))
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

dag_macros::workflow_bundle! {
    name: s3_branching_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Demonstrates if_! and switch! branching";

    let trigger = http_trigger_node_spec();
    let route = route_node_spec();
    let then_branch = then_branch_node_spec();
    let else_branch = else_branch_node_spec();
    let mode_router = mode_router_node_spec();
    let branch_a = branch_a_node_spec();
    let branch_b = branch_b_node_spec();
    let branch_default = branch_default_node_spec();
    let capture = capture_node_spec();

    connect!(trigger -> route);
    connect!(route -> then_branch);
    connect!(route -> else_branch);

    if_!(
        source = route,
        selector_pointer = "/flag",
        then = then_branch,
        else = else_branch
    );

    connect!(then_branch -> mode_router);
    connect!(mode_router -> branch_a);
    connect!(mode_router -> branch_b);
    connect!(mode_router -> branch_default);

    switch!(
        source = mode_router,
        selector_pointer = "/mode",
        cases = { "a" => branch_a, "b" => branch_b },
        default = branch_default
    );

    connect!(branch_a -> capture);
    connect!(branch_b -> capture);
    connect!(branch_default -> capture);
    connect!(else_branch -> capture);

    entrypoint!({
        trigger: "trigger",
        capture: "capture",
        route: "/branch",
        method: "POST",
        deadline_ms: 250,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flow_contains_expected_nodes() {
        let ir = flow();
        let aliases: Vec<_> = ir.nodes.iter().map(|node| node.alias.as_str()).collect();
        assert!(aliases.contains(&"route"));
        assert!(aliases.contains(&"mode_router"));
        assert_eq!(ir.edges.len(), 11);
        assert_eq!(ir.control_surfaces.len(), 2);
    }
}
