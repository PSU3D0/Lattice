#[cfg(all(feature = "host-bundle", not(target_arch = "wasm32")))]
use std::sync::Arc;

use dag_core::{FlowIR, NodeResult};
use dag_macros::def_node;
use kernel_plan::{ValidatedIR, validate};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TextRequest {
    pub value: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TextResponse {
    pub value: String,
    pub flow: String,
}

#[def_node(
    trigger,
    name = "TextTrigger",
    summary = "Ingress trigger for the multiflow wasm smoke example",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn text_trigger(input: TextRequest) -> NodeResult<TextRequest> {
    Ok(input)
}

#[def_node(
    name = "UppercaseText",
    summary = "Uppercase the input payload",
    effects = "Pure",
    determinism = "Strict"
)]
async fn uppercase_text(input: TextRequest) -> NodeResult<TextResponse> {
    Ok(TextResponse {
        value: input.value.to_uppercase(),
        flow: "upper".to_string(),
    })
}

#[def_node(
    name = "ReverseText",
    summary = "Reverse the input payload",
    effects = "Pure",
    determinism = "Strict"
)]
async fn reverse_text(input: TextRequest) -> NodeResult<TextResponse> {
    Ok(TextResponse {
        value: input.value.chars().rev().collect(),
        flow: "reverse".to_string(),
    })
}

#[def_node(
    name = "Capture",
    summary = "Capture the transformed payload",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture(input: TextResponse) -> NodeResult<TextResponse> {
    Ok(input)
}

mod upper_bundle_def {
    #[cfg(feature = "host-bundle")]
    use super::{capture_register, text_trigger_register, uppercase_text_register};
    use dag_macros::node;

    dag_macros::flow! {
        name: s10_multiflow_upper_flow,
        version: "1.0.0",
        profile: Web,
        summary: "Uppercase branch of the minimal multiflow wasm bundle example";

        let trigger = node!(text_trigger);
        let transform = node!(uppercase_text);
        let capture = node!(capture);

        connect!(trigger -> transform);
        connect!(transform -> capture);

        entrypoint!({
            trigger: "trigger",
            capture: "capture",
            route_aliases: ["/multi/upper"],
            method: "POST",
            deadline_ms: 1_000,
        });
    }
}

mod reverse_bundle_def {
    #[cfg(feature = "host-bundle")]
    use super::{capture_register, reverse_text_register, text_trigger_register};
    use dag_macros::node;

    dag_macros::flow! {
        name: s10_multiflow_reverse_flow,
        version: "1.0.0",
        profile: Web,
        summary: "Reverse branch of the minimal multiflow wasm bundle example";

        let trigger = node!(text_trigger);
        let transform = node!(reverse_text);
        let capture = node!(capture);

        connect!(trigger -> transform);
        connect!(transform -> capture);

        entrypoint!({
            trigger: "trigger",
            capture: "capture",
            route_aliases: ["/multi/reverse"],
            method: "POST",
            deadline_ms: 1_000,
        });
    }
}

pub fn upper_flow() -> FlowIR {
    upper_bundle_def::flow()
}

pub fn reverse_flow() -> FlowIR {
    reverse_bundle_def::flow()
}

pub fn validated_upper_ir() -> ValidatedIR {
    validate(&upper_flow()).expect("s10 upper flow should validate")
}

pub fn validated_reverse_ir() -> ValidatedIR {
    validate(&reverse_flow()).expect("s10 reverse flow should validate")
}

#[cfg(all(feature = "host-bundle", not(target_arch = "wasm32")))]
pub fn upper_bundle() -> host_inproc::FlowBundle {
    bundle_for(validated_upper_ir(), upper_flow(), "/multi/upper", || {
        let mut registry = kernel_exec::NodeRegistry::new();
        text_trigger_register(&mut registry).expect("register trigger");
        uppercase_text_register(&mut registry).expect("register uppercase");
        capture_register(&mut registry).expect("register capture");
        registry
    })
}

#[cfg(all(feature = "host-bundle", not(target_arch = "wasm32")))]
pub fn reverse_bundle() -> host_inproc::FlowBundle {
    bundle_for(
        validated_reverse_ir(),
        reverse_flow(),
        "/multi/reverse",
        || {
            let mut registry = kernel_exec::NodeRegistry::new();
            text_trigger_register(&mut registry).expect("register trigger");
            reverse_text_register(&mut registry).expect("register reverse");
            capture_register(&mut registry).expect("register capture");
            registry
        },
    )
}

#[cfg(all(feature = "host-bundle", not(target_arch = "wasm32")))]
fn bundle_for(
    validated_ir: ValidatedIR,
    flow: FlowIR,
    route_path: &str,
    registry_builder: impl FnOnce() -> kernel_exec::NodeRegistry,
) -> host_inproc::FlowBundle {
    use std::time::Duration;

    use host_inproc::{FlowBundle, FlowEntrypoint, NodeContract, NodeSource};
    use kernel_exec::RegistryResolver;

    let registry = registry_builder();
    let node_contracts = flow
        .nodes
        .iter()
        .map(|node| NodeContract {
            identifier: node.identifier.clone(),
            contract_hash: None,
            source: NodeSource::Local,
        })
        .collect();

    FlowBundle {
        validated_ir,
        entrypoints: vec![FlowEntrypoint {
            trigger_alias: "trigger".to_string(),
            capture_alias: "capture".to_string(),
            route_path: Some(route_path.to_string()),
            method: Some("POST".to_string()),
            deadline: Some(Duration::from_millis(1_000)),
            route_aliases: vec![route_path.to_string()],
        }],
        resolver: Arc::new(RegistryResolver::new(Arc::new(registry))),
        node_contracts,
        environment_plugins: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_flows_validate() {
        let upper = validated_upper_ir();
        let reverse = validated_reverse_ir();
        assert_ne!(upper.flow().id, reverse.flow().id);
    }
}
