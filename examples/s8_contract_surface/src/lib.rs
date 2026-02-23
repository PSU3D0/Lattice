use dag_core::NodeResult;
use dag_macros::{def_node, node, subflow};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExternalPayload {
    pub value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChildInput {
    pub value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChildOutput {
    pub value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParentOutput {
    pub value: String,
}

#[def_node(
    trigger,
    name = "ChildTrigger",
    summary = "Child flow trigger",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn child_trigger(input: ChildInput) -> NodeResult<ChildInput> {
    Ok(input)
}

#[def_node(
    name = "ChildCapture",
    summary = "Child flow capture",
    effects = "Pure",
    determinism = "Strict"
)]
async fn child_capture(input: ChildInput) -> NodeResult<ChildOutput> {
    Ok(ChildOutput { value: input.value })
}

dag_macros::flow! {
    name: child_contract_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Child flow with exported typed entry contract";
    let trigger = node!(child_trigger);
    let capture = node!(child_capture);
    connect!(trigger -> capture);
    entrypoint!({
        trigger: "trigger",
        capture: "capture",
    });
}

impl From<ExternalPayload> for child_contract_flow::contract::trigger::In {
    fn from(value: ExternalPayload) -> Self {
        let raw = child_contract_flow::contract::trigger::RawIn { value: value.value };
        Self(raw)
    }
}

#[def_node(
    trigger,
    name = "ParentTrigger",
    summary = "Parent ingress trigger",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn parent_trigger(input: ExternalPayload) -> NodeResult<ExternalPayload> {
    Ok(input)
}

#[def_node(
    name = "AdaptExternalToChild",
    summary = "Explicit adapter: external payload -> child contract input",
    effects = "Pure",
    determinism = "Strict"
)]
async fn adapt_external_to_child(
    input: ExternalPayload,
) -> NodeResult<child_contract_flow::contract::trigger::RawIn> {
    let wrapped: child_contract_flow::contract::trigger::In = input.into();
    Ok(wrapped.into())
}

#[def_node(
    name = "ParentCapture",
    summary = "Parent capture receives child subflow output",
    effects = "Pure",
    determinism = "Strict"
)]
async fn parent_capture(
    input: child_contract_flow::contract::trigger::RawOut,
) -> NodeResult<ParentOutput> {
    Ok(ParentOutput { value: input.value })
}

dag_macros::workflow! {
    name: contract_surface_parent_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Parent flow using explicit adapter + child contract exports";
    let trigger = node!(parent_trigger);
    let adapt = node!(adapt_external_to_child);
    let child = subflow!(child_contract_flow::trigger);
    let capture = node!(parent_capture);
    connect!(trigger -> adapt);
    connect!(adapt -> child);
    connect!(child -> capture);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn child_contract_exports_are_stable() {
        let _: dag_core::FlowEntrypoint<
            child_contract_flow::contract::trigger::RawIn,
            child_contract_flow::contract::trigger::RawOut,
        > = child_contract_flow::contract::trigger::ENTRY;

        assert_eq!(
            child_contract_flow::contract::trigger::CONTRACT_ID,
            "child_contract_flow@1.0.0:trigger->capture"
        );

        assert_eq!(
            std::mem::size_of::<child_contract_flow::contract::trigger::In>(),
            std::mem::size_of::<child_contract_flow::contract::trigger::RawIn>()
        );
    }

    #[test]
    fn parent_flow_builds_with_explicit_adapter_path() {
        let ir = contract_surface_parent_flow();
        assert_eq!(ir.nodes.len(), 4);
        assert_eq!(ir.edges.len(), 3);
    }
}
