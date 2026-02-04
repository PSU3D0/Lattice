use dag_core::{Determinism, Effects, NodeKind, NodeResult};
use dag_macros::{def_node, node};

#[def_node(trigger, name = "OnWebhook")]
async fn on_webhook(input: String) -> NodeResult<String> {
    Ok(input)
}

#[test]
fn node_trigger_shorthand_sets_kind() {
    let spec = node!(on_webhook);
    assert_eq!(spec.kind, NodeKind::Trigger);
    assert_eq!(spec.effects, Effects::ReadOnly);
    assert_eq!(spec.determinism, Determinism::Strict);
}
