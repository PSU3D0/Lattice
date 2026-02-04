use dag_core::prelude::*;
use dag_macros::{def_node, node, workflow};

#[def_node(trigger, name = "Trigger")]
async fn trigger(_input: ()) -> NodeResult<()> {
    Ok(())
}

#[def_node(name = "Sink")]
async fn sink(_input: ()) -> NodeResult<()> {
    Ok(())
}

workflow! {
    name: timeout_flow,
    version: "1.0.0",
    profile: Dev;

    let trigger = node!(trigger);
    let sink = node!(sink);

    connect!(trigger -> sink);
    timeout!(trigger -> sink, ms = 250);
}

#[test]
fn workflow_timeout_emits_edge_timeout_ms() {
    let flow = timeout_flow();
    assert_eq!(flow.edges.len(), 1);
    assert_eq!(flow.edges[0].timeout_ms, Some(250));
}
