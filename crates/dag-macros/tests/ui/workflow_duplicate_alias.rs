use dag_core::NodeResult;
use dag_macros::{node, workflow};

#[node(name = "First")]
async fn first(input: ()) -> NodeResult<()> {
    let _ = input;
    Ok(())
}

#[node(name = "Second")]
async fn second(input: ()) -> NodeResult<()> {
    let _ = input;
    Ok(())
}

workflow! {
    name: dup_flow,
    version: "1.2.3",
    profile: Web,
    nodes: {
        first_alias => first_node_spec(),
        first_alias => second_node_spec(),
    },
    edges: [
        first_alias => first_alias,
    ],
}
