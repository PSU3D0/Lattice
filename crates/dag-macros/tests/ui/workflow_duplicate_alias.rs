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
    profile: Web;
    let first_alias = first_node_spec();
    let first_alias = second_node_spec();
    connect!(first_alias -> first_alias);
}

fn main() {}
