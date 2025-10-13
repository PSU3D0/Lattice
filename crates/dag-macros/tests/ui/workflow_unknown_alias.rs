use dag_core::NodeResult;
use dag_macros::{node, workflow};

#[node(name = "Producer")]
async fn producer(_: ()) -> NodeResult<()> {
    Ok(())
}

#[node(name = "Consumer")]
async fn consumer(_: ()) -> NodeResult<()> {
    Ok(())
}

workflow! {
    name: missing_alias_flow,
    version: "0.1.0",
    profile: Web;
    let prod = producer_node_spec();
    let cons = consumer_node_spec();
    connect!(prod -> missing);
}

fn main() {}
