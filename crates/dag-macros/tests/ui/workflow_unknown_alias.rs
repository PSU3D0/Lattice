use dag_core::NodeResult;
use dag_macros::{def_node, node, workflow};

#[def_node(name = "Producer")]
async fn producer(_: ()) -> NodeResult<()> {
    Ok(())
}

#[def_node(name = "Consumer")]
async fn consumer(_: ()) -> NodeResult<()> {
    Ok(())
}

workflow! {
    name: missing_alias_flow,
    version: "0.1.0",
    profile: Web;
    let prod = node!(producer);
    let cons = node!(consumer);
    connect!(prod -> missing);
}

fn main() {}
