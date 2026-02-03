#![allow(unused_imports)]

use dag_core::NodeResult;
use dag_macros::{def_node, node, workflow};

#[def_node(name = "First")]
async fn first(input: ()) -> NodeResult<()> {
    let _ = input;
    Ok(())
}

#[def_node(name = "Second")]
async fn second(input: ()) -> NodeResult<()> {
    let _ = input;
    Ok(())
}

workflow! {
    name: dup_flow,
    version: "1.2.3",
    profile: Web;
    let first_alias = node!(first);
    let first_alias = node!(second);
    connect!(first_alias -> first_alias);
}

fn main() {}
