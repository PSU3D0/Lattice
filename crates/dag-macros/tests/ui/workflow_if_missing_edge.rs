#![allow(unused_imports)]

use dag_core::prelude::*;
use dag_macros::{def_node, node, workflow};

#[def_node(
    name = "Route",
    summary = "Route",
    effects = "Pure",
    determinism = "Strict"
)]
async fn route(_input: ()) -> NodeResult<()> {
    Ok(())
}

#[def_node(
    name = "Branch",
    summary = "Branch",
    effects = "Pure",
    determinism = "Strict"
)]
async fn branch(_input: ()) -> NodeResult<()> {
    Ok(())
}

workflow! {
    name: if_missing_edge,
    version: "1.0.0",
    profile: Dev;

    let route = node!(route);
    let then_branch = node!(branch);
    let else_branch = node!(branch);

    connect!(route -> then_branch);

    if_!(
        source = route,
        selector_pointer = "/ok",
        then = then_branch,
        else = else_branch
    );
}

fn main() {}
