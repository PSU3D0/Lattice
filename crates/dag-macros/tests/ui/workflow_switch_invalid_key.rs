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
    name: switch_invalid_key,
    version: "1.0.0",
    profile: Dev;

    let route = node!(route);
    let a = node!(branch);

    connect!(route -> a);

    switch!(
        source = route,
        pointer = "/type",
        cases = { "a" => a }
    );
}

fn main() {}
