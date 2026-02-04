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
    name: switch_duplicate_case_key,
    version: "1.0.0",
    profile: Dev;

    let route = node!(route);
    let a = node!(branch);
    let b = node!(branch);

    connect!(route -> a);
    connect!(route -> b);

    switch!(
        source = route,
        selector_pointer = "/type",
        cases = { "a" => a, "a" => b }
    );
}

fn main() {}
