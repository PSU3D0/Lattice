use dag_core::prelude::*;
use dag_macros::{def_node, flow, node};

#[def_node(
    trigger,
    name = "Entry",
    summary = "Entry",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn entry(_input: ()) -> NodeResult<()> {
    Ok(())
}

#[def_node(
    name = "Capture",
    summary = "Capture",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture(_input: ()) -> NodeResult<()> {
    Ok(())
}

flow! {
    name: flow_bundle_host_gated,
    version: "1.0.0",
    profile: Dev;
    let entry = node!(entry);
    let capture = node!(capture);
    connect!(entry -> capture);
    entrypoint!({
        trigger: "entry",
        capture: "capture",
    });
}

fn main() {}
