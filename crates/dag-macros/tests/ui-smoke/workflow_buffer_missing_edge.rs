#![allow(unused_imports)]

use dag_core::prelude::*;
use dag_macros::{def_node, node, workflow};

#[def_node(
    trigger,
    name = "Trigger",
    summary = "Trigger",
    effects = "Pure",
    determinism = "Strict"
)]
async fn trigger(_input: ()) -> NodeResult<()> {
    Ok(())
}

#[def_node(
    name = "Sink",
    summary = "Sink",
    effects = "Pure",
    determinism = "Strict"
)]
async fn sink(_input: ()) -> NodeResult<()> {
    Ok(())
}

workflow! {
    name: buffer_missing_edge,
    version: "1.0.0",
    profile: Dev;

    let trigger = node!(trigger);
    let sink = node!(sink);

    buffer!(trigger -> sink, max_items = 10);
}

fn main() {}
