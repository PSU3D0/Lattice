#![allow(unused_imports)]

use dag_core::NodeResult;
use dag_macros::{def_node, node, workflow};

#[def_node(
    name = "ProduceString",
    summary = "ProduceString",
    effects = "Pure",
    determinism = "Strict"
)]
async fn produce_string(_input: ()) -> NodeResult<String> {
    Ok("ok".to_string())
}

#[def_node(
    name = "ConsumeU8",
    summary = "ConsumeU8",
    effects = "Pure",
    determinism = "Strict"
)]
async fn consume_u8(_input: u8) -> NodeResult<()> {
    Ok(())
}

workflow! {
    name: connect_requires_into,
    version: "1.0.0",
    profile: Dev;

    let producer = node!(produce_string);
    let consumer = node!(consume_u8);

    connect!(producer -> consumer);
}

fn main() {}
