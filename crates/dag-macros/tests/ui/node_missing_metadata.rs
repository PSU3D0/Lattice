#![deny(warnings)]

use dag_core::NodeResult;
use dag_macros::def_node;

#[def_node(name = "MissingMetadata")]
async fn missing_metadata(input: ()) -> NodeResult<()> {
    let _ = input;
    Ok(())
}

fn main() {}
