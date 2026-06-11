use dag_core::NodeResult;
use dag_macros::def_node;

#[def_node(summary = "Missing name", effects = "Pure", determinism = "Strict")]
async fn missing(input: ()) -> NodeResult<()> {
    let _ = input;
    Ok(())
}

fn main() {}
