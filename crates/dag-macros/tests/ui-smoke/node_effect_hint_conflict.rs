use dag_core::NodeResult;
use dag_macros::def_node;

#[def_node(name = "Writer", effects = "Pure", resources(http(HttpWrite)))]
async fn writer(input: ()) -> NodeResult<()> {
    let _ = input;
    Ok(())
}

fn main() {}
