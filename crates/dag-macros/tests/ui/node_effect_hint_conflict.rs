use dag_core::NodeResult;
use dag_macros::node;

#[node(name = "Writer", effects = "Pure", resources(http(HttpWrite)))]
async fn writer(input: ()) -> NodeResult<()> {
    let _ = input;
    Ok(())
}

fn main() {}
