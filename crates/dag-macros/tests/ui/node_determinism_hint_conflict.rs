use dag_core::NodeResult;
use dag_macros::node;

#[node(
    name = "Clocky",
    determinism = "Strict",
    resources(clock(Clock))
)]
async fn clocky(input: ()) -> NodeResult<()> {
    let _ = input;
    Ok(())
}

fn main() {}
