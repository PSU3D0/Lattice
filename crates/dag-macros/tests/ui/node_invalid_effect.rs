use dag_core::NodeResult;
use dag_macros::node;

#[node(name = "Invalid", effects = "Unknown")]
async fn invalid(input: ()) -> NodeResult<()> {
    let _ = input;
    Ok(())
}
