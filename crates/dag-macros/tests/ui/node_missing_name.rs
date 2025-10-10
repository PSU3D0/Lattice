use dag_core::NodeResult;
use dag_macros::node;

#[node]
async fn missing(input: ()) -> NodeResult<()> {
    let _ = input;
    Ok(())
}
