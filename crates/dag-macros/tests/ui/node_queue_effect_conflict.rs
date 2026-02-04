use dag_core::NodeResult;
use dag_macros::def_node;

struct QueueClient;

#[def_node(
    name = "QueuePublisher",
    effects = "Pure",
    determinism = "BestEffort",
    resources(queue_publish(QueueClient))
)]
async fn queue_publish_node(input: ()) -> NodeResult<()> {
    let _ = input;
    Ok(())
}

fn main() {}
