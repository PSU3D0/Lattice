use dag_core::NodeResult;
use dag_macros::def_node;

struct BlobClient;

#[def_node(
    name = "BlobReader",
    effects = "ReadOnly",
    determinism = "Strict",
    resources(blob_fetch(BlobClient))
)]
async fn blob_reader(input: ()) -> NodeResult<()> {
    let _ = input;
    Ok(())
}

fn main() {}
