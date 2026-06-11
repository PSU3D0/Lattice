use dag_core::NodeResult;
use dag_macros::def_node;

struct DbWriter;

#[def_node(
    name = "DbWriter",
    effects = "Pure",
    determinism = "BestEffort",
    resources(db_writer(DbWriter))
)]
async fn db_writer(input: ()) -> NodeResult<()> {
    let _ = input;
    Ok(())
}

fn main() {}
