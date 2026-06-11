use dag_macros::flow_struct;

// `#[flow_struct]` is a derive bundle for struct/enum payloads; applying it to a
// function is a misuse and must produce a clear diagnostic.
#[flow_struct]
fn not_a_struct() {}

fn main() {}
