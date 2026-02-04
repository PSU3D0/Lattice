use dag_core::prelude::*;
use dag_macros::workflow;

const SOURCE_SPEC: NodeSpec = NodeSpec::inline(
    "tests::source",
    "Source",
    SchemaSpec::Opaque,
    SchemaSpec::Opaque,
    Effects::Pure,
    Determinism::Strict,
    None,
);

const SINK_SPEC: NodeSpec = NodeSpec::inline(
    "tests::sink",
    "Sink",
    SchemaSpec::Opaque,
    SchemaSpec::Opaque,
    Effects::Pure,
    Determinism::Strict,
    None,
);

workflow! {
    name: connect_requires_typed_binding,
    version: "1.0.0",
    profile: Dev;

    let source = &SOURCE_SPEC;
    let sink = &SINK_SPEC;

    connect!(source -> sink);
}

fn main() {}
