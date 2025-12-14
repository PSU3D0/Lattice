use dag_core::prelude::*;
use dag_macros::workflow;

const TRIGGER_SPEC: NodeSpec = NodeSpec {
    identifier: "tests::trigger",
    name: "Trigger",
    kind: NodeKind::Trigger,
    summary: None,
    in_schema: SchemaSpec::Opaque,
    out_schema: SchemaSpec::Opaque,
    effects: Effects::Pure,
    determinism: Determinism::Strict,
    determinism_hints: &[],
    effect_hints: &[],
};

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
    name: spill_missing_edge,
    version: "1.0.0",
    profile: Dev;

    let trigger = &TRIGGER_SPEC;
    let sink = &SINK_SPEC;

    spill!(trigger -> sink, tier = "local");
}

fn main() {}
