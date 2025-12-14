use dag_core::prelude::*;
use dag_macros::workflow;

const ROUTE_SPEC: NodeSpec = NodeSpec::inline(
    "tests::route",
    "Route",
    SchemaSpec::Opaque,
    SchemaSpec::Opaque,
    Effects::Pure,
    Determinism::Strict,
    None,
);

const BRANCH_SPEC: NodeSpec = NodeSpec::inline(
    "tests::branch",
    "Branch",
    SchemaSpec::Opaque,
    SchemaSpec::Opaque,
    Effects::Pure,
    Determinism::Strict,
    None,
);

workflow! {
    name: switch_missing_edge,
    version: "1.0.0",
    profile: Dev;

    let route = &ROUTE_SPEC;
    let a = &BRANCH_SPEC;
    let b = &BRANCH_SPEC;

    connect!(route -> a);

    switch!(
        source = route,
        selector_pointer = "/type",
        cases = { "a" => a, "b" => b }
    );
}

fn main() {}
