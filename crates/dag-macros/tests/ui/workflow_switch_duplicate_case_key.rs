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
    name: switch_duplicate_case_key,
    version: "1.0.0",
    profile: Dev;

    let route = &ROUTE_SPEC;
    let a = &BRANCH_SPEC;
    let b = &BRANCH_SPEC;

    connect!(route -> a);
    connect!(route -> b);

    switch!(
        source = route,
        selector_pointer = "/type",
        cases = { "a" => a, "a" => b }
    );
}

fn main() {}
