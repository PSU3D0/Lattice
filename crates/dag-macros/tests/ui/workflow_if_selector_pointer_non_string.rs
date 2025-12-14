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
    name: if_selector_pointer_non_string,
    version: "1.0.0",
    profile: Dev;

    let route = &ROUTE_SPEC;
    let a = &BRANCH_SPEC;
    let b = &BRANCH_SPEC;

    connect!(route -> a);
    connect!(route -> b);

    if_!(
        source = route,
        selector_pointer = 123,
        then = a,
        else = b
    );
}

fn main() {}
