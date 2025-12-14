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
    name: if_missing_edge,
    version: "1.0.0",
    profile: Dev;

    let route = &ROUTE_SPEC;
    let then_branch = &BRANCH_SPEC;
    let else_branch = &BRANCH_SPEC;

    connect!(route -> then_branch);

    if_!(
        source = route,
        selector_pointer = "/ok",
        then = then_branch,
        else = else_branch
    );
}

fn main() {}
