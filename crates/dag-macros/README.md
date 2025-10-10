# dag-macros

`dag-macros` hosts the procedural macros and attribute helpers that make the LatticeFlow DSL ergonomic for humans and agents. It expands `#[node]`, `workflow!`, `switch!`, and the control-flow attribute wrappers so Flow IR metadata stays in sync with idiomatic Rust code.

## Surface
- Proc-macro entry points for nodes, triggers, workflows, inline nodes, and attribute helpers like `#[flow::switch]`.
- Diagnostics infrastructure keyed to the error-code registry (`DAG*`, `CTRL*`, etc.).
- Glue for emitting Flow IR JSON alongside the generated Rust.

## Next steps
- Implement macro parsing/expansion per RFC §4 and emit rich diagnostics via `syn`/`quote`.
- Integrate the control-surface metadata hooks for branching/looping helpers.
- Add `trybuild` test suites covering success/failure cases and lint signalling.

## Depends on
- `dag-core` must provide the trait and IR definitions consumed by the macros.
- Error-code registry from Phase 0 should be available for diagnostic IDs.
