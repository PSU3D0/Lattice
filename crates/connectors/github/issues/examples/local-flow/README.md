# example-connector-github-issues-local-flow

A canonical local flow example for `connector_github_issues`.

## What it is

This is a **full example crate** that wires a small flow:

- local trigger
- `connector.github.issues.list`
- local capture

The example runs through:
- `dag_macros::flow!`
- `NodeRegistry`
- `host-inproc::FlowBundle`
- a real HTTP capability implementation (`ReqwestHttpClient`)

## Default mode: mock-first

If no endpoint override is configured, the example starts a local mock GitHub-like
upstream and runs the flow against it.

```bash
cd codebase/.sessions/lat-000028-impl
cargo run -p example-connector-github-issues-local-flow
```

## Real endpoint mode

Point the example at a real GitHub-compatible endpoint:

```bash
export LATTICE_CONNECTOR_ENDPOINT_GITHUB_DEFAULT_BASE_URL=https://api.github.com
export LATTICE_EXAMPLE_GITHUB_OWNER=rust-lang
export LATTICE_EXAMPLE_GITHUB_REPO=cargo
cargo run -p example-connector-github-issues-local-flow
```

Optional auth:

```bash
export LATTICE_CONNECTOR_AUTH_GITHUB_PAT=ghp_your_token_here
```

## Why this crate exists

This example is intentionally a **full Cargo package**, not just a crate-local
`examples/*.rs` target, so that connector examples can eventually carry richer
package/deployment/entrypoint semantics the same way the top-level workspace
examples do.
