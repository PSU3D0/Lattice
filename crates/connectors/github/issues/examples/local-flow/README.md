# example-connector-github-issues-local-flow

A canonical portable flow example for `connector_github_issues`.

## What it is

This crate defines a small flow:

- local trigger
- `connector.github.issues.list`
- local capture

The flow is intentionally shaped like the top-level examples:
- `dag_macros::flow!` defines the workflow
- `bundle()` is macro-generated via the standard `host-bundle` path
- `flows run local --example ...` and `flows run serve --example ...` are the canonical native execution surfaces
- the package can also be bundled with `flows bundle -p ... --wasm`

## Native execution

Run locally through the standard CLI:

```bash
cargo run -p flows-cli -- run local \
  --example connector_github_issues_local_flow \
  --bindings-lock <bindings.lock.json> \
  --payload '{"owner":"rust-lang","repo":"cargo"}'
```

Serve over Axum through the standard CLI:

```bash
cargo run -p flows-cli -- run serve \
  --example connector_github_issues_local_flow \
  --bindings-lock <bindings.lock.json>
```

Then POST JSON to `/github/issues/local`.

## Why this crate exists

This example remains a full Cargo package so connector-owned examples can carry real flow/package/bundle semantics just like the numbered workspace examples.

## WASM / bundle note

This package is now shaped so `flows bundle -p example-connector-github-issues-local-flow --wasm` is the intended portable artifact path.

Actual runtime execution of HTTP/connector-heavy bundles still depends on the current host-side WASM capability/runtime support; the package shape is now portable even where every host runtime is not yet feature-parity complete.
