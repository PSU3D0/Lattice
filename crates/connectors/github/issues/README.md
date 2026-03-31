# connector_github_issues

Generated Phase-B example connector for GitHub Issues.

## Canonical local flow example

The canonical runnable local example now lives as a **full example crate** under:

- `crates/connectors/github/issues/examples/local-flow/`

Run it from the workspace root through the standard CLI:

```bash
cargo run -p flows-cli -- run local \
  --example connector_github_issues_local_flow \
  --bindings-lock <bindings.lock.json> \
  --payload '{"owner":"rust-lang","repo":"cargo"}'
```

That example is:
- a real flow crate with standard `bundle()` support,
- runnable through the normal CLI/Axum example path,
- and shaped for `flows bundle -p ... --wasm` as the portable artifact path.

See:
- `crates/connectors/github/issues/examples/local-flow/README.md`

## Crate-level coverage

This connector crate also includes:
- manifest + registration tests
- runtime tests against a mocked upstream via `httpmock`

So the package now has both:
- close-to-the-connector tests,
- and a fuller connector-owned example crate with its own Cargo/package boundary.
