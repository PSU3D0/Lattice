# connector_github_issues

Generated Phase-B example connector for GitHub Issues.

## Canonical local flow example

The canonical runnable local example now lives as a **full example crate** under:

- `crates/connectors/github/issues/examples/local-flow/`

Run it from the workspace root:

```bash
cd codebase/.sessions/lat-000028-impl
cargo run -p example-connector-github-issues-local-flow
```

That example is:
- mock-first by default,
- runnable as a real local flow through `host-inproc`,
- and easy to point at a real GitHub-compatible endpoint via env override.

See:
- `crates/connectors/github/issues/examples/local-flow/README.md`

## Crate-level coverage

This connector crate also includes:
- manifest + registration tests
- runtime tests against a mocked upstream via `httpmock`

So the package now has both:
- close-to-the-connector tests,
- and a fuller connector-owned example crate with its own Cargo/package boundary.
