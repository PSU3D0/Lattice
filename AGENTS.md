# Repository Guidelines

## Project Structure & Module Organization
- Core crates live under `crates/` (e.g., `dag-core`, `dag-macros`, `kernel-plan`, `kernel-exec`).
- Capability adapters (`cap-*`), hosts (`host-*`), plugins (`plugin-*`), registry tooling, and CLI (`crates/cli`) follow the workspace layout defined in `impl-docs/surface-and-buildout.md`.
- Specifications and RFC addenda reside in `impl-docs/`; Flow IR schema lives in `schemas/` with reference artifacts.
- Examples and future scenario flows will be added under `examples/` (currently stubbed).

## Build, Test, and Development Commands
- `cargo check` — fast validation for all crates using workspace metadata.
- `cargo test` — runs unit tests across the workspace (use `-p <crate>` to scope).
- `cargo fmt -- --check` / `cargo clippy --all-targets --all-features -D warnings` — enforce formatting and lint rules.
- `cargo run -p flows-cli -- <subcommand>` — execute CLI once implemented (e.g., `graph check`, `run local`).

## Coding Style & Naming Conventions
- Rust edition 2024, MSRV 1.77; follow Rustfmt defaults (4-space indentation, trailing commas).
- Module names use `snake_case`; public types and traits use `CamelCase`; macros use `snake_case!`.
- Keep crate READMEs current with purpose, surface, and next steps.
- Error/diagnostic codes follow prefixes (`DAG`, `CTRL`, `IDEM`, etc.) defined in `impl-docs`.

## Testing Guidelines
- Add `#[cfg(test)]` modules within crates; integrate property/trybuild tests where relevant.
- Idempotency harness lives in `crates/testing-harness-idem`; extend for effectful node tests.
- Prefer descriptive test names (`test_<feature>_when_<condition>_<expectation>`).
- Future certification suites (`registry-cert`) should be run via CLI (`flows certify`) once implemented.

## Commit & Pull Request Guidelines
- Use concise commit messages (`feat: …`, `fix: …`, `docs: …`) mirroring conventional commits seen in history.
- Squash related scaffolding changes when possible; avoid mixing code generation with manual edits.
- PRs should include: summary of changes, linked RFC/issue, test commands executed, and any follow-up TODOs.
- Flag additions to `impl-docs/` or schema changes for reviewer attention; attach schema diff or sample artifact when relevant.

## Agent-Specific Tips
- Use the workspace dependencies in `Cargo.toml` to keep versions aligned; avoid ad-hoc versions in individual crates.
- When adding new macros or IR fields, update both schema (`schemas/flow_ir.schema.json`) and docs (`impl-docs/rust-workflow-tdd-rfc.md`).
- Prefer targeted `cargo check -p <crate>` during focused development to keep feedback loops fast.
