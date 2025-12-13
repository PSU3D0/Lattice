# Repository Guidelines

## Recontextualize (Always Read First)

If you're starting fresh (human or agent), read these in order:

- `README.md`
- `impl-docs/README.md`
- `impl-docs/roadmap/epics.md`
- 0.1 contract specs (canonical target surfaces):
  - `impl-docs/spec/flow-ir.md`
  - `impl-docs/spec/invocation-abi.md`
  - `impl-docs/spec/control-surfaces.md`
  - `impl-docs/spec/capabilities-and-binding.md`
- Diagnostics (when touching validator/runtime/policy):
  - `impl-docs/error-codes.md`
  - `impl-docs/error-taxonomy.md`
- Recent intent and repo state notes:
  - `impl-docs/work-log.md`

## Documentation Structure (Canonical)

- `impl-docs/spec/`: normative semantics (0.1 contract targets)
- `impl-docs/adrs/`: irreversible decisions ("forever" choices)
- `impl-docs/roadmap/`: epics + phase ticket docs (implementation plan)
- `impl-docs/user-stories.md`: scenario/acceptance targets
- `impl-docs/work-log.md`: running log; add entries for shipped milestones/epic completions
- `impl-docs/archive/`: historical snapshots/notes (not canonical)

## Private Docs (Encrypted)

Some docs are intentionally private and must never be committed as plaintext.

- Decrypted (local-only, gitignored): `private/impl-docs/...`
- Encrypted (committed): `impl-docs/_encrypted/.../*.age`
- Recipients file: `.age-recipients`
- Tooling:
  - `mise run encrypt`
  - `mise run decrypt`
  - or `python scripts/crypto.py status`

Rules:
- Keep specs short and stable; move implementation detail into epic phase docs.
- Keep ADRs decision-focused; avoid implementation walkthroughs.
- Update `work-log.md` when an epic meaningfully advances or a contract changes.
- Never commit plaintext under `private/`.

## Work-log Pattern (Required)

When you complete a meaningful subphase (e.g., an epic phase acceptance gate), append a dated entry to `impl-docs/work-log.md` with:
- Header: `## YYYY-MM-DD — <milestone name> (Epic NN.N)`
- 3–8 bullets: what changed (facts), where (crates/files), and why.
- Acceptance gates: name the tests/commands that prove it.
- Any compatibility/contract notes (new codes, envelope changes, IR field semantics).

Avoid:
- Re-stating the entire roadmap.
- Writing speculative future intent in the work-log; put that in `impl-docs/roadmap/*`.

## Roadmap Learnings (Required)

When you learn something that will affect future implementation choices:
- Add it under the corresponding epic phase in `impl-docs/roadmap/<epic>.md` as a `Learnings / notes (<phase>)` block colocated with that phase.
- Keep it prescriptive and stable: 2–6 bullets about boundaries, invariants, and “don’t do X”.
- If a learning implies a contract change (IR/ABI/envelope/diagnostic codes), update `impl-docs/spec/*` and/or `impl-docs/error-codes.md` and call it out in the work-log.

## Project Structure & Module Organization

- Core crates live under `crates/` (e.g., `dag-core`, `dag-macros`, `kernel-plan`, `kernel-exec`).
- Capability adapters (`cap-*`), hosts (`host-*`), plugins (`plugin-*`), registry tooling, and CLI (`crates/cli`) follow the workspace layout defined in `impl-docs/surface-and-buildout.md`.
- Specs and RFC umbrella reside in `impl-docs/`; Flow IR schema lives in `schemas/`.

## Build, Test, and Development Commands

- `cargo check` — fast validation for all crates.
- `cargo test --workspace` — runs unit tests across the workspace (use `-p <crate>` to scope).
- `cargo fmt --check` / `cargo clippy --all-targets --all-features -D warnings` — formatting and lint rules.
- `cargo run -p flows-cli -- <subcommand>` — execute CLI.

## Coding Style & Naming Conventions

- Rust edition 2024; MSRV is defined by workspace `Cargo.toml` (`rust-version`).
- Module names use `snake_case`; public types and traits use `CamelCase`; macros use `snake_case!`.
- Keep crate READMEs current with purpose, surface, and next steps.
- Error/diagnostic codes follow prefixes (`DAG`, `CTRL`, `IDEM`, etc.) defined in `impl-docs/error-codes.md`.

## Testing Guidelines

- Add `#[cfg(test)]` modules within crates; integrate property/trybuild tests where relevant.
- Idempotency harness lives in `crates/testing-harness-idem`; extend for effectful node tests.
- Prefer descriptive test names (`test_<feature>_when_<condition>_<expectation>`).

## Commit & Pull Request Guidelines

- Use concise commit messages (`feat: ...`, `fix: ...`, `docs: ...`) mirroring conventional commits seen in history.
- Squash related scaffolding changes when possible; avoid mixing code generation with manual edits.
- PRs should include: summary of changes, linked issue/RFC/epic, test commands executed, and follow-up TODOs.
- Flag additions to `impl-docs/spec/` or schema changes for reviewer attention.

## Agent-Specific Tips

- Prefer targeted `cargo check -p <crate>` during focused development.
- When adding new macros or IR fields:
  - update schema: `schemas/flow_ir.schema.json`
  - update semantics: `impl-docs/spec/*`
  - add/adjust ADRs when a decision is irreversible
