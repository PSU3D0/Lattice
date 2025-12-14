# host-temporal

`host-temporal` bridges Lattice plans into Temporal workflows and activities. It handles code generation, Rust activity workers, and orchestration semantics like signals, timers, and Continue-As-New thresholds.

## Surface
- Lowering utilities that translate Execution Plans into Temporal workflow/activity code.
- Activity worker harness wrapping `kernel-exec` nodes.
- Integration hooks for retry ownership and history budget enforcement.

## Next steps
- Implement minimal code generation that targets Temporal Go/TypeScript SDKs.
- Build a Rust activity worker that embeds `kernel-exec`.
- Add integration tests using Temporal's dev server for the payments/onboarding stories.

## Depends on
- Execution plans from `kernel-plan` and runtime from `kernel-exec`.
- Policy signals from `policy-engine` for retry ownership and history guardrails.
