# flows-cli

`flows-cli` is the command-line entry point for authors and agents. It exposes commands to scaffold projects, validate graphs, run workflows locally, interact with queues, and publish artifacts to the registry.

## Surface
- `flows graph check`, `flows run local`, `flows queue up`, `flows certify`, `flows publish`, and import/export commands.
- Shared logging/tracing configuration and JSON output for automation.
- Integration glue with planner, executor, registry, and importer crates.

## Next steps
- Scaffold CLI command structure with Clap and ensure machine-readable output.
- Wire commands to the underlying crates as they gain functionality (Phases 1–6).
- Add integration tests for each subcommand once subsystems are ready.

## Depends on
- `dag-core`, `dag-macros`, `kernel-plan`, `kernel-exec`, `registry-client`, and `registry-cert`.
