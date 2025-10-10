# exporters

`exporters` houses pluggable exporters that turn Flow IR into artifacts such as JSON manifests, DOT graphs, OpenAPI specs, and WIT packages. These outputs feed the CLI, Studio, and registry certification flows.

## Surface
- Trait implementations for JSON, DOT, and planned OpenAPI/WIT exporters.
- Shared helpers for schema bundling and artifact metadata.
- Integration points for CLI commands (`flows graph check`, `flows export`).

## Next steps
- Implement JSON + DOT exporters against the schema in `schemas/flow_ir.schema.json`.
- Add OpenAPI exporter to support agent/tool declarations per the platform roadmap.
- Snapshot tests verifying output stability across sample flows.

## Depends on
- Requires IR types from `dag-core` and metadata from `kernel-plan` once available.
