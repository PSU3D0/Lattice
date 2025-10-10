# importer-n8n

`importer-n8n` ingests n8n JSON workflows, analyses their connectors/expressions, and emits equivalent LatticeFlow Rust flows plus lossiness reports. It underpins the connector farming roadmap.

## Surface
- Parsers for n8n workflow JSON and expression ASTs.
- Mapping layer that translates nodes/connectors into our DSL and control surfaces.
- Reporting utilities (coverage metrics, lossy semantics catalog).

## Next steps
- Implement the JSON parser and node mapping tables outlined in RFC §12.
- Integrate with `dag-macros` to generate Rust source + run the compile loop.
- Produce lossy semantics reports and import success metrics for CLI consumption.

## Depends on
- Requires `dag-core`, `connector-spec`, and `types-common` for schema fidelity.
- Will feed into `flows-cli` workflows for automated harvesting.
