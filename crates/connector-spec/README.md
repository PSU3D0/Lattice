# connector-spec

`connector-spec` defines the schema for connector YAML manifests and generates the corresponding Rust code, manifests, and tests. It is the bridge between declarative specs and executable connector crates.

## Surface
- Serde/schemars models for connector manifests.
- Code generation helpers that emit node structs, params, and manifests.
- Validation utilities shared by CLI and registry certification.

## Next steps
- Implement schema structures per RFC §11 and provide serde validation.
- Add code-generation routines for Rust node templates and test scaffolding.
- Snapshot tests verifying generator output for canonical connectors (Stripe, SendGrid).

## Depends on
- Core types from `dag-core` and shared serde helpers from `types-common` once populated.
- Will be consumed by `importer-n8n`, `flows-cli`, and `connectors-std`.
