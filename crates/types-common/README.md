# types-common

`types-common` centralises reusable domain types (email addresses, money, URLs, attachments) that appear across connectors, packs, and flows. Each type carries serde/schemars metadata for policy and registry enforcement.

## Surface
- Strongly typed wrappers with validation logic.
- JsonSchema derivations with data-class annotations.
- Conversion helpers bridging connectors and core flow structs.

## Next steps
- Implement canonical types required by early connectors (emails, money, file references).
- Add validation tests and schema snapshots to guarantee stability.
- Coordinate with `connector-spec` so generated code reuses these types.

## Depends on
- Only standard libraries; consumed by most downstream crates once stabilized.
