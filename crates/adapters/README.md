# adapters

`adapters` hosts reusable conversions (`From`/`TryFrom`/mappers) that bridge between connector payloads, domain types, and Flow IR schemas. It keeps conversion logic consistent and auditable across agents and humans.

## Surface
- Adapter traits and blanket implementations for common payload shapes.
- Utility functions for normalising connector responses into typed structs.
- Test helpers to verify lossless conversions.

## Next steps
- Populate adapters needed for the first wave of connectors (Stripe, SendGrid, Slack).
- Add property tests to ensure round-trip conversions where applicable.
- Coordinate with `connectors-std` and `flows-cli` templates for reuse.

## Depends on
- Core types from `dag-core` and shared domain models in `types-common`.
