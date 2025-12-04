# cap-opendal-core

Shared OpenDAL helpers used by capability crates.

## Surface
- `OperatorFactory` trait with async `build` semantics for schemes + config maps.
- `SchemeOperatorFactory` implementing the trait via `Operator::via_iter`.
- Error translation utilities mapping `opendal::ErrorKind` into portable capability errors.
- `OperatorLayerExt` helper for optional layer composition.

## Status
- Phase 1 scaffold from `impl-docs/opendal-capability-plan.md`.
- Feature matrix mirrors OpenDAL services/layers; defaults to no backends enabled.

## Next steps
- Expose higher-level builder wrappers for blob/kv capabilities.
- Extend error mapping once downstream capability crates define domain-specific errors.
