# capabilities

`capabilities` defines the typestate-based resource contracts that nodes can request at runtime. It provides the trait definitions, registries, and policy hooks that underpin effect/determinism enforcement across the platform.

## Surface
- Capability traits (HTTP, KV, Blob, Cache, Dedupe, Clock/Rng, etc.) with type-state annotations.
- Registry/lookup APIs for hosts to provide concrete implementations.
- Policy metadata linking capabilities to effects, determinism, and residency rules.

## Next steps
- Implement trait signatures from RFC §6.3 and integrate tracing/metrics guards.
- Provide a registry data structure with permission checks and capability version tracking.
- Add unit tests ensuring effect/determinism contracts are enforced at compile time.

## Depends on
- Builds on `dag-core` type definitions.
- Adapter crates (`cap-http-reqwest`, `cap-kv-sqlite`, etc.) will use these traits once finalised.
