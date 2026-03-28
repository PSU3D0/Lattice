# capabilities

`capabilities` defines the typestate-based resource contracts that nodes can request at runtime. It provides the trait definitions, registries, and policy hooks that underpin effect/determinism enforcement across the platform.

## Surface
- Capability traits (HTTP, KV, Blob, Cache, Dedupe, Clock/Rng, etc.) with type-state annotations.
- Registry/lookup APIs for hosts to provide concrete implementations.
- Policy metadata linking capabilities to effects, determinism, and residency rules.

## Next steps
- Continue tightening effect/determinism and runtime-policy guardrails around the existing traits.
- Improve builder-facing docs for the canonical local stacks (HTTP/Workspace/Blob/KV).
- Add more example-driven coverage that demonstrates when to prefer workspace vs blob and KV vs future SQL-like surfaces.

## Depends on
- Builds on `dag-core` type definitions.
- Adapter crates (`cap-http-reqwest`, `cap-kv-opendal`, `cap-kv-workers`, `cap-blob-opendal`, etc.) use these traits.
