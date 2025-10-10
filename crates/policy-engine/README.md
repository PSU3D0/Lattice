# policy-engine

`policy-engine` evaluates CEL-based policies against Flow IR and runtime evidence. It enforces organisational rules for effects, determinism, egress, residency, and retry ownership before publish or deploy.

## Surface
- Policy definition loader and CEL execution runtime.
- Evidence builders producing JSON bundles for the registry.
- Lint adapters that integrate with `kernel-plan` diagnostics.

## Next steps
- Implement the CEL runtime bindings and rule templates from RFC §15.
- Add unit tests covering policy allow/deny paths and waiver handling.
- Integrate with `registry-cert` so publishes collect policy evidence automatically.

## Depends on
- IR metadata from `dag-core`/`kernel-plan`.
- Output artifacts consumed by `registry-client` and `registry-cert`.
