# plugin-wasi

`plugin-wasi` runs WASM-based nodes using the WASI component model. It enforces declared capability allowlists, sandbox limits, and streams data to and from the kernel via async handles.

## Surface
- Host runtime wrapping Wasmtime with capability shims exposed via WIT.
- ABI definitions shared with WASM plugins and code generators.
- Sandbox policies (memory limits, fuel, timeout integration).

## Next steps
- Define the WIT interface per RFC §9 and implement host bindings.
- Add integration tests running example WASM nodes with strict determinism.
- Provide developer tooling (scaffolding, local runner) for custom plugins.

## Depends on
- Capability traits from `capabilities` and execution APIs from `kernel-exec`.
- Coordination with `exporters` for WIT manifest generation.
