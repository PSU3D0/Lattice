# host-workers

`host-workers` packages the Lattice Flow host runtime for Cloudflare Workers. It wires the
in-process host (`host-inproc`) into a Workers-compatible entrypoint, enabling wasm32
deployments.

## Surface
- Workers entrypoint that bridges requests into workflow invocations.
- In-process execution wiring over `kernel-exec` and `kernel-plan`.
- Capability registry access for Workers-friendly adapters.

## Local development
- `cargo check -p host-workers --target wasm32-unknown-unknown`
- `wrangler dev --local` (requires `wrangler` and `worker-build`)

## Build
- `wrangler build` produces `build/index.js` and the wasm bundle.

## Depends on
- `host-inproc`, `kernel-exec`, `kernel-plan`, `capabilities`
