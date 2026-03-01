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

## Cloudflare durability/resume bindings
For alarm-driven resume on deployed Workers, configure these bindings/vars in `wrangler.toml`:

- Durable Object binding:
  - `FLOW_DO` -> `FlowDurableObject`
- Internal resume dispatch:
  - `LATTICE_RESUME_SERVICE_BINDING` (recommended, Worker service binding name), or
  - `LATTICE_RESUME_DISPATCH_URL` (fallback URL, e.g. `https://<worker>.workers.dev/__lattice/resume`)
- Internal route auth:
  - `LATTICE_INTERNAL_RESUME_TOKEN` (validated by `POST /__lattice/resume`)
- Optional explicit bundle pin identity override:
  - `LATTICE_BUNDLE_ID` (otherwise host defaults to `flow://<flow_id>@<version>`)

When service binding dispatch is used, add a worker service binding for the name referenced by `LATTICE_RESUME_SERVICE_BINDING`.

For a portable deploy/evidence harness, see:
- `crates/host-workers/workerd-tests/wrangler.service-binding.template.toml`
- `crates/host-workers/workerd-tests/wrangler.cpu-proof.template.toml`
- wrapper runbook: `ops/cloudflare-resume-proof-kit.md`

## Depends on
- `host-inproc`, `kernel-exec`, `kernel-plan`, `capabilities`
