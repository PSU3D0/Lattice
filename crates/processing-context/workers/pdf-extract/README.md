# Dedicated PDF extraction Worker

This Worker is the platform-contained backend for
`lattice.pdf.extract_text.v1`. It is deliberately separate from the flow
Worker.

## Containment contract

- no public `workers.dev`, preview, or route exposure;
- no network, service, storage, queue, secret, or variable bindings;
- one internal `POST /v1/transform` protocol with pinned transform and ABI
  headers;
- the exact checked W0 module is hash/size/ABI-admitted during `npm run build`;
- the precompiled module binding is instantiated fresh for every request with
  `{}` imports;
- one request may execute per isolate, with immediate `503 busy` and no waiter
  queue;
- input is stream-counted to 8 MiB before guest copy and output is bounded to
  `4 + 512 KiB` before response copy;
- no request bytes, text, filenames, paths, hashes, or diagnostics are logged.

The `x-lattice-module-sha256-attestation` response header is a **build-time
attestation**, not a runtime measurement. Cloudflare does not expose the bytes
of a precompiled wasm binding for runtime hashing.

`limits.cpu_ms = 30000` is Workers platform policy. It is not native fuel,
epoch interruption, Wasmtime `StoreLimits`, observed guest CPU, or evidence of
which platform limit terminated an invocation. A service-binding termination
is reported by the flow adapter as the generic `platform_terminated` class;
post-hoc Cloudflare observability is a separate evidence source.

## Local gates

```bash
npm install
npm test
```

The tests use Miniflare only as a local mock/runtime. They prove checked
protocol classes, bounded I/O, the W0 artifact attestation, and fresh instance
state. They do not prove production Cloudflare CPU or isolate-memory
termination behavior.
