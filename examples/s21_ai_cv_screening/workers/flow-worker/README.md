# S21 Workers deployment proof

This application-owned package builds the S21 flow Worker and asks `flows deploy render` to produce the pinned two-Worker package under `deploy/`. The PDF extraction Worker remains private (`workers_dev = false`, no preview URL) and is reachable only through the generated `LATTICE_EXTRACT_PDF` service binding.

## Local qualification

```bash
mise run qualify-s21-workers-package
```

The gate rebuilds and renders the package, verifies every `deploy-manifest.json` hash, runs the render-derived Miniflare golden against the real extraction Worker and a private mock-provider service, audits npm dependencies, and runs Wrangler dry-runs for the flow, extraction, and mock-provider Workers.

`backend-config.json` selects the exact typed `sandboxed_transform:lattice.pdf.extract_text.v1` implementation contract. The renderer independently pins and verifies the extraction sources, guest manifest, module hash/size, ABI, compatibility date, 64 MiB guest maximum, 30,000 ms platform CPU policy, bounded I/O, single-flight admission, and fresh-instance model. Configuration is not inferred from a node identifier.

## Disposable Cloudflare proof

The guarded script requires an explicitly approved account, a disposable name prefix, creation approval, cleanup approval, and an evidence directory:

```bash
CLOUDFLARE_API_TOKEN=... npm run cloudflare:w4 -- \
  --account-id <approved-32-hex-account-id> \
  --prefix lattice-w4-<unique-suffix> \
  --evidence-dir <private-evidence-dir> \
  --approve-create-disposable \
  --approve-cleanup
```

It qualifies locally before mutation, verifies that the token can see the exact account, rejects Worker/KV/R2 name collisions, creates uniquely named KV/R2 resources, and initially deploys both the mock provider and flow Worker privately. After random proof-only secrets are installed, those two proof endpoints are temporarily exposed on `workers.dev`; the extraction Worker remains private throughout. The script exercises success/sequential redelivery/hostile-input cases, writes sanitized evidence, deletes Workers/KV/R2/Durable Object namespaces, and verifies their absence. `resource-state.json` is written incrementally for recovery if the process is interrupted. Never commit the evidence directory or generated configs.

Interrupted runs can be cleaned independently:

```bash
CLOUDFLARE_API_TOKEN=... node scripts/cloudflare-w4-cleanup.mjs \
  --state <private-evidence-dir>/resource-state.json \
  --approve-cleanup
```

The 30,000 ms value is a conservative Cloudflare policy ceiling. The proof verifies the deployed containment configuration, but does **not** claim that platform termination was observed. It does not establish native Wasmtime fuel/epoch/`StoreLimits` parity or CPU-versus-memory termination attribution.
