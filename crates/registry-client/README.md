# registry-client

`registry-client` provides the SDK for publishing, fetching, and verifying artifacts in the Lattice registry. It abstracts storage backends, handles signing, and exposes friendly APIs for CLI and Studio consumers.

## Surface
- Upload/download operations for Flow/Node/Connector manifests.
- Local cache management and artifact verification.
- Pluggable backend adapter interface (local filesystem first, remote later).

## Next steps
- Implement local file-store backend for MVP along with SHA/Sigstore stubs.
- Add unit tests covering publish/download cycles and artifact integrity checks.
- Expose async API consumed by `flows-cli` and `studio-backend`.

## Depends on
- Schema definitions from `dag-core` and exporters from `exporters`.
- Certification tooling in `registry-cert` to attach evidence bundles.
