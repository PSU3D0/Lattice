# cap-blob-fs

`cap-blob-fs` implements a filesystem-backed blob capability that supports deterministic by-hash access, spill targets, and local caching for dev/test profiles.

## Surface
- `BlobRead`/`BlobWrite` providers with hash verification and content-addressed paths.
- Spill utilities used by `kernel-exec` when buffers overflow.
- Configuration helpers for pointing at workspace-relative storage.

## Next steps
- Implement streaming read/write APIs with checksum validation.
- Add tests covering deterministic by-hash fetches and spill/resume cycles.
- Hook into spill policy wiring from `kernel-exec` once implemented.

## Depends on
- Needs capability traits from `capabilities` and spill signalling from `kernel-exec`.
