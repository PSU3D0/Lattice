# cap-blob-opendal

OpenDAL-backed blob store capability for Lattice.

## Surface
- `OpendalBlobStore` implementing `capabilities::blob::BlobStore`.
- `BlobStoreBuilder` consuming any `cap-opendal-core::OperatorFactory`.
- Builder helpers for layering operators (`with_layer`, `map_operator`).

## Status
- Phase 2 of the OpenDAL capability plan — async blob trait satisfied, ready for spill integration in later phases.

## Next steps
- Add streaming helpers once executor spill path migrates.
- Extend builder extensions for backend-specific tuning (e.g., S3 multipart configuration, Cloudflare R2 defaults).
