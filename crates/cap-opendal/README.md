# cap-opendal

OpenDAL-backed blob and key-value capabilities for Lattice.

This crate merges the former `cap-opendal-core`, `cap-blob-opendal`, and
`cap-kv-opendal` micro-crates into a single feature-gated surface (packet E2,
verifiability-substrate-hardening plan). The three crates had no external
consumers, so collapsing them removes two path-dependency hops and a duplicated
feature matrix with no downstream churn.

## Surface
- `core` module: shared OpenDAL plumbing — `OperatorFactory`,
  `SchemeOperatorFactory`, `OperatorLayerExt`, and `OpendalError`.
- `blob` module (feature `blob`, default on): `OpendalBlobStore` /
  `BlobStoreBuilder` implementing `capabilities::blob::BlobStore`.
- `kv` module (feature `kv`, default on): `OpendalKvStore` / `KvStoreBuilder`
  implementing `capabilities::kv::KeyValue`, with TTL/consistency descriptors.

## Features
- `blob`, `kv`: enable the respective capability module (both default on).
- `services-*`, `layers-*`: forwarded to `opendal` to select backends/layers.

## Migration
- `cap_opendal_core::X` → `cap_opendal::X` (re-exported at the crate root) or
  `cap_opendal::core::X`.
- `cap_blob_opendal::X` → `cap_opendal::blob::X` (or crate root re-export).
- `cap_kv_opendal::X` → `cap_opendal::kv::X` (or crate root re-export).
