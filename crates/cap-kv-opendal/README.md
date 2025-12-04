# cap-kv-opendal

OpenDAL-backed key-value capability for LatticeFlow.

## Surface
- `OpendalKvStore` implementing `capabilities::kv::KeyValue` with optional capability metadata (`KvCapabilityInfo`).
- `KvStoreBuilder` bridging any `cap-opendal-core::OperatorFactory`, including layer composition and backend-specific defaults.
- TTL/consistency descriptors so validators/runtime can reason about backend guarantees.

## Status
- Phase 3 deliverable from the OpenDAL capability plan. Supports blocking integration via Tokio, with per-write TTL guarded by capability metadata.

## Next steps
- Extend builder extensions for Cloudflare KV (namespace TTL floor) and other services.
- Surface capability metadata through `ResourceBag` once consumers are ready.
