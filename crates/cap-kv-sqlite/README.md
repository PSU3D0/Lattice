# cap-kv-sqlite

`cap-kv-sqlite` provides a lightweight SQLite-backed KV capability for development and test profiles. It allows Strict/Stable reads via content-addressing and offers simple local persistence.

## Surface
- `KvRead`/`KvWrite` implementations with optional TTL and transactional guards.
- Schema migration helpers for evolving local state.
- Utilities for test harnesses to seed fixtures.

## Next steps
- Implement CRUD operations with deterministic read options and JSON value storage.
- Cover optimistic-locking and conflict scenarios in unit tests.
- Integrate with spill/cache policies once `capabilities` exposes them.

## Depends on
- Trait contracts from `capabilities`.
- Optionally consumes shared type definitions from `types-common` when available.
