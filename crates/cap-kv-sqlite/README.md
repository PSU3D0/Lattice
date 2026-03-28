# cap-kv-sqlite

`cap-kv-sqlite` is currently **deferred / under review**.

The current preferred direction for local/native KV is to use:
- `cap-kv-opendal`
- with an OpenDAL SQLite backend when SQLite-backed local persistence is desired

That keeps `resource::kv` provider selection cleaner and avoids introducing a one-off local KV crate unless we discover a concrete semantic gap that OpenDAL SQLite cannot cover.

## Why it is deferred
A dedicated crate only becomes worth implementing if we need SQLite-specific KV semantics that are meaningfully stronger than the OpenDAL path, for example:
- richer local list/metadata/expiration support,
- explicit schema/migration ownership in the adapter,
- or stronger local transaction/performance guarantees.

## Important boundary
If Lattice later grows a true SQL/DB capability, that should be modeled as a **separate capability family**, not as an extension of `resource::kv` just because some KV providers use SQLite internally.
