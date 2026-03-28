# cap-blob-fs

`cap-blob-fs` is currently **deferred / under review**.

The current preferred direction for durable blob/object storage is:
- `cap-blob-opendal`
- using an OpenDAL filesystem backend locally when a native filesystem-backed blob store is sufficient

## Why it is deferred
A dedicated filesystem blob crate only becomes worth implementing if we need stronger local-native semantics than the OpenDAL path gives us, for example:
- opinionated content-addressed layout owned by Lattice,
- stronger local hash/atomic-write guarantees,
- or other durable-artifact behaviors that are materially different from generic object-store semantics.

## Important distinction from workspace
This crate should not be conflated with the workspace capability:
- **workspace** = run-scoped scratch volume with host-owned cleanup
- **blob** = durable artifact/object store across runs

Those remain different capabilities even if both happen to use the local filesystem underneath.
