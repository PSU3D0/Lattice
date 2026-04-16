# dispatch-backend

Minimal host-agnostic dispatch backend contract for external completion backends in Lattice.

This crate intentionally starts small:
- `DispatchBackend`
- `DispatchRequest`
- `DispatchReceipt`
- `TrackingMode`
- `DispatchBackendHost`
- `ResourceAccessDispatchHost`

It is meant to support the first extraction of dispatch-backend plumbing out of
example-local code without prematurely freezing a larger polling/cancellation
surface.
