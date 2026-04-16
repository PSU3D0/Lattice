# connector_zoom_platform

Minimal Zoom provider helpers for transcript-oriented Lattice workflows.

This crate intentionally stays narrow. It currently owns:

- server-to-server OAuth token request construction
- transcript request/download helpers that return Lattice-compatible `HttpRequest` values
- transcript DTOs plus provider-level availability mapping (`ready` vs `not_ready` vs terminal unavailable reasons)
- small request config overrides for base URLs and timeout

It intentionally does **not** own:

- retry or reconciliation policy
- meeting classification or transcript source resolution
- workflow/job state-machine behavior
- broad Zoom product coverage beyond the transcript slice
