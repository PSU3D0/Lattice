//! LLM types and traits for Lattice.
//!
//! Derived from [rig](https://github.com/0xPlaygrounds/rig) v0.33.0 (MIT license).
//! Copyright (c) 2024, Playgrounds Analytics Inc.
//!
//! This crate provides the foundation types for LLM integration:
//! - `CompletionModel` trait — the provider abstraction
//! - `CompletionRequest` / `CompletionResponse` — request/response types
//! - `Message` types — system, user, assistant, tool messages
//! - `Tool` trait — typed tool definitions for agent loops
//! - `HttpClientExt` trait — pluggable HTTP layer (no reqwest dependency)
//! - SSE streaming types and parser
//!
//! All types compile to `wasm32-unknown-unknown` with zero platform dependencies.

// TODO: Phase 1 implementation — copy modules from ref-libs/rig/rig/rig-core/src/
