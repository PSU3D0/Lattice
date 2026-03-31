//! LLM agent loop and structured extraction for Lattice.
//!
//! Derived from [rig](https://github.com/0xPlaygrounds/rig) v0.33.0 (MIT license).
//! Copyright (c) 2024, Playgrounds Analytics Inc.
//!
//! This crate provides:
//! - `Agent` builder — configure model + tools + preamble + hooks
//! - `PromptRequest` — bounded tool-use loop with `max_turns`
//! - `PromptHook` trait — intercept completions, tool calls, tool results
//! - `Extractor<M, T>` — structured output extraction via forced tool_choice
//!
//! All types compile to `wasm32-unknown-unknown` with zero platform dependencies.

// TODO: Phase 2 implementation — copy modules from ref-libs/rig/rig/rig-core/src/
