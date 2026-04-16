//! LLM types and traits for Lattice.
//!
//! Derived from [rig](https://github.com/0xPlaygrounds/rig) (MIT license).
//! Copyright (c) 2024, Playgrounds Analytics Inc.

pub mod completion;
pub mod embeddings;
pub mod http_client;
pub mod json_utils;
pub mod one_or_many;
pub mod streaming;
pub mod tool;
pub mod wasm_compat;

pub use embeddings::embed::Embed;
pub use one_or_many::OneOrMany;

pub mod message {
    pub use crate::completion::message::*;
}
