// Anthropic Messages API provider for Lattice LLM.
//
// # Example
// ```ignore
// use llm_provider_anthropic::completion::CLAUDE_3_5_SONNET;
// ```

pub mod client;
pub mod completion;
pub mod decoders;
pub mod streaming;

pub use client::{Client, ClientBuilder};
