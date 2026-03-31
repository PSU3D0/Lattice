//! LLM agent loop and structured extraction for Lattice.
//!
//! Derived from [rig](https://github.com/0xPlaygrounds/rig) v0.33.0 (MIT license).
//! Copyright (c) 2024, Playgrounds Analytics Inc.

pub mod agent;
pub mod client;
pub mod compat;
pub mod extractor;
pub mod tools;

pub mod completion {
    pub use llm_types::completion::*;
}

pub mod message {
    pub use llm_types::message::*;
}

pub mod streaming {
    pub use llm_types::streaming::*;

    use crate::agent::prompt_request::hooks::PromptHook;
    use crate::agent::prompt_request::streaming::StreamingPromptRequest;
    use crate::completion::{CompletionModel, GetTokenUsage};
    use crate::message::Message;
    use crate::wasm_compat::{WasmCompatSend, WasmCompatSync};

    pub trait StreamingPrompt<M, R>
    where
        M: CompletionModel + 'static,
        <M as CompletionModel>::StreamingResponse: WasmCompatSend,
        R: Clone + Unpin + GetTokenUsage,
    {
        type Hook: PromptHook<M>;

        fn stream_prompt(
            &self,
            prompt: impl Into<Message> + WasmCompatSend,
        ) -> StreamingPromptRequest<M, Self::Hook>;
    }

    pub trait StreamingChat<M, R>: WasmCompatSend + WasmCompatSync
    where
        M: CompletionModel + 'static,
        <M as CompletionModel>::StreamingResponse: WasmCompatSend,
        R: Clone + Unpin + GetTokenUsage,
    {
        type Hook: PromptHook<M>;

        fn stream_chat<I, T>(
            &self,
            prompt: impl Into<Message> + WasmCompatSend,
            chat_history: I,
        ) -> StreamingPromptRequest<M, Self::Hook>
        where
            I: IntoIterator<Item = T>,
            T: Into<Message>;
    }
}

pub use llm_types::{Embed, OneOrMany};

pub mod one_or_many {
    pub use llm_types::one_or_many::*;
}

pub mod json_utils {
    pub use llm_types::json_utils::*;
}

pub mod wasm_compat {
    pub use llm_types::wasm_compat::*;
}

pub mod embeddings {
    pub use llm_types::embeddings::*;
}

pub mod http_client {
    pub use llm_types::http_client::*;
}

pub mod tool {
    pub use llm_types::tool::*;

    pub mod server;
}

pub mod model;
pub mod prelude;
pub mod transcription;
pub mod audio_generation;
pub mod image_generation;
pub mod vector_store;
