use llm_agent::client::{self, BearerAuth, Capabilities, Capable, DebugExt, Nothing, Provider, ProviderBuilder};
use llm_types::http_client::{self, HttpClientExt};
use serde::Deserialize;
use std::fmt::Debug;

// ================================================================
// Main OpenAI Client
// ================================================================
const OPENAI_API_BASE_URL: &str = "https://api.openai.com/v1";

// ================================================================
// OpenAI Completions API Extension
// ================================================================
#[derive(Debug, Default, Clone, Copy)]
pub struct OpenAICompletionsExt;

#[derive(Debug, Default, Clone, Copy)]
pub struct OpenAICompletionsExtBuilder;

type OpenAIApiKey = BearerAuth;

pub type Client<H> = client::Client<OpenAICompletionsExt, H>;
pub type ClientBuilder<H> = client::ClientBuilder<OpenAICompletionsExtBuilder, OpenAIApiKey, H>;
pub type CompletionsClient<H> = Client<H>;
pub type CompletionsClientBuilder<H> = ClientBuilder<H>;

impl Provider for OpenAICompletionsExt {
    type Builder = OpenAICompletionsExtBuilder;
    const VERIFY_PATH: &'static str = "/models";
}

impl<H> Capabilities<H> for OpenAICompletionsExt {
    type Completion = Capable<super::completion::CompletionModel<H>>;
    type Embeddings = Capable<super::EmbeddingModel<H>>;
    type Transcription = Capable<super::TranscriptionModel<H>>;
    type ModelListing = Nothing;
}

impl DebugExt for OpenAICompletionsExt {}

impl ProviderBuilder for OpenAICompletionsExtBuilder {
    type Extension<H>
        = OpenAICompletionsExt
    where
        H: HttpClientExt;
    type ApiKey = OpenAIApiKey;

    const BASE_URL: &'static str = OPENAI_API_BASE_URL;

    fn build<H>(
        _builder: &client::ClientBuilder<Self, Self::ApiKey, H>,
    ) -> http_client::Result<Self::Extension<H>>
    where
        H: HttpClientExt,
    {
        Ok(OpenAICompletionsExt)
    }
}

#[derive(Debug, Deserialize)]
pub struct ApiErrorResponse {
    pub(crate) message: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum ApiResponse<T> {
    Ok(T),
    Err(ApiErrorResponse),
}
