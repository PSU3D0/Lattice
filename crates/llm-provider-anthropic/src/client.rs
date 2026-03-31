//! Anthropic client api implementation
use http::{HeaderMap, HeaderName, HeaderValue};

use super::completion::{ANTHROPIC_VERSION_LATEST, CompletionModel};
use llm_agent::client::{
    self, ApiKey, Capabilities, Capable, DebugExt, Nothing, Provider, ProviderBuilder,
};
use llm_types::http_client::{self, HttpClientExt};

// ================================================================
// Main Anthropic Client
// ================================================================
#[derive(Debug, Default, Clone)]
pub struct AnthropicExt;

impl Provider for AnthropicExt {
    type Builder = AnthropicBuilder;
    const VERIFY_PATH: &'static str = "/v1/models";
}

impl<H> Capabilities<H> for AnthropicExt {
    type Completion = Capable<CompletionModel<H>>;

    type Embeddings = Nothing;
    type Transcription = Nothing;
    type ModelListing = Nothing;

}

#[derive(Debug, Clone)]
pub struct AnthropicBuilder {
    anthropic_version: String,
    anthropic_betas: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct AnthropicKey(String);

impl<S> From<S> for AnthropicKey
where
    S: Into<String>,
{
    fn from(value: S) -> Self {
        Self(value.into())
    }
}

impl ApiKey for AnthropicKey {
    fn into_header(self) -> Option<http_client::Result<(http::HeaderName, HeaderValue)>> {
        Some(
            HeaderValue::from_str(&self.0)
                .map(|val| (HeaderName::from_static("x-api-key"), val))
                .map_err(Into::into),
        )
    }
}

pub type Client<H> = client::Client<AnthropicExt, H>;
pub type ClientBuilder<H> =
    client::ClientBuilder<AnthropicBuilder, AnthropicKey, H>;

impl Default for AnthropicBuilder {
    fn default() -> Self {
        Self {
            anthropic_version: ANTHROPIC_VERSION_LATEST.into(),
            anthropic_betas: Vec::new(),
        }
    }
}

impl ProviderBuilder for AnthropicBuilder {
    type Extension<H>
        = AnthropicExt
    where
        H: HttpClientExt;
    type ApiKey = AnthropicKey;

    const BASE_URL: &'static str = "https://api.anthropic.com";

    fn build<H>(
        _builder: &client::ClientBuilder<Self, Self::ApiKey, H>,
    ) -> http_client::Result<Self::Extension<H>>
    where
        H: HttpClientExt,
    {
        Ok(AnthropicExt)
    }

    fn finish<H>(
        &self,
        builder: client::ClientBuilder<Self, AnthropicKey, H>,
    ) -> http_client::Result<client::ClientBuilder<Self, AnthropicKey, H>> {
        let mut headers = HeaderMap::new();
        headers.insert(
            "anthropic-version",
            HeaderValue::from_str(&self.anthropic_version)?,
        );

        if !self.anthropic_betas.is_empty() {
            headers.insert(
                "anthropic-beta",
                HeaderValue::from_str(&self.anthropic_betas.join(","))?,
            );
        }

        Ok(builder.http_headers(headers))
    }
}

impl DebugExt for AnthropicExt {}



