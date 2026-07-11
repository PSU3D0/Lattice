//! Handwritten LLM runtime: a typed completion op over the existing `llm-*`
//! crates.
//!
//! Endpoint + credential resolve from the current connector context (bindings
//! lock / `EnvConnectorRuntime`), exactly like the Gmail/Sheets handwritten
//! runtimes. The wire itself is composed by the existing provider crates
//! (`llm-provider-openai`, `llm-provider-anthropic`) and dispatched through
//! `llm-lattice`'s [`LatticeHttpClient`], so every request still routes
//! through the node's *granted* scoped HTTP capabilities: an undeclared
//! `http_write` fails closed with a CAP110 denial before anything leaves the
//! process.

use capabilities::http::{HttpMethod, HttpRequest};
use connectors_std::endpoint::ResolvedEndpointProfile;
use connectors_std::{
    CurrentConnectorContext, apply_outbound_auth_with_context, current_connector_context,
    resolve_endpoint_with_context,
};
use llm_agent::client::CompletionClient;
use llm_lattice::LatticeHttpClient;
use llm_types::completion::{CompletionModel, CompletionResponse};
use llm_types::message::{AssistantContent, Message};

use crate::generated::profiles::{LLM_API_KEY_OUTBOUND_AUTH, LLM_DEFAULT_ENDPOINT_PROFILE};
use crate::generated::types::{LlmCompleteInput, LlmCompleteOutput, LlmProvider, LlmUsage};
use crate::runtime::errors::LlmConnectorError;

pub struct LlmApi {
    endpoint: ResolvedEndpointProfile,
    api_key: String,
}

impl LlmApi {
    pub async fn for_action(action_id: &'static str) -> Result<Self, LlmConnectorError> {
        let context = current_connector_context(action_id).await?;
        let endpoint =
            resolve_endpoint_with_context(&LLM_DEFAULT_ENDPOINT_PROFILE, &context).await?;
        let api_key = resolve_api_key(&context, &endpoint).await?;
        Ok(Self { endpoint, api_key })
    }

    pub async fn complete(
        &self,
        input: &LlmCompleteInput,
    ) -> Result<LlmCompleteOutput, LlmConnectorError> {
        // Capture the currently scoped resources: enforcement (CAP110) happens
        // inside this bridge when the provider client dispatches the POST.
        let http = LatticeHttpClient::current();

        match input.provider {
            LlmProvider::OpenaiCompat => {
                let client = llm_provider_openai::Client::<LatticeHttpClient>::builder()
                    .base_url(&self.endpoint.base_url)
                    .api_key(self.api_key.clone())
                    .http_client(http)
                    .build()?;
                self.run(client.completion_model(&input.model), input).await
            }
            LlmProvider::Anthropic => {
                let client = llm_provider_anthropic::Client::<LatticeHttpClient>::builder()
                    .base_url(&self.endpoint.base_url)
                    .api_key(self.api_key.clone())
                    .http_client(http)
                    .build()?;
                self.run(client.completion_model(&input.model), input).await
            }
        }
    }

    async fn run<M>(
        &self,
        model: M,
        input: &LlmCompleteInput,
    ) -> Result<LlmCompleteOutput, LlmConnectorError>
    where
        M: CompletionModel,
    {
        let mut request = model
            .completion_request(Message::user(input.prompt.clone()))
            .temperature_opt(input.temperature)
            .max_tokens_opt(input.max_tokens)
            .output_schema_opt(parse_output_schema(input.output_schema.as_ref())?);
        if let Some(system) = &input.system {
            request = request.preamble(system.clone());
        }

        let response = request.send().await?;
        output_from_response(input, response)
    }
}

async fn resolve_api_key(
    context: &CurrentConnectorContext,
    endpoint: &ResolvedEndpointProfile,
) -> Result<String, LlmConnectorError> {
    // The provider crates own header composition (`Authorization: Bearer` for
    // OpenAI-compatible APIs, `x-api-key` for Anthropic), so the connector
    // runtime's bearer application is used as the secret source, not as the
    // final wire header.
    let mut request = HttpRequest::new(HttpMethod::Post, endpoint.base_url.clone());
    apply_outbound_auth_with_context(&LLM_API_KEY_OUTBOUND_AUTH, &mut request, context).await?;

    request
        .headers
        .get("authorization")
        .or_else(|| request.headers.get("Authorization"))
        .and_then(|header| header.strip_prefix("Bearer "))
        .map(str::to_string)
        .ok_or(LlmConnectorError::MissingBearerSecret)
}

fn parse_output_schema(
    schema: Option<&serde_json::Value>,
) -> Result<Option<schemars::Schema>, LlmConnectorError> {
    schema
        .map(|value| {
            schemars::Schema::try_from(value.clone())
                .map_err(|err| LlmConnectorError::InvalidOutputSchema(err.to_string()))
        })
        .transpose()
}

fn output_from_response<T>(
    input: &LlmCompleteInput,
    response: CompletionResponse<T>,
) -> Result<LlmCompleteOutput, LlmConnectorError> {
    let text = response
        .choice
        .iter()
        .filter_map(|content| match content {
            AssistantContent::Text(text) => Some(text.text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("");

    if text.is_empty() {
        return Err(LlmConnectorError::invalid_response(
            "completion contained no assistant text content",
        ));
    }

    let structured = input
        .output_schema
        .as_ref()
        .map(|_| {
            serde_json::from_str::<serde_json::Value>(&text).map_err(|err| {
                LlmConnectorError::invalid_response(format!(
                    "`output_schema` was set but the completion text is not valid JSON: {err}"
                ))
            })
        })
        .transpose()?;

    let usage = response.usage;
    let total_tokens = if usage.total_tokens > 0 {
        usage.total_tokens
    } else {
        usage.input_tokens + usage.output_tokens
    };

    Ok(LlmCompleteOutput {
        text,
        structured,
        model: input.model.clone(),
        usage: LlmUsage {
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            total_tokens,
        },
    })
}
