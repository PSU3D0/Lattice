use crate::generated::types::{LlmCompleteInput, LlmCompleteOutput};
use crate::runtime::errors::LlmConnectorError;
use crate::runtime::llm_api::LlmApi;

pub struct LlmComplete;

impl LlmComplete {
    pub const META: ::dag_core::ConnectorOpMetadata = ::dag_core::ConnectorOpMetadata {
        operation_id: "connector.llm.complete",
        connector_id: "connector.llm",
        summary: "Generate one typed completion from a lock-selected LLM provider",
        // Effectful: the call consumes billed provider quota and rides the
        // http_write capability (the transport vocabulary routes POST through
        // write); replays are not free, so the delivery gate must see it.
        min_effects: ::dag_core::Effects::Effectful,
        // Sampled output differs across identical inputs — never Strict.
        max_determinism: ::dag_core::Determinism::Nondeterministic,
        determinism_hints: &[capabilities::http::HINT_HTTP],
        effect_hints: &[capabilities::http::HINT_HTTP_WRITE],
        broker_contract: None,
        roles: &[
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::EndpointProfile,
                name: "llm_default",
                expected_handle_kind: "endpoint.profile",
                required: true,
            },
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::OutboundAuth,
                name: "llm_api_key",
                expected_handle_kind: "http.bearer",
                required: true,
            },
        ],
        resolution: ::dag_core::ConnectorResolutionContract {
            supported_modes: &[::dag_core::ConnectorResolutionModeDecl::BoundConnection],
            default_mode: ::dag_core::ConnectorResolutionModeDecl::BoundConnection,
        },
    };

    pub async fn invoke(input: &LlmCompleteInput) -> Result<LlmCompleteOutput, LlmConnectorError> {
        LlmApi::for_action(Self::META.operation_id)
            .await?
            .complete(input)
            .await
    }
}
