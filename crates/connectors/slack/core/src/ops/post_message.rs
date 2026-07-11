use crate::generated::types::{SlackPostMessageInput, SlackPostMessageOutput};
use crate::runtime::errors::ConnectorRuntimeError;
use crate::runtime::slack_api::SlackApi;

pub struct SlackPostMessage;

impl SlackPostMessage {
    pub const META: ::dag_core::ConnectorOpMetadata = ::dag_core::ConnectorOpMetadata {
        operation_id: "connector.slack.core.post_message",
        connector_id: "connector.slack.core",
        summary: "Post a message to a Slack channel",
        min_effects: ::dag_core::Effects::Effectful,
        max_determinism: ::dag_core::Determinism::BestEffort,
        determinism_hints: &[capabilities::http::HINT_HTTP],
        effect_hints: &[capabilities::http::HINT_HTTP_WRITE],
        roles: &[
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::EndpointProfile,
                name: "slack_default",
                expected_handle_kind: "endpoint.profile",
                required: true,
            },
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::OutboundAuth,
                name: "slack_auth",
                expected_handle_kind: "http.bearer",
                required: true,
            },
        ],
        resolution: ::dag_core::ConnectorResolutionContract {
            supported_modes: &[::dag_core::ConnectorResolutionModeDecl::BoundConnection],
            default_mode: ::dag_core::ConnectorResolutionModeDecl::BoundConnection,
        },
    };

    pub async fn invoke(
        input: &SlackPostMessageInput,
    ) -> Result<SlackPostMessageOutput, ConnectorRuntimeError> {
        SlackApi::for_action(Self::META.operation_id)
            .await?
            .post_message(input)
            .await
    }
}
