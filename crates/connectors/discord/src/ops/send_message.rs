use crate::generated::types::{DiscordSendMessageInput, DiscordSendMessageOutput};
use crate::runtime::discord_api::DiscordApi;
use crate::runtime::errors::ConnectorRuntimeError;

pub struct DiscordSendMessage;

impl DiscordSendMessage {
    pub const META: ::dag_core::ConnectorOpMetadata = ::dag_core::ConnectorOpMetadata {
        operation_id: "connector.discord.send_message",
        connector_id: "connector.discord",
        summary: "Post one message (content plus rich embeds) to a Discord webhook",
        min_effects: ::dag_core::Effects::Effectful,
        max_determinism: ::dag_core::Determinism::BestEffort,
        determinism_hints: &[capabilities::http::HINT_HTTP],
        effect_hints: &[capabilities::http::HINT_HTTP_WRITE],
        broker_contract: None,
        roles: &[
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::EndpointProfile,
                name: "discord_webhook_default",
                expected_handle_kind: "endpoint.profile",
                required: true,
            },
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::OutboundAuth,
                name: "discord_webhook_auth",
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
        input: &DiscordSendMessageInput,
    ) -> Result<DiscordSendMessageOutput, ConnectorRuntimeError> {
        DiscordApi::for_action(Self::META.operation_id)
            .await?
            .send_message(input)
            .await
    }
}
