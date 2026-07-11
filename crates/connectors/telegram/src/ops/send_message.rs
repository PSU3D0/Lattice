use crate::generated::types::{TelegramSendMessageInput, TelegramSendMessageOutput};
use crate::runtime::errors::ConnectorRuntimeError;
use crate::runtime::telegram_api::TelegramApi;

pub struct TelegramSendMessage;

impl TelegramSendMessage {
    pub const META: ::dag_core::ConnectorOpMetadata = ::dag_core::ConnectorOpMetadata {
        operation_id: "connector.telegram.send_message",
        connector_id: "connector.telegram",
        summary: "Send one text message to a chat as the authenticated bot",
        min_effects: ::dag_core::Effects::Effectful,
        max_determinism: ::dag_core::Determinism::BestEffort,
        determinism_hints: &[capabilities::http::HINT_HTTP],
        effect_hints: &[capabilities::http::HINT_HTTP_WRITE],
        roles: &[
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::EndpointProfile,
                name: "telegram_bot_default",
                expected_handle_kind: "endpoint.profile",
                required: true,
            },
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::OutboundAuth,
                name: "telegram_bot_auth",
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
        input: &TelegramSendMessageInput,
    ) -> Result<TelegramSendMessageOutput, ConnectorRuntimeError> {
        TelegramApi::for_action(Self::META.operation_id)
            .await?
            .send_message(input)
            .await
    }
}
