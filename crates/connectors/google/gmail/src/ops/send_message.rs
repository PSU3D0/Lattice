use crate::generated::types::{GoogleGmailSendMessageInput, GoogleGmailSendMessageOutput};
use crate::runtime::errors::ConnectorRuntimeError;
use crate::runtime::gmail_api::GmailApi;

pub struct GoogleGmailSendMessage;

impl GoogleGmailSendMessage {
    pub const BROKER_CONTRACT: Option<::dag_core::BrokerContractMetadata> = Some(
        ::dag_core::BrokerContractMetadata {
            contract_id: "connector.google.gmail.send_message@1",
            contract_hash: "sha256:8fbdd2dbb63877b92004b7b5e6a7dc665a0ec5788850e4a466c0b6200639de2a",
        },
    );

    pub const META: ::dag_core::ConnectorOpMetadata = ::dag_core::ConnectorOpMetadata {
        operation_id: "connector.google.gmail.send_message",
        connector_id: "connector.google.gmail",
        summary: "Send one plain-text email from the authenticated mailbox",
        min_effects: ::dag_core::Effects::Effectful,
        max_determinism: ::dag_core::Determinism::BestEffort,
        determinism_hints: &[capabilities::http::HINT_HTTP],
        effect_hints: &[capabilities::http::HINT_HTTP_WRITE],
        roles: &[
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::EndpointProfile,
                name: "google_gmail_default",
                expected_handle_kind: "endpoint.profile",
                required: true,
            },
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::OutboundAuth,
                name: "google_workspace_auth",
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
        input: &GoogleGmailSendMessageInput,
    ) -> Result<GoogleGmailSendMessageOutput, ConnectorRuntimeError> {
        GmailApi::for_action(Self::META.operation_id)
            .await?
            .send_message(input)
            .await
    }
}
