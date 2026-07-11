use crate::generated::types::{NotionCreatePageInput, NotionCreatePageOutput};
use crate::runtime::errors::ConnectorRuntimeError;
use crate::runtime::notion_api::NotionApi;

pub struct NotionCreatePage;

impl NotionCreatePage {
    pub const META: ::dag_core::ConnectorOpMetadata = ::dag_core::ConnectorOpMetadata {
        operation_id: "connector.notion.create_page",
        connector_id: "connector.notion",
        summary: "Create one page in a Notion database",
        min_effects: ::dag_core::Effects::Effectful,
        max_determinism: ::dag_core::Determinism::BestEffort,
        determinism_hints: &[capabilities::http::HINT_HTTP],
        effect_hints: &[capabilities::http::HINT_HTTP_WRITE],
        roles: &[
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::EndpointProfile,
                name: "notion_default",
                expected_handle_kind: "endpoint.profile",
                required: true,
            },
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::OutboundAuth,
                name: "notion_api_auth",
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
        input: &NotionCreatePageInput,
    ) -> Result<NotionCreatePageOutput, ConnectorRuntimeError> {
        NotionApi::for_action(Self::META.operation_id)
            .await?
            .create_page(input)
            .await
    }
}
