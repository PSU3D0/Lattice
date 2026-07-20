use crate::generated::types::{GoogleSheetsAppendRowInput, GoogleSheetsAppendRowOutput};
use crate::runtime::errors::ConnectorRuntimeError;
use crate::runtime::sheets_api::SheetsApi;

pub struct GoogleSheetsAppendRow;

impl GoogleSheetsAppendRow {
    pub const BROKER_CONTRACT: Option<::dag_core::BrokerContractMetadata> = Some(
        ::dag_core::BrokerContractMetadata {
            contract_id: "connector.google.sheets.append_row@1",
            contract_hash: "sha256:d02ed39536d396d66895f97672551a9eb443e701900112613865170bf157e999",
        },
    );

    pub const META: ::dag_core::ConnectorOpMetadata = ::dag_core::ConnectorOpMetadata {
        operation_id: "connector.google.sheets.append_row",
        connector_id: "connector.google.sheets",
        summary: "Append one semantic row to a sheet",
        min_effects: ::dag_core::Effects::Effectful,
        max_determinism: ::dag_core::Determinism::BestEffort,
        determinism_hints: &[capabilities::http::HINT_HTTP],
        effect_hints: &[
            capabilities::http::HINT_HTTP_READ,
            capabilities::http::HINT_HTTP_WRITE,
        ],
        roles: &[
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::EndpointProfile,
                name: "google_sheets_default",
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
        input: &GoogleSheetsAppendRowInput,
    ) -> Result<GoogleSheetsAppendRowOutput, ConnectorRuntimeError> {
        SheetsApi::for_action(Self::META.operation_id)
            .await?
            .append_row(input)
            .await
    }
}
