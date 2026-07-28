use crate::generated::types::{
    GoogleSheetsCreateSpreadsheetInput, GoogleSheetsCreateSpreadsheetOutput,
};
use crate::runtime::errors::ConnectorRuntimeError;
use crate::runtime::sheets_api::SheetsApi;

pub struct GoogleSheetsCreateSpreadsheet;

impl GoogleSheetsCreateSpreadsheet {
    pub const BROKER_CONTRACT: Option<::dag_core::BrokerContractMetadata> = Some(
        ::dag_core::BrokerContractMetadata {
            contract_id: "connector.google.sheets.create_spreadsheet@1",
            contract_hash: "sha256:8d01e95713c226e5200a113f6a718e6b48e1e79ce4362f04bfae059d51926165",
        },
    );

    pub const META: ::dag_core::ConnectorOpMetadata = ::dag_core::ConnectorOpMetadata {
        operation_id: "connector.google.sheets.create_spreadsheet",
        connector_id: "connector.google.sheets",
        summary: "Create a spreadsheet",
        min_effects: ::dag_core::Effects::Effectful,
        max_determinism: ::dag_core::Determinism::BestEffort,
        determinism_hints: &[capabilities::http::HINT_HTTP],
        effect_hints: &[capabilities::http::HINT_HTTP_WRITE],
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
        input: &GoogleSheetsCreateSpreadsheetInput,
    ) -> Result<GoogleSheetsCreateSpreadsheetOutput, ConnectorRuntimeError> {
        SheetsApi::for_action(Self::META.operation_id)
            .await?
            .create_spreadsheet(input)
            .await
    }
}
