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
            contract_hash: "sha256:d6c81edf333ec75dfa4d1b3bdda03d2c9b6be51d6d7a7bc2b7e218723240cd76",
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
