use crate::generated::types::{GoogleSheetsCreateSheetInput, GoogleSheetsCreateSheetOutput};
use crate::runtime::errors::ConnectorRuntimeError;
use crate::runtime::sheets_api::SheetsApi;

pub struct GoogleSheetsCreateSheet;

impl GoogleSheetsCreateSheet {
    pub const META: ::dag_core::ConnectorOpMetadata = ::dag_core::ConnectorOpMetadata {
        operation_id: "connector.google.sheets.create_sheet",
        connector_id: "connector.google.sheets",
        summary: "Add a new sheet tab to an existing spreadsheet",
        min_effects: ::dag_core::Effects::Effectful,
        max_determinism: ::dag_core::Determinism::BestEffort,
        determinism_hints: &[capabilities::http::HINT_HTTP],
        effect_hints: &[capabilities::http::HINT_HTTP_WRITE],
        roles: &[
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::EndpointProfile,
                name: "google_sheets_default",
                expected_handle_kind: "endpoint.profile",
            },
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::OutboundAuth,
                name: "google_workspace_auth",
                expected_handle_kind: "http.bearer",
            },
        ],
        resolution: ::dag_core::ConnectorResolutionContract {
            supported_modes: &[::dag_core::ConnectorResolutionModeDecl::BoundConnection],
            default_mode: ::dag_core::ConnectorResolutionModeDecl::BoundConnection,
        },
    };

    pub async fn invoke(
        input: &GoogleSheetsCreateSheetInput,
    ) -> Result<GoogleSheetsCreateSheetOutput, ConnectorRuntimeError> {
        SheetsApi::for_action(Self::META.operation_id)
            .await?
            .create_sheet(input)
            .await
    }
}
