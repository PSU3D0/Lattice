use crate::generated::types::{GoogleSheetsFindRowsInput, GoogleSheetsFindRowsOutput};
use crate::runtime::errors::ConnectorRuntimeError;
use crate::runtime::sheets_api::SheetsApi;

pub struct GoogleSheetsFindRows;

impl GoogleSheetsFindRows {
    pub const META: ::dag_core::ConnectorOpMetadata = ::dag_core::ConnectorOpMetadata {
        operation_id: "connector.google.sheets.find_rows",
        connector_id: "connector.google.sheets",
        summary: "Find semantic rows in a sheet by column filters",
        min_effects: ::dag_core::Effects::ReadOnly,
        max_determinism: ::dag_core::Determinism::BestEffort,
        determinism_hints: &[capabilities::http::HINT_HTTP],
        effect_hints: &[capabilities::http::HINT_HTTP_READ],
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
    };

    pub async fn invoke(
        input: &GoogleSheetsFindRowsInput,
    ) -> Result<GoogleSheetsFindRowsOutput, ConnectorRuntimeError> {
        SheetsApi::for_action(Self::META.operation_id)
            .await?
            .find_rows(input)
            .await
    }
}
