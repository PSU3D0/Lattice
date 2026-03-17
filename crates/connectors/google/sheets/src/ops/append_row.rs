use crate::generated::types::{GoogleSheetsAppendRowInput, GoogleSheetsAppendRowOutput};
use crate::runtime::errors::ConnectorRuntimeError;
use crate::runtime::sheets_api::SheetsApi;

pub struct GoogleSheetsAppendRow;

impl GoogleSheetsAppendRow {
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
            },
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::OutboundAuth,
                name: "google_workspace_auth",
                expected_handle_kind: "http.bearer",
            },
        ],
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
