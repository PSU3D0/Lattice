use crate::generated::types::{GoogleDriveSearchFilesInput, GoogleDriveSearchFilesOutput};
use crate::runtime::drive_api::DriveApi;
use crate::runtime::errors::ConnectorRuntimeError;

pub struct GoogleDriveSearchFiles;

impl GoogleDriveSearchFiles {
    pub const META: ::dag_core::ConnectorOpMetadata = ::dag_core::ConnectorOpMetadata {
        operation_id: "connector.google.drive.search_files",
        connector_id: "connector.google.drive",
        summary: "Search files by Drive query string, returning sharing metadata per file",
        min_effects: ::dag_core::Effects::ReadOnly,
        max_determinism: ::dag_core::Determinism::BestEffort,
        determinism_hints: &[capabilities::http::HINT_HTTP],
        effect_hints: &[capabilities::http::HINT_HTTP_READ],
        roles: &[
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::EndpointProfile,
                name: "google_drive_default",
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
        input: &GoogleDriveSearchFilesInput,
    ) -> Result<GoogleDriveSearchFilesOutput, ConnectorRuntimeError> {
        DriveApi::for_action(Self::META.operation_id)
            .await?
            .search_files(input)
            .await
    }
}
