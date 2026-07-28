use crate::generated::types::{AirtableCreateRecordInput, AirtableCreateRecordOutput};
use crate::runtime::airtable_api::AirtableApi;
use crate::runtime::errors::ConnectorRuntimeError;

pub struct AirtableCreateRecord;

impl AirtableCreateRecord {
    pub const META: ::dag_core::ConnectorOpMetadata = ::dag_core::ConnectorOpMetadata {
        operation_id: "connector.airtable.create_record",
        connector_id: "connector.airtable",
        summary: "Create one record in an Airtable table",
        min_effects: ::dag_core::Effects::Effectful,
        max_determinism: ::dag_core::Determinism::BestEffort,
        determinism_hints: &[capabilities::http::HINT_HTTP],
        effect_hints: &[capabilities::http::HINT_HTTP_WRITE],
        broker_contract: None,
        roles: &[
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::EndpointProfile,
                name: "airtable_default",
                expected_handle_kind: "endpoint.profile",
                required: true,
            },
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::OutboundAuth,
                name: "airtable_token_auth",
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
        input: &AirtableCreateRecordInput,
    ) -> Result<AirtableCreateRecordOutput, ConnectorRuntimeError> {
        AirtableApi::for_action(Self::META.operation_id)
            .await?
            .create_record(input)
            .await
    }
}
