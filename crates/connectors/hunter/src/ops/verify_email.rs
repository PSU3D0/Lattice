use crate::generated::types::{HunterVerifyEmailInput, HunterVerifyEmailOutput};
use crate::runtime::errors::ConnectorRuntimeError;
use crate::runtime::hunter_api::HunterApi;

pub struct HunterVerifyEmail;

impl HunterVerifyEmail {
    pub const META: ::dag_core::ConnectorOpMetadata = ::dag_core::ConnectorOpMetadata {
        operation_id: "connector.hunter.verify_email",
        connector_id: "connector.hunter",
        summary: "Verify the deliverability of one email address via Hunter.io",
        min_effects: ::dag_core::Effects::ReadOnly,
        max_determinism: ::dag_core::Determinism::BestEffort,
        determinism_hints: &[capabilities::http::HINT_HTTP],
        effect_hints: &[capabilities::http::HINT_HTTP_READ],
        roles: &[
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::EndpointProfile,
                name: "hunter_default",
                expected_handle_kind: "endpoint.profile",
            },
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::OutboundAuth,
                name: "hunter_api_key_auth",
                expected_handle_kind: "http.bearer",
            },
        ],
        resolution: ::dag_core::ConnectorResolutionContract {
            supported_modes: &[::dag_core::ConnectorResolutionModeDecl::BoundConnection],
            default_mode: ::dag_core::ConnectorResolutionModeDecl::BoundConnection,
        },
    };

    pub async fn invoke(
        input: &HunterVerifyEmailInput,
    ) -> Result<HunterVerifyEmailOutput, ConnectorRuntimeError> {
        HunterApi::for_action(Self::META.operation_id)
            .await?
            .verify_email(input)
            .await
    }
}
