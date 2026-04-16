use crate::generated::types::{GithubIssueCreateInput, GithubIssueSummary};
use crate::runtime::transport::{
    ActionDescriptor, FieldBinding, RequestDescriptor, ResponseDescriptor, run_action_from_current,
};

const GITHUB_ISSUES_CREATE_REQUEST: RequestDescriptor = RequestDescriptor {
    method: capabilities::http::HttpMethod::Post,
    path_template: "/repos/{owner}/{repo}/issues",
    path_params: &[
        FieldBinding {
            wire_name: "owner",
            input_field: "owner",
        },
        FieldBinding {
            wire_name: "repo",
            input_field: "repo",
        },
    ],
    query: &[],
    body: &[
        FieldBinding {
            wire_name: "body",
            input_field: "body",
        },
        FieldBinding {
            wire_name: "title",
            input_field: "title",
        },
    ],
    headers: &[],
};

const GITHUB_ISSUES_CREATE_RESPONSE: ResponseDescriptor = ResponseDescriptor {
    root_path: "body",
    collection_field: None,
};

const GITHUB_ISSUES_CREATE_ACTION: ActionDescriptor = ActionDescriptor {
    identifier: "connector.github.issues.create",
    endpoint: &crate::generated::profiles::GITHUB_DEFAULT_ENDPOINT_PROFILE,
    auth: Some(&crate::generated::profiles::GITHUB_PAT_OUTBOUND_AUTH),
    request: &GITHUB_ISSUES_CREATE_REQUEST,
    pagination: None,
    response: &GITHUB_ISSUES_CREATE_RESPONSE,
};

pub struct GithubIssuesCreate;

impl GithubIssuesCreate {
    pub const META: ::dag_core::ConnectorOpMetadata = ::dag_core::ConnectorOpMetadata {
        operation_id: "connector.github.issues.create",
        connector_id: "connector.github.issues",
        summary: "Create an issue for a repository",
        min_effects: ::dag_core::Effects::Effectful,
        max_determinism: ::dag_core::Determinism::BestEffort,
        determinism_hints: &[capabilities::http::HINT_HTTP],
        effect_hints: &[capabilities::http::HINT_HTTP_WRITE],
        roles: &[
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::EndpointProfile,
                name: "github_default",
                expected_handle_kind: "endpoint.profile",
            },
            ::dag_core::ConnectorRoleRequirement {
                kind: ::dag_core::ConnectorRoleKindDecl::OutboundAuth,
                name: "github_pat",
                expected_handle_kind: "http.bearer",
            },
        ],
        resolution: ::dag_core::ConnectorResolutionContract {
            supported_modes: &[::dag_core::ConnectorResolutionModeDecl::BoundConnection],
            default_mode: ::dag_core::ConnectorResolutionModeDecl::BoundConnection,
        },
    };

    pub async fn invoke(
        input: &GithubIssueCreateInput,
    ) -> Result<GithubIssueSummary, crate::runtime::errors::ConnectorRuntimeError> {
        run_action_from_current(input, &GITHUB_ISSUES_CREATE_ACTION).await
    }
}
