use crate::generated::types::{GithubIssuesListInput, GithubIssuesListOutput};
use crate::runtime::transport::PaginationDescriptor;
use crate::runtime::transport::{
    ActionDescriptor, FieldBinding, RequestDescriptor, ResponseDescriptor, run_action_from_current,
};

const GITHUB_ISSUES_LIST_REQUEST: RequestDescriptor = RequestDescriptor {
    method: capabilities::http::HttpMethod::Get,
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
    query: &[FieldBinding {
        wire_name: "state",
        input_field: "state",
    }],
    body: &[],
    headers: &[],
};

const GITHUB_ISSUES_LIST_PAGINATION: PaginationDescriptor = PaginationDescriptor {
    enabled_from: "return_all",
    page_size_param: "per_page",
    page_size: 100,
    max_items_from: Some("limit"),
};

const GITHUB_ISSUES_LIST_RESPONSE: ResponseDescriptor = ResponseDescriptor {
    root_path: "body",
    collection_field: Some("items"),
};

const GITHUB_ISSUES_LIST_ACTION: ActionDescriptor = ActionDescriptor {
    identifier: "connector.github.issues.list",
    endpoint: &crate::generated::profiles::GITHUB_DEFAULT_ENDPOINT_PROFILE,
    auth: None,
    request: &GITHUB_ISSUES_LIST_REQUEST,
    pagination: Some(&GITHUB_ISSUES_LIST_PAGINATION),
    response: &GITHUB_ISSUES_LIST_RESPONSE,
};

pub struct GithubIssuesList;

impl GithubIssuesList {
    pub const META: ::dag_core::ConnectorOpMetadata = ::dag_core::ConnectorOpMetadata {
        operation_id: "connector.github.issues.list",
        connector_id: "connector.github.issues",
        summary: "List issues for a repository",
        min_effects: ::dag_core::Effects::ReadOnly,
        max_determinism: ::dag_core::Determinism::BestEffort,
        determinism_hints: &[capabilities::http::HINT_HTTP],
        effect_hints: &[capabilities::http::HINT_HTTP_READ],
        roles: &[::dag_core::ConnectorRoleRequirement {
            kind: ::dag_core::ConnectorRoleKindDecl::EndpointProfile,
            name: "github_default",
            expected_handle_kind: "endpoint.profile",
        }],
        resolution: ::dag_core::ConnectorResolutionContract {
            supported_modes: &[::dag_core::ConnectorResolutionModeDecl::BoundConnection],
            default_mode: ::dag_core::ConnectorResolutionModeDecl::BoundConnection,
        },
    };

    pub async fn invoke(
        input: &GithubIssuesListInput,
    ) -> Result<GithubIssuesListOutput, crate::runtime::errors::ConnectorRuntimeError> {
        run_action_from_current(input, &GITHUB_ISSUES_LIST_ACTION).await
    }
}
