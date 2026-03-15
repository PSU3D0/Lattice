use crate::generated::types::{GithubIssueGetInput, GithubIssueSummary};
use crate::runtime::transport::{ActionDescriptor, FieldBinding, RequestDescriptor, ResponseDescriptor, run_action_from_current};

const GITHUB_ISSUES_GET_REQUEST: RequestDescriptor = RequestDescriptor {
    method: capabilities::http::HttpMethod::Get,
    path_template: "/repos/{owner}/{repo}/issues/{issue_number}",
    path_params: &[FieldBinding { wire_name: "issue_number", input_field: "issue_number" }, FieldBinding { wire_name: "owner", input_field: "owner" }, FieldBinding { wire_name: "repo", input_field: "repo" }],
    query: &[],
    body: &[],
    headers: &[],
};

const GITHUB_ISSUES_GET_RESPONSE: ResponseDescriptor = ResponseDescriptor {
    root_path: "body",
    collection_field: None,
};

const GITHUB_ISSUES_GET_ACTION: ActionDescriptor = ActionDescriptor {
    identifier: "connector.github.issues.get",
    endpoint: &crate::generated::profiles::GITHUB_DEFAULT_ENDPOINT_PROFILE,
    auth: None,
    request: &GITHUB_ISSUES_GET_REQUEST,
    pagination: None,
    response: &GITHUB_ISSUES_GET_RESPONSE,
};

pub struct GithubIssuesGet;

impl GithubIssuesGet {
    pub const META: ::dag_core::ConnectorOpMetadata = ::dag_core::ConnectorOpMetadata {
        operation_id: "connector.github.issues.get",
        connector_id: "connector.github.issues",
        summary: "Fetch one issue by number",
        min_effects: ::dag_core::Effects::ReadOnly,
        max_determinism: ::dag_core::Determinism::BestEffort,
        determinism_hints: &[capabilities::http::HINT_HTTP],
        effect_hints: &[capabilities::http::HINT_HTTP_READ],
        roles: &[::dag_core::ConnectorRoleRequirement { kind: ::dag_core::ConnectorRoleKindDecl::EndpointProfile, name: "github_default", expected_handle_kind: "endpoint.profile" }],
    };

    pub async fn invoke(input: &GithubIssueGetInput) -> Result<GithubIssueSummary, crate::runtime::errors::ConnectorRuntimeError> {
        run_action_from_current(input, &GITHUB_ISSUES_GET_ACTION).await
    }
}
