use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

use crate::generated::types::{GithubIssueGetInput, GithubIssueSummary};
use crate::runtime::transport::{
    ActionDescriptor, FieldBinding, RequestDescriptor, ResponseDescriptor, run_action_from_current,
};

const GITHUB_ISSUES_GET_REQUEST: RequestDescriptor = RequestDescriptor {
    method: capabilities::http::HttpMethod::Get,
    path_template: "/repos/{owner}/{repo}/issues/{issue_number}",
    path_params: &[
        FieldBinding {
            wire_name: "issue_number",
            input_field: "issue_number",
        },
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

#[def_node(
    name = "GithubIssuesGet",
    summary = "Fetch one issue by number",
    identifier = "connector.github.issues.get",
    effects = "ReadOnly",
    determinism = "BestEffort",
    resources(http_read(capabilities::http::HttpRead))
)]
pub async fn github_issues_get(input: GithubIssueGetInput) -> NodeResult<GithubIssueSummary> {
    run_action_from_current(&input, &GITHUB_ISSUES_GET_ACTION)
        .await
        .map_err(|err| NodeError::new(format!("connector.github.issues.get failed: {err}")))
}
