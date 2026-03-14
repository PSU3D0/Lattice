use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

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

#[def_node(
    name = "GithubIssuesCreate",
    summary = "Create an issue for a repository",
    identifier = "connector.github.issues.create",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(http_write(capabilities::http::HttpWrite))
)]
pub async fn github_issues_create(input: GithubIssueCreateInput) -> NodeResult<GithubIssueSummary> {
    run_action_from_current(&input, &GITHUB_ISSUES_CREATE_ACTION)
        .await
        .map_err(|err| NodeError::new(format!("connector.github.issues.create failed: {err}")))
}
