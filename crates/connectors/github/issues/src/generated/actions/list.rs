use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

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

#[def_node(
    name = "GithubIssuesList",
    summary = "List issues for a repository",
    identifier = "connector.github.issues.list",
    effects = "ReadOnly",
    determinism = "BestEffort",
    resources(http_read(capabilities::http::HttpRead))
)]
pub async fn github_issues_list(
    input: GithubIssuesListInput,
) -> NodeResult<GithubIssuesListOutput> {
    run_action_from_current(&input, &GITHUB_ISSUES_LIST_ACTION)
        .await
        .map_err(|err| NodeError::new(format!("connector.github.issues.list failed: {err}")))
}
