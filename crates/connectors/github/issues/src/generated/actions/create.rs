use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

use crate::generated::ops::GithubIssuesCreate;
use crate::generated::types::{GithubIssueCreateInput, GithubIssueSummary};

#[def_node(
    name = "GithubIssuesCreate",
    summary = "Create an issue for a repository",
    identifier = "connector.github.issues.create",
    connector_ops(crate::generated::ops::GithubIssuesCreate)
)]
pub async fn github_issues_create(input: GithubIssueCreateInput) -> NodeResult<GithubIssueSummary> {
    GithubIssuesCreate::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.github.issues.create failed: {err}")))
}
