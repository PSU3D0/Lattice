use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

use crate::generated::ops::GithubIssuesList;
use crate::generated::types::{GithubIssuesListInput, GithubIssuesListOutput};

#[def_node(
    name = "GithubIssuesList",
    summary = "List issues for a repository",
    identifier = "connector.github.issues.list",
    connector_ops(crate::generated::ops::GithubIssuesList)
)]
pub async fn github_issues_list(input: GithubIssuesListInput) -> NodeResult<GithubIssuesListOutput> {
    GithubIssuesList::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.github.issues.list failed: {err}")))
}
