use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

use crate::generated::ops::GithubIssuesGet;
use crate::generated::types::{GithubIssueGetInput, GithubIssueSummary};

#[def_node(
    name = "GithubIssuesGet",
    summary = "Fetch one issue by number",
    identifier = "connector.github.issues.get",
    connector_ops(crate::generated::ops::GithubIssuesGet)
)]
pub async fn github_issues_get(input: GithubIssueGetInput) -> NodeResult<GithubIssueSummary> {
    GithubIssuesGet::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.github.issues.get failed: {err}")))
}
