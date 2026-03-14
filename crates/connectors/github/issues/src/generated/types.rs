use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GithubIssueCreateInput {
    #[serde(rename = "body")]
    pub body: Option<String>,
    #[serde(rename = "owner")]
    pub owner: String,
    #[serde(rename = "repo")]
    pub repo: String,
    #[serde(rename = "title")]
    pub title: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GithubIssueGetInput {
    #[serde(rename = "issue_number")]
    pub issue_number: u64,
    #[serde(rename = "owner")]
    pub owner: String,
    #[serde(rename = "repo")]
    pub repo: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum GithubIssueState {
    #[serde(rename = "open")]
    Open,
    #[serde(rename = "closed")]
    Closed,
    #[serde(rename = "all")]
    All,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GithubIssueSummary {
    #[serde(rename = "html_url")]
    pub html_url: String,
    #[serde(rename = "number")]
    pub number: u64,
    #[serde(rename = "state")]
    pub state: String,
    #[serde(rename = "title")]
    pub title: String,
}

fn __default_github_issues_list_input_return_all() -> bool {
    false
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GithubIssuesListInput {
    #[serde(rename = "limit")]
    pub limit: Option<u32>,
    #[serde(rename = "owner")]
    pub owner: String,
    #[serde(rename = "repo")]
    pub repo: String,
    #[serde(rename = "return_all")]
    #[serde(default = "__default_github_issues_list_input_return_all")]
    pub return_all: bool,
    #[serde(rename = "state")]
    pub state: Option<GithubIssueState>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GithubIssuesListOutput {
    #[serde(rename = "items")]
    pub items: Vec<GithubIssueSummary>,
}
