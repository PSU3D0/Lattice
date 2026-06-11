//! Live smoke — env-gated, NEVER part of default CI.
//!
//! Both gates must be opened deliberately:
//! - the test is `#[ignore]`d, so `cargo test -p connector_github_issues`
//!   skips it even when credentials are present;
//! - it asserts `LATTICE_LIVE_SMOKE=1`, so `--ignored` sweeps cannot hit the
//!   real API by accident.
//!
//! Run:
//! ```sh
//! LATTICE_LIVE_SMOKE=1 \
//! LATTICE_LIVE_SMOKE_GITHUB_REPO=owner/repo \
//! LATTICE_CONNECTOR_AUTH_GITHUB_PAT=ghp_... \
//!   cargo test -p connector_github_issues --test live_smoke -- --ignored --nocapture
//! ```
//!
//! Read-only by design: live smoke proves auth + endpoint + decode against the
//! real API. Effectful ops are NOT exercised live from tests; their behavior is
//! proven offline (runtime.rs, honesty.rs).

use std::sync::Arc;

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::{ResourceBag, context};
use connector_github_issues::runtime::transport::EnvConnectorRuntime;
use connector_github_issues::{GithubIssueState, GithubIssuesListInput, github_issues_list};

fn live_resources() -> Arc<ResourceBag> {
    let client = Arc::new(ReqwestHttpClient::default());
    Arc::new(
        ResourceBag::default()
            .with_http_read(Arc::clone(&client))
            .with_http_write(client)
            .with_connector_runtime(Arc::new(EnvConnectorRuntime))
            .with_connector_scope(capabilities::connector::ConnectorBindingScope::new(
                "flow://live-smoke",
                "live_smoke",
                "connector.github.issues.list",
                "connector.github.issues",
            )),
    )
}

#[tokio::test]
#[ignore = "live smoke: requires LATTICE_LIVE_SMOKE=1 and network access"]
async fn list_issues_against_live_api() {
    assert_eq!(
        std::env::var("LATTICE_LIVE_SMOKE").as_deref(),
        Ok("1"),
        "set LATTICE_LIVE_SMOKE=1 to run live smoke deliberately"
    );

    let repo_spec = std::env::var("LATTICE_LIVE_SMOKE_GITHUB_REPO")
        .unwrap_or_else(|_| "octocat/Hello-World".to_string());
    let (owner, repo) = repo_spec
        .split_once('/')
        .expect("LATTICE_LIVE_SMOKE_GITHUB_REPO must be owner/repo");

    let output = context::with_resources(live_resources(), async {
        github_issues_list(GithubIssuesListInput {
            owner: owner.to_string(),
            repo: repo.to_string(),
            state: Some(GithubIssueState::All),
            return_all: false,
            limit: Some(5),
        })
        .await
        .expect("live list succeeds")
    })
    .await;

    println!(
        "live smoke: listed {} issues from {repo_spec}",
        output.items.len()
    );
}
