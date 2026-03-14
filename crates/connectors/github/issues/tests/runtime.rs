use std::sync::{Arc, Mutex};

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::{ResourceBag, context};
use connector_github_issues::{
    GithubIssueCreateInput, GithubIssueGetInput, GithubIssueState, GithubIssuesListInput,
    github_issues_create, github_issues_get, github_issues_list,
};
use httpmock::Method::{GET, POST};
use httpmock::MockServer;

static ENV_LOCK: Mutex<()> = Mutex::new(());
const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_GITHUB_DEFAULT_BASE_URL";
const AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_GITHUB_PAT";

struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, previous }
    }

    fn remove(key: &'static str) -> Self {
        let previous = std::env::var(key).ok();
        unsafe {
            std::env::remove_var(key);
        }
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(previous) => unsafe {
                std::env::set_var(self.key, previous);
            },
            None => unsafe {
                std::env::remove_var(self.key);
            },
        }
    }
}

fn http_resources() -> Arc<ResourceBag> {
    let client = Arc::new(ReqwestHttpClient::default());
    Arc::new(
        ResourceBag::default()
            .with_http_read(Arc::clone(&client))
            .with_http_write(client),
    )
}

#[tokio::test]
async fn get_action_round_trips_against_mock_server() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let mock = server.mock(|when, then| {
        when.method(GET).path("/repos/octo/demo/issues/42");
        then.status(200).json_body_obj(&serde_json::json!({
            "number": 42,
            "title": "Connector substrate",
            "state": "open",
            "html_url": "https://example.test/issues/42"
        }));
    });

    let output = context::with_resources(http_resources(), async {
        github_issues_get(GithubIssueGetInput {
            owner: "octo".to_string(),
            repo: "demo".to_string(),
            issue_number: 42,
        })
        .await
        .expect("get succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(output.number, 42);
    assert_eq!(output.title, "Connector substrate");
}

#[tokio::test]
async fn list_action_follows_link_header_when_enabled() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let next_url = format!(
        "{}/repos/octo/demo/issues/page/2?per_page=100&state=open",
        server.base_url()
    );

    let page_one = server.mock(|when, then| {
        when.method(GET)
            .path("/repos/octo/demo/issues")
            .header("accept", "application/json")
            .header("x-github-api-version", "2022-11-28")
            .query_param("per_page", "100")
            .query_param("state", "open");
        then.status(200)
            .header("link", &format!("<{next_url}>; rel=\"next\""))
            .header("content-type", "application/json")
            .body(
                r#"[{"number":1,"title":"first","state":"open","html_url":"https://example.test/issues/1"}]"#,
            );
    });

    let page_two = server.mock(|when, then| {
        when.method(GET)
            .path("/repos/octo/demo/issues/page/2")
            .header("accept", "application/json")
            .header("x-github-api-version", "2022-11-28")
            .query_param("per_page", "100")
            .query_param("state", "open");
        then.status(200)
            .header("content-type", "application/json")
            .body(
                r#"[{"number":2,"title":"second","state":"open","html_url":"https://example.test/issues/2"}]"#,
            );
    });

    let output = context::with_resources(http_resources(), async {
        github_issues_list(GithubIssuesListInput {
            owner: "octo".to_string(),
            repo: "demo".to_string(),
            state: Some(GithubIssueState::Open),
            return_all: true,
            limit: None,
        })
        .await
        .expect("list succeeds")
    })
    .await;

    page_one.assert();
    page_two.assert();
    assert_eq!(output.items.len(), 2);
    assert_eq!(output.items[0].number, 1);
    assert_eq!(output.items[1].number, 2);
}

#[tokio::test]
async fn create_action_uses_bearer_auth_env_override() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "super-secret-token");

    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/repos/octo/demo/issues")
            .header("authorization", "Bearer super-secret-token")
            .json_body_obj(&serde_json::json!({
                "title": "created from lattice",
                "body": "hello from codegen"
            }));
        then.status(201).json_body_obj(&serde_json::json!({
            "number": 99,
            "title": "created from lattice",
            "state": "open",
            "html_url": "https://example.test/issues/99"
        }));
    });

    let output = context::with_resources(http_resources(), async {
        github_issues_create(GithubIssueCreateInput {
            owner: "octo".to_string(),
            repo: "demo".to_string(),
            title: "created from lattice".to_string(),
            body: Some("hello from codegen".to_string()),
        })
        .await
        .expect("create succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(output.number, 99);
    assert_eq!(output.title, "created from lattice");
}
