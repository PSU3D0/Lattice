use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use cap_http_reqwest::ReqwestHttpClient;
use capabilities::ResourceBag;
use connector_github_issues::{GithubIssueState, GithubIssuesListInput, GithubIssuesListOutput};
use dag_core::NodeResult;
use dag_macros::{def_node, node};
use host_inproc::{FlowBundle, FlowEntrypoint, NodeContract, NodeSource};
use httpmock::MockServer;
use kernel_exec::{ExecutionResult, NodeRegistry, NodeResolver, RegistryResolver};
use serde::{Deserialize, Serialize};

pub const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_GITHUB_DEFAULT_BASE_URL";
pub const AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_GITHUB_PAT";
pub const OWNER_ENV: &str = "LATTICE_EXAMPLE_GITHUB_OWNER";
pub const REPO_ENV: &str = "LATTICE_EXAMPLE_GITHUB_REPO";

static ENV_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExampleTriggerInput {
    pub owner: String,
    pub repo: String,
}

#[def_node(
    trigger,
    name = "ExampleTrigger",
    summary = "Seed the GitHub issues list connector input",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn example_trigger(input: ExampleTriggerInput) -> NodeResult<GithubIssuesListInput> {
    Ok(GithubIssuesListInput {
        owner: input.owner,
        repo: input.repo,
        state: Some(GithubIssueState::Open),
        return_all: false,
        limit: Some(5),
    })
}

#[def_node(
    name = "ExampleCapture",
    summary = "Return connector output unchanged",
    effects = "Pure",
    determinism = "Strict"
)]
async fn example_capture(input: GithubIssuesListOutput) -> NodeResult<GithubIssuesListOutput> {
    Ok(input)
}

dag_macros::flow! {
    name: connector_github_issues_local_flow,
    version: "0.1.0",
    profile: Dev,
    summary: "Connector-owned local flow example for the GitHub issues connector";
    let trigger = node!(example_trigger);
    let list = node!(connector_github_issues::github_issues_list);
    let capture = node!(example_capture);
    connect!(trigger -> list);
    connect!(list -> capture);
    entrypoint!({
        trigger: "trigger",
        capture: "capture",
    });
}

pub struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    pub fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, previous }
    }

    pub fn remove(key: &'static str) -> Self {
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

pub fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    ENV_LOCK.lock().expect("env lock")
}

pub fn example_bundle() -> FlowBundle {
    let validated_ir = validated_ir();
    let mut registry = NodeRegistry::new();
    example_trigger_register(&mut registry).expect("register example trigger");
    example_capture_register(&mut registry).expect("register example capture");
    connector_github_issues::register_all(&mut registry).expect("register connector nodes");
    let registry = Arc::new(registry);
    let resolver: Arc<dyn NodeResolver> = Arc::new(RegistryResolver::new(Arc::clone(&registry)));
    let entrypoints = vec![FlowEntrypoint {
        trigger_alias: "trigger".to_string(),
        capture_alias: "capture".to_string(),
        route_path: Some("/github/issues/local".to_string()),
        method: Some("POST".to_string()),
        deadline: Some(Duration::from_millis(5_000)),
        route_aliases: vec!["/github/issues/local".to_string()],
    }];
    let node_contracts = validated_ir
        .flow()
        .nodes
        .iter()
        .map(|node| NodeContract {
            identifier: node.identifier.clone(),
            contract_hash: None,
            source: NodeSource::Local,
        })
        .collect();

    FlowBundle {
        validated_ir,
        entrypoints,
        resolver,
        node_contracts,
        environment_plugins: Vec::new(),
    }
}

pub fn http_resources() -> ResourceBag {
    let client = Arc::new(ReqwestHttpClient::default());
    ResourceBag::default()
        .with_http_read(Arc::clone(&client))
        .with_http_write(client)
}

pub fn example_input_from_env() -> ExampleTriggerInput {
    ExampleTriggerInput {
        owner: std::env::var(OWNER_ENV).unwrap_or_else(|_| "rust-lang".to_string()),
        repo: std::env::var(REPO_ENV).unwrap_or_else(|_| "cargo".to_string()),
    }
}

pub struct LocalMockHandle {
    _server: MockServer,
    _endpoint: EnvGuard,
}

pub fn maybe_start_mock_server() -> Option<LocalMockHandle> {
    if std::env::var(ENDPOINT_ENV).is_ok() {
        println!(
            "Using configured upstream from {ENDPOINT_ENV}; no local mock server will be started."
        );
        return None;
    }

    let server = MockServer::start();
    server.mock(|_when, then| {
        then.status(200).json_body_obj(&serde_json::json!([
            {
                "number": 101,
                "title": "connector local mock issue",
                "state": "open",
                "html_url": "https://example.test/issues/101"
            },
            {
                "number": 102,
                "title": "flow-level connector smoke test",
                "state": "open",
                "html_url": "https://example.test/issues/102"
            }
        ]));
    });

    let endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    println!(
        "No {ENDPOINT_ENV} override detected; started local mock GitHub at {}",
        server.base_url()
    );
    Some(LocalMockHandle {
        _server: server,
        _endpoint: endpoint,
    })
}

pub async fn run_flow(input: ExampleTriggerInput) -> Result<GithubIssuesListOutput> {
    let bundle = example_bundle();
    let entrypoint = bundle.entrypoints.first().expect("entrypoint");
    let payload = serde_json::to_value(&input).expect("serialize input");

    let result = bundle
        .executor()
        .with_resource_bag(http_resources())
        .run_once(
            &bundle.validated_ir,
            entrypoint.trigger_alias.as_str(),
            payload,
            entrypoint.capture_alias.as_str(),
            entrypoint.deadline,
        )
        .await?;

    let value = match result {
        ExecutionResult::Value(value) => value,
        ExecutionResult::Stream(_) => anyhow::bail!("expected a value response"),
        ExecutionResult::Halt { alias, .. } => {
            anyhow::bail!("expected a completed value response, flow halted at {alias}")
        }
    };

    Ok(serde_json::from_value(value)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example_flow_contains_connector_node() {
        let ir = flow();
        let identifiers = ir
            .nodes
            .iter()
            .map(|node| node.identifier.as_str())
            .collect::<Vec<_>>();
        assert!(identifiers.contains(&connector_github_issues::GITHUB_ISSUES_LIST_IDENTIFIER));
    }

    #[tokio::test]
    async fn local_flow_runs_against_mock_server() {
        let _env_lock = env_lock();
        let server = MockServer::start();
        let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
        let _auth = EnvGuard::remove(AUTH_ENV);

        let issues = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/repos/octo/demo/issues")
                .header("accept", "application/json")
                .header("x-github-api-version", "2022-11-28")
                .query_param("per_page", "100")
                .query_param("state", "open");
            then.status(200).json_body_obj(&serde_json::json!([
                {
                    "number": 7,
                    "title": "from example crate",
                    "state": "open",
                    "html_url": "https://example.test/issues/7"
                }
            ]));
        });

        let output = run_flow(ExampleTriggerInput {
            owner: "octo".to_string(),
            repo: "demo".to_string(),
        })
        .await
        .expect("example flow runs");

        issues.assert();
        assert_eq!(output.items.len(), 1);
        assert_eq!(output.items[0].number, 7);
        assert_eq!(output.items[0].title, "from example crate");
    }
}
