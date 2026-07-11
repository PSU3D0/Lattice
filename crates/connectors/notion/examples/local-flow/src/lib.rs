use connector_notion::{NotionCreatePageInput, NotionCreatePageOutput};
use dag_core::NodeResult;
use dag_macros::{def_node, node};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExampleTriggerInput {
    pub database_id: String,
    pub title: String,
}

#[def_node(
    trigger,
    name = "ExampleTrigger",
    summary = "Seed the Notion create-page connector input",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn example_trigger(input: ExampleTriggerInput) -> NodeResult<NotionCreatePageInput> {
    Ok(NotionCreatePageInput {
        database_id: input.database_id,
        properties: json!({
            "Name": { "title": [{ "text": { "content": input.title } }] }
        }),
        icon: None,
    })
}

#[def_node(
    name = "ExampleCapture",
    summary = "Return connector output unchanged",
    effects = "Pure",
    determinism = "Strict"
)]
async fn example_capture(input: NotionCreatePageOutput) -> NodeResult<NotionCreatePageOutput> {
    Ok(input)
}

dag_macros::flow! {
    name: connector_notion_local_flow,
    version: "0.1.0",
    profile: Dev,
    summary: "Connector-owned local flow example for the Notion connector";
    let trigger = node!(example_trigger);
    let create = node!(connector_notion::notion_create_page);
    let capture = node!(example_capture);
    connect!(trigger -> create);
    connect!(create -> capture);
    entrypoint!({
        trigger: "trigger",
        capture: "capture",
        route_aliases: ["/notion/local"],
        method: "POST",
        deadline_ms: 5_000,
    });
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use cap_http_reqwest::ReqwestHttpClient;
    use capabilities::ResourceBag;
    use connector_notion::runtime::transport::EnvConnectorRuntime;
    use host_inproc::FlowBundle;
    use httpmock::Method::POST;
    use httpmock::MockServer;
    use kernel_exec::ExecutionResult;

    use super::*;

    const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_NOTION_DEFAULT_BASE_URL";
    const AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_NOTION_API_AUTH";

    static ENV_LOCK: Mutex<()> = Mutex::new(());

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

    fn http_resources() -> ResourceBag {
        let client = Arc::new(ReqwestHttpClient::default());
        ResourceBag::default()
            .with_http_read(Arc::clone(&client))
            .with_http_write(client)
            .with_connector_runtime(Arc::new(EnvConnectorRuntime))
    }

    async fn execute_flow(
        bundle: FlowBundle,
        input: ExampleTriggerInput,
    ) -> anyhow::Result<NotionCreatePageOutput> {
        let entrypoint = bundle.entrypoints.first().expect("entrypoint");
        let payload = serde_json::to_value(&input)?;

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

    #[test]
    fn example_flow_contains_connector_node() {
        let ir = flow();
        let identifiers = ir
            .nodes
            .iter()
            .map(|node| node.identifier.as_str())
            .collect::<Vec<_>>();
        assert!(identifiers.contains(&connector_notion::NOTION_CREATE_PAGE_IDENTIFIER));
    }

    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn example_flow_creates_page_against_mock_server() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let server = MockServer::start();
        let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
        let _auth = EnvGuard::set(AUTH_ENV, "local-flow-token");

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/pages")
                .header("authorization", "Bearer local-flow-token");
            then.status(200).json_body_obj(&serde_json::json!({
                "object": "page",
                "id": "page-local-1",
                "url": "https://www.notion.so/page-local-1"
            }));
        });

        let output = execute_flow(
            bundle(),
            ExampleTriggerInput {
                database_id: "db-1".to_string(),
                title: "local flow".to_string(),
            },
        )
        .await
        .expect("flow executes");

        mock.assert();
        assert_eq!(output.id, "page-local-1");
    }
}
