use connector_http::{HttpJsonOutput, HttpReadInput, HttpWriteInput};
use dag_core::NodeResult;
use dag_macros::{def_node, node};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExampleTriggerInput {
    pub read_path: String,
    pub write_path: String,
}

#[def_node(
    trigger,
    name = "ExampleTrigger",
    summary = "Seed the connector.http GET input",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn example_trigger(input: ExampleTriggerInput) -> NodeResult<HttpReadInput> {
    Ok(HttpReadInput {
        path: input.read_path,
        ..Default::default()
    })
}

#[def_node(
    name = "ExampleShape",
    summary = "Shape the GET response into a POST body",
    effects = "Pure",
    determinism = "Strict"
)]
async fn example_shape(input: HttpJsonOutput) -> NodeResult<HttpWriteInput> {
    Ok(HttpWriteInput {
        path: "/collect".to_string(),
        body: Some(input.body),
        ..Default::default()
    })
}

#[def_node(
    name = "ExampleCapture",
    summary = "Return connector output unchanged",
    effects = "Pure",
    determinism = "Strict"
)]
async fn example_capture(input: HttpJsonOutput) -> NodeResult<HttpJsonOutput> {
    Ok(input)
}

dag_macros::flow! {
    name: connector_http_local_flow,
    version: "0.1.0",
    profile: Dev,
    summary: "Connector-owned local flow example for the generic HTTP connector";
    let trigger = node!(example_trigger);
    let fetch = node!(connector_http::http_get);
    let shape = node!(example_shape);
    let push = node!(connector_http::http_post);
    let capture = node!(example_capture);
    connect!(trigger -> fetch);
    connect!(fetch -> shape);
    connect!(shape -> push);
    connect!(push -> capture);
    entrypoint!({
        trigger: "trigger",
        capture: "capture",
        route_aliases: ["/http/local"],
        method: "POST",
        deadline_ms: 5_000,
    });
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use cap_http_reqwest::ReqwestHttpClient;
    use capabilities::ResourceBag;
    use connector_http::runtime::transport::EnvConnectorRuntime;
    use host_inproc::FlowBundle;
    use httpmock::Method::{GET, POST};
    use httpmock::MockServer;
    use kernel_exec::ExecutionResult;

    use super::*;

    const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_HTTP_TARGET_BASE_URL";

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
    ) -> anyhow::Result<HttpJsonOutput> {
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
    fn example_flow_contains_connector_nodes() {
        let ir = flow();
        let identifiers = ir
            .nodes
            .iter()
            .map(|node| node.identifier.as_str())
            .collect::<Vec<_>>();
        assert!(identifiers.contains(&"connector.http.get"));
        assert!(identifiers.contains(&"connector.http.post"));
    }

    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn example_flow_gets_then_posts_against_mock_server() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let server = MockServer::start();
        let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());

        let get_mock = server.mock(|when, then| {
            when.method(GET).path("/source");
            then.status(200)
                .json_body_obj(&serde_json::json!({ "value": 7 }));
        });
        let post_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/collect")
                .json_body_obj(&serde_json::json!({ "value": 7 }));
            then.status(200)
                .json_body_obj(&serde_json::json!({ "ok": true }));
        });

        let output = execute_flow(
            bundle(),
            ExampleTriggerInput {
                read_path: "/source".to_string(),
                write_path: "/collect".to_string(),
            },
        )
        .await
        .expect("flow executes");

        get_mock.assert();
        post_mock.assert();
        assert_eq!(output.body, serde_json::json!({ "ok": true }));
    }
}
