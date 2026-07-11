use connector_llm::{LlmCompleteInput, LlmCompleteOutput, LlmProvider};
use dag_core::NodeResult;
use dag_macros::{def_node, node};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExampleTriggerInput {
    pub model: String,
    pub prompt: String,
    #[serde(default)]
    pub system: Option<String>,
}

#[def_node(
    trigger,
    name = "ExampleTrigger",
    summary = "Seed the LLM completion connector input",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn example_trigger(input: ExampleTriggerInput) -> NodeResult<LlmCompleteInput> {
    Ok(LlmCompleteInput {
        provider: LlmProvider::OpenaiCompat,
        model: input.model,
        prompt: input.prompt,
        system: input.system,
        temperature: Some(0.2),
        max_tokens: Some(256),
        output_schema: None,
    })
}

#[def_node(
    name = "ExampleCapture",
    summary = "Return connector output unchanged",
    effects = "Pure",
    determinism = "Strict"
)]
async fn example_capture(input: LlmCompleteOutput) -> NodeResult<LlmCompleteOutput> {
    Ok(input)
}

dag_macros::flow! {
    name: connector_llm_local_flow,
    version: "0.1.0",
    profile: Dev,
    summary: "Connector-owned local flow example for the LLM completion connector";
    let trigger = node!(example_trigger);
    let complete = node!(connector_llm::llm_complete);
    let capture = node!(example_capture);
    connect!(trigger -> complete);
    connect!(complete -> capture);
    entrypoint!({
        trigger: "trigger",
        capture: "capture",
        route_aliases: ["/llm/local"],
        method: "POST",
        deadline_ms: 30_000,
    });
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use cap_http_reqwest::ReqwestHttpClient;
    use capabilities::ResourceBag;
    use connector_llm::runtime::transport::EnvConnectorRuntime;
    use host_inproc::FlowBundle;
    use httpmock::Method::POST;
    use httpmock::MockServer;
    use kernel_exec::ExecutionResult;

    use super::*;

    const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_LLM_DEFAULT_BASE_URL";
    const AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_LLM_API_KEY";

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
    ) -> anyhow::Result<LlmCompleteOutput> {
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

    fn mock_completion_body(text: &str) -> serde_json::Value {
        serde_json::json!({
            "id": "chatcmpl-local",
            "object": "chat.completion",
            "created": 1,
            "model": "mock-model-1",
            "system_fingerprint": null,
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": text,
                    "tool_calls": []
                },
                "logprobs": null,
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 9,
                "completion_tokens": 5,
                "total_tokens": 14,
                "prompt_tokens_details": { "cached_tokens": 0 }
            }
        })
    }

    #[test]
    fn example_flow_contains_connector_node() {
        let ir = flow();
        let identifiers = ir
            .nodes
            .iter()
            .map(|node| node.identifier.as_str())
            .collect::<Vec<_>>();
        assert!(identifiers.contains(&connector_llm::LLM_COMPLETE_IDENTIFIER));
    }

    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn example_flow_completes_against_mock_provider() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let server = MockServer::start();
        let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
        let _auth = EnvGuard::set(AUTH_ENV, "local-flow-key");

        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/chat/completions")
                .header("authorization", "Bearer local-flow-key");
            then.status(200)
                .json_body(mock_completion_body("mock completion text"));
        });

        let output = execute_flow(
            bundle(),
            ExampleTriggerInput {
                model: "mock-model-1".to_string(),
                prompt: "Summarize the audit findings.".to_string(),
                system: Some("You are terse.".to_string()),
            },
        )
        .await
        .expect("flow executes");

        mock.assert();
        assert_eq!(output.text, "mock completion text");
        assert_eq!(output.model, "mock-model-1");
        assert_eq!(output.usage.input_tokens, 9);
        assert_eq!(output.usage.output_tokens, 5);
        assert_eq!(output.usage.total_tokens, 14);
    }
}
