//! Mock-provider runtime contract for `connector.llm.complete`: dialect
//! routing + request assertions per provider (path, auth header, mapped
//! params), API error mapping, auth misconfiguration, structured output, and
//! connector-op reuse from a custom `def_node`. No real LLM API is called.

// The env-var mutex must deliberately serialize whole async tests.
#![allow(clippy::await_holding_lock)]

use std::sync::{Arc, Mutex};

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::{ResourceBag, context};
use connector_llm::runtime::errors::LlmConnectorError;
use connector_llm::runtime::transport::EnvConnectorRuntime;
use connector_llm::{LlmCompleteInput, LlmProvider, llm_complete};
use dag_core::{Effects, NodeError, NodeResult};
use dag_macros::def_node;
use httpmock::Method::POST;
use httpmock::MockServer;

static ENV_LOCK: Mutex<()> = Mutex::new(());
const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_LLM_DEFAULT_BASE_URL";
const AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_LLM_API_KEY";

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
            .with_http_write(client)
            .with_connector_runtime(Arc::new(EnvConnectorRuntime))
            .with_connector_scope(capabilities::connector::ConnectorBindingScope::new(
                "flow://tests",
                "runtime_test",
                "connector.llm.test",
                "connector.llm",
            )),
    )
}

fn sample_input() -> LlmCompleteInput {
    LlmCompleteInput {
        provider: LlmProvider::OpenaiCompat,
        model: "mock-model-1".to_string(),
        prompt: "Summarize the incident.".to_string(),
        system: Some("You are terse.".to_string()),
        temperature: Some(0.1),
        max_tokens: Some(128),
        output_schema: None,
    }
}

fn openai_completion_body(content: &str) -> serde_json::Value {
    serde_json::json!({
        "id": "chatcmpl-runtime",
        "object": "chat.completion",
        "created": 1,
        "model": "mock-model-1",
        "system_fingerprint": null,
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": content,
                "tool_calls": []
            },
            "logprobs": null,
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 11,
            "completion_tokens": 6,
            "total_tokens": 17,
            "prompt_tokens_details": { "cached_tokens": 0 }
        }
    })
}

fn anthropic_completion_body(text: &str) -> serde_json::Value {
    serde_json::json!({
        "id": "msg-runtime",
        "type": "message",
        "role": "assistant",
        "model": "mock-claude-1",
        "content": [{ "type": "text", "text": text }],
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": { "input_tokens": 21, "output_tokens": 8 }
    })
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct SummarizeInput {
    text: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct SummarizeOutput {
    summary: String,
}

#[def_node(
    name = "SummarizeIncident",
    summary = "Custom node that reuses the LLM completion connector operation",
    connector_ops(connector_llm::ops::LlmComplete)
)]
async fn summarize_incident(input: SummarizeInput) -> NodeResult<SummarizeOutput> {
    let output = connector_llm::ops::LlmComplete::invoke(&LlmCompleteInput {
        provider: LlmProvider::OpenaiCompat,
        model: "mock-model-1".to_string(),
        prompt: input.text,
        system: None,
        temperature: None,
        max_tokens: None,
        output_schema: None,
    })
    .await
    .map_err(|err| NodeError::new(err.to_string()))?;

    Ok(SummarizeOutput {
        summary: output.text,
    })
}

#[test]
fn custom_node_spec_auto_hoists_connector_op_requirements() {
    let spec = summarize_incident_node_spec();
    assert_eq!(spec.effects, Effects::Effectful);
    assert_eq!(spec.determinism, dag_core::Determinism::Nondeterministic);
    assert!(
        spec.effect_hints
            .contains(&capabilities::http::HINT_HTTP_WRITE)
    );
    assert!(
        spec.connector_ops
            .iter()
            .any(|op| op.operation_id == "connector.llm.complete")
    );

    let generated = connector_llm::llm_complete_node_spec();
    assert_eq!(generated.effects, Effects::Effectful);
    assert_eq!(
        generated.determinism,
        dag_core::Determinism::Nondeterministic
    );
    assert!(
        generated
            .effect_hints
            .contains(&capabilities::http::HINT_HTTP_WRITE)
    );
}

#[tokio::test]
async fn openai_compat_dialect_posts_mapped_completion_with_bearer_auth() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "llm-secret-key");

    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/chat/completions")
            .header("authorization", "Bearer llm-secret-key")
            .header("content-type", "application/json")
            .json_body_partial(r#"{"model":"mock-model-1","temperature":0.1,"max_tokens":128}"#)
            .body_contains("You are terse.")
            .body_contains("Summarize the incident.");
        then.status(200)
            .json_body(openai_completion_body("Two files were exposed."));
    });

    let output = context::with_resources(http_resources(), async {
        connector_llm::ops::LlmComplete::invoke(&sample_input())
            .await
            .expect("completion succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(output.text, "Two files were exposed.");
    assert_eq!(output.model, "mock-model-1");
    assert_eq!(output.structured, None);
    assert_eq!(output.usage.input_tokens, 11);
    assert_eq!(output.usage.output_tokens, 6);
    assert_eq!(output.usage.total_tokens, 17);
}

#[tokio::test]
async fn anthropic_dialect_posts_messages_with_x_api_key() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "llm-secret-key");

    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/v1/messages")
            .header("x-api-key", "llm-secret-key")
            .header_exists("anthropic-version")
            .json_body_partial(r#"{"model":"mock-claude-1","max_tokens":128}"#)
            .body_contains("Summarize the incident.");
        then.status(200)
            .json_body(anthropic_completion_body("Two files were exposed."));
    });

    let mut input = sample_input();
    input.provider = LlmProvider::Anthropic;
    input.model = "mock-claude-1".to_string();

    let output = context::with_resources(http_resources(), async {
        connector_llm::ops::LlmComplete::invoke(&input)
            .await
            .expect("completion succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(output.text, "Two files were exposed.");
    assert_eq!(output.model, "mock-claude-1");
    assert_eq!(output.usage.input_tokens, 21);
    assert_eq!(output.usage.output_tokens, 8);
    assert_eq!(output.usage.total_tokens, 29);
}

#[tokio::test]
async fn structured_output_parses_completion_text_as_json() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "llm-secret-key");

    let structured = serde_json::json!({ "severity": "high", "count": 2 });
    let mock = server.mock(|when, then| {
        when.method(POST).path("/chat/completions");
        then.status(200)
            .json_body(openai_completion_body(&structured.to_string()));
    });

    let mut input = sample_input();
    input.output_schema = Some(serde_json::json!({
        "type": "object",
        "title": "incident_summary",
        "properties": {
            "severity": { "type": "string" },
            "count": { "type": "integer" }
        },
        "required": ["severity", "count"],
        "additionalProperties": false
    }));

    let output = context::with_resources(http_resources(), async {
        connector_llm::ops::LlmComplete::invoke(&input)
            .await
            .expect("structured completion succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(output.structured, Some(structured));
}

#[tokio::test]
async fn custom_node_reuses_connector_operation() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "llm-secret-key");

    let mock = server.mock(|when, then| {
        when.method(POST).path("/chat/completions");
        then.status(200)
            .json_body(openai_completion_body("condensed"));
    });

    let output = context::with_resources(http_resources(), async {
        summarize_incident(SummarizeInput {
            text: "long incident report".to_string(),
        })
        .await
        .expect("custom node succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(
        output,
        SummarizeOutput {
            summary: "condensed".to_string(),
        }
    );
}

#[tokio::test]
async fn complete_maps_api_error_status_and_body() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "llm-secret-key");

    let mock = server.mock(|when, then| {
        when.method(POST).path("/chat/completions");
        then.status(400).json_body(serde_json::json!({
            "error": {
                "message": "temperature out of range",
                "type": "invalid_request_error",
                "code": null
            }
        }));
    });

    let err = context::with_resources(http_resources(), async {
        connector_llm::ops::LlmComplete::invoke(&sample_input())
            .await
            .expect_err("bad request must fail")
    })
    .await;

    mock.assert();
    assert!(matches!(err, LlmConnectorError::Completion(_)));
    let message = err.to_string();
    assert!(
        message.contains("temperature out of range"),
        "got: {message}"
    );

    // The def_node action wrapper must keep the provider failure visible.
    let node_err = context::with_resources(http_resources(), async {
        llm_complete(sample_input())
            .await
            .expect_err("bad request must fail through the action")
    })
    .await;
    let message = node_err.to_string();
    assert!(
        message.contains("temperature out of range"),
        "got: {message}"
    );
}

#[tokio::test]
async fn complete_with_missing_auth_fails_actionably_without_calling_api() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let mock = server.mock(|when, then| {
        when.method(POST).path("/chat/completions");
        then.status(200).json_body(openai_completion_body("never"));
    });

    let err = context::with_resources(http_resources(), async {
        connector_llm::ops::LlmComplete::invoke(&sample_input())
            .await
            .expect_err("missing auth must fail")
    })
    .await;

    // Nothing left the process, and the failure names role + env var.
    assert_eq!(mock.hits(), 0);
    let message = err.to_string();
    assert!(message.contains("llm_api_key"), "got: {message}");
    assert!(message.contains(AUTH_ENV), "got: {message}");
}
