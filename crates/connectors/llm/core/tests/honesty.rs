//! Capability honesty + idempotency evidence for the LLM family.
//!
//! - `complete` must succeed under a scoped bag granting exactly its declared
//!   hints (`http_write` only) with zero CAP110 denials — the declaration is
//!   *sufficient*;
//! - under an empty grant set it must fail closed with a recorded CAP110
//!   `http_write` denial before any bytes leave the process — the declaration
//!   is *load-bearing* (the provider POST rides `llm-lattice`'s capability
//!   bridge, which refuses to dispatch without the granted write client);
//! - duplicate injection through a dedupe reservation proves the Effectful op
//!   composes with the `Delivery::ExactlyOnce` gate: three deliveries, one
//!   outbound POST (LLM replays are billed — never free).

// The env-var mutex must deliberately serialize whole async tests.
#![allow(clippy::await_holding_lock)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::dedupe::DedupeStore;
use capabilities::scoped::ScopedResources;
use capabilities::{ResourceAccess, ResourceBag, context};
use connector_llm::runtime::errors::LlmConnectorError;
use connector_llm::runtime::transport::EnvConnectorRuntime;
use connector_llm::{LlmCompleteInput, LlmProvider};
use connectors_std::dev::MemoryDedupeStore;
use dag_core::EffectHint;
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

fn full_bag() -> Arc<dyn ResourceAccess> {
    let client = Arc::new(ReqwestHttpClient::default());
    Arc::new(
        ResourceBag::default()
            .with_http_read(Arc::clone(&client))
            .with_http_write(client)
            .with_connector_runtime(Arc::new(EnvConnectorRuntime))
            .with_connector_scope(capabilities::connector::ConnectorBindingScope::new(
                "flow://tests",
                "honesty_test",
                "connector.llm.test",
                "connector.llm",
            )),
    )
}

fn scoped_to_declared(op_meta: &dag_core::ConnectorOpMetadata) -> Arc<ScopedResources> {
    let grants = op_meta
        .effect_hints
        .iter()
        .map(|hint| EffectHint::parse(hint).expect("declared hint parses"));
    Arc::new(ScopedResources::new(
        op_meta.operation_id,
        full_bag(),
        grants,
    ))
}

fn scoped_to_nothing(op_meta: &dag_core::ConnectorOpMetadata) -> Arc<ScopedResources> {
    Arc::new(ScopedResources::new(op_meta.operation_id, full_bag(), []))
}

fn sample_input() -> LlmCompleteInput {
    LlmCompleteInput {
        provider: LlmProvider::OpenaiCompat,
        model: "mock-model-1".to_string(),
        prompt: "Summarize the incident.".to_string(),
        system: None,
        temperature: None,
        max_tokens: None,
        output_schema: None,
    }
}

fn openai_completion_body(content: &str) -> serde_json::Value {
    serde_json::json!({
        "id": "chatcmpl-honesty",
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
            "prompt_tokens": 3,
            "completion_tokens": 2,
            "total_tokens": 5,
            "prompt_tokens_details": { "cached_tokens": 0 }
        }
    })
}

#[tokio::test]
async fn complete_succeeds_under_exactly_declared_hints() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "honesty-key");

    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/chat/completions")
            .header("authorization", "Bearer honesty-key");
        then.status(200)
            .json_body(openai_completion_body("scoped completion"));
    });

    let meta = &connector_llm::ops::LlmComplete::META;
    let scoped = scoped_to_declared(meta);
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let output = context::with_resources(view, async {
        connector_llm::ops::LlmComplete::invoke(&sample_input())
            .await
            .expect("complete succeeds with only declared hints granted")
    })
    .await;

    mock.assert();
    assert_eq!(output.text, "scoped completion");
    assert!(
        scoped.take_denials().is_empty(),
        "declared hints must be sufficient: no CAP110 denials"
    );
}

#[tokio::test]
async fn undeclared_write_access_is_denied_with_cap110() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "honesty-key");

    let mock = server.mock(|when, then| {
        when.method(POST).path("/chat/completions");
        then.status(200).json_body(openai_completion_body("never"));
    });

    let meta = &connector_llm::ops::LlmComplete::META;
    let scoped = scoped_to_nothing(meta);
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let err = context::with_resources(view, async {
        connector_llm::ops::LlmComplete::invoke(&sample_input())
            .await
            .expect_err("undeclared http_write must be denied")
    })
    .await;

    assert_eq!(mock.hits(), 0, "denial must happen before any request");
    assert!(matches!(err, LlmConnectorError::Completion(_)));
    let message = err.to_string();
    assert!(
        message.contains("write capability"),
        "denial must name the missing write capability, got: {message}"
    );
    let denials = scoped.take_denials();
    assert!(
        denials
            .iter()
            .any(|denial| denial.capability == "http_write"),
        "expected an http_write denial, got: {denials:?}"
    );
    assert!(denials[0].message().contains("CAP110"));
}

#[tokio::test]
async fn complete_under_duplicate_injection_posts_exactly_once() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "honesty-key");

    let mock = server.mock(|when, then| {
        when.method(POST).path("/chat/completions");
        then.status(200)
            .json_body(openai_completion_body("completed once"));
    });

    let store = MemoryDedupeStore::new();
    // Stable idempotency key: op + model + prompt (a scheduled flow would key
    // on scheduled_time_ms).
    let idempotency_key = b"connector.llm.complete:mock-model-1:Summarize the incident.";
    let ttl = Duration::from_secs(300);

    let (applied, blocked) = context::with_resources(full_bag(), async {
        let mut applied = 0usize;
        let mut blocked = 0usize;
        for _ in 0..3 {
            if store
                .put_if_absent(idempotency_key, ttl)
                .await
                .expect("dedupe reservation")
            {
                connector_llm::ops::LlmComplete::invoke(&sample_input())
                    .await
                    .expect("gated completion succeeds");
                applied += 1;
            } else {
                blocked += 1;
            }
        }
        (applied, blocked)
    })
    .await;

    assert_eq!(applied, 1);
    assert_eq!(blocked, 2);
    assert_eq!(mock.hits(), 1, "exactly one POST despite three deliveries");

    let report = testing_harness_idem::verify_dedupe_store(
        &store,
        b"harness-certification-key",
        Duration::from_millis(40),
        4,
    )
    .await;
    assert!(report.passed(), "dedupe store harness failed: {report:?}");
}
