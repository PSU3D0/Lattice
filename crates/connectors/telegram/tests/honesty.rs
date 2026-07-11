//! Capability honesty + idempotency evidence for the Telegram family.
//!
//! - `send_message` must succeed under a scoped bag granting exactly its
//!   declared hints (`http_write` only) with zero CAP110 denials — the
//!   declaration is *sufficient*;
//! - under an empty grant set it must fail closed with `MissingHttpWrite` and
//!   a recorded CAP110 denial — the declaration is *load-bearing*;
//! - duplicate injection through a dedupe reservation proves the Effectful op
//!   composes with the `Delivery::ExactlyOnce` gate: three deliveries, one
//!   outbound POST.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::dedupe::DedupeStore;
use capabilities::scoped::ScopedResources;
use capabilities::{ResourceAccess, ResourceBag, context};
use connector_telegram::TelegramSendMessageInput;
use connector_telegram::runtime::errors::ConnectorRuntimeError;
use connector_telegram::runtime::transport::EnvConnectorRuntime;
use connectors_std::dev::MemoryDedupeStore;
use dag_core::EffectHint;
use httpmock::Method::POST;
use httpmock::MockServer;

static ENV_LOCK: Mutex<()> = Mutex::new(());
const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_TELEGRAM_BOT_DEFAULT_BASE_URL";
const AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_TELEGRAM_BOT_AUTH";

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
                "connector.telegram.test",
                "connector.telegram",
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

fn sample_input() -> TelegramSendMessageInput {
    TelegramSendMessageInput {
        chat_id: "1001".to_string(),
        text: "scoped send".to_string(),
    }
}

#[tokio::test]
async fn send_message_succeeds_under_exactly_declared_hints() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "123456:HONESTY");

    let mock = server.mock(|when, then| {
        when.method(POST).path_contains("/sendMessage");
        then.status(200).json_body_obj(&serde_json::json!({
            "ok": true, "result": { "message_id": 3, "date": 1 }
        }));
    });

    let meta = &connector_telegram::ops::TelegramSendMessage::META;
    let scoped = scoped_to_declared(meta);
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let output = context::with_resources(view, async {
        connector_telegram::ops::TelegramSendMessage::invoke(&sample_input())
            .await
            .expect("send succeeds with only declared hints granted")
    })
    .await;

    mock.assert();
    assert_eq!(output.message_id, 3);
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
    let _auth = EnvGuard::set(AUTH_ENV, "123456:HONESTY");

    let mock = server.mock(|when, then| {
        when.method(POST).path_contains("/sendMessage");
        then.status(200).json_body_obj(
            &serde_json::json!({ "ok": true, "result": { "message_id": 1, "date": 1 } }),
        );
    });

    let meta = &connector_telegram::ops::TelegramSendMessage::META;
    let scoped = scoped_to_nothing(meta);
    let view: Arc<dyn ResourceAccess> = scoped.clone();

    let err = context::with_resources(view, async {
        connector_telegram::ops::TelegramSendMessage::invoke(&sample_input())
            .await
            .expect_err("undeclared http_write must be denied")
    })
    .await;

    assert_eq!(mock.hits(), 0, "denial must happen before any request");
    assert!(matches!(
        err,
        ConnectorRuntimeError::MissingHttpWrite { action } if action == meta.operation_id
    ));
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
async fn send_under_duplicate_injection_posts_exactly_once() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "123456:HONESTY");

    let mock = server.mock(|when, then| {
        when.method(POST).path_contains("/sendMessage");
        then.status(200).json_body_obj(
            &serde_json::json!({ "ok": true, "result": { "message_id": 1, "date": 1 } }),
        );
    });

    let store = MemoryDedupeStore::new();
    // Stable idempotency key: op + chat + text (a keyed flow would key on the
    // broadcast_id, like the s17 example).
    let idempotency_key = b"connector.telegram.send_message:1001:scoped send";
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
                connector_telegram::ops::TelegramSendMessage::invoke(&sample_input())
                    .await
                    .expect("gated send succeeds");
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
