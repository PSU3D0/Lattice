//! Canned-transport runtime contract for `connector.google.drive.search_files`:
//! success path with full request assertions (path, query, fields projection,
//! bearer auth), API error mapping, auth misconfiguration, and connector-op
//! reuse from a custom `def_node`.

use std::sync::{Arc, Mutex};

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::{ResourceBag, context};
use connector_google_drive::runtime::errors::ConnectorRuntimeError;
use connector_google_drive::runtime::transport::EnvConnectorRuntime;
use connector_google_drive::{GoogleDriveSearchFilesInput, google_drive_search_files};
use dag_core::{Effects, NodeError, NodeResult};
use dag_macros::def_node;
use httpmock::Method::GET;
use httpmock::MockServer;

static ENV_LOCK: Mutex<()> = Mutex::new(());
const ENDPOINT_ENV: &str = "LATTICE_CONNECTOR_ENDPOINT_GOOGLE_DRIVE_DEFAULT_BASE_URL";
const AUTH_ENV: &str = "LATTICE_CONNECTOR_AUTH_GOOGLE_WORKSPACE_AUTH";

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
                "connector.google.drive.test",
                "connector.google.drive",
            )),
    )
}

fn sample_input() -> GoogleDriveSearchFilesInput {
    GoogleDriveSearchFilesInput {
        query: "modifiedTime > '2026-07-01T06:00:00Z' and trashed = false".to_string(),
        page_size: Some(50),
    }
}

const EXPECTED_FIELDS: &str = "nextPageToken,files(id,name,mimeType,shared,webViewLink,permissions(id,type,role,emailAddress))";

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct CountRiskyInput {
    query: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct CountRiskyOutput {
    total: usize,
    public: usize,
}

#[def_node(
    name = "CountRisky",
    summary = "Custom node that reuses the Drive search-files connector operation",
    connector_ops(connector_google_drive::ops::GoogleDriveSearchFiles)
)]
async fn count_risky(input: CountRiskyInput) -> NodeResult<CountRiskyOutput> {
    let found =
        connector_google_drive::ops::GoogleDriveSearchFiles::invoke(&GoogleDriveSearchFilesInput {
            query: input.query,
            page_size: None,
        })
        .await
        .map_err(|err| NodeError::new(err.to_string()))?;

    let public = found
        .items
        .iter()
        .filter(|file| {
            file.permissions
                .iter()
                .any(|p| p.grantee_type.as_deref() == Some("anyone"))
        })
        .count();
    Ok(CountRiskyOutput {
        total: found.items.len(),
        public,
    })
}

#[test]
fn custom_node_spec_auto_hoists_connector_op_requirements() {
    let spec = count_risky_node_spec();
    assert_eq!(spec.effects, Effects::ReadOnly);
    assert_eq!(spec.determinism, dag_core::Determinism::BestEffort);
    assert!(
        spec.effect_hints
            .contains(&capabilities::http::HINT_HTTP_READ)
    );
    assert!(
        !spec
            .effect_hints
            .contains(&capabilities::http::HINT_HTTP_WRITE)
    );
    assert!(
        spec.connector_ops
            .iter()
            .any(|op| op.operation_id == "connector.google.drive.search_files")
    );
}

#[tokio::test]
async fn search_files_round_trips_with_query_fields_and_bearer_auth() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "drive-secret-token");

    let mock = server.mock(|when, then| {
        when.method(GET)
            .path("/drive/v3/files")
            .header("accept", "application/json")
            .header("authorization", "Bearer drive-secret-token")
            .query_param(
                "q",
                "modifiedTime > '2026-07-01T06:00:00Z' and trashed = false",
            )
            .query_param("fields", EXPECTED_FIELDS)
            .query_param("pageSize", "50");
        then.status(200).json_body_obj(&serde_json::json!({
            "files": [
                {
                    "id": "doc-1",
                    "name": "quarterly plan",
                    "mimeType": "application/vnd.google-apps.document",
                    "shared": true,
                    "webViewLink": "https://docs.example.test/doc-1",
                    "permissions": [
                        { "id": "p0", "type": "user", "role": "owner",
                          "emailAddress": "me@example.test" },
                        { "id": "p1", "type": "anyone", "role": "reader" }
                    ]
                },
                { "id": "doc-2" }
            ],
            "nextPageToken": "page-2"
        }));
    });

    let output = context::with_resources(http_resources(), async {
        connector_google_drive::ops::GoogleDriveSearchFiles::invoke(&sample_input())
            .await
            .expect("search succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(output.items.len(), 2);
    assert_eq!(output.next_page_token.as_deref(), Some("page-2"));

    let doc = &output.items[0];
    assert_eq!(doc.id, "doc-1");
    assert_eq!(doc.name.as_deref(), Some("quarterly plan"));
    assert_eq!(doc.shared, Some(true));
    assert_eq!(doc.permissions.len(), 2);
    assert_eq!(doc.permissions[1].grantee_type.as_deref(), Some("anyone"));
    assert_eq!(doc.permissions[1].role.as_deref(), Some("reader"));
    assert_eq!(
        doc.permissions[0].email_address.as_deref(),
        Some("me@example.test")
    );

    // Sparse files decode with defaults (Drive omits unrequested/absent keys).
    let sparse = &output.items[1];
    assert_eq!(sparse.id, "doc-2");
    assert!(sparse.permissions.is_empty());
    assert_eq!(sparse.shared, None);
}

#[tokio::test]
async fn custom_node_reuses_connector_operation() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "drive-secret-token");

    let mock = server.mock(|when, then| {
        when.method(GET).path("/drive/v3/files");
        then.status(200).json_body_obj(&serde_json::json!({
            "files": [
                { "id": "a", "permissions": [{ "type": "anyone", "role": "reader" }] },
                { "id": "b", "permissions": [{ "type": "user", "role": "writer" }] }
            ]
        }));
    });

    let output = context::with_resources(http_resources(), async {
        count_risky(CountRiskyInput {
            query: "trashed = false".to_string(),
        })
        .await
        .expect("custom node succeeds")
    })
    .await;

    mock.assert();
    assert_eq!(
        output,
        CountRiskyOutput {
            total: 2,
            public: 1
        }
    );
}

#[tokio::test]
async fn search_files_maps_api_error_status_and_body() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::set(AUTH_ENV, "drive-secret-token");

    let mock = server.mock(|when, then| {
        when.method(GET).path("/drive/v3/files");
        then.status(403).json_body_obj(&serde_json::json!({
            "error": { "code": 403, "message": "Insufficient drive scope", "status": "PERMISSION_DENIED" }
        }));
    });

    let err = context::with_resources(http_resources(), async {
        connector_google_drive::ops::GoogleDriveSearchFiles::invoke(&sample_input())
            .await
            .expect_err("forbidden search must fail")
    })
    .await;

    mock.assert();
    match err {
        ConnectorRuntimeError::HttpStatus { status, body } => {
            assert_eq!(status, 403);
            assert!(body.contains("Insufficient drive scope"), "got: {body}");
        }
        other => panic!("expected HttpStatus error, got: {other}"),
    }

    // The def_node action wrapper keeps status + provider message visible.
    let node_err = context::with_resources(http_resources(), async {
        google_drive_search_files(sample_input())
            .await
            .expect_err("forbidden search must fail through the action")
    })
    .await;
    let message = node_err.to_string();
    assert!(message.contains("403"), "got: {message}");
    assert!(
        message.contains("Insufficient drive scope"),
        "got: {message}"
    );
}

#[tokio::test]
async fn search_files_with_missing_auth_fails_actionably_without_calling_api() {
    let _env_lock = ENV_LOCK.lock().expect("env lock");
    let server = MockServer::start();
    let _endpoint = EnvGuard::set(ENDPOINT_ENV, &server.base_url());
    let _auth = EnvGuard::remove(AUTH_ENV);

    let mock = server.mock(|when, then| {
        when.method(GET).path("/drive/v3/files");
        then.status(200)
            .json_body_obj(&serde_json::json!({ "files": [] }));
    });

    let err = context::with_resources(http_resources(), async {
        connector_google_drive::ops::GoogleDriveSearchFiles::invoke(&sample_input())
            .await
            .expect_err("missing auth must fail")
    })
    .await;

    assert_eq!(mock.hits(), 0);
    let message = err.to_string();
    assert!(message.contains("google_workspace_auth"), "got: {message}");
    assert!(message.contains(AUTH_ENV), "got: {message}");
}
