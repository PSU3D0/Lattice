//! Dispatch backend capability for Lattice multi-flow bundles.
//!
//! KEEP as its own crate (decision recorded packet E2,
//! verifiability-substrate-hardening plan). Rationale: it has a real external
//! consumer (`examples/s13_github_issue_investigator`), so it is not an
//! unconsumed stub eligible for folding into the CLI.

use async_trait::async_trait;
use capabilities::{
    ResourceAccess,
    http::{HttpMethod, HttpRequest, HttpWrite},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DispatchRequest {
    pub job_kind: String,
    pub job_id: String,
    pub payload: JsonValue,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TrackingMode {
    CallbackOnly,
    PollOnly,
    CallbackPreferredPollFallback,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DispatchReceipt {
    pub backend_kind: String,
    pub job_id: String,
    pub tracking_mode: TrackingMode,
    pub metadata: JsonValue,
}

#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    #[error("dispatch backend requires HTTP write capability")]
    MissingHttpWrite,
    #[error("dispatch request serialization failed: {0}")]
    Serialize(String),
    #[error("dispatch backend failed: {0}")]
    Backend(String),
}

pub trait DispatchBackendHost: Send + Sync {
    fn http_write(&self) -> Option<&dyn HttpWrite>;
}

pub struct ResourceAccessDispatchHost<'a> {
    resources: &'a dyn ResourceAccess,
}

impl<'a> ResourceAccessDispatchHost<'a> {
    pub fn new(resources: &'a dyn ResourceAccess) -> Self {
        Self { resources }
    }
}

impl DispatchBackendHost for ResourceAccessDispatchHost<'_> {
    fn http_write(&self) -> Option<&dyn HttpWrite> {
        self.resources.http_write()
    }
}

#[async_trait]
pub trait DispatchBackend: Send + Sync {
    async fn dispatch(
        &self,
        host: &dyn DispatchBackendHost,
        request: DispatchRequest,
    ) -> Result<DispatchReceipt, DispatchError>;
}

#[derive(Clone, Debug)]
pub struct HttpDispatchBackend {
    backend_kind: String,
    url: String,
    tracking_mode: TrackingMode,
}

impl HttpDispatchBackend {
    pub fn new(
        backend_kind: impl Into<String>,
        url: impl Into<String>,
        tracking_mode: TrackingMode,
    ) -> Self {
        Self {
            backend_kind: backend_kind.into(),
            url: url.into(),
            tracking_mode,
        }
    }
}

#[async_trait]
impl DispatchBackend for HttpDispatchBackend {
    async fn dispatch(
        &self,
        host: &dyn DispatchBackendHost,
        request: DispatchRequest,
    ) -> Result<DispatchReceipt, DispatchError> {
        let client = host.http_write().ok_or(DispatchError::MissingHttpWrite)?;
        let mut http_request = HttpRequest::new(HttpMethod::Post, self.url.clone());
        http_request
            .headers
            .insert("content-type".to_string(), "application/json".to_string());
        http_request.body = Some(
            serde_json::to_vec(&request.payload)
                .map_err(|err| DispatchError::Serialize(err.to_string()))?,
        );

        let response = client
            .send(http_request)
            .await
            .map_err(|err| DispatchError::Backend(err.to_string()))?;
        if !(200..300).contains(&response.status) {
            return Err(DispatchError::Backend(format!(
                "HTTP dispatch failed with status {}",
                response.status
            )));
        }

        Ok(DispatchReceipt {
            backend_kind: self.backend_kind.clone(),
            job_id: request.job_id,
            tracking_mode: self.tracking_mode.clone(),
            metadata: serde_json::json!({
                "dispatch_url": self.url,
                "http_status": response.status,
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use capabilities::http::{HttpError, HttpResponse, HttpResult};
    use futures::executor::block_on;

    #[derive(Clone)]
    enum PlannedResponse {
        Success(u16),
        InvalidResponse(&'static str),
    }

    #[derive(Clone)]
    struct RecordingHttpWrite {
        requests: Arc<Mutex<Vec<HttpRequest>>>,
        response: PlannedResponse,
    }

    impl RecordingHttpWrite {
        fn success(status: u16) -> Self {
            Self {
                requests: Arc::new(Mutex::new(Vec::new())),
                response: PlannedResponse::Success(status),
            }
        }

        fn requests(&self) -> Vec<HttpRequest> {
            self.requests.lock().expect("requests lock").clone()
        }
    }

    #[async_trait]
    impl HttpWrite for RecordingHttpWrite {
        async fn send(&self, request: HttpRequest) -> HttpResult<HttpResponse> {
            self.requests.lock().expect("requests lock").push(request);
            match self.response {
                PlannedResponse::Success(status) => Ok(HttpResponse {
                    status,
                    headers: Default::default(),
                    body: Vec::new(),
                }),
                PlannedResponse::InvalidResponse(message) => {
                    Err(HttpError::InvalidResponse(message.to_string()))
                }
            }
        }
    }

    struct TestHost {
        http_write: Option<RecordingHttpWrite>,
    }

    impl DispatchBackendHost for TestHost {
        fn http_write(&self) -> Option<&dyn HttpWrite> {
            self.http_write
                .as_ref()
                .map(|client| client as &dyn HttpWrite)
        }
    }

    #[test]
    fn http_dispatch_backend_serializes_payload_and_returns_receipt() {
        let client = RecordingHttpWrite::success(202);
        let host = TestHost {
            http_write: Some(client.clone()),
        };
        let backend = HttpDispatchBackend::new(
            "sandbox_http",
            "https://sandbox.invalid/jobs/investigate",
            TrackingMode::CallbackOnly,
        );

        let receipt = block_on(backend.dispatch(
            &host,
            DispatchRequest {
                job_kind: "github_issue_investigation".to_string(),
                job_id: "job-123".to_string(),
                payload: serde_json::json!({ "hello": "world" }),
            },
        ))
        .expect("dispatch succeeds");

        let requests = client.requests();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(request.method, HttpMethod::Post);
        assert_eq!(request.url, "https://sandbox.invalid/jobs/investigate");
        assert_eq!(
            request.headers.get("content-type").map(String::as_str),
            Some("application/json")
        );
        assert_eq!(
            request.body.as_deref(),
            Some(br#"{"hello":"world"}"# as &[u8])
        );

        assert_eq!(receipt.backend_kind, "sandbox_http");
        assert_eq!(receipt.job_id, "job-123");
        assert_eq!(receipt.tracking_mode, TrackingMode::CallbackOnly);
        assert_eq!(
            receipt.metadata,
            serde_json::json!({
                "dispatch_url": "https://sandbox.invalid/jobs/investigate",
                "http_status": 202,
            })
        );
    }

    #[test]
    fn http_dispatch_backend_requires_http_write() {
        let host = TestHost { http_write: None };
        let backend = HttpDispatchBackend::new(
            "sandbox_http",
            "https://sandbox.invalid/jobs/investigate",
            TrackingMode::CallbackOnly,
        );

        let err = block_on(backend.dispatch(
            &host,
            DispatchRequest {
                job_kind: "github_issue_investigation".to_string(),
                job_id: "job-123".to_string(),
                payload: serde_json::json!({}),
            },
        ))
        .expect_err("dispatch should fail without http write");

        assert!(matches!(err, DispatchError::MissingHttpWrite));
    }

    #[test]
    fn http_dispatch_backend_rejects_non_success_status() {
        let client = RecordingHttpWrite::success(503);
        let host = TestHost {
            http_write: Some(client),
        };
        let backend = HttpDispatchBackend::new(
            "sandbox_http",
            "https://sandbox.invalid/jobs/investigate",
            TrackingMode::CallbackOnly,
        );

        let err = block_on(backend.dispatch(
            &host,
            DispatchRequest {
                job_kind: "github_issue_investigation".to_string(),
                job_id: "job-123".to_string(),
                payload: serde_json::json!({}),
            },
        ))
        .expect_err("dispatch should fail for non-success status");

        match err {
            DispatchError::Backend(message) => {
                assert!(message.contains("HTTP dispatch failed with status 503"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn http_dispatch_backend_surfaces_transport_errors() {
        let client = RecordingHttpWrite {
            requests: Arc::new(Mutex::new(Vec::new())),
            response: PlannedResponse::InvalidResponse("boom"),
        };
        let host = TestHost {
            http_write: Some(client),
        };
        let backend = HttpDispatchBackend::new(
            "sandbox_http",
            "https://sandbox.invalid/jobs/investigate",
            TrackingMode::CallbackOnly,
        );

        let err = block_on(backend.dispatch(
            &host,
            DispatchRequest {
                job_kind: "github_issue_investigation".to_string(),
                job_id: "job-123".to_string(),
                payload: serde_json::json!({}),
            },
        ))
        .expect_err("dispatch should fail for transport errors");

        match err {
            DispatchError::Backend(message) => assert!(message.contains("boom")),
            other => panic!("unexpected error: {other}"),
        }
    }
}
