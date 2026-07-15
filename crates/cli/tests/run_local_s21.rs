//! S21 native-full golden: start the real CLI TCP listener with FsWorkspace,
//! send bounded multipart PDF traffic, execute the checked transform and mocked
//! connector chain, prove cleanup/redelivery, and export sanitized metrics.

use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::Duration;

use assert_cmd::prelude::*;
use serde_json::Value;

struct ChildGuard {
    child: Option<Child>,
    stdout_path: std::path::PathBuf,
    stderr_path: std::path::PathBuf,
}

struct ChildOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl ChildGuard {
    fn new(child: Child, stdout_path: std::path::PathBuf, stderr_path: std::path::PathBuf) -> Self {
        Self {
            child: Some(child),
            stdout_path,
            stderr_path,
        }
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("child is still owned")
    }

    fn collect(&self, status: ExitStatus) -> ChildOutput {
        ChildOutput {
            status,
            stdout: std::fs::read(&self.stdout_path).unwrap_or_default(),
            stderr: std::fs::read(&self.stderr_path).unwrap_or_default(),
        }
    }

    fn shutdown(mut self) -> ChildOutput {
        let child = self.child.as_mut().expect("child is still owned");
        let signal_status = Command::new("kill")
            .args(["-INT", &child.id().to_string()])
            .status()
            .expect("send SIGINT to serve process");
        assert!(signal_status.success(), "SIGINT command failed");

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let status = loop {
            if let Some(status) = child.try_wait().expect("poll serve shutdown") {
                break status;
            }
            if std::time::Instant::now() >= deadline {
                child.kill().expect("kill serve after graceful timeout");
                break child.wait().expect("wait after forced serve shutdown");
            }
            std::thread::sleep(Duration::from_millis(25));
        };
        self.child.take();
        self.collect(status)
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

const ENCRYPTED_PDF: &[u8] =
    include_bytes!("../../processing-context/tests/fixtures/qpdf-encrypted.pdf");
const EXPANSION_PDF: &[u8] =
    include_bytes!("../../processing-context/tests/fixtures/flate-output-expansion.pdf");
const PANIC_PDF: &[u8] =
    include_bytes!("../../processing-context/tests/fixtures/type0-missing-descendants.pdf");

fn canonical_json_without_hash(value: &Value) -> String {
    let mut value = value.clone();
    if let Some(object) = value.as_object_mut() {
        object.remove("content_hash");
    }
    canonical_json(&value)
}

fn canonical_json(value: &Value) -> String {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            serde_json::to_string(value).expect("json scalar")
        }
        Value::Array(items) => {
            let mut out = String::from("[");
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&canonical_json(item));
            }
            out.push(']');
            out
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut out = String::from("{");
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(key).expect("json key"));
                out.push(':');
                out.push_str(&canonical_json(map.get(key).expect("key present")));
            }
            out.push('}');
            out
        }
    }
}

/// Build the s21 bindings.lock against a mock base URL. Three connections
/// (llm + sheets + gmail); sheets and gmail share the google bearer, the LLM
/// connection binds its own.
fn s21_lock(flow_id: &str, base_url: &str) -> Value {
    let mut lock = serde_json::json!({
        "version": 1,
        "generated_at": "2026-07-11T00:00:00Z",
        "content_hash": "",
        "instances": {
            "http1": {
                "provider_kind": "http.reqwest",
                "provides": ["resource::http"],
                "connect": {},
                "config": {},
                "isolation": []
            },
            "kv1": {
                "provider_kind": "kv.memory",
                "provides": ["resource::kv"],
                "connect": {},
                "config": {},
                "isolation": []
            }
        },
        "flows": {
            flow_id: {
                "use": {
                    "resource::http": "http1",
                    "resource::kv": "kv1"
                }
            }
        },
        "connector_handles": {
            "auth.google_workspace": {
                "provider_kind": "auth.static_bearer",
                "handle_kind": "http.bearer",
                "connect": { "secret_ref": "S21_GOOGLE_BEARER" },
                "config": {},
                "grants": {}
            },
            "auth.llm": {
                "provider_kind": "auth.static_bearer",
                "handle_kind": "http.bearer",
                "connect": { "secret_ref": "S21_LLM_BEARER" },
                "config": {},
                "grants": {}
            },
            "endpoint.llm_local": {
                "provider_kind": "endpoint.profile.static",
                "handle_kind": "endpoint.profile",
                "connect": {},
                "config": {
                    "base_url": base_url,
                    "default_headers": { "Accept": "application/json" }
                },
                "grants": {}
            },
            "endpoint.sheets_local": {
                "provider_kind": "endpoint.profile.static",
                "handle_kind": "endpoint.profile",
                "connect": {},
                "config": {
                    "base_url": base_url,
                    "default_headers": { "Accept": "application/json" }
                },
                "grants": {}
            },
            "endpoint.gmail_local": {
                "provider_kind": "endpoint.profile.static",
                "handle_kind": "endpoint.profile",
                "connect": {},
                "config": {
                    "base_url": base_url,
                    "default_headers": { "Accept": "application/json" }
                },
                "grants": {}
            }
        },
        "connector_connections": {
            "llm_local": {
                "connector_id": "connector.llm",
                "roles": {
                    "endpoint_profile.llm_default": "endpoint.llm_local",
                    "outbound_auth.llm_api_key": "auth.llm"
                }
            },
            "sheets_local": {
                "connector_id": "connector.google.sheets",
                "roles": {
                    "endpoint_profile.google_sheets_default": "endpoint.sheets_local",
                    "outbound_auth.google_workspace_auth": "auth.google_workspace"
                }
            },
            "gmail_local": {
                "connector_id": "connector.google.gmail",
                "roles": {
                    "endpoint_profile.google_gmail_default": "endpoint.gmail_local",
                    "outbound_auth.google_workspace_auth": "auth.google_workspace"
                }
            }
        },
        "connector_bindings": {
            flow_id: {
                "defaults": {
                    "connector.llm": "llm_local",
                    "connector.google.sheets": "sheets_local",
                    "connector.google.gmail": "gmail_local"
                },
                "nodes": {},
                "resolved_effect_hints": {
                    "rate_candidate": [],
                    "record_candidate": [],
                    "confirm_candidate": [],
                    "notify_hr": []
                }
            }
        }
    });

    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(canonical_json_without_hash(&lock).as_bytes());
    let hash = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    lock["content_hash"] = serde_json::json!(hash);
    lock
}

fn openai_completion_body(content: &str) -> Value {
    serde_json::json!({
        "id": "chatcmpl-golden",
        "object": "chat.completion",
        "created": 1,
        "model": "gemini-1.5-flash",
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": content, "tool_calls": [] },
            "logprobs": null,
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 40,
            "completion_tokens": 20,
            "total_tokens": 60,
            "prompt_tokens_details": { "cached_tokens": 0 }
        }
    })
}

fn synthetic_pdf_pages(texts: &[String]) -> Vec<u8> {
    let page_count = texts.len();
    let font_id = 3 + page_count * 2;
    let kids = (0..page_count)
        .map(|index| format!("{} 0 R", 3 + index * 2))
        .collect::<Vec<_>>()
        .join(" ");
    let mut objects = vec![
        b"<< /Type /Catalog /Pages 2 0 R >>".to_vec(),
        format!("<< /Type /Pages /Kids [{kids}] /Count {page_count} >>").into_bytes(),
    ];
    for (index, text) in texts.iter().enumerate() {
        let page_id = 3 + index * 2;
        let content_id = page_id + 1;
        objects.push(
            format!("<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 {font_id} 0 R >> >> /Contents {content_id} 0 R >>").into_bytes(),
        );
        let stream = format!("BT /F1 12 Tf 72 720 Td ({text}) Tj ET");
        objects.push(
            format!(
                "<< /Length {} >>\nstream\n{}\nendstream",
                stream.len(),
                stream
            )
            .into_bytes(),
        );
    }
    objects.push(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>".to_vec());
    let mut pdf = b"%PDF-1.4\n% independent S21 fixture\n".to_vec();
    let mut offsets = vec![0usize];
    for (index, object) in objects.iter().enumerate() {
        offsets.push(pdf.len());
        pdf.extend_from_slice(format!("{} 0 obj\n", index + 1).as_bytes());
        pdf.extend_from_slice(object);
        pdf.extend_from_slice(b"\nendobj\n");
    }
    let xref = pdf.len();
    pdf.extend_from_slice(
        format!("xref\n0 {}\n0000000000 65535 f \n", objects.len() + 1).as_bytes(),
    );
    for offset in offsets.into_iter().skip(1) {
        pdf.extend_from_slice(format!("{offset:010} 00000 n \n").as_bytes());
    }
    pdf.extend_from_slice(
        format!(
            "trailer\n<< /Size {} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n",
            objects.len() + 1
        )
        .as_bytes(),
    );
    pdf
}

fn synthetic_pdf(text: &str) -> Vec<u8> {
    synthetic_pdf_pages(&[text.to_string()])
}

fn multipart_body(pdf: &[u8]) -> (String, Vec<u8>) {
    multipart_body_with_file("cv", "../../ignored.pdf", "application/pdf", pdf)
}

fn multipart_body_with_file(
    field: &str,
    filename: &str,
    mime: &str,
    pdf: &[u8],
) -> (String, Vec<u8>) {
    let boundary = "lattice-s21-golden";
    let mut body = multipart_text_prefix(boundary);
    body.extend_from_slice(format!("--{boundary}\r\nContent-Disposition: form-data; name=\"{field}\"; filename=\"{filename}\"\r\nContent-Type: {mime}\r\n\r\n").as_bytes());
    body.extend_from_slice(pdf);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    (format!("multipart/form-data; boundary={boundary}"), body)
}

fn multipart_text_prefix(boundary: &str) -> Vec<u8> {
    let mut body = Vec::new();
    for (name, value) in [
        ("full_name", "Ada Lovelace"),
        ("email", "ada@applicant.test"),
        ("expectation", "5000-6000"),
        ("linkedin", "https://linkedin.test/in/ada"),
    ] {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
            )
            .as_bytes(),
        );
    }
    body
}

fn multipart_without_file() -> (String, Vec<u8>) {
    let boundary = "lattice-s21-golden";
    let mut body = multipart_text_prefix(boundary);
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    (format!("multipart/form-data; boundary={boundary}"), body)
}

fn spawn_serve(
    lock_path: &std::path::Path,
    workspace: &std::path::Path,
    metrics: &std::path::Path,
) -> (ChildGuard, String) {
    let mut failures = Vec::new();
    for attempt in 0..3 {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve probe port");
        let port = probe.local_addr().expect("probe address").port();
        drop(probe);

        let stdout_path = metrics.with_extension(format!("{attempt}.stdout"));
        let stderr_path = metrics.with_extension(format!("{attempt}.stderr"));
        let stdout = std::fs::File::create(&stdout_path).expect("create serve stdout file");
        let stderr = std::fs::File::create(&stderr_path).expect("create serve stderr file");
        let child = Command::cargo_bin("flows")
            .expect("flows binary")
            .args([
                "run",
                "serve",
                "--example",
                "s21_ai_cv_screening",
                "--bindings-lock",
            ])
            .arg(lock_path)
            .args([
                "--multipart-pdf-field",
                "cv",
                "--multipart-filename-field",
                "cv_filename",
            ])
            .args(["--workspace-dir"])
            .arg(workspace)
            .args(["--metrics-out"])
            .arg(metrics)
            .args(["--addr", &format!("127.0.0.1:{port}")])
            .env("S21_GOOGLE_BEARER", "s21-google-token")
            .env("S21_LLM_BEARER", "s21-llm-token")
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("spawn serve");
        let mut guard = ChildGuard::new(child, stdout_path, stderr_path);
        let addr = format!("127.0.0.1:{port}");
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            if TcpStream::connect(&addr).is_ok() {
                return (guard, addr);
            }
            if let Some(status) = guard
                .child_mut()
                .try_wait()
                .expect("poll serve process status")
            {
                let stderr = std::fs::read(&guard.stderr_path).unwrap_or_default();
                failures.push(format!(
                    "attempt {attempt} exited {status}: {}",
                    String::from_utf8_lossy(&stderr)
                ));
                break;
            }
            if std::time::Instant::now() >= deadline {
                failures.push(format!("attempt {attempt} timed out at {addr}"));
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }
    panic!(
        "serve process failed readiness retries: {}",
        failures.join(" | ")
    );
}

fn raw_route_saturation(addr: &str) -> (u16, Value) {
    let pending_headers = format!(
        "POST /cv-screening HTTP/1.1\r\nHost: {addr}\r\nContent-Type: multipart/form-data; boundary=pending\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n"
    );
    let mut admitted = Vec::new();
    for _ in 0..4 {
        let mut stream = TcpStream::connect(addr).expect("connect admitted multipart request");
        stream
            .write_all(pending_headers.as_bytes())
            .expect("write admitted multipart headers");
        admitted.push(stream);
    }
    std::thread::sleep(Duration::from_millis(100));

    let mut rejected = TcpStream::connect(addr).expect("connect saturated multipart request");
    rejected
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set saturation read timeout");
    write!(
        rejected,
        "POST /cv-screening HTTP/1.1\r\nHost: {addr}\r\nContent-Type: multipart/form-data; boundary=pending\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
    )
    .expect("write saturated multipart headers");
    let mut response = Vec::new();
    rejected
        .read_to_end(&mut response)
        .expect("read saturation response");
    drop(admitted);
    let split = response
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .expect("HTTP response separator");
    let headers = String::from_utf8_lossy(&response[..split]);
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .expect("HTTP saturation status")
        .parse()
        .expect("numeric saturation status");
    let body = serde_json::from_slice(&response[split + 4..]).expect("saturation JSON body");
    (status, body)
}

fn raw_chunked_total_limit_response(addr: &str) -> (u16, Value) {
    let boundary = "total-limit-stream";
    let mut stream = TcpStream::connect(addr).expect("connect chunked request");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set read timeout");
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .expect("set write timeout");
    write!(
        stream,
        "POST /cv-screening HTTP/1.1\r\nHost: {addr}\r\nContent-Type: multipart/form-data; boundary={boundary}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
    )
    .expect("write chunked request headers");

    let prefix = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"cv\"; filename=\"stream.pdf\"\r\nContent-Type: application/pdf\r\n\r\n%PDF-"
    );
    let mut chunks =
        std::iter::once(prefix.into_bytes()).chain((0..176).map(|_| vec![b'X'; 64 * 1024]));
    for chunk in chunks.by_ref() {
        if write!(stream, "{:x}\r\n", chunk.len()).is_err()
            || stream.write_all(&chunk).is_err()
            || stream.write_all(b"\r\n").is_err()
        {
            break;
        }
    }
    let _ = stream.write_all(b"0\r\n\r\n");
    let _ = stream.shutdown(Shutdown::Write);

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .expect("read chunked limit response");
    let split = response
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .expect("HTTP response separator");
    let status = String::from_utf8_lossy(&response[..split])
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .expect("HTTP status code")
        .parse()
        .expect("numeric HTTP status");
    let body = serde_json::from_slice(&response[split + 4..]).expect("limit JSON body");
    (status, body)
}

async fn assert_transport_error(
    response: reqwest::Response,
    status: reqwest::StatusCode,
    class: &str,
) {
    assert_eq!(response.status(), status);
    let body: Value = response.json().await.expect("transport error JSON");
    assert_eq!(body["class"], class);
}

async fn assert_document_error(response: reqwest::Response, expected: &[(&str, &str)]) {
    assert_eq!(
        response.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
    let body: Value = response.json().await.expect("document error JSON");
    let code = body["code"].as_str().expect("document error code");
    let class = body["class"].as_str().expect("document error class");
    assert!(
        expected.contains(&(code, class)),
        "unexpected document code/class {code}/{class}: {body}"
    );
}

fn metric_entry<'a>(evidence: &'a Value, name: &str, labels: &[(&str, &str)]) -> Option<&'a Value> {
    evidence["metrics"].as_array()?.iter().find(|entry| {
        entry["name"] == name
            && labels
                .iter()
                .all(|(key, value)| entry["labels"][*key] == *value)
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_s21_real_tcp_pdf_golden_redelivery_and_metrics() {
    let server = httpmock::MockServer::start();
    let flow_id = example_s21_ai_cv_screening::validated_ir()
        .flow()
        .id
        .as_str()
        .to_string();
    let lock = s21_lock(&flow_id, &server.base_url());
    let temp = tempfile::tempdir().expect("tempdir");
    let lock_path = temp.path().join("bindings.lock.json");
    let workspace = temp.path().join("workspaces");
    let negative_metrics = temp.path().join("negative-metrics.json");
    let metrics = temp.path().join("metrics.json");
    std::fs::write(&lock_path, serde_json::to_vec_pretty(&lock).unwrap()).unwrap();

    let rating = "Rating: 9/10. Strong match. Recommend an interview.";
    let llm_complete = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path_contains("/chat/completions")
            .header("authorization", "Bearer s21-llm-token");
        then.status(200).json_body(openai_completion_body(rating));
    });
    let values_read = server.mock(|when, then| {
        when.method(httpmock::Method::GET).path_contains("/values/");
        then.status(200).json_body_obj(&serde_json::json!({"values": [["full_name", "email", "expectation", "linkedin", "cv_filename", "ai_rating"]]}));
    });
    let values_append = server.mock(|when, then| {
        when.method(httpmock::Method::POST).path_contains("append");
        then.status(200)
            .json_body_obj(&serde_json::json!({"updates": {"updatedRange": "'Candidates'!A2:F2"}}));
    });
    let gmail_send = server.mock(|when, then| {
        when.method(httpmock::Method::POST)
            .path("/gmail/v1/users/me/messages/send")
            .header("authorization", "Bearer s21-google-token");
        then.status(200).json_body_obj(
            &serde_json::json!({"id":"msg-golden","threadId":"thread-golden","labelIds":["SENT"]}),
        );
    });

    let (negative_child, addr) = spawn_serve(&lock_path, &workspace, &negative_metrics);
    let client = reqwest::Client::new();

    let (saturation_status, saturation_body) = tokio::task::spawn_blocking({
        let addr = addr.clone();
        move || raw_route_saturation(&addr)
    })
    .await
    .expect("saturation task");
    assert_eq!(saturation_status, 503);
    assert_eq!(saturation_body["class"], "busy");
    llm_complete.assert_hits(0);
    values_read.assert_hits(0);
    values_append.assert_hits(0);
    gmail_send.assert_hits(0);

    let missing_content_type = client
        .post(format!("http://{addr}/cv-screening"))
        .body("not multipart")
        .send()
        .await
        .unwrap();
    assert_transport_error(
        missing_content_type,
        reqwest::StatusCode::BAD_REQUEST,
        "bad_request",
    )
    .await;

    let wrong_top_level = client
        .post(format!("http://{addr}/cv-screening"))
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_transport_error(
        wrong_top_level,
        reqwest::StatusCode::BAD_REQUEST,
        "bad_request",
    )
    .await;

    let (missing_file_type, missing_file_body) = multipart_without_file();
    let missing_file = client
        .post(format!("http://{addr}/cv-screening"))
        .header("content-type", missing_file_type)
        .body(missing_file_body)
        .send()
        .await
        .unwrap();
    assert_transport_error(
        missing_file,
        reqwest::StatusCode::BAD_REQUEST,
        "bad_request",
    )
    .await;

    let valid_transport_pdf = synthetic_pdf("transport negative fixture");
    let (wrong_mime_type, wrong_mime_body) =
        multipart_body_with_file("cv", "wrong-mime.pdf", "text/plain", &valid_transport_pdf);
    let wrong_mime = client
        .post(format!("http://{addr}/cv-screening"))
        .header("content-type", wrong_mime_type)
        .body(wrong_mime_body)
        .send()
        .await
        .unwrap();
    assert_transport_error(
        wrong_mime,
        reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "unsupported_media_type",
    )
    .await;

    let (unknown_file_type, unknown_file_body) = multipart_body_with_file(
        "other",
        "unknown-field.pdf",
        "application/pdf",
        &valid_transport_pdf,
    );
    let unknown_file = client
        .post(format!("http://{addr}/cv-screening"))
        .header("content-type", unknown_file_type)
        .body(unknown_file_body)
        .send()
        .await
        .unwrap();
    assert_transport_error(
        unknown_file,
        reqwest::StatusCode::BAD_REQUEST,
        "bad_request",
    )
    .await;

    let (bad_magic_type, bad_magic_body) = multipart_body(b"not a PDF");
    let bad_magic = client
        .post(format!("http://{addr}/cv-screening"))
        .header("content-type", bad_magic_type)
        .body(bad_magic_body)
        .send()
        .await
        .unwrap();
    assert_transport_error(
        bad_magic,
        reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "unsupported_media_type",
    )
    .await;

    let (malformed_type, malformed_body) = multipart_body(b"%PDF-malformed");
    let malformed = client
        .post(format!("http://{addr}/cv-screening"))
        .header("content-type", malformed_type)
        .body(malformed_body)
        .send()
        .await
        .unwrap();
    assert_document_error(
        malformed,
        &[
            ("STD-DOC-002", "unsupported_document"),
            ("STD-DOC-002", "guest_failed"),
        ],
    )
    .await;

    let hostile_cases: &[(&[u8], &[(&str, &str)])] = &[
        (ENCRYPTED_PDF, &[("STD-DOC-002", "unsupported_document")]),
        (
            EXPANSION_PDF,
            &[
                ("STD-DOC-002", "unsupported_document"),
                ("STD-DOC-002", "fuel_exhausted"),
                ("STD-DOC-002", "wall_time_exceeded"),
                ("STD-DOC-002", "memory_exhausted"),
            ],
        ),
        (PANIC_PDF, &[("STD-DOC-002", "guest_failed")]),
    ];
    for (hostile_pdf, expected) in hostile_cases {
        let (hostile_type, hostile_body) = multipart_body(hostile_pdf);
        let hostile = client
            .post(format!("http://{addr}/cv-screening"))
            .header("content-type", hostile_type)
            .body(hostile_body)
            .send()
            .await
            .unwrap();
        assert_document_error(hostile, expected).await;
    }

    let (empty_type, empty_body) = multipart_body(&synthetic_pdf(""));
    let empty = client
        .post(format!("http://{addr}/cv-screening"))
        .header("content-type", empty_type)
        .body(empty_body)
        .send()
        .await
        .unwrap();
    assert_document_error(empty, &[("STD-DOC-002", "unsupported_document")]).await;

    let (zero_page_type, zero_page_body) = multipart_body(&synthetic_pdf_pages(&[]));
    let zero_page = client
        .post(format!("http://{addr}/cv-screening"))
        .header("content-type", zero_page_type)
        .body(zero_page_body)
        .send()
        .await
        .unwrap();
    assert_document_error(zero_page, &[("STD-DOC-002", "unsupported_document")]).await;

    let pages = (0..201)
        .map(|index| format!("page {index}"))
        .collect::<Vec<_>>();
    let (page_limit_type, page_limit_body) = multipart_body(&synthetic_pdf_pages(&pages));
    let page_limit = client
        .post(format!("http://{addr}/cv-screening"))
        .header("content-type", page_limit_type)
        .body(page_limit_body)
        .send()
        .await
        .unwrap();
    assert_document_error(page_limit, &[("STD-DOC-002", "unsupported_document")]).await;

    let oversized = vec![b'X'; 8 * 1024 * 1024 + 1];
    let (oversized_type, oversized_body) =
        multipart_body(&[b"%PDF-".as_slice(), &oversized].concat());
    let oversized = client
        .post(format!("http://{addr}/cv-screening"))
        .header("content-type", oversized_type)
        .body(oversized_body)
        .send()
        .await
        .unwrap();
    assert_transport_error(
        oversized,
        reqwest::StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
    )
    .await;

    let total_limit = client
        .post(format!("http://{addr}/cv-screening"))
        .header("content-type", "multipart/form-data; boundary=total-limit")
        .header("content-length", (10 * 1024 * 1024 + 1).to_string())
        .body(Vec::new())
        .send()
        .await
        .unwrap();
    assert_transport_error(
        total_limit,
        reqwest::StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
    )
    .await;

    let (chunked_status, chunked_body) = tokio::task::spawn_blocking({
        let addr = addr.clone();
        move || raw_chunked_total_limit_response(&addr)
    })
    .await
    .expect("chunked request task");
    assert_eq!(chunked_status, 413);
    assert_eq!(chunked_body["class"], "payload_too_large");

    let boundary = "lattice-s21-golden";
    let closing = format!("--{boundary}--\r\n");
    let valid_pdf = valid_transport_pdf;
    let (text_type, mut text_body) = multipart_body(&valid_pdf);
    text_body.truncate(text_body.len() - closing.len());
    text_body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"oversized_text\"\r\n\r\n{}\r\n{closing}",
            "x".repeat(32 * 1024 + 1)
        )
        .as_bytes(),
    );
    let text_limit = client
        .post(format!("http://{addr}/cv-screening"))
        .header("content-type", text_type)
        .body(text_body)
        .send()
        .await
        .unwrap();
    assert_transport_error(
        text_limit,
        reqwest::StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
    )
    .await;

    let (duplicate_type, mut duplicate_body) = multipart_body(&valid_pdf);
    duplicate_body.truncate(duplicate_body.len() - closing.len());
    duplicate_body.extend_from_slice(format!("--{boundary}\r\nContent-Disposition: form-data; name=\"cv\"; filename=\"duplicate.pdf\"\r\nContent-Type: application/pdf\r\n\r\n").as_bytes());
    duplicate_body.extend_from_slice(&valid_pdf);
    duplicate_body.extend_from_slice(format!("\r\n{closing}").as_bytes());
    let duplicate = client
        .post(format!("http://{addr}/cv-screening"))
        .header("content-type", duplicate_type)
        .body(duplicate_body)
        .send()
        .await
        .unwrap();
    assert_transport_error(duplicate, reqwest::StatusCode::BAD_REQUEST, "bad_request").await;

    let malformed_multipart = client
        .post(format!("http://{addr}/cv-screening"))
        .header("content-type", "multipart/form-data; boundary=broken")
        .body("--broken\r\ninvalid")
        .send()
        .await
        .unwrap();
    assert_transport_error(
        malformed_multipart,
        reqwest::StatusCode::BAD_REQUEST,
        "bad_request",
    )
    .await;

    llm_complete.assert_hits(0);
    values_read.assert_hits(0);
    values_append.assert_hits(0);
    gmail_send.assert_hits(0);

    let negative_output = negative_child.shutdown();
    assert!(
        negative_output.status.success(),
        "negative serve stderr: {}",
        String::from_utf8_lossy(&negative_output.stderr)
    );
    let negative_evidence: Value =
        serde_json::from_slice(&std::fs::read(&negative_metrics).unwrap()).unwrap();

    let (child, addr) = spawn_serve(&lock_path, &workspace, &metrics);
    let (content_type, body) = multipart_body(&synthetic_pdf(
        "Ten years building analytical engines and Rust services.",
    ));
    let first = client
        .post(format!("http://{addr}/cv-screening"))
        .header("content-type", &content_type)
        .body(body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), reqwest::StatusCode::OK);
    let first: Value = first.json().await.unwrap();
    assert_eq!(first["stored"], true);
    assert_eq!(first["ai_rating"], rating);
    let first_wire = first.to_string();
    assert!(!first_wire.contains("%PDF-"));
    assert!(!first_wire.contains("analytical engines"));
    assert!(!first_wire.contains("ignored.pdf"));

    let second = client
        .post(format!("http://{addr}/cv-screening"))
        .header("content-type", &content_type)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), reqwest::StatusCode::OK);
    let second: Value = second.json().await.unwrap();
    assert_eq!(second["stored"], false);
    assert_eq!(second["key"], first["key"]);

    llm_complete.assert_hits(1);
    values_read.assert_hits(1);
    values_append.assert_hits(1);
    gmail_send.assert_hits(2);
    fn contains_file(path: &std::path::Path) -> bool {
        std::fs::read_dir(path)
            .map(|entries| {
                entries.filter_map(Result::ok).any(|entry| {
                    let path = entry.path();
                    path.is_file() || (path.is_dir() && contains_file(&path))
                })
            })
            .unwrap_or(false)
    }
    assert!(
        !contains_file(&workspace),
        "terminal success must remove staged files"
    );

    let output = child.shutdown();
    assert!(
        output.status.success(),
        "serve stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let evidence: Value = serde_json::from_slice(&std::fs::read(&metrics).unwrap()).unwrap();
    let serialized = evidence.to_string();
    let combined_serialized = format!(
        "{}{}{}{}{}{}",
        negative_evidence,
        serialized,
        String::from_utf8_lossy(&negative_output.stdout),
        String::from_utf8_lossy(&negative_output.stderr),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    for name in [
        "lattice.transform.duration_ms",
        "lattice.transform.input_bytes",
        "lattice.transform.input_hash_comparisons_total",
        "lattice.transform.output_bytes",
        "lattice.transform.fuel_consumed",
        "lattice.transform.fuel_ceiling",
        "lattice.transform.peak_requested_memory_bytes",
        "lattice.transform.memory_ceiling_bytes",
        "lattice.transform.terminations_total",
        "lattice.host.http_requests_total",
        "lattice.host.multipart_admission_total",
        "lattice.host.multipart_rejected_total",
        "lattice.host.multipart_request_bytes",
        "lattice.host.multipart_file_bytes",
        "lattice.host.ingress_staged_attachments_total",
        "lattice.host.ingress_staged_bytes",
        "lattice.host.workspace_cleanup_total",
    ] {
        assert!(combined_serialized.contains(name), "missing metric {name}");
    }
    let hash_comparisons = metric_entry(
        &evidence,
        "lattice.transform.input_hash_comparisons_total",
        &[
            ("backend", "native"),
            ("transform", stdlib::document::PDF_EXTRACT_TRANSFORM_ID),
            ("outcome", "matched"),
        ],
    )
    .expect("native matched hash-comparison counter");
    assert_eq!(
        hash_comparisons["value"], 2,
        "valid delivery plus sequential redelivery each compare the staged hash"
    );

    let node_invocations = |alias: &str| {
        metric_entry(
            &evidence,
            "lattice.executor.node_latency_ms",
            &[("node", alias)],
        )
        .and_then(|entry| entry["value"].as_array())
        .map(Vec::len)
    };
    assert_eq!(
        node_invocations("check_redelivery"),
        Some(2),
        "only valid delivery and redelivery reach the KV check"
    );
    assert_eq!(
        node_invocations("record_screening"),
        Some(1),
        "exactly one terminal KV write"
    );

    let all_evidence = combined_serialized;
    let mut oversized_pdf = b"%PDF-".to_vec();
    oversized_pdf.resize(8 * 1024 * 1024 + 6, b'X');
    let request_hashes = vec![
        capabilities::artifact::sha256_hex(ENCRYPTED_PDF),
        capabilities::artifact::sha256_hex(EXPANSION_PDF),
        capabilities::artifact::sha256_hex(PANIC_PDF),
        capabilities::artifact::sha256_hex(b"not a PDF"),
        capabilities::artifact::sha256_hex(b"%PDF-malformed"),
        capabilities::artifact::sha256_hex(&valid_pdf),
        capabilities::artifact::sha256_hex(&synthetic_pdf("")),
        capabilities::artifact::sha256_hex(&synthetic_pdf_pages(&[])),
        capabilities::artifact::sha256_hex(&synthetic_pdf_pages(&pages)),
        capabilities::artifact::sha256_hex(&oversized_pdf),
        capabilities::artifact::sha256_hex(&synthetic_pdf(
            "Ten years building analytical engines and Rust services.",
        )),
    ];
    let forbidden = [
        "Ada Lovelace",
        "ada@applicant.test",
        "applicant.test",
        "5000-6000",
        "linkedin.test/in/ada",
        "../../ignored.pdf",
        "ignored.pdf",
        "wrong-mime.pdf",
        "unknown-field.pdf",
        "duplicate.pdf",
        "stream.pdf",
        "analytical engines",
        "transport negative fixture",
        "s21-google-token",
        "s21-llm-token",
        lock_path.to_str().unwrap(),
        workspace.to_str().unwrap(),
        metrics.to_str().unwrap(),
        negative_metrics.to_str().unwrap(),
        temp.path().to_str().unwrap(),
        "type0-missing-descendants",
        "flate-output-expansion",
        "qpdf-encrypted",
        "parser panic",
        "lopdf",
    ];
    for value in forbidden
        .into_iter()
        .chain(request_hashes.iter().map(String::as_str))
    {
        assert!(
            !all_evidence.contains(value),
            "sanitized evidence leaked forbidden value `{value}`"
        );
    }
}
