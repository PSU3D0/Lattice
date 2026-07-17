use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::StreamExt;
use host_inproc::IngressAttachment;
use serde_json::{Map as JsonMap, Value as JsonValue};
use worker::{Env, Request};

pub const DEFAULT_MULTIPART_MAX_TOTAL_BYTES: usize = 10 * 1024 * 1024;
pub const DEFAULT_MULTIPART_MAX_FILE_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_MULTIPART_MAX_TEXT_BYTES: usize = 64 * 1024;
const MAX_MULTIPART_FIELDS: usize = 32;
const MAX_FIELD_NAME_BYTES: usize = 128;
const MAX_FILENAME_BYTES: usize = 1024;

thread_local! {
    static MULTIPART_ACTIVE: Cell<bool> = const { Cell::new(false) };
}

struct IsolateSendStream<S>(Rc<RefCell<S>>);

impl<S> IsolateSendStream<S> {
    fn new(stream: S) -> Self {
        Self(Rc::new(RefCell::new(stream)))
    }
}

impl<S> Clone for IsolateSendStream<S> {
    fn clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

// SAFETY: Cloudflare's wasm Worker executes this stream only on its isolate-local,
// single-threaded event loop. This wrapper satisfies multer's native-oriented Send bound;
// it is private and cannot cross an isolate or native thread boundary.
unsafe impl<S> Send for IsolateSendStream<S> {}

impl<S: futures::Stream + Unpin> futures::Stream for IsolateSendStream<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut *self.0.borrow_mut()).poll_next(context)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkersMultipartIngressConfig {
    pub file_field: String,
    pub artifact_payload_field: String,
    pub filename_metadata_field: Option<String>,
    pub expected_content_type: String,
    pub required_magic: Vec<u8>,
    pub max_total_bytes: usize,
    pub max_file_bytes: usize,
    pub max_text_bytes: usize,
}

impl WorkersMultipartIngressConfig {
    pub fn from_env(env: &Env) -> Option<Result<Self, String>> {
        let file_field = env_string(env, "LATTICE_MULTIPART_FILE_FIELD")?;
        Some((|| {
            Self {
                artifact_payload_field: env_string(env, "LATTICE_MULTIPART_ARTIFACT_FIELD")
                    .unwrap_or_else(|| file_field.clone()),
                filename_metadata_field: env_string(
                    env,
                    "LATTICE_MULTIPART_FILENAME_METADATA_FIELD",
                ),
                expected_content_type: env_string(env, "LATTICE_MULTIPART_EXPECTED_CONTENT_TYPE")
                    .unwrap_or_else(|| "application/pdf".to_string()),
                required_magic: env_string(env, "LATTICE_MULTIPART_REQUIRED_MAGIC")
                    .unwrap_or_else(|| "%PDF-".to_string())
                    .into_bytes(),
                max_total_bytes: env_usize_checked(env, "LATTICE_MULTIPART_MAX_TOTAL_BYTES")?
                    .unwrap_or(DEFAULT_MULTIPART_MAX_TOTAL_BYTES),
                max_file_bytes: env_usize_checked(env, "LATTICE_MULTIPART_MAX_FILE_BYTES")?
                    .unwrap_or(DEFAULT_MULTIPART_MAX_FILE_BYTES),
                max_text_bytes: env_usize_checked(env, "LATTICE_MULTIPART_MAX_TEXT_BYTES")?
                    .unwrap_or(DEFAULT_MULTIPART_MAX_TEXT_BYTES),
                file_field,
            }
            .validate()
        })())
    }

    fn validate(self) -> Result<Self, String> {
        if self.file_field.is_empty()
            || self.artifact_payload_field.is_empty()
            || self.expected_content_type.is_empty()
            || self.required_magic.is_empty()
        {
            return Err("Workers multipart field, MIME type, and magic must be non-empty".into());
        }
        if self
            .filename_metadata_field
            .as_deref()
            .is_some_and(|field| field.is_empty() || field == self.artifact_payload_field)
        {
            return Err("Workers multipart filename field is invalid or reserved".into());
        }
        if self.max_total_bytes == 0
            || self.max_file_bytes == 0
            || self.max_text_bytes == 0
            || self.max_file_bytes > self.max_total_bytes
            || self.max_total_bytes > DEFAULT_MULTIPART_MAX_TOTAL_BYTES
            || self.max_file_bytes > DEFAULT_MULTIPART_MAX_FILE_BYTES
            || self.max_text_bytes > DEFAULT_MULTIPART_MAX_TEXT_BYTES
        {
            return Err(
                "Workers multipart limits are invalid or exceed isolate-safe maxima".into(),
            );
        }
        Ok(self)
    }
}

pub struct MultipartAdmissionGuard {
    active: bool,
}

// SAFETY: the guard is only constructed and dropped by a Cloudflare wasm isolate's
// single-threaded event loop. The Send bound is required by HostRuntime's staging callback;
// no native/threaded constructor is exported.
unsafe impl Send for MultipartAdmissionGuard {}

impl MultipartAdmissionGuard {
    pub fn try_acquire() -> Option<Self> {
        let acquired = MULTIPART_ACTIVE.with(|active| {
            if active.get() {
                false
            } else {
                active.set(true);
                true
            }
        });
        acquired.then_some(Self { active: true })
    }
}

impl Drop for MultipartAdmissionGuard {
    fn drop(&mut self) {
        if self.active {
            MULTIPART_ACTIVE.with(|active| active.set(false));
            self.active = false;
        }
    }
}

#[derive(Debug)]
pub struct ParsedMultipartIngress {
    pub payload: JsonValue,
    pub attachment: IngressAttachment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultipartErrorClass {
    BadRequest,
    PayloadTooLarge,
    UnsupportedMediaType,
    BodyRead,
}

#[derive(Debug)]
pub struct MultipartError {
    pub class: MultipartErrorClass,
    pub message: &'static str,
}

impl MultipartError {
    fn bad_request(message: &'static str) -> Self {
        Self {
            class: MultipartErrorClass::BadRequest,
            message,
        }
    }

    fn too_large(message: &'static str) -> Self {
        Self {
            class: MultipartErrorClass::PayloadTooLarge,
            message,
        }
    }

    fn unsupported(message: &'static str) -> Self {
        Self {
            class: MultipartErrorClass::UnsupportedMediaType,
            message,
        }
    }
}

pub fn is_multipart(req: &Request) -> bool {
    req.headers()
        .get("content-type")
        .ok()
        .flatten()
        .is_some_and(|value| value.starts_with("multipart/form-data;"))
}

pub async fn parse_multipart(
    req: &mut Request,
    config: &WorkersMultipartIngressConfig,
) -> Result<ParsedMultipartIngress, MultipartError> {
    if declared_content_length(req)?.is_some_and(|length| length > config.max_total_bytes) {
        return Err(MultipartError::too_large("multipart request exceeds limit"));
    }
    let content_type = req
        .headers()
        .get("content-type")
        .ok()
        .flatten()
        .ok_or_else(|| MultipartError::bad_request("multipart boundary is missing"))?;
    let boundary = multer::parse_boundary(&content_type)
        .map_err(|_| MultipartError::bad_request("multipart boundary is malformed"))?;

    let total = Arc::new(AtomicUsize::new(0));
    let exceeded = Arc::new(AtomicBool::new(false));
    let stream_total = Arc::clone(&total);
    let stream_exceeded = Arc::clone(&exceeded);
    let max_total = config.max_total_bytes;
    let stream = req
        .stream()
        .map_err(|_| MultipartError {
            class: MultipartErrorClass::BodyRead,
            message: "multipart body is unavailable",
        })?
        .map(move |chunk| {
            let chunk = chunk.map_err(|_| std::io::Error::other("request body read failed"))?;
            let previous = stream_total.fetch_add(chunk.len(), Ordering::Relaxed);
            if previous.saturating_add(chunk.len()) > max_total {
                stream_exceeded.store(true, Ordering::Relaxed);
                return Err(std::io::Error::other("multipart request exceeds limit"));
            }
            Ok::<Bytes, std::io::Error>(Bytes::from(chunk))
        });
    let mut drain = IsolateSendStream::new(stream);
    let mut multipart = multer::Multipart::new(drain.clone(), boundary);
    let mut payload = JsonMap::new();
    let mut attachment = None;
    let mut text_bytes = 0usize;
    let mut fields = 0usize;
    let mut seen = BTreeMap::<String, ()>::new();

    while let Some(field) = multipart.next_field().await.map_err(|_| {
        if exceeded.load(Ordering::Relaxed) {
            MultipartError::too_large("multipart request exceeds limit")
        } else {
            MultipartError::bad_request("malformed multipart body")
        }
    })? {
        fields += 1;
        if fields > MAX_MULTIPART_FIELDS {
            return Err(MultipartError::too_large(
                "multipart field count exceeds limit",
            ));
        }
        let name = field
            .name()
            .map(str::to_string)
            .ok_or_else(|| MultipartError::bad_request("multipart field name is missing"))?;
        if name.is_empty()
            || name.len() > MAX_FIELD_NAME_BYTES
            || seen.insert(name.clone(), ()).is_some()
        {
            return Err(MultipartError::bad_request(
                "multipart field is duplicated or invalid",
            ));
        }
        let filename = field.file_name().map(str::to_string);
        let field_content_type = field.content_type().map(ToString::to_string);

        if name == config.file_field {
            let filename = filename.ok_or_else(|| {
                MultipartError::bad_request("multipart file field requires a filename")
            })?;
            if filename.is_empty()
                || filename.len() > MAX_FILENAME_BYTES
                || filename.chars().any(char::is_control)
            {
                return Err(MultipartError::bad_request("multipart filename is unsafe"));
            }
            if attachment.is_some() {
                return Err(MultipartError::bad_request(
                    "multipart file field is duplicated",
                ));
            }
            if field_content_type.as_deref() != Some(config.expected_content_type.as_str()) {
                return Err(MultipartError::unsupported(
                    "multipart file has unsupported media type",
                ));
            }
            let bytes = field.bytes().await.map_err(|_| {
                if exceeded.load(Ordering::Relaxed) {
                    MultipartError::too_large("multipart request exceeds limit")
                } else {
                    MultipartError::bad_request("malformed multipart file field")
                }
            })?;
            if bytes.len() > config.max_file_bytes {
                return Err(MultipartError::too_large("multipart file exceeds limit"));
            }
            if !bytes.starts_with(&config.required_magic) {
                return Err(MultipartError::unsupported(
                    "multipart file does not match required magic",
                ));
            }
            if let Some(metadata_field) = config.filename_metadata_field.as_ref() {
                if payload.contains_key(metadata_field) {
                    return Err(MultipartError::bad_request(
                        "multipart filename metadata field is duplicated",
                    ));
                }
                payload.insert(metadata_field.clone(), JsonValue::String(filename));
            }
            attachment = Some(IngressAttachment::new(
                config.artifact_payload_field.clone(),
                config.expected_content_type.clone(),
                bytes,
            ));
            continue;
        }

        if filename.is_some()
            || field_content_type
                .as_deref()
                .is_some_and(|content_type| content_type != "text/plain")
            || name == config.artifact_payload_field
            || config.filename_metadata_field.as_deref() == Some(name.as_str())
        {
            return Err(MultipartError::bad_request(
                "multipart contains an unknown or reserved field",
            ));
        }
        let bytes = field
            .bytes()
            .await
            .map_err(|_| MultipartError::bad_request("malformed multipart text field"))?;
        text_bytes = text_bytes
            .checked_add(bytes.len())
            .ok_or_else(|| MultipartError::too_large("multipart text fields exceed limit"))?;
        if text_bytes > config.max_text_bytes {
            return Err(MultipartError::too_large(
                "multipart text fields exceed limit",
            ));
        }
        let value = std::str::from_utf8(&bytes)
            .map_err(|_| MultipartError::bad_request("multipart text field is not UTF-8"))?;
        payload.insert(name, JsonValue::String(value.to_string()));
    }

    let attachment = attachment
        .ok_or_else(|| MultipartError::bad_request("multipart required file field is missing"))?;

    // Multer can recognize the terminal boundary before polling the request stream to EOF.
    // Drain only after a valid, size-bounded form so workerd can release the body reader
    // cleanly. Error paths remain fail-fast rather than consuming attacker-controlled tails.
    drop(multipart);
    while let Some(chunk) = drain.next().await {
        chunk.map_err(|_| MultipartError {
            class: MultipartErrorClass::BodyRead,
            message: "multipart body read failed",
        })?;
    }

    Ok(ParsedMultipartIngress {
        payload: JsonValue::Object(payload),
        attachment,
    })
}

fn declared_content_length(req: &Request) -> Result<Option<usize>, MultipartError> {
    let Some(value) = req
        .headers()
        .get("content-length")
        .map_err(|_| MultipartError::bad_request("content-length header is malformed"))?
    else {
        return Ok(None);
    };
    let length = value
        .parse::<usize>()
        .map_err(|_| MultipartError::bad_request("content-length header is malformed"))?;
    Ok(Some(length))
}

fn env_string(env: &Env, name: &str) -> Option<String> {
    env.var(name).ok().map(|value| value.to_string())
}

fn env_usize_checked(env: &Env, name: &str) -> Result<Option<usize>, String> {
    let Some(value) = env_string(env, name) else {
        return Ok(None);
    };
    value
        .parse::<usize>()
        .map(Some)
        .map_err(|_| format!("Workers multipart integer variable `{name}` is invalid"))
}
