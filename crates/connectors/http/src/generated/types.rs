use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Tier 0/1 read input (GET/HEAD): a relative path against a lock-bound
/// origin. No body field — reads carry no request body (spec §3).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HttpReadInput {
    /// Absolute-path reference; must start with `/` (validated per §10).
    pub path: String,
    /// Query pairs, percent-encoded via `append_query_pair`.
    #[serde(default)]
    pub query: Vec<(String, String)>,
    /// Header name → value; names pass the §10 denylist, values are CR/LF-rejected.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// Tier 1 only: role name of the granted endpoint profile to use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
}

/// Tier 0/1 write input (POST/PUT/PATCH/DELETE): read input plus a JSON body.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HttpWriteInput {
    pub path: String,
    #[serde(default)]
    pub query: Vec<(String, String)>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
}

/// Tier 2 read input (`*_any_origin`): a runtime-supplied absolute URL.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HttpReadAnyOriginInput {
    /// Absolute URL (https required by default; SSRF-guarded per §10).
    pub url: String,
    #[serde(default)]
    pub query: Vec<(String, String)>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}

/// Tier 2 write input (`*_any_origin`): an absolute URL plus a JSON body.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HttpWriteAnyOriginInput {
    pub url: String,
    #[serde(default)]
    pub query: Vec<(String, String)>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<JsonValue>,
}

/// Default response mode output: the decoded 2xx JSON body (spec §5 `json`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HttpJsonOutput {
    pub body: JsonValue,
}

/// `text` response mode output.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HttpTextOutput {
    pub body: String,
}

/// `full_response` envelope (spec §5): status moves in-band; the body is
/// decoded leniently (JSON-or-null) with a text excerpt and a UTF-8 flag.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HttpFullResponse {
    pub status: u16,
    /// Allowlisted response headers only (content-type, etag, location,
    /// retry-after, x-request-id) — never a full header dump (§10).
    pub headers: BTreeMap<String, String>,
    /// Leniently-decoded JSON body, or `null` when the body is not JSON.
    pub body: JsonValue,
    /// The raw body as text (lossy if the bytes were not valid UTF-8).
    pub body_text: String,
    /// True when `body_text` required lossy UTF-8 replacement.
    #[serde(default)]
    pub utf8_lossy: bool,
}
