//! Handwritten Airtable API runtime: semantic record create over the shared
//! connectors-std transport (endpoint + auth resolved from the current
//! connector context, mirroring `connector_google_sheets`'s `SheetsApi`).
//!
//! Airtable's "create record" is a single `POST /v0/{baseId}/{table}` whose
//! JSON body carries the record `fields` map. The op is a semantic
//! pass-through: callers supply `fields` and get back the created record's id.

use capabilities::http::{HttpMethod, HttpRequest};
use connectors_std::endpoint::{ResolvedEndpointProfile, apply_default_headers};
use connectors_std::{
    CurrentConnectorContext, apply_outbound_auth_with_context, current_connector_context,
    decode_json_response_body, resolve_endpoint_with_context, send_request_from_current,
};
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};

use crate::generated::profiles::{
    AIRTABLE_DEFAULT_ENDPOINT_PROFILE, AIRTABLE_TOKEN_AUTH_OUTBOUND_AUTH,
};
use crate::generated::types::{AirtableCreateRecordInput, AirtableCreateRecordOutput};
use crate::runtime::errors::ConnectorRuntimeError;

/// Airtable Web API v0 origin. The `/v0/{baseId}/{table}` path is appended per
/// request; the profile base URL is overridable via env/bindings for testing.
pub const AIRTABLE_BASE_URL: &str = "https://api.airtable.com";

/// Build the create-record path for a base + table, percent-encoding each
/// segment so human-readable table names survive as a single path component.
pub fn airtable_records_path(base_id: &str, table: &str) -> String {
    format!(
        "/v0/{}/{}",
        encode_path_segment(base_id),
        encode_path_segment(table)
    )
}

/// Percent-encode everything outside the unreserved set (RFC 3986 2.3) so a
/// value is safe as one path segment.
fn encode_path_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for byte in segment.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

pub struct AirtableApi {
    action_id: &'static str,
    context: CurrentConnectorContext,
    endpoint: ResolvedEndpointProfile,
}

impl AirtableApi {
    pub async fn for_action(action_id: &'static str) -> Result<Self, ConnectorRuntimeError> {
        let context = current_connector_context(action_id).await?;
        let endpoint =
            resolve_endpoint_with_context(&AIRTABLE_DEFAULT_ENDPOINT_PROFILE, &context).await?;
        Ok(Self {
            action_id,
            context,
            endpoint,
        })
    }

    pub async fn create_record(
        &self,
        input: &AirtableCreateRecordInput,
    ) -> Result<AirtableCreateRecordOutput, ConnectorRuntimeError> {
        let mut body = json!({ "fields": input.fields.clone() });
        if let Some(typecast) = input.typecast {
            body["typecast"] = JsonValue::Bool(typecast);
        }

        let response = self
            .request_json(
                HttpMethod::Post,
                &airtable_records_path(&input.base_id, &input.table),
                Some(body),
            )
            .await?;

        create_record_output_from_response(&response)
    }

    async fn request_json(
        &self,
        method: HttpMethod,
        path: &str,
        body: Option<JsonValue>,
    ) -> Result<JsonValue, ConnectorRuntimeError> {
        let url = format!("{}{}", self.endpoint.base_url.trim_end_matches('/'), path);

        let mut request = HttpRequest::new(method, url);
        apply_default_headers(&mut request.headers, &self.endpoint);
        apply_outbound_auth_with_context(
            &AIRTABLE_TOKEN_AUTH_OUTBOUND_AUTH,
            &mut request,
            &self.context,
        )
        .await?;

        if let Some(body) = body {
            request.headers.insert("Content-Type", "application/json");
            request.body = Some(serde_json::to_vec(&body)?);
        }

        let response = send_request_from_current(self.action_id, method, request).await?;
        decode_json_response_body(&response)
    }
}

#[derive(Deserialize)]
struct WireRecord {
    id: String,
    #[serde(default, rename = "createdTime")]
    created_time: Option<String>,
}

fn create_record_output_from_response(
    body: &JsonValue,
) -> Result<AirtableCreateRecordOutput, ConnectorRuntimeError> {
    let record: WireRecord = serde_json::from_value(body.clone()).map_err(|err| {
        ConnectorRuntimeError::invalid_response(format!(
            "Airtable create-record response did not match the expected shape: {err}"
        ))
    })?;

    Ok(AirtableCreateRecordOutput {
        id: record.id,
        created_time: record.created_time,
    })
}
