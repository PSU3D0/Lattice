//! Handwritten Gmail API runtime: semantic send-message over the shared
//! connectors-std transport (endpoint + auth resolved from the current
//! connector context, exactly like `connector_google_sheets`'s `SheetsApi`).

use capabilities::http::{HttpMethod, HttpRequest};
use connector_google_platform::gmail::{
    base64url_no_pad, build_plain_text_email, gmail_send_message_path,
};
use connectors_std::endpoint::{ResolvedEndpointProfile, apply_default_headers};
use connectors_std::{
    CurrentConnectorContext, apply_outbound_auth_with_context, current_connector_context,
    decode_json_response_body, resolve_endpoint_with_context, send_request_from_current,
};
use serde_json::{Value as JsonValue, json};

use crate::generated::profiles::{
    GOOGLE_GMAIL_DEFAULT_ENDPOINT_PROFILE, GOOGLE_WORKSPACE_AUTH_OUTBOUND_AUTH,
};
use crate::generated::types::{GoogleGmailSendMessageInput, GoogleGmailSendMessageOutput};
use crate::runtime::errors::ConnectorRuntimeError;

pub struct GmailApi {
    action_id: &'static str,
    context: CurrentConnectorContext,
    endpoint: ResolvedEndpointProfile,
}

impl GmailApi {
    pub async fn for_action(action_id: &'static str) -> Result<Self, ConnectorRuntimeError> {
        let context = current_connector_context(action_id).await?;
        let endpoint =
            resolve_endpoint_with_context(&GOOGLE_GMAIL_DEFAULT_ENDPOINT_PROFILE, &context).await?;
        Ok(Self {
            action_id,
            context,
            endpoint,
        })
    }

    pub async fn send_message(
        &self,
        input: &GoogleGmailSendMessageInput,
    ) -> Result<GoogleGmailSendMessageOutput, ConnectorRuntimeError> {
        let mime = build_plain_text_email(
            &input.to,
            input.cc.as_deref(),
            input.bcc.as_deref(),
            &input.subject,
            &input.text_body,
        );
        let body = json!({ "raw": base64url_no_pad(mime.as_bytes()) });

        let response = self
            .request_json(HttpMethod::Post, gmail_send_message_path(), Some(body))
            .await?;

        send_message_output_from_response(&response)
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
            &GOOGLE_WORKSPACE_AUTH_OUTBOUND_AUTH,
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

fn send_message_output_from_response(
    body: &JsonValue,
) -> Result<GoogleGmailSendMessageOutput, ConnectorRuntimeError> {
    let id = body
        .get("id")
        .and_then(JsonValue::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            ConnectorRuntimeError::invalid_response(
                "Gmail send response did not contain string field `id`",
            )
        })?;
    let thread_id = body
        .get("threadId")
        .and_then(JsonValue::as_str)
        .map(str::to_string);
    let label_ids = match body.get("labelIds") {
        Some(JsonValue::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str().map(str::to_string).ok_or_else(|| {
                    ConnectorRuntimeError::invalid_response(
                        "Gmail send response field `labelIds` must contain strings",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
        Some(_) => {
            return Err(ConnectorRuntimeError::invalid_response(
                "Gmail send response field `labelIds` must be an array",
            ));
        }
        None => Vec::new(),
    };

    Ok(GoogleGmailSendMessageOutput {
        id,
        thread_id,
        label_ids,
    })
}
