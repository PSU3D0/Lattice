//! Handwritten Slack Web API runtime: semantic `chat.postMessage` over the
//! shared connectors-std transport (endpoint + auth resolved from the current
//! connector context, exactly like `connector_google_gmail`'s `GmailApi`).
//!
//! Slack returns HTTP 200 even for logical failures, signalling them with an
//! `"ok": false` body plus an `"error"` code. This op therefore post-processes
//! the decoded response and maps `ok=false` to a structured error — the
//! semantic behaviour that keeps this a `handwritten_semantic` op rather than a
//! plain request-mapped descriptor.

use capabilities::http::{HttpMethod, HttpRequest};
use connectors_std::endpoint::{ResolvedEndpointProfile, apply_default_headers};
use connectors_std::{
    CurrentConnectorContext, apply_outbound_auth_with_context, current_connector_context,
    decode_json_response_body, resolve_endpoint_with_context, send_request_from_current,
};
use serde_json::{Map, Value as JsonValue};

use crate::generated::profiles::{SLACK_AUTH_OUTBOUND_AUTH, SLACK_DEFAULT_ENDPOINT_PROFILE};
use crate::generated::types::{SlackPostMessageInput, SlackPostMessageOutput};
use crate::runtime::errors::ConnectorRuntimeError;

const CHAT_POST_MESSAGE_PATH: &str = "/chat.postMessage";

pub struct SlackApi {
    action_id: &'static str,
    context: CurrentConnectorContext,
    endpoint: ResolvedEndpointProfile,
}

impl SlackApi {
    pub async fn for_action(action_id: &'static str) -> Result<Self, ConnectorRuntimeError> {
        let context = current_connector_context(action_id).await?;
        let endpoint =
            resolve_endpoint_with_context(&SLACK_DEFAULT_ENDPOINT_PROFILE, &context).await?;
        Ok(Self {
            action_id,
            context,
            endpoint,
        })
    }

    pub async fn post_message(
        &self,
        input: &SlackPostMessageInput,
    ) -> Result<SlackPostMessageOutput, ConnectorRuntimeError> {
        let mut body = Map::new();
        body.insert(
            "channel".to_string(),
            JsonValue::String(input.channel.clone()),
        );
        body.insert("text".to_string(), JsonValue::String(input.text.clone()));
        if let Some(blocks) = &input.blocks {
            body.insert("blocks".to_string(), blocks.clone());
        }

        let response = self
            .request_json(
                HttpMethod::Post,
                CHAT_POST_MESSAGE_PATH,
                Some(JsonValue::Object(body)),
            )
            .await?;

        post_message_output_from_response(&response)
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
        apply_outbound_auth_with_context(&SLACK_AUTH_OUTBOUND_AUTH, &mut request, &self.context)
            .await?;

        if let Some(body) = body {
            request
                .headers
                .insert("Content-Type", "application/json; charset=utf-8");
            request.body = Some(serde_json::to_vec(&body)?);
        }

        let response = send_request_from_current(self.action_id, method, request).await?;
        decode_json_response_body(&response)
    }
}

fn post_message_output_from_response(
    body: &JsonValue,
) -> Result<SlackPostMessageOutput, ConnectorRuntimeError> {
    let ok = body.get("ok").and_then(JsonValue::as_bool).unwrap_or(false);
    if !ok {
        let error = body
            .get("error")
            .and_then(JsonValue::as_str)
            .unwrap_or("unknown_error");
        return Err(ConnectorRuntimeError::invalid_response(format!(
            "Slack chat.postMessage returned ok=false: {error}"
        )));
    }

    let channel = body
        .get("channel")
        .and_then(JsonValue::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            ConnectorRuntimeError::invalid_response(
                "Slack chat.postMessage response did not contain string field `channel`",
            )
        })?;
    let ts = body
        .get("ts")
        .and_then(JsonValue::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            ConnectorRuntimeError::invalid_response(
                "Slack chat.postMessage response did not contain string field `ts`",
            )
        })?;

    Ok(SlackPostMessageOutput { channel, ts })
}
