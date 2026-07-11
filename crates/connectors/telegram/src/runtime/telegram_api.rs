//! Handwritten Telegram Bot API runtime: semantic `sendMessage` over the shared
//! connectors-std transport (endpoint + auth resolved from the current
//! connector context, exactly like `connector_google_gmail`'s `GmailApi`).
//!
//! ## Token-in-path auth
//!
//! The Telegram Bot API embeds the bot token in the request PATH
//! (`/bot<token>/<method>`), not in an `Authorization` header. The connector
//! auth framework only mutates a request (it never hands back the raw secret),
//! so we resolve the bearer secret through the standard
//! `apply_outbound_auth` path against a throwaway probe request, read the
//! `Authorization: Bearer <token>` it wrote, and splice `<token>` into the URL
//! path. This keeps the whole secret-resolution story honest: a missing/denied
//! binding still fails with the framework's role+env-named error, and the token
//! never travels in a header the API would ignore.

use capabilities::http::{HttpMethod, HttpRequest};
use connectors_std::endpoint::{ResolvedEndpointProfile, apply_default_headers};
use connectors_std::{
    CurrentConnectorContext, apply_outbound_auth_with_context, current_connector_context,
    decode_json_response_body, resolve_endpoint_with_context, send_request_from_current,
};
use serde_json::{Value as JsonValue, json};

use crate::generated::profiles::{
    TELEGRAM_BOT_AUTH_OUTBOUND_AUTH, TELEGRAM_BOT_DEFAULT_ENDPOINT_PROFILE,
};
use crate::generated::types::{TelegramSendMessageInput, TelegramSendMessageOutput};
use crate::runtime::errors::ConnectorRuntimeError;

pub struct TelegramApi {
    action_id: &'static str,
    context: CurrentConnectorContext,
    endpoint: ResolvedEndpointProfile,
}

impl TelegramApi {
    pub async fn for_action(action_id: &'static str) -> Result<Self, ConnectorRuntimeError> {
        let context = current_connector_context(action_id).await?;
        let endpoint =
            resolve_endpoint_with_context(&TELEGRAM_BOT_DEFAULT_ENDPOINT_PROFILE, &context).await?;
        Ok(Self {
            action_id,
            context,
            endpoint,
        })
    }

    pub async fn send_message(
        &self,
        input: &TelegramSendMessageInput,
    ) -> Result<TelegramSendMessageOutput, ConnectorRuntimeError> {
        let token = self.resolve_bot_token().await?;
        let body = json!({ "chat_id": input.chat_id, "text": input.text });

        let response = self
            .request_json(&telegram_method_path(&token, "sendMessage"), body)
            .await?;

        send_message_output_from_response(&response)
    }

    /// Resolve the bot token by applying the standard outbound-auth flow to a
    /// throwaway probe request and reading the bearer value it writes.
    async fn resolve_bot_token(&self) -> Result<String, ConnectorRuntimeError> {
        let mut probe = HttpRequest::new(HttpMethod::Post, "telegram://bot-token-probe");
        apply_outbound_auth_with_context(
            &TELEGRAM_BOT_AUTH_OUTBOUND_AUTH,
            &mut probe,
            &self.context,
        )
        .await?;

        probe
            .headers
            .get("Authorization")
            .and_then(|value| value.strip_prefix("Bearer "))
            .filter(|token| !token.is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                ConnectorRuntimeError::invalid_response(
                    "telegram bot token unavailable after outbound auth resolution",
                )
            })
    }

    async fn request_json(
        &self,
        path: &str,
        body: JsonValue,
    ) -> Result<JsonValue, ConnectorRuntimeError> {
        let url = format!("{}{}", self.endpoint.base_url.trim_end_matches('/'), path);

        let mut request = HttpRequest::new(HttpMethod::Post, url);
        apply_default_headers(&mut request.headers, &self.endpoint);
        request.headers.insert("Content-Type", "application/json");
        request.body = Some(serde_json::to_vec(&body)?);

        let response = send_request_from_current(self.action_id, HttpMethod::Post, request).await?;
        decode_json_response_body(&response)
    }
}

/// `/bot<token>/<method>` — the Telegram Bot API path shape.
fn telegram_method_path(token: &str, method: &str) -> String {
    format!("/bot{token}/{method}")
}

fn send_message_output_from_response(
    body: &JsonValue,
) -> Result<TelegramSendMessageOutput, ConnectorRuntimeError> {
    if body.get("ok").and_then(JsonValue::as_bool) != Some(true) {
        return Err(ConnectorRuntimeError::invalid_response(
            "Telegram sendMessage response did not report `ok: true`",
        ));
    }
    let result = body.get("result").ok_or_else(|| {
        ConnectorRuntimeError::invalid_response(
            "Telegram sendMessage response did not contain `result`",
        )
    })?;

    let message_id = result
        .get("message_id")
        .and_then(JsonValue::as_i64)
        .ok_or_else(|| {
            ConnectorRuntimeError::invalid_response(
                "Telegram sendMessage `result.message_id` must be an integer",
            )
        })?;
    let date = result
        .get("date")
        .and_then(JsonValue::as_i64)
        .ok_or_else(|| {
            ConnectorRuntimeError::invalid_response(
                "Telegram sendMessage `result.date` must be an integer",
            )
        })?;
    let text = result
        .get("text")
        .and_then(JsonValue::as_str)
        .map(str::to_string);

    Ok(TelegramSendMessageOutput {
        message_id,
        date,
        text,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn method_path_embeds_token() {
        assert_eq!(
            telegram_method_path("123456:ABCDEF", "sendMessage"),
            "/bot123456:ABCDEF/sendMessage"
        );
    }

    #[test]
    fn decodes_ok_envelope() {
        let body = json!({
            "ok": true,
            "result": { "message_id": 42, "date": 1_700_000_000_i64, "text": "hi" }
        });
        let out = send_message_output_from_response(&body).expect("decode");
        assert_eq!(out.message_id, 42);
        assert_eq!(out.date, 1_700_000_000);
        assert_eq!(out.text.as_deref(), Some("hi"));
    }

    #[test]
    fn rejects_not_ok_envelope() {
        let body = json!({ "ok": false, "description": "chat not found" });
        let err = send_message_output_from_response(&body).expect_err("must reject");
        assert!(matches!(err, ConnectorRuntimeError::InvalidResponse(_)));
    }

    #[test]
    fn missing_message_id_is_invalid() {
        let body = json!({ "ok": true, "result": { "date": 1 } });
        assert!(send_message_output_from_response(&body).is_err());
    }
}
