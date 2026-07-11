use serde::{Deserialize, Serialize};

/// One Discord rich embed. Every field is optional; the connector only serializes
/// the fields the caller actually set.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DiscordEmbed {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Integer color (Discord expects a decimal RGB integer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<i64>,
    /// Embed author name; serialized as `{ "author": { "name": ... } }`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DiscordSendMessageInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(default)]
    pub embeds: Vec<DiscordEmbed>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DiscordSendMessageOutput {
    /// `true` once Discord accepts the post (204 No Content, or 200 with body).
    pub delivered: bool,
    /// Present only when the webhook is executed with `wait=true`; the plain
    /// webhook post returns 204 with no body.
    pub message_id: Option<String>,
}
