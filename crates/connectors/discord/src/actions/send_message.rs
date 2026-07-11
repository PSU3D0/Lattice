use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

use crate::generated::types::{DiscordSendMessageInput, DiscordSendMessageOutput};
use crate::ops::DiscordSendMessage;

#[def_node(
    name = "DiscordSendMessage",
    summary = "Post one message (content plus rich embeds) to a Discord webhook",
    identifier = "connector.discord.send_message",
    connector_ops(crate::ops::DiscordSendMessage)
)]
pub async fn discord_send_message(
    input: DiscordSendMessageInput,
) -> NodeResult<DiscordSendMessageOutput> {
    DiscordSendMessage::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.discord.send_message failed: {err}")))
}
