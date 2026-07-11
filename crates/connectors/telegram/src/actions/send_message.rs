use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

use crate::generated::types::{TelegramSendMessageInput, TelegramSendMessageOutput};
use crate::ops::TelegramSendMessage;

#[def_node(
    name = "TelegramSendMessage",
    summary = "Send one text message to a chat as the authenticated bot",
    identifier = "connector.telegram.send_message",
    connector_ops(crate::ops::TelegramSendMessage)
)]
pub async fn telegram_send_message(
    input: TelegramSendMessageInput,
) -> NodeResult<TelegramSendMessageOutput> {
    TelegramSendMessage::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.telegram.send_message failed: {err}")))
}
