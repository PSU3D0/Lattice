use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

use crate::generated::types::{GoogleGmailSendMessageInput, GoogleGmailSendMessageOutput};
use crate::ops::GoogleGmailSendMessage;

#[def_node(
    name = "GoogleGmailSendMessage",
    summary = "Send one plain-text email from the authenticated mailbox",
    identifier = "connector.google.gmail.send_message",
    connector_ops(crate::ops::GoogleGmailSendMessage)
)]
pub async fn google_gmail_send_message(
    input: GoogleGmailSendMessageInput,
) -> NodeResult<GoogleGmailSendMessageOutput> {
    GoogleGmailSendMessage::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.google.gmail.send_message failed: {err}")))
}
