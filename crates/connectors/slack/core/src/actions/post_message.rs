use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

use crate::generated::types::{SlackPostMessageInput, SlackPostMessageOutput};
use crate::ops::SlackPostMessage;

#[def_node(
    name = "SlackPostMessage",
    summary = "Post a message to a Slack channel",
    identifier = "connector.slack.core.post_message",
    connector_ops(crate::ops::SlackPostMessage)
)]
pub async fn slack_post_message(
    input: SlackPostMessageInput,
) -> NodeResult<SlackPostMessageOutput> {
    SlackPostMessage::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.slack.core.post_message failed: {err}")))
}
