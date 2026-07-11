use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TelegramSendMessageInput {
    pub chat_id: String,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TelegramSendMessageOutput {
    pub message_id: i64,
    pub date: i64,
    pub text: Option<String>,
}
