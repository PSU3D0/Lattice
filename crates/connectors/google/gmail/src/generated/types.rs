use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GoogleGmailSendMessageInput {
    pub to: String,
    pub cc: Option<String>,
    pub bcc: Option<String>,
    pub subject: String,
    pub text_body: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GoogleGmailSendMessageOutput {
    pub id: String,
    pub thread_id: Option<String>,
    pub label_ids: Vec<String>,
}
