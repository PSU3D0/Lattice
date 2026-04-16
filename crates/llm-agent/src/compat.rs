use crate::completion::request::PromptError;
use crate::message::{Message, Text, UserContent};

pub trait MessageRagTextExt {
    fn rag_text(&self) -> Option<String>;
}

impl MessageRagTextExt for Message {
    fn rag_text(&self) -> Option<String> {
        match self {
            Message::User { content } => {
                for item in content.iter() {
                    if let UserContent::Text(Text { text }) = item {
                        return Some(text.clone());
                    }
                }
                None
            }
            Message::System { .. } => None,
            _ => None,
        }
    }
}

pub trait PromptErrorExt {
    fn prompt_cancelled(
        chat_history: impl IntoIterator<Item = Message>,
        reason: impl Into<String>,
    ) -> Self;
}

impl PromptErrorExt for PromptError {
    fn prompt_cancelled(
        chat_history: impl IntoIterator<Item = Message>,
        reason: impl Into<String>,
    ) -> Self {
        Self::PromptCancelled {
            chat_history: chat_history.into_iter().collect(),
            reason: reason.into(),
        }
    }
}
