use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HunterVerifyEmailInput {
    pub email: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HunterVerifyEmailOutput {
    /// The address that was verified (echoed from the response).
    pub email: String,
    /// Verification status (e.g. `valid`, `invalid`, `accept_all`, `webmail`,
    /// `disposable`, `unknown`).
    pub status: String,
    /// Deliverability verdict (`deliverable`, `undeliverable`, `risky`, `unknown`).
    pub result: String,
    /// Confidence score, 0-100.
    pub score: i64,
}
