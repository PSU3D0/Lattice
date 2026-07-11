//! S20 — clone of n8n shortlist template #10 (a webhook newsletter-signup form).
//!
//! Behavioral spec (independent Rust implementation of an audited workflow's
//! behavior; no third-party workflow content is embedded here):
//!
//! 1. A **webhook (form) trigger** receives a newsletter signup submission
//!    (email plus optional survey fields).
//! 2. The signup's contact fields are appended to a fixed Google Sheet via
//!    `connector.google.sheets.append_row` — the "capture the email first" step.
//! 3. A Slack channel is notified that a new signup arrived via
//!    `connector.slack.core.post_message` (the op this clone introduces).
//! 4. The same sheet row is enriched with the survey/profile fields via
//!    `connector.google.sheets.upsert_row` (matched on the signup email) — the
//!    template's follow-on "update" step.
//! 5. A pure capture returns a small record of what happened.
//!
//! ## Idempotency posture
//!
//! Unlike the scheduled s16 clone, a webhook fire carries no natural
//! monotonic key, so this flow has no terminal KV record. Every effectful
//! payload is a pure function of the submission (the append columns, the Slack
//! text, the upsert match key), so a webhook redelivery replays byte-identical
//! requests; provider-side duplicate suppression for the sheet/Slack writes is
//! the delivery gate's job (`Delivery::ExactlyOnce` composition is proven
//! per-op in each connector's honesty tests).

use connector_google_sheets::ops::{GoogleSheetsAppendRow, GoogleSheetsUpsertRow};
use connector_google_sheets::{GoogleSheetsAppendRowInput, GoogleSheetsUpsertRowInput};
use connector_slack_core::SlackPostMessageInput;
use connector_slack_core::ops::SlackPostMessage;
use dag_core::{NodeError, NodeResult};
use dag_macros::{def_node, node};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub const FLOW_NAME: &str = "s20_form_signup_notify_flow";
pub const TRIGGER_ALIAS: &str = "signup_trigger";

/// The fixed spreadsheet + tab collecting newsletter signups. Real deployments
/// would source these from config/bindings; constants keep the example
/// self-contained.
pub const SIGNUP_SPREADSHEET_ID: &str = "newsletter-signups";
pub const SIGNUP_SHEET: &str = "Sheet1";
/// The Slack channel that receives signup notifications.
pub const SLACK_CHANNEL: &str = "#newsletter";

// ---------------------------------------------------------------------------
// Flow data types
// ---------------------------------------------------------------------------

/// The inbound webhook form submission.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SignupSubmission {
    pub email: String,
    #[serde(default)]
    pub first_name: String,
    #[serde(default)]
    pub last_name: String,
    #[serde(default)]
    pub job_level: String,
    #[serde(default)]
    pub product_goals: String,
}

/// After the initial append: the signup plus where it landed.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CapturedSignup {
    pub submission: SignupSubmission,
    pub appended_range: String,
}

/// After the Slack notification.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NotifiedSignup {
    pub submission: SignupSubmission,
    pub appended_range: String,
    pub slack_channel: String,
    pub slack_ts: String,
}

/// Terminal record of the whole fire.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SignupRecord {
    pub email: String,
    pub appended_range: String,
    pub updated_range: String,
    pub slack_channel: String,
    pub slack_ts: String,
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

/// The Slack notification text — a pure function of the submission.
pub fn notification_text(submission: &SignupSubmission) -> String {
    format!("{} just signed up to the newsletter!", submission.email)
}

fn node_error(err: impl std::fmt::Display) -> NodeError {
    NodeError::new(err.to_string())
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

/// Webhook ingress: passes the typed submission through.
#[def_node(
    trigger,
    name = "SignupTrigger",
    summary = "Webhook ingress; receives the typed newsletter signup submission",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn signup_trigger(submission: SignupSubmission) -> NodeResult<SignupSubmission> {
    Ok(submission)
}

/// Append the signup's contact fields to the sheet (captures the email first).
#[def_node(
    name = "RecordSignup",
    identifier = "connector.google.sheets.record_signup",
    summary = "Append the signup contact fields via connector.google.sheets.append_row",
    connector_ops(GoogleSheetsAppendRow)
)]
async fn record_signup(submission: SignupSubmission) -> NodeResult<CapturedSignup> {
    let appended = GoogleSheetsAppendRow::invoke(&GoogleSheetsAppendRowInput {
        spreadsheet_id: SIGNUP_SPREADSHEET_ID.to_string(),
        sheet: SIGNUP_SHEET.to_string(),
        row: json!({
            "email": submission.email,
            "first_name": submission.first_name,
            "last_name": submission.last_name,
        }),
        header_row: 1,
        value_input_option: None,
    })
    .await
    .map_err(|err| node_error(format!("connector.google.sheets.append_row failed: {err}")))?;

    Ok(CapturedSignup {
        submission,
        appended_range: appended.updated_range,
    })
}

/// Notify the Slack channel that a new signup arrived.
#[def_node(
    name = "NotifySignup",
    identifier = "connector.slack.core.notify_signup",
    summary = "Post a signup notification via connector.slack.core.post_message",
    connector_ops(SlackPostMessage)
)]
async fn notify_signup(captured: CapturedSignup) -> NodeResult<NotifiedSignup> {
    let posted = SlackPostMessage::invoke(&SlackPostMessageInput {
        channel: SLACK_CHANNEL.to_string(),
        text: notification_text(&captured.submission),
        blocks: None,
    })
    .await
    .map_err(|err| node_error(format!("connector.slack.core.post_message failed: {err}")))?;

    Ok(NotifiedSignup {
        submission: captured.submission,
        appended_range: captured.appended_range,
        slack_channel: posted.channel,
        slack_ts: posted.ts,
    })
}

/// Enrich the same row with the survey/profile fields (matched on email).
#[def_node(
    name = "EnrichSignup",
    identifier = "connector.google.sheets.enrich_signup",
    summary = "Update the signup row with survey fields via connector.google.sheets.upsert_row",
    connector_ops(GoogleSheetsUpsertRow)
)]
async fn enrich_signup(notified: NotifiedSignup) -> NodeResult<SignupRecord> {
    let submission = &notified.submission;
    let updated = GoogleSheetsUpsertRow::invoke(&GoogleSheetsUpsertRowInput {
        spreadsheet_id: SIGNUP_SPREADSHEET_ID.to_string(),
        sheet: SIGNUP_SHEET.to_string(),
        match_on: vec!["email".to_string()],
        row: json!({
            "email": submission.email,
            "job_level": submission.job_level,
            "product_goals": submission.product_goals,
        }),
        header_row: 1,
        value_input_option: None,
    })
    .await
    .map_err(|err| node_error(format!("connector.google.sheets.upsert_row failed: {err}")))?;

    Ok(SignupRecord {
        email: notified.submission.email,
        appended_range: notified.appended_range,
        updated_range: updated.updated_range,
        slack_channel: notified.slack_channel,
        slack_ts: notified.slack_ts,
    })
}

/// Terminal capture: the durable effects are the sheet rows and the Slack post.
#[def_node(
    name = "Capture",
    summary = "Capture the signup record",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture(record: SignupRecord) -> NodeResult<SignupRecord> {
    Ok(record)
}

dag_macros::flow! {
    name: s20_form_signup_notify_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Clone of n8n template #10: a webhook newsletter-signup form that appends the signup to a Google Sheet, posts a Slack notification, and updates the row with survey details";

    let signup_trigger = node!(signup_trigger);
    let record_signup = node!(record_signup);
    let notify_signup = node!(notify_signup);
    let enrich_signup = node!(enrich_signup);
    let capture = node!(capture);

    connect!(signup_trigger -> record_signup);
    connect!(record_signup -> notify_signup);
    connect!(notify_signup -> enrich_signup);
    connect!(enrich_signup -> capture);

    entrypoint!({
        trigger: "signup_trigger",
        capture: "capture",
        route_aliases: ["/newsletter-signup"],
        method: "POST",
        deadline_ms: 5_000,
    });
}

#[cfg(test)]
mod tests;
