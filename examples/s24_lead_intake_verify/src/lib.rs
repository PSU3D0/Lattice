//! S24 — clone of n8n shortlist template #8 (a webhook lead-intake form with
//! email verification).
//!
//! Behavioral spec (independent Rust implementation of an audited workflow's
//! behavior; no third-party workflow content is embedded here):
//!
//! 1. A **webhook (form) trigger** receives a lead submission
//!    (`{name, email, query, submitted_at}`).
//! 2. The submitted email is verified for deliverability via
//!    `connector.hunter.verify_email` (Hunter.io email-verifier, a ReadOnly
//!    GET whose API key is an `api_key` query parameter).
//! 3. A pure **guard** halts the flow when the email is not deliverable — the
//!    template's "don't move forward on a fake/invalid email" gate — so the
//!    downstream sinks only fire for a validated lead.
//! 4. A validated lead is fanned into three sinks: it upserts the lead row into
//!    a Google Sheet (`connector.google.sheets.upsert_row`, matched on email),
//!    sends a plain-text notification email (`connector.google.gmail.send_message`),
//!    and posts a rich embed to a Discord webhook
//!    (`connector.discord.send_message`).
//! 5. A terminal **KV upsert** records the delivery keyed
//!    `<flow>:<trigger>:{email}` — the webhook's natural idempotency key.
//!
//! ## Idempotency
//!
//! Every effectful payload is a pure function of the inbound submission: the
//! sheet upsert (matched on email), the email body, the Discord embed, and the
//! terminal KV key all derive from the lead's fields and its email. A
//! redelivery of the same lead replays byte-identical requests and the terminal
//! KV record dedupes to a single row. Provider-side duplicate suppression for
//! the three writes remains the delivery gate's job (`Delivery::ExactlyOnce`
//! composition is proven per-op in each connector's honesty tests) — the same
//! posture as the s18 clone.

use capabilities::context;
use connector_discord::ops::DiscordSendMessage;
use connector_discord::{DiscordEmbed, DiscordSendMessageInput};
use connector_google_gmail::GoogleGmailSendMessageInput;
use connector_google_gmail::ops::GoogleGmailSendMessage;
use connector_google_sheets::GoogleSheetsUpsertRowInput;
use connector_google_sheets::ops::GoogleSheetsUpsertRow;
use connector_hunter::HunterVerifyEmailInput;
use connector_hunter::ops::HunterVerifyEmail;
use dag_core::{NodeError, NodeResult};
use dag_macros::{def_node, node};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub const FLOW_NAME: &str = "s24_lead_intake_verify_flow";
pub const TRIGGER_ALIAS: &str = "lead_trigger";

/// The deliverability verdict a lead must earn to proceed past the guard. (Our
/// own constant — not template text.)
pub const DELIVERABLE: &str = "deliverable";

/// Fixed sink targets. Real deployments would source these from
/// config/bindings; constants keep the example self-contained.
pub const LEADS_SPREADSHEET_ID: &str = "lattice-leads-inbox";
pub const LEADS_SHEET: &str = "Leads";
/// The mailbox that receives the lead notification email.
pub const LEAD_NOTIFY_ADDRESS: &str = "sales@lattice-pilot.test";
/// The embed accent color (decimal RGB), used in the Discord post.
pub const DISCORD_EMBED_COLOR: i64 = 0x00_FF_F2;

// ---------------------------------------------------------------------------
// Flow data types
// ---------------------------------------------------------------------------

/// The inbound webhook form submission.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LeadSubmission {
    pub name: String,
    pub email: String,
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub submitted_at: String,
}

/// After Hunter verification: the submission plus its deliverability verdict.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct VerifiedLead {
    pub submission: LeadSubmission,
    pub status: String,
    pub result: String,
    pub score: i64,
}

/// After the sheet upsert.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SheetRecordedLead {
    pub submission: LeadSubmission,
    pub updated_range: String,
}

/// After the Gmail notification.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EmailedLead {
    pub submission: LeadSubmission,
    pub updated_range: String,
    pub gmail_message_id: String,
}

/// After the Discord announcement.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AnnouncedLead {
    pub submission: LeadSubmission,
    pub updated_range: String,
    pub gmail_message_id: String,
    pub discord_delivered: bool,
}

/// Terminal record of the whole delivery.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LeadRecord {
    /// `<flow>:<trigger>:{email}`.
    pub key: String,
    /// `false` on a redelivery of an already-recorded lead.
    pub stored: bool,
    pub email: String,
    pub updated_range: String,
    pub gmail_message_id: String,
    pub discord_delivered: bool,
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

/// The idempotency key for one lead delivery (spec shape
/// `<flow>:<trigger>:{email}`).
pub fn lead_key(email: &str) -> String {
    format!("{FLOW_NAME}:{TRIGGER_ALIAS}:{email}")
}

/// A lead earns the fan-out only when Hunter reports it deliverable.
pub fn is_deliverable(result: &str) -> bool {
    result.eq_ignore_ascii_case(DELIVERABLE)
}

/// The plain-text notification body — a pure function of the submission. Shared
/// by the Gmail email and the Discord embed description.
pub fn notification_body(submission: &LeadSubmission) -> String {
    format!(
        "Name: {}\n\nEmail: {}\n\nQuery: {}\n\nSubmitted on: {}",
        submission.name, submission.email, submission.query, submission.submitted_at
    )
}

/// The notification subject line.
pub fn notification_subject(submission: &LeadSubmission) -> String {
    format!("New lead from {}", submission.name)
}

fn node_error(err: impl std::fmt::Display) -> NodeError {
    NodeError::new(err.to_string())
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

/// Webhook ingress: passes the typed lead submission through.
#[def_node(
    trigger,
    name = "LeadTrigger",
    summary = "Webhook ingress; receives the typed lead submission",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn lead_trigger(submission: LeadSubmission) -> NodeResult<LeadSubmission> {
    Ok(submission)
}

/// Verify the submitted email's deliverability via Hunter.
#[def_node(
    name = "VerifyLead",
    identifier = "connector.hunter.verify_lead_email",
    summary = "Verify the lead email deliverability via connector.hunter.verify_email",
    connector_ops(HunterVerifyEmail)
)]
async fn verify_lead(submission: LeadSubmission) -> NodeResult<VerifiedLead> {
    let verdict = HunterVerifyEmail::invoke(&HunterVerifyEmailInput {
        email: submission.email.clone(),
    })
    .await
    .map_err(|err| node_error(format!("connector.hunter.verify_email failed: {err}")))?;

    Ok(VerifiedLead {
        submission,
        status: verdict.status,
        result: verdict.result,
        score: verdict.score,
    })
}

/// Guard: only a deliverable lead moves forward; a fake/invalid email halts the
/// flow before any sink fires.
#[def_node(
    name = "GuardDeliverable",
    summary = "Halt the flow unless the lead email verified as deliverable",
    effects = "Pure",
    determinism = "Strict"
)]
async fn guard_deliverable(verified: VerifiedLead) -> NodeResult<VerifiedLead> {
    if !is_deliverable(&verified.result) {
        return Err(NodeError::new(format!(
            "lead `{}` not forwarded: email verification result `{}` (status `{}`) is not `{DELIVERABLE}`",
            verified.submission.email, verified.result, verified.status
        )));
    }
    Ok(verified)
}

/// Upsert the lead row into the Google Sheet, matched on email.
#[def_node(
    name = "RecordLeadInSheet",
    identifier = "connector.google.sheets.record_lead",
    summary = "Upsert the lead row via connector.google.sheets.upsert_row",
    connector_ops(GoogleSheetsUpsertRow)
)]
async fn record_lead_in_sheet(verified: VerifiedLead) -> NodeResult<SheetRecordedLead> {
    let submission = verified.submission;
    let updated = GoogleSheetsUpsertRow::invoke(&GoogleSheetsUpsertRowInput {
        spreadsheet_id: LEADS_SPREADSHEET_ID.to_string(),
        sheet: LEADS_SHEET.to_string(),
        match_on: vec!["Email".to_string()],
        row: json!({
            "Name": submission.name,
            "Email": submission.email,
            "Query": submission.query,
            "Submitted On": submission.submitted_at,
        }),
        header_row: 1,
        value_input_option: None,
    })
    .await
    .map_err(|err| node_error(format!("connector.google.sheets.upsert_row failed: {err}")))?;

    Ok(SheetRecordedLead {
        submission,
        updated_range: updated.updated_range,
    })
}

/// Send the lead notification email.
#[def_node(
    name = "NotifyLeadByEmail",
    identifier = "connector.google.gmail.notify_lead",
    summary = "Email the lead notification via connector.google.gmail.send_message",
    connector_ops(GoogleGmailSendMessage)
)]
async fn notify_lead_by_email(recorded: SheetRecordedLead) -> NodeResult<EmailedLead> {
    let submission = recorded.submission;
    let sent = GoogleGmailSendMessage::invoke(&GoogleGmailSendMessageInput {
        to: LEAD_NOTIFY_ADDRESS.to_string(),
        cc: None,
        bcc: None,
        subject: notification_subject(&submission),
        text_body: notification_body(&submission),
    })
    .await
    .map_err(|err| node_error(format!("connector.google.gmail.send_message failed: {err}")))?;

    Ok(EmailedLead {
        submission,
        updated_range: recorded.updated_range,
        gmail_message_id: sent.id,
    })
}

/// Post a rich embed of the lead to the Discord webhook.
#[def_node(
    name = "AnnounceLeadOnDiscord",
    identifier = "connector.discord.announce_lead",
    summary = "Post the lead embed via connector.discord.send_message",
    connector_ops(DiscordSendMessage)
)]
async fn announce_lead_on_discord(emailed: EmailedLead) -> NodeResult<AnnouncedLead> {
    let submission = emailed.submission;
    let posted = DiscordSendMessage::invoke(&DiscordSendMessageInput {
        content: None,
        embeds: vec![DiscordEmbed {
            title: Some(format!("New Lead from {}", submission.name)),
            description: Some(notification_body(&submission)),
            color: Some(DISCORD_EMBED_COLOR),
            author_name: Some("Lattice Automation".to_string()),
        }],
    })
    .await
    .map_err(|err| node_error(format!("connector.discord.send_message failed: {err}")))?;

    Ok(AnnouncedLead {
        submission,
        updated_range: emailed.updated_range,
        gmail_message_id: emailed.gmail_message_id,
        discord_delivered: posted.delivered,
    })
}

/// Terminal KV upsert keyed on the lead email: redeliveries of one lead collapse
/// to a single record.
#[def_node(
    name = "RecordLead",
    summary = "Upsert the delivery record into KV, keyed on the lead email",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(
        kv_read(capabilities::kv::KeyValue),
        kv_write(capabilities::kv::KeyValue)
    )
)]
async fn record_lead(announced: AnnouncedLead) -> NodeResult<LeadRecord> {
    let key = lead_key(&announced.submission.email);
    let record = LeadRecord {
        key: key.clone(),
        stored: true,
        email: announced.submission.email.clone(),
        updated_range: announced.updated_range,
        gmail_message_id: announced.gmail_message_id,
        discord_delivered: announced.discord_delivered,
    };

    let stored_now = context::with_current_async(|resources| {
        let key = key.clone();
        let record = record.clone();
        async move {
            let kv = resources.kv().ok_or_else(|| {
                NodeError::new("record_lead requires a KV capability (declare resource::kv::write)")
            })?;

            if kv
                .get(&key)
                .await
                .map_err(|err| node_error(format!("kv get failed: {err}")))?
                .is_some()
            {
                return Ok::<bool, NodeError>(false);
            }

            let row = serde_json::to_vec(&record)
                .map_err(|err| node_error(format!("serialize lead record: {err}")))?;
            kv.put(&key, &row, None)
                .await
                .map_err(|err| node_error(format!("kv put failed: {err}")))?;
            Ok(true)
        }
    })
    .await
    .ok_or_else(|| NodeError::new("record_lead missing ResourceAccess context"))??;

    Ok(LeadRecord {
        stored: stored_now,
        ..record
    })
}

/// Terminal capture: the durable effects are the sheet row, the email, the
/// Discord post, and the KV record.
#[def_node(
    name = "Capture",
    summary = "Capture the lead delivery record",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture(record: LeadRecord) -> NodeResult<LeadRecord> {
    Ok(record)
}

dag_macros::flow! {
    name: s24_lead_intake_verify_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Clone of n8n template #8: a webhook lead-intake form — verify the email with Hunter, then fan a deliverable lead into Google Sheets, Gmail, and a Discord webhook; keyed on email";

    let lead_trigger = node!(lead_trigger);
    let verify_lead = node!(verify_lead);
    let guard_deliverable = node!(guard_deliverable);
    let record_lead_in_sheet = node!(record_lead_in_sheet);
    let notify_lead_by_email = node!(notify_lead_by_email);
    let announce_lead_on_discord = node!(announce_lead_on_discord);
    let record_lead = node!(record_lead);
    let capture = node!(capture);

    connect!(lead_trigger -> verify_lead);
    connect!(verify_lead -> guard_deliverable);
    connect!(guard_deliverable -> record_lead_in_sheet);
    connect!(record_lead_in_sheet -> notify_lead_by_email);
    connect!(notify_lead_by_email -> announce_lead_on_discord);
    connect!(announce_lead_on_discord -> record_lead);
    connect!(record_lead -> capture);

    entrypoint!({
        trigger: "lead_trigger",
        capture: "capture",
        route_aliases: ["/lead-intake"],
        method: "POST",
        deadline_ms: 15_000,
    });
}

#[cfg(test)]
mod tests;
