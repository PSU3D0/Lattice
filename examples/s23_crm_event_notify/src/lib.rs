//! S23 — clone of n8n shortlist template #4 (webhook: event-triggered
//! notifications on preferred messaging channels for a CRM).
//!
//! Behavioral spec (independent Rust implementation of an audited workflow's
//! behavior; no third-party workflow content is embedded here):
//!
//! 1. An **HTTP webhook** (`POST /crm-event`) receives a CRM change event with
//!    an `event_name` (e.g. `person.created`, `company.deleted`), the changed
//!    object's metadata (`id`, `name_singular`), and the affected record
//!    (`id`, `type_name`).
//! 2. A pure **filter/normalize** node keeps the mandatory fields — `event_name`
//!    is required — and derives the event `action` (the segment after the first
//!    `.`) and the routing `channel`: `delete` actions go to the **email**
//!    channel, everything else to the **message** channel.
//! 3. Every event is logged as one row in a fixed Google Sheet via
//!    `connector.google.sheets.append_row` (the always-on event log).
//! 4. A **switch** on the derived channel fans out: `delete` events email a
//!    record-deleted notice via `connector.google.gmail.send_message`; all other
//!    events post to Slack via `connector.slack.core.post_message`.
//! 5. Both branches converge on a terminal **KV upsert** keyed
//!    `<flow>:<trigger>:{object_id}` — the CRM event id is the webhook's natural
//!    idempotency key.
//!
//! ## Idempotency posture
//!
//! Every effectful payload is a pure function of the inbound event: the sheet
//! row, the email/Slack body, and the terminal KV key all derive from the
//! normalized fields and `object_id`. A webhook redelivery of the same event id
//! replays byte-identical requests and the terminal KV record dedupes to a
//! single row. Provider-side duplicate suppression for the sheet append and the
//! channel write remains the delivery gate's job (`Delivery::ExactlyOnce`
//! composition is proven per-op in each connector's honesty tests) — the same
//! posture as the s16 pilot and the s18 webhook clone.

use capabilities::context;
use connector_google_gmail::GoogleGmailSendMessageInput;
use connector_google_gmail::ops::GoogleGmailSendMessage;
use connector_google_sheets::GoogleSheetsAppendRowInput;
use connector_google_sheets::ops::GoogleSheetsAppendRow;
use connector_slack_core::SlackPostMessageInput;
use connector_slack_core::ops::SlackPostMessage;
use dag_core::{NodeError, NodeResult};
use dag_macros::{def_node, node};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub const FLOW_NAME: &str = "s23_crm_event_notify_flow";
pub const TRIGGER_ALIAS: &str = "crm_event_trigger";

/// The event action (segment after the first `.` in `event_name`) that routes
/// to the email channel; everything else routes to Slack.
pub const DELETE_ACTION: &str = "delete";
/// The routing channel discriminants used by the `switch!`.
pub const EMAIL_CHANNEL: &str = "email";
pub const MESSAGE_CHANNEL: &str = "message";

/// Fixed sink targets. Real deployments would source these from
/// config/bindings; constants keep the example self-contained.
pub const LOG_SPREADSHEET_ID: &str = "crm-events-log";
pub const LOG_SHEET: &str = "Events";
/// The Slack channel that receives non-delete event notifications.
pub const SLACK_CHANNEL: &str = "#crm-events";
/// The mailbox that receives record-deleted notices.
pub const NOTIFY_EMAIL: &str = "crm-alerts@lattice-pilot.test";

// ---------------------------------------------------------------------------
// Webhook payload (our own schema; models a CRM change event)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CrmEvent {
    pub event_name: String,
    pub object_metadata: ObjectMetadata,
    pub record: RecordRef,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ObjectMetadata {
    pub id: String,
    #[serde(default)]
    pub name_singular: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RecordRef {
    pub id: String,
    #[serde(default)]
    pub type_name: String,
}

/// The normalized event — a pure projection plus the channel routing decision.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NormalizedEvent {
    pub event_name: String,
    pub action: String,
    pub object_id: String,
    pub object_name: String,
    pub record_id: String,
    pub record_type: String,
    pub channel: String,
}

/// After the always-on sheet log: the channel (hoisted to the top level so the
/// `switch!` can select on it) plus the normalized event and where it landed.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LoggedEvent {
    pub channel: String,
    pub event: NormalizedEvent,
    pub logged_range: String,
}

/// The result of a channel delivery (email or Slack) — a common type so both
/// switch branches converge on the terminal record node.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EventNotification {
    pub channel: String,
    pub event_name: String,
    pub object_id: String,
    pub record_id: String,
    pub logged_range: String,
    /// The Gmail message id (email branch) or the Slack ts (message branch).
    pub delivery_ref: String,
}

/// Terminal record of the whole delivery.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EventDeliveryRecord {
    /// `<flow>:<trigger>:{object_id}`.
    pub key: String,
    /// `false` on a redelivery of an already-recorded event id.
    pub stored: bool,
    pub event_name: String,
    pub channel: String,
    pub object_id: String,
    pub record_id: String,
    pub logged_range: String,
    pub delivery_ref: String,
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

/// The event action: the segment after the first `.` in `event_name`
/// (`company.deleted` -> `deleted`). Empty when there is no `.` segment.
pub fn action_of(event_name: &str) -> &str {
    event_name.split('.').nth(1).unwrap_or("")
}

/// The routing channel for an action: `delete`* -> email, else message.
pub fn channel_for(action: &str) -> &'static str {
    if action.eq_ignore_ascii_case(DELETE_ACTION) {
        EMAIL_CHANNEL
    } else {
        MESSAGE_CHANNEL
    }
}

/// Project a CRM event into the normalized, channel-routed record.
pub fn normalize(event: &CrmEvent) -> NormalizedEvent {
    let action = action_of(&event.event_name).to_string();
    let channel = channel_for(&action).to_string();
    NormalizedEvent {
        event_name: event.event_name.clone(),
        action,
        object_id: event.object_metadata.id.clone(),
        object_name: event.object_metadata.name_singular.clone(),
        record_id: event.record.id.clone(),
        record_type: event.record.type_name.clone(),
        channel,
    }
}

/// The flat column map logged to the sheet — a pure function of the event.
pub fn event_columns(event: &NormalizedEvent) -> serde_json::Value {
    json!({
        "event_name": event.event_name,
        "action": event.action,
        "object_id": event.object_id,
        "object_name": event.object_name,
        "record_id": event.record_id,
        "record_type": event.record_type,
        "channel": event.channel,
    })
}

/// The email subject for a delete-channel notification.
pub fn email_subject(event: &NormalizedEvent) -> String {
    format!("Record deleted in CRM: {}", event.object_name)
}

/// The plain-text email body for a delete-channel notification.
pub fn email_body(event: &NormalizedEvent) -> String {
    format!(
        "A record was deleted in the CRM.\n\nevent: {}\nobject_id: {}\nrecord_id: {}\nrecord_type: {}",
        event.event_name, event.object_id, event.record_id, event.record_type
    )
}

/// The Slack message text for a non-delete-channel notification.
pub fn slack_text(event: &NormalizedEvent) -> String {
    format!(
        "event: {}\nevent_id: {}\nrecord_id: {}",
        event.event_name, event.object_id, event.record_id
    )
}

/// The idempotency key for one event delivery (`<flow>:<trigger>:{object_id}`).
pub fn delivery_key(object_id: &str) -> String {
    format!("{FLOW_NAME}:{TRIGGER_ALIAS}:{object_id}")
}

fn node_error(err: impl std::fmt::Display) -> NodeError {
    NodeError::new(err.to_string())
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

/// Webhook ingress: passes the typed CRM event through.
#[def_node(
    trigger,
    name = "CrmEventTrigger",
    summary = "HTTP webhook ingress; receives the CRM change event",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn crm_event_trigger(event: CrmEvent) -> NodeResult<CrmEvent> {
    Ok(event)
}

/// Filter + normalize: `event_name` is mandatory; derive the action + channel.
#[def_node(
    name = "NormalizeEvent",
    summary = "Require event_name and derive the event action and routing channel",
    effects = "Pure",
    determinism = "Strict"
)]
async fn normalize_event(event: CrmEvent) -> NodeResult<NormalizedEvent> {
    if event.event_name.trim().is_empty() {
        return Err(NodeError::new(
            "ignoring event: `event_name` is a mandatory field",
        ));
    }
    Ok(normalize(&event))
}

/// Append the event to the fixed Google Sheet (the always-on event log).
#[def_node(
    name = "LogEvent",
    identifier = "connector.google.sheets.log_event",
    summary = "Append the event row via connector.google.sheets.append_row",
    connector_ops(GoogleSheetsAppendRow)
)]
async fn log_event(event: NormalizedEvent) -> NodeResult<LoggedEvent> {
    let appended = GoogleSheetsAppendRow::invoke(&GoogleSheetsAppendRowInput {
        spreadsheet_id: LOG_SPREADSHEET_ID.to_string(),
        sheet: LOG_SHEET.to_string(),
        row: event_columns(&event),
        header_row: 1,
        value_input_option: None,
    })
    .await
    .map_err(|err| node_error(format!("connector.google.sheets.append_row failed: {err}")))?;

    Ok(LoggedEvent {
        channel: event.channel.clone(),
        event,
        logged_range: appended.updated_range,
    })
}

/// Email channel: notify by email that a record was deleted.
#[def_node(
    name = "NotifyEmail",
    identifier = "connector.google.gmail.notify_email",
    summary = "Send a record-deleted email via connector.google.gmail.send_message",
    connector_ops(GoogleGmailSendMessage)
)]
async fn notify_email(logged: LoggedEvent) -> NodeResult<EventNotification> {
    let event = &logged.event;
    let sent = GoogleGmailSendMessage::invoke(&GoogleGmailSendMessageInput {
        to: NOTIFY_EMAIL.to_string(),
        cc: None,
        bcc: None,
        subject: email_subject(event),
        text_body: email_body(event),
    })
    .await
    .map_err(|err| node_error(format!("connector.google.gmail.send_message failed: {err}")))?;

    Ok(EventNotification {
        channel: logged.channel,
        event_name: event.event_name.clone(),
        object_id: event.object_id.clone(),
        record_id: event.record_id.clone(),
        logged_range: logged.logged_range,
        delivery_ref: sent.id,
    })
}

/// Message channel: notify a Slack channel of a non-delete event.
#[def_node(
    name = "NotifySlack",
    identifier = "connector.slack.core.notify_slack",
    summary = "Post an event notification via connector.slack.core.post_message",
    connector_ops(SlackPostMessage)
)]
async fn notify_slack(logged: LoggedEvent) -> NodeResult<EventNotification> {
    let event = &logged.event;
    let posted = SlackPostMessage::invoke(&SlackPostMessageInput {
        channel: SLACK_CHANNEL.to_string(),
        text: slack_text(event),
        blocks: None,
    })
    .await
    .map_err(|err| node_error(format!("connector.slack.core.post_message failed: {err}")))?;

    Ok(EventNotification {
        channel: logged.channel,
        event_name: event.event_name.clone(),
        object_id: event.object_id.clone(),
        record_id: event.record_id.clone(),
        logged_range: logged.logged_range,
        delivery_ref: posted.ts,
    })
}

/// Terminal KV upsert keyed on the CRM event id: redeliveries of one event
/// collapse to a single record.
#[def_node(
    name = "RecordDelivery",
    summary = "Upsert the delivery record into KV, keyed on the CRM event id",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(
        kv_read(capabilities::kv::KeyValue),
        kv_write(capabilities::kv::KeyValue)
    )
)]
async fn record_delivery(notification: EventNotification) -> NodeResult<EventDeliveryRecord> {
    let key = delivery_key(&notification.object_id);
    let record = EventDeliveryRecord {
        key: key.clone(),
        stored: true,
        event_name: notification.event_name.clone(),
        channel: notification.channel.clone(),
        object_id: notification.object_id.clone(),
        record_id: notification.record_id.clone(),
        logged_range: notification.logged_range.clone(),
        delivery_ref: notification.delivery_ref.clone(),
    };

    let stored_now = context::with_current_async(|resources| {
        let key = key.clone();
        let record = record.clone();
        async move {
            let kv = resources.kv().ok_or_else(|| {
                NodeError::new(
                    "record_delivery requires a KV capability (declare resource::kv::write)",
                )
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
                .map_err(|err| node_error(format!("serialize delivery record: {err}")))?;
            kv.put(&key, &row, None)
                .await
                .map_err(|err| node_error(format!("kv put failed: {err}")))?;
            Ok(true)
        }
    })
    .await
    .ok_or_else(|| NodeError::new("record_delivery missing ResourceAccess context"))??;

    Ok(EventDeliveryRecord {
        stored: stored_now,
        ..record
    })
}

/// Terminal capture: the durable effects are the sheet row, the channel write,
/// and the KV record.
#[def_node(
    name = "Capture",
    summary = "Capture the event delivery record",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture(record: EventDeliveryRecord) -> NodeResult<EventDeliveryRecord> {
    Ok(record)
}

dag_macros::flow! {
    name: s23_crm_event_notify_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Clone of n8n template #4: a webhook CRM-event router that logs every event to a Google Sheet, then emails delete events and posts all others to Slack; keyed on the CRM event id";

    let crm_event_trigger = node!(crm_event_trigger);
    let normalize_event = node!(normalize_event);
    let log_event = node!(log_event);
    let notify_email = node!(notify_email);
    let notify_slack = node!(notify_slack);
    let record_delivery = node!(record_delivery);
    let capture = node!(capture);

    connect!(crm_event_trigger -> normalize_event);
    connect!(normalize_event -> log_event);
    connect!(log_event -> notify_email);
    connect!(log_event -> notify_slack);

    switch!(
        source = log_event,
        selector_pointer = "/channel",
        cases = { "email" => notify_email },
        default = notify_slack
    );

    connect!(notify_email -> record_delivery);
    connect!(notify_slack -> record_delivery);
    connect!(record_delivery -> capture);

    entrypoint!({
        trigger: "crm_event_trigger",
        capture: "capture",
        route_aliases: ["/crm-event"],
        method: "POST",
        deadline_ms: 15_000,
    });
}

#[cfg(test)]
mod tests;
