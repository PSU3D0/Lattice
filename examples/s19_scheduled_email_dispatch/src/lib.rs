//! S19 — clone (packet N4-T9): scheduled outbound email dispatch from a
//! spreadsheet message queue.
//!
//! Behavioral spec (independent Rust implementation of an audited workflow's
//! *behavior*; no third-party workflow text, parameter strings, or prose are
//! embedded here):
//!
//! 1. A **cron trigger** fires daily at 08:00 UTC.
//! 2. `connector.google.sheets.find_rows` reads a fixed "message queue"
//!    spreadsheet for rows whose `status` column is `queued`.
//! 3. A pure node keeps the rows that are *due* — every required field
//!    (`id`, `email`, `subject`, `body`) is present and the row's `send_date`
//!    has arrived (`send_date <= ` the fire's date, derived from
//!    `scheduled_time_ms`, never wall clock).
//! 4. For each due row, a plain-text email is sent via
//!    `connector.google.gmail.send_message` (the Gmail node's default `send`
//!    operation — this flow reads nothing from Gmail).
//! 5. Each sent row is marked `sent` back in the sheet via
//!    `connector.google.sheets.upsert_row`, matched on the `id` column.
//! 6. A terminal KV upsert records the fire, keyed
//!    `<flow>:<trigger>:{scheduled_time_ms}` (the s15/s16 idempotency pattern).
//!
//! ## Idempotency
//!
//! The queue filter and the due-date cutoff are pure functions of
//! `scheduled_time_ms`; each per-row email/upsert payload is a pure function of
//! the sheet contents. A redelivery of the same fire therefore replays
//! byte-identical requests, and the terminal KV record dedupes to one row.
//! Against a live sheet, the `queued -> sent` status flip additionally means a
//! genuine redelivery re-reads the queue and finds nothing still `queued` and
//! due, naturally suppressing re-sends; provider-side dedupe of the email sends
//! and status writes otherwise remains the delivery gate's job (same documented
//! posture as s15/s16). The static mock in the tests replays the same table, so
//! the compose test asserts dedupe at the terminal KV record.

use capabilities::context;
use connector_google_gmail::GoogleGmailSendMessageInput;
use connector_google_gmail::ops::GoogleGmailSendMessage;
use connector_google_sheets::ops::{GoogleSheetsFindRows, GoogleSheetsUpsertRow};
use connector_google_sheets::{
    GoogleSheetsFindRowsInput, GoogleSheetsRowMatch, GoogleSheetsUpsertRowInput,
};
use dag_core::{NodeError, NodeResult, ScheduledEvent};
use dag_macros::{def_node, node};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

/// Cadence: daily at 08:00 UTC (Cloudflare cron dialect).
pub const SCHEDULE_CRON: &str = "0 8 * * *";
pub const TRIGGER_ALIAS: &str = "dispatch_trigger";
pub const FLOW_NAME: &str = "s19_scheduled_email_dispatch_flow";

/// The fixed spreadsheet holding the outbound message queue. Real deployments
/// would source this from config/bindings; a constant keeps the example
/// self-contained.
pub const QUEUE_SPREADSHEET_ID: &str = "message-queue-log";
/// Tab within that spreadsheet.
pub const QUEUE_SHEET: &str = "Queue";
/// Status of a row awaiting dispatch.
pub const STATUS_QUEUED: &str = "queued";
/// Status written back once a row has been dispatched.
pub const STATUS_SENT: &str = "sent";
/// Bound on how many queued rows one fire will consider.
const MAX_QUEUE_ROWS: u32 = 500;

// ---------------------------------------------------------------------------
// Flow data types
// ---------------------------------------------------------------------------

/// Everything downstream nodes need, derived once from the fire.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DispatchPlan {
    pub scheduled_time_ms: u64,
    pub cron: String,
    /// `YYYY-MM-DD`, derived from `scheduled_time_ms`. Rows are due when their
    /// `send_date` is at or before this date.
    pub date: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct QueueScan {
    pub plan: DispatchPlan,
    pub rows: Vec<GoogleSheetsRowMatch>,
}

/// A queued row that has passed the due-date + required-field checks.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DueMessage {
    pub id: String,
    pub email: String,
    pub name: String,
    pub subject: String,
    pub body: String,
    pub send_date: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DueBatch {
    pub plan: DispatchPlan,
    pub due: Vec<DueMessage>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SentMessage {
    pub id: String,
    pub message_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SentBatch {
    pub plan: DispatchPlan,
    pub sent: Vec<SentMessage>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MarkedBatch {
    pub plan: DispatchPlan,
    pub message_ids: Vec<String>,
    pub sent_count: u32,
    pub marked_count: u32,
}

/// Terminal capture: what this fire did.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DispatchRecord {
    /// `<flow>:<trigger>:{scheduled_time_ms}`.
    pub key: String,
    /// `false` on a redelivery of an already-recorded fire.
    pub stored: bool,
    pub scheduled_time_ms: u64,
    pub date: String,
    pub sent_count: u32,
    pub marked_count: u32,
    pub message_ids: Vec<String>,
}

// ---------------------------------------------------------------------------
// Pure helpers (all deterministic in scheduled_time_ms / sheet contents)
// ---------------------------------------------------------------------------

/// The idempotency key for one scheduled fire (spec shape
/// `<flow>:<trigger>:{scheduled_time_ms}`).
pub fn dispatch_key(scheduled_time_ms: u64) -> String {
    format!("{FLOW_NAME}:{TRIGGER_ALIAS}:{scheduled_time_ms}")
}

/// Civil date from days since the Unix epoch (Howard Hinnant's algorithm).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

/// `YYYY-MM-DD` for an epoch-milliseconds timestamp (UTC).
pub fn utc_date(epoch_ms: u64) -> String {
    let (year, month, day) = civil_from_days((epoch_ms / 86_400_000) as i64);
    format!("{year:04}-{month:02}-{day:02}")
}

/// Read a string cell from a sheet row's `header -> value` object. Missing or
/// non-string cells read as empty.
fn cell(values: &JsonValue, column: &str) -> String {
    values
        .get(column)
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .trim()
        .to_string()
}

/// Project a queued sheet row into a `DueMessage` when it is due for `date` and
/// carries every field a send needs; otherwise `None`.
///
/// `date` strings are ISO `YYYY-MM-DD`, which sort lexicographically, so a
/// simple string comparison is a correct calendar comparison.
pub fn due_message(row: &GoogleSheetsRowMatch, date: &str) -> Option<DueMessage> {
    let id = cell(&row.values, "id");
    let email = cell(&row.values, "email");
    let subject = cell(&row.values, "subject");
    let body = cell(&row.values, "body");
    let send_date = cell(&row.values, "send_date");

    let required_present =
        !id.is_empty() && !email.is_empty() && !subject.is_empty() && !body.is_empty();
    let has_arrived = !send_date.is_empty() && send_date.as_str() <= date;
    if !required_present || !has_arrived {
        return None;
    }

    Some(DueMessage {
        id,
        email,
        name: cell(&row.values, "name"),
        subject,
        body,
        send_date,
    })
}

/// The plain-text email body for a due message (a pure function of the row).
pub fn message_text(message: &DueMessage) -> String {
    let greeting = if message.name.is_empty() {
        "Hello,".to_string()
    } else {
        format!("Hi {},", message.name)
    };
    format!("{greeting}\n\n{}\n", message.body)
}

fn node_error(err: impl std::fmt::Display) -> NodeError {
    NodeError::new(err.to_string())
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

/// Schedule trigger: passes the typed fire through.
#[def_node(
    trigger,
    name = "DispatchTrigger",
    summary = "Cron ingress; receives the typed ScheduledEvent for this fire",
    effects = "Pure",
    determinism = "Strict"
)]
async fn dispatch_trigger(event: ScheduledEvent) -> NodeResult<ScheduledEvent> {
    Ok(event)
}

/// Derive the dispatch plan (the due-date cutoff) from the fire.
#[def_node(
    name = "PlanDispatch",
    summary = "Derive the due-date cutoff from the fire's scheduled time",
    effects = "Pure",
    determinism = "Strict"
)]
async fn plan_dispatch(event: ScheduledEvent) -> NodeResult<DispatchPlan> {
    Ok(DispatchPlan {
        date: utc_date(event.scheduled_time_ms),
        scheduled_time_ms: event.scheduled_time_ms,
        cron: event.cron,
    })
}

/// Read the queued rows from the message-queue spreadsheet.
#[def_node(
    name = "LoadQueue",
    identifier = "connector.google.sheets.load_message_queue",
    summary = "Read status=queued rows via connector.google.sheets.find_rows",
    connector_ops(GoogleSheetsFindRows)
)]
async fn load_queue(plan: DispatchPlan) -> NodeResult<QueueScan> {
    let found = GoogleSheetsFindRows::invoke(&GoogleSheetsFindRowsInput {
        spreadsheet_id: QUEUE_SPREADSHEET_ID.to_string(),
        sheet: QUEUE_SHEET.to_string(),
        filters: json!({ "status": STATUS_QUEUED }),
        limit: Some(MAX_QUEUE_ROWS),
        header_row: 1,
    })
    .await
    .map_err(|err| node_error(format!("connector.google.sheets.find_rows failed: {err}")))?;

    Ok(QueueScan {
        plan,
        rows: found.items,
    })
}

/// Pure due-date + completeness filter.
#[def_node(
    name = "SelectDue",
    summary = "Keep queued rows that are complete and whose send_date has arrived",
    effects = "Pure",
    determinism = "Strict"
)]
async fn select_due(scan: QueueScan) -> NodeResult<DueBatch> {
    let due: Vec<DueMessage> = scan
        .rows
        .iter()
        .filter_map(|row| due_message(row, &scan.plan.date))
        .collect();
    Ok(DueBatch {
        plan: scan.plan,
        due,
    })
}

/// Send one plain-text email per due message.
#[def_node(
    name = "DispatchMessages",
    identifier = "connector.google.gmail.dispatch_messages",
    summary = "Send each due email via connector.google.gmail.send_message",
    connector_ops(GoogleGmailSendMessage)
)]
async fn dispatch_messages(batch: DueBatch) -> NodeResult<SentBatch> {
    let mut sent = Vec::with_capacity(batch.due.len());
    for message in &batch.due {
        let result = GoogleGmailSendMessage::invoke(&GoogleGmailSendMessageInput {
            to: message.email.clone(),
            cc: None,
            bcc: None,
            subject: message.subject.clone(),
            text_body: message_text(message),
        })
        .await
        .map_err(|err| node_error(format!("connector.google.gmail.send_message failed: {err}")))?;
        sent.push(SentMessage {
            id: message.id.clone(),
            message_id: result.id,
        });
    }

    Ok(SentBatch {
        plan: batch.plan,
        sent,
    })
}

/// Mark each dispatched row `sent` back in the queue sheet, matched on `id`.
#[def_node(
    name = "MarkMessagesSent",
    identifier = "connector.google.sheets.mark_messages_sent",
    summary = "Flip each sent row's status via connector.google.sheets.upsert_row",
    connector_ops(GoogleSheetsUpsertRow)
)]
async fn mark_messages_sent(batch: SentBatch) -> NodeResult<MarkedBatch> {
    let mut marked_count = 0u32;
    let mut message_ids = Vec::with_capacity(batch.sent.len());
    for message in &batch.sent {
        GoogleSheetsUpsertRow::invoke(&GoogleSheetsUpsertRowInput {
            spreadsheet_id: QUEUE_SPREADSHEET_ID.to_string(),
            sheet: QUEUE_SHEET.to_string(),
            match_on: vec!["id".to_string()],
            row: json!({ "id": message.id, "status": STATUS_SENT }),
            header_row: 1,
            value_input_option: None,
        })
        .await
        .map_err(|err| node_error(format!("connector.google.sheets.upsert_row failed: {err}")))?;
        marked_count += 1;
        message_ids.push(message.message_id.clone());
    }

    Ok(MarkedBatch {
        plan: batch.plan,
        sent_count: batch.sent.len() as u32,
        marked_count,
        message_ids,
    })
}

/// Terminal KV upsert keyed on the fire's scheduled time (s15/s16 pattern):
/// redeliveries of one fire collapse to a single record.
#[def_node(
    name = "RecordDispatch",
    summary = "Upsert the fire's dispatch record into KV, keyed on scheduled_time_ms",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(
        kv_read(capabilities::kv::KeyValue),
        kv_write(capabilities::kv::KeyValue)
    )
)]
async fn record_dispatch(batch: MarkedBatch) -> NodeResult<DispatchRecord> {
    let key = dispatch_key(batch.plan.scheduled_time_ms);
    let record = DispatchRecord {
        key: key.clone(),
        stored: true,
        scheduled_time_ms: batch.plan.scheduled_time_ms,
        date: batch.plan.date.clone(),
        sent_count: batch.sent_count,
        marked_count: batch.marked_count,
        message_ids: batch.message_ids.clone(),
    };

    let stored = context::with_current_async(|resources| {
        let key = key.clone();
        let record = record.clone();
        async move {
            let kv = resources.kv().ok_or_else(|| {
                NodeError::new(
                    "record_dispatch requires a KV capability (declare resource::kv::write)",
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
                .map_err(|err| node_error(format!("serialize dispatch record: {err}")))?;
            kv.put(&key, &row, None)
                .await
                .map_err(|err| node_error(format!("kv put failed: {err}")))?;
            Ok(true)
        }
    })
    .await
    .ok_or_else(|| NodeError::new("record_dispatch missing ResourceAccess context"))??;

    Ok(DispatchRecord { stored, ..record })
}

/// Terminal capture: logged by the scheduler; the durable effects are the sent
/// emails, the flipped statuses, and the KV record.
#[def_node(
    name = "Capture",
    summary = "Capture the fire's dispatch record",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture(record: DispatchRecord) -> NodeResult<DispatchRecord> {
    Ok(record)
}

dag_macros::flow! {
    name: s19_scheduled_email_dispatch_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Clone: daily 08:00 UTC dispatch of queued spreadsheet emails — read status=queued rows, send those whose send_date has arrived, mark them sent; payloads keyed on scheduled_time_ms";

    let dispatch_trigger = node!(dispatch_trigger);
    let plan_dispatch = node!(plan_dispatch);
    let load_queue = node!(load_queue);
    let select_due = node!(select_due);
    let dispatch_messages = node!(dispatch_messages);
    let mark_messages_sent = node!(mark_messages_sent);
    let record_dispatch = node!(record_dispatch);
    let capture = node!(capture);

    connect!(dispatch_trigger -> plan_dispatch);
    connect!(plan_dispatch -> load_queue);
    connect!(load_queue -> select_due);
    connect!(select_due -> dispatch_messages);
    connect!(dispatch_messages -> mark_messages_sent);
    connect!(mark_messages_sent -> record_dispatch);
    connect!(record_dispatch -> capture);

    entrypoint!({
        trigger: "dispatch_trigger",
        capture: "capture",
        schedule: "0 8 * * *",
        deadline_ms: 60_000,
    });
}

#[cfg(test)]
mod tests;
