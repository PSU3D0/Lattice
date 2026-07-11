//! S22 — clone of n8n shortlist template #3 (a webhook-triggered hydration
//! reminder over Google Sheets + an LLM + Slack).
//!
//! Behavioral spec (independent Rust implementation of an audited workflow's
//! behavior; no third-party workflow content is embedded here):
//!
//! 1. A **webhook** (`POST /hydration-reminder`) receives a reminder request:
//!    who to remind, the civil date whose intake log to read, the daily
//!    hydration target, and a stable `reminder_id` (the delivery's idempotency
//!    key, supplied by the caller/scheduler — the webhook analogue of s16's
//!    `scheduled_time_ms`).
//! 2. Today's intake rows are read from a fixed **Google Sheets** log via
//!    `connector.google.sheets.find_rows`, filtered on the request date.
//! 3. A pure **summarize/progress** node folds the rows into hydration state:
//!    total millilitres consumed, drink count, a progress fraction against the
//!    target, a `💧`/`⬜` progress bar, and the most recent drink time.
//! 4. An **LLM** (`connector.llm.complete`, `openai_compat` dialect, native
//!    structured output `{message}`) writes a short, personalised reminder from
//!    that state.
//! 5. The reminder is posted to a **Slack** channel via
//!    `connector.slack.core.post_message` (text + a small Block Kit payload
//!    carrying the message and the progress bar).
//! 6. A terminal **KV upsert** records the delivery keyed
//!    `<flow>:<trigger>:{reminder_id}` — redeliveries of one reminder collapse
//!    to a single record.
//!
//! ## What this clone deliberately does NOT reproduce
//!
//! The source workflow bundles two independent trigger paths in one export; a
//! Lattice flow has a single entrypoint, so this clone takes the largest honest
//! subset — the reminder-generation path (sheets read → summarize → LLM → Slack)
//! that exercises all three services template #3 demands. Cut, with the exact
//! blocking primitive named in the packet report:
//! - the **conditional durable delay** ("if the user drank in the last 30 min,
//!   suspend this execution for a random 21–31 min, then re-check and post"):
//!   Lattice's webhook run-once path cannot suspend a *single triggered
//!   execution* mid-flow for a wall-clock delay and resume it against
//!   re-fetched state. This needs a durable in-flow timer/suspend-resume
//!   primitive the flow surface does not expose today (the stdlib
//!   `std.timer.wait`/`std.callback.wait` are fire-and-forget/callback nodes,
//!   not an inline resumable delay). The reminder is posted unconditionally.
//! - the **Slack interaction (button-click) handler** — a *second* webhook that
//!   appends the logged drink value and posts a threaded confirmation. That is a
//!   separate single-entrypoint flow (sheets `append_row` + a Slack post) and is
//!   not built here.
//! - the **randomised cron schedule** that fires the reminder: cadence is the
//!   caller/scheduler's concern; this clone is webhook-triggered per the packet.
//! - the source's `setting`-sheet read for the target and its `Limit`/`Merge`
//!   data-shaping nodes: folded into the request payload / the pure summarize
//!   node respectively.
//!
//! ## Idempotency posture
//!
//! Every effectful payload is a pure function of the inbound request and the
//! read log (the Slack text/blocks derive from the summarized state; the
//! terminal KV key derives from `reminder_id`). A redelivery replays the read
//! and — because the LLM is Nondeterministic and Slack is Effectful — re-invokes
//! the model and the post; the terminal KV record dedupes to a single row.
//! Provider-side duplicate suppression for the LLM/Slack writes remains the
//! delivery gate's job (`Delivery::ExactlyOnce` composition is proven per-op in
//! each connector's honesty tests) — the same posture as the s16/s18 clones.

use capabilities::context;
use connector_google_sheets::ops::GoogleSheetsFindRows;
use connector_google_sheets::{GoogleSheetsFindRowsInput, GoogleSheetsRowMatch};
use connector_llm::ops::LlmComplete;
use connector_llm::{LlmCompleteInput, LlmProvider};
use connector_slack_core::SlackPostMessageInput;
use connector_slack_core::ops::SlackPostMessage;
use dag_core::{NodeError, NodeResult};
use dag_macros::{def_node, node};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const FLOW_NAME: &str = "s22_hydration_reminder_flow";
pub const TRIGGER_ALIAS: &str = "reminder_trigger";

/// The fixed spreadsheet + tab holding the intake log. Real deployments would
/// source these from config/bindings; constants keep the example self-contained.
pub const LOG_SPREADSHEET_ID: &str = "hydration-log";
pub const LOG_SHEET: &str = "log";
/// The Slack channel that receives hydration reminders.
pub const SLACK_CHANNEL: &str = "#hydration";
/// Default model when the request omits one.
pub const DEFAULT_MODEL: &str = "gpt-4o-mini";
/// Width (in cells) of the `💧`/`⬜` progress bar.
pub const PROGRESS_CELLS: u32 = 10;

/// System instruction for the reminder model (our own prose, not template text).
pub const REMINDER_SYSTEM: &str = "You are a warm, concise wellness assistant. \
Reply ONLY as JSON of the form {\"message\": \"...\"}. Keep the message under \
200 words: encourage the reader to drink water, name one concrete benefit and \
one downside of dehydration, and end with a clear call to action. Respond in \
English.";

fn default_model() -> String {
    DEFAULT_MODEL.to_string()
}

// ---------------------------------------------------------------------------
// Flow data types
// ---------------------------------------------------------------------------

/// The inbound webhook reminder request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ReminderRequest {
    /// Stable idempotency key for this delivery (caller/scheduler-supplied).
    pub reminder_id: String,
    /// Who to remind (personalisation + log context).
    pub user: String,
    /// Civil date (`YYYY-MM-DD`) whose intake rows to read. Caller-supplied so
    /// the read is deterministic (no in-flow clock).
    pub date: String,
    /// Daily hydration goal in millilitres.
    pub target_ml: f64,
    /// LLM model id; defaults to [`DEFAULT_MODEL`].
    #[serde(default = "default_model")]
    pub model: String,
}

/// The request paired with the raw intake rows read from the sheet.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LogRead {
    pub request: ReminderRequest,
    pub rows: Vec<GoogleSheetsRowMatch>,
}

/// Pure projection of the intake log into hydration state.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct HydrationState {
    pub reminder_id: String,
    pub user: String,
    pub date: String,
    pub consumed_ml: f64,
    pub target_ml: f64,
    pub drink_count: u32,
    /// Progress fraction in `[0.0, 1.0]`.
    pub progress: f64,
    /// `💧`/`⬜` bar, [`PROGRESS_CELLS`] cells wide.
    pub progress_bar: String,
    /// Most recent drink time (`HH:MM:SS`) or `"none"`.
    pub last_drink_time: String,
    pub model: String,
}

/// Hydration state plus the model-authored reminder message.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Reminder {
    pub state: HydrationState,
    pub message: String,
}

/// The reminder after the Slack post.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PostedReminder {
    pub reminder_id: String,
    pub user: String,
    pub message: String,
    pub slack_channel: String,
    pub slack_ts: String,
}

/// Terminal capture: what the KV write did for this delivery.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ReminderRecord {
    /// `<flow>:<trigger>:{reminder_id}`.
    pub key: String,
    /// `false` on a redelivery of an already-recorded reminder.
    pub stored: bool,
    pub reminder_id: String,
    pub slack_channel: String,
    pub slack_ts: String,
    pub message: String,
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

/// The idempotency key for one reminder delivery.
pub fn reminder_key(reminder_id: &str) -> String {
    format!("{FLOW_NAME}:{TRIGGER_ALIAS}:{reminder_id}")
}

/// The date filter passed to `find_rows` for the request date.
pub fn log_filters(date: &str) -> Value {
    json!({ "date": date })
}

/// Parse a sheet cell (string or number) into millilitres; non-numeric → 0.
fn cell_ml(value: &Value) -> f64 {
    match value {
        Value::Number(n) => n.as_f64().unwrap_or(0.0),
        Value::String(s) => s.trim().parse::<f64>().unwrap_or(0.0),
        _ => 0.0,
    }
}

/// A `💧`/`⬜` progress bar for a fraction in `[0.0, 1.0]`.
pub fn progress_bar(fraction: f64) -> String {
    let cells = PROGRESS_CELLS as i64;
    let filled = (fraction * cells as f64).round().clamp(0.0, cells as f64) as i64;
    let empty = cells - filled;
    format!(
        "{}{}",
        "💧".repeat(filled as usize),
        "⬜".repeat(empty as usize)
    )
}

/// Fold the intake rows into hydration state (pure).
pub fn summarize(read: &LogRead) -> HydrationState {
    let req = &read.request;
    let mut consumed_ml = 0.0;
    let mut last_drink_time = String::new();
    for row in &read.rows {
        if let Some(value) = row.values.get("value") {
            consumed_ml += cell_ml(value);
        }
        if let Some(time) = row.values.get("time").and_then(Value::as_str) {
            // Times are `HH:MM:SS`; lexicographic max == chronological max.
            if time > last_drink_time.as_str() {
                last_drink_time = time.to_string();
            }
        }
    }
    let progress = if req.target_ml > 0.0 {
        (consumed_ml / req.target_ml).clamp(0.0, 1.0)
    } else {
        0.0
    };
    HydrationState {
        reminder_id: req.reminder_id.clone(),
        user: req.user.clone(),
        date: req.date.clone(),
        consumed_ml,
        target_ml: req.target_ml,
        drink_count: read.rows.len() as u32,
        progress,
        progress_bar: progress_bar(progress),
        last_drink_time: if last_drink_time.is_empty() {
            "none".to_string()
        } else {
            last_drink_time
        },
        model: req.model.clone(),
    }
}

/// The user prompt handed to the model — a pure function of hydration state.
pub fn reminder_prompt(state: &HydrationState) -> String {
    let remaining = (state.target_ml - state.consumed_ml).max(0.0);
    format!(
        "Hydration status for {user} on {date}:\n\
         - consumed today: {consumed} ml of a {target} ml goal ({pct}%)\n\
         - remaining to goal: {remaining} ml\n\
         - drinks logged today: {count}\n\
         - last drink at: {last}\n\
         Write the reminder message.",
        user = state.user,
        date = state.date,
        consumed = state.consumed_ml,
        target = state.target_ml,
        pct = (state.progress * 100.0).round(),
        remaining = remaining,
        count = state.drink_count,
        last = state.last_drink_time,
    )
}

/// The JSON Schema pinning the model's structured `{message}` output.
pub fn reminder_schema() -> Value {
    json!({
        "type": "object",
        "properties": { "message": { "type": "string" } },
        "required": ["message"],
        "additionalProperties": false
    })
}

/// The Block Kit payload for the Slack post — a pure function of the reminder.
pub fn reminder_blocks(message: &str, progress_bar: &str) -> Value {
    json!({
        "blocks": [
            { "type": "section", "text": { "type": "mrkdwn", "text": message } },
            { "type": "section", "text": { "type": "mrkdwn", "text": progress_bar } }
        ]
    })
}

fn node_error(err: impl std::fmt::Display) -> NodeError {
    NodeError::new(err.to_string())
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

/// Webhook ingress: passes the typed reminder request through.
#[def_node(
    trigger,
    name = "ReminderTrigger",
    summary = "Webhook ingress; receives the typed hydration reminder request",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn reminder_trigger(request: ReminderRequest) -> NodeResult<ReminderRequest> {
    Ok(request)
}

/// Read today's intake rows from the Google Sheets log.
#[def_node(
    name = "ReadWaterLog",
    identifier = "connector.google.sheets.read_water_log",
    summary = "Read the day's intake rows via connector.google.sheets.find_rows",
    connector_ops(GoogleSheetsFindRows)
)]
async fn read_water_log(request: ReminderRequest) -> NodeResult<LogRead> {
    let found = GoogleSheetsFindRows::invoke(&GoogleSheetsFindRowsInput {
        spreadsheet_id: LOG_SPREADSHEET_ID.to_string(),
        sheet: LOG_SHEET.to_string(),
        filters: log_filters(&request.date),
        limit: None,
        header_row: 1,
    })
    .await
    .map_err(|err| node_error(format!("connector.google.sheets.find_rows failed: {err}")))?;

    Ok(LogRead {
        request,
        rows: found.items,
    })
}

/// Fold the intake rows into hydration state (pure).
#[def_node(
    name = "SummarizeProgress",
    summary = "Summarize the day's intake into hydration progress state",
    effects = "Pure",
    determinism = "Strict"
)]
async fn summarize_progress(read: LogRead) -> NodeResult<HydrationState> {
    Ok(summarize(&read))
}

/// Ask the model for a short, personalised reminder (structured `{message}`).
#[def_node(
    name = "GenerateReminder",
    identifier = "connector.llm.generate_reminder",
    summary = "Generate the reminder text via connector.llm.complete",
    connector_ops(LlmComplete)
)]
async fn generate_reminder(state: HydrationState) -> NodeResult<Reminder> {
    let completion = LlmComplete::invoke(&LlmCompleteInput {
        provider: LlmProvider::OpenaiCompat,
        model: state.model.clone(),
        prompt: reminder_prompt(&state),
        system: Some(REMINDER_SYSTEM.to_string()),
        temperature: Some(1.0),
        max_tokens: Some(400),
        output_schema: Some(reminder_schema()),
    })
    .await
    .map_err(|err| node_error(format!("connector.llm.complete failed: {err}")))?;

    // Prefer the structured `{message}`; fall back to raw text, then a default.
    let message = completion
        .structured
        .as_ref()
        .and_then(|value| value.get("message"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|s| !s.is_empty())
        .or_else(|| {
            let text = completion.text.trim();
            (!text.is_empty()).then(|| text.to_string())
        })
        .unwrap_or_else(|| "Time to drink some water!".to_string());

    Ok(Reminder { state, message })
}

/// Post the reminder to Slack (text + a small Block Kit payload).
#[def_node(
    name = "PostReminder",
    identifier = "connector.slack.core.post_reminder",
    summary = "Post the reminder via connector.slack.core.post_message",
    connector_ops(SlackPostMessage)
)]
async fn post_reminder(reminder: Reminder) -> NodeResult<PostedReminder> {
    let posted = SlackPostMessage::invoke(&SlackPostMessageInput {
        channel: SLACK_CHANNEL.to_string(),
        text: reminder.message.clone(),
        blocks: Some(reminder_blocks(
            &reminder.message,
            &reminder.state.progress_bar,
        )),
    })
    .await
    .map_err(|err| node_error(format!("connector.slack.core.post_message failed: {err}")))?;

    Ok(PostedReminder {
        reminder_id: reminder.state.reminder_id,
        user: reminder.state.user,
        message: reminder.message,
        slack_channel: posted.channel,
        slack_ts: posted.ts,
    })
}

/// Terminal KV upsert keyed on the reminder id: redeliveries collapse to one
/// record.
#[def_node(
    name = "RecordReminder",
    summary = "Upsert the delivery record into KV, keyed on reminder_id",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(
        kv_read(capabilities::kv::KeyValue),
        kv_write(capabilities::kv::KeyValue)
    )
)]
async fn record_reminder(posted: PostedReminder) -> NodeResult<ReminderRecord> {
    let key = reminder_key(&posted.reminder_id);
    let record = ReminderRecord {
        key: key.clone(),
        stored: true,
        reminder_id: posted.reminder_id.clone(),
        slack_channel: posted.slack_channel.clone(),
        slack_ts: posted.slack_ts.clone(),
        message: posted.message.clone(),
    };

    let stored_now = context::with_current_async(|resources| {
        let key = key.clone();
        let record = record.clone();
        async move {
            let kv = resources.kv().ok_or_else(|| {
                NodeError::new(
                    "record_reminder requires a KV capability (declare resource::kv::write)",
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
                .map_err(|err| node_error(format!("serialize reminder record: {err}")))?;
            kv.put(&key, &row, None)
                .await
                .map_err(|err| node_error(format!("kv put failed: {err}")))?;
            Ok(true)
        }
    })
    .await
    .ok_or_else(|| NodeError::new("record_reminder missing ResourceAccess context"))??;

    Ok(ReminderRecord {
        stored: stored_now,
        ..record
    })
}

/// Terminal capture: the durable effects are the Slack post and the KV record.
#[def_node(
    name = "Capture",
    summary = "Capture the reminder record",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture(record: ReminderRecord) -> NodeResult<ReminderRecord> {
    Ok(record)
}

dag_macros::flow! {
    name: s22_hydration_reminder_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Clone of n8n template #3: a webhook-triggered hydration reminder — read the day's intake log from Google Sheets, summarize progress, generate a personalised reminder with an LLM, post it to Slack, and record the delivery keyed on reminder_id";

    let reminder_trigger = node!(reminder_trigger);
    let read_water_log = node!(read_water_log);
    let summarize_progress = node!(summarize_progress);
    let generate_reminder = node!(generate_reminder);
    let post_reminder = node!(post_reminder);
    let record_reminder = node!(record_reminder);
    let capture = node!(capture);

    connect!(reminder_trigger -> read_water_log);
    connect!(read_water_log -> summarize_progress);
    connect!(summarize_progress -> generate_reminder);
    connect!(generate_reminder -> post_reminder);
    connect!(post_reminder -> record_reminder);
    connect!(record_reminder -> capture);

    entrypoint!({
        trigger: "reminder_trigger",
        capture: "capture",
        route_aliases: ["/hydration-reminder"],
        method: "POST",
        deadline_ms: 30_000,
    });
}

#[cfg(test)]
mod tests;
