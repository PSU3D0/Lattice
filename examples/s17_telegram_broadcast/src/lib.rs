//! S17 — clone: a manually triggered Telegram broadcast.
//!
//! Behavioral spec (independent Rust implementation of an audited workflow's
//! behavior; no third-party workflow content is embedded here):
//!
//! 1. A **manual (HTTP) trigger** starts the flow with a `BroadcastRequest`
//!    (an operator-supplied `broadcast_id`, the roster spreadsheet + tab, and
//!    the message text).
//! 2. `connector.google.sheets.find_rows` reads the roster tab and the
//!    `chat_id` column is projected into a de-duplicated list of recipients.
//! 3. For each recipient, `connector.telegram.send_message` posts the message
//!    to that chat as the authenticated bot.
//! 4. A terminal KV upsert records the broadcast, keyed
//!    `<flow>:<trigger>:{broadcast_id}` — a redelivery of the same broadcast id
//!    collapses to one record (the manual-trigger analogue of s15/s16's
//!    `scheduled_time_ms` keying).
//!
//! ## Idempotency
//!
//! The KV record is keyed on the operator's `broadcast_id`, so a redelivery of
//! the same broadcast dedupes to a single row. Per-message provider-side
//! duplicate suppression across redeliveries remains the delivery gate's job
//! (`Delivery::ExactlyOnce` composition is proven per-op in the Telegram
//! connector's honesty tests); the roster read and each send payload are pure
//! functions of the request, so a replay reissues byte-identical calls.

use capabilities::context;
use connector_google_sheets::ops::GoogleSheetsFindRows;
use connector_google_sheets::{GoogleSheetsFindRowsInput, GoogleSheetsRowMatch};
use connector_telegram::TelegramSendMessageInput;
use connector_telegram::ops::TelegramSendMessage;
use dag_core::{NodeError, NodeResult};
use dag_macros::{def_node, node};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

pub const TRIGGER_ALIAS: &str = "broadcast_trigger";
pub const FLOW_NAME: &str = "s17_telegram_broadcast_flow";
/// The roster column carrying Telegram chat ids.
pub const ROSTER_HEADER: &str = "chat_id";
/// Bound roster size so one broadcast stays bounded.
const MAX_RECIPIENTS: u32 = 500;

// ---------------------------------------------------------------------------
// Flow data types
// ---------------------------------------------------------------------------

/// The operator-supplied broadcast request (manual trigger input).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BroadcastRequest {
    /// Stable id for this broadcast; the KV record is keyed on it.
    pub broadcast_id: String,
    /// Spreadsheet holding the recipient roster.
    pub spreadsheet_id: String,
    /// Tab (sheet) name holding the roster; row 1 is the header row.
    pub sheet: String,
    /// The text to send to every recipient.
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RosterPlan {
    pub request: BroadcastRequest,
    pub chat_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Delivery {
    pub chat_id: String,
    pub message_id: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BroadcastResult {
    pub request: BroadcastRequest,
    pub recipients: u32,
    pub deliveries: Vec<Delivery>,
}

/// Terminal capture: what the KV write did for this broadcast.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BroadcastRecord {
    /// `<flow>:<trigger>:{broadcast_id}`.
    pub key: String,
    /// `false` on a redelivery of an already-recorded broadcast.
    pub stored: bool,
    pub broadcast_id: String,
    pub recipients: u32,
    pub delivered: u32,
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

/// The idempotency key for one broadcast (spec shape
/// `<flow>:<trigger>:{broadcast_id}`).
pub fn broadcast_key(broadcast_id: &str) -> String {
    format!("{FLOW_NAME}:{TRIGGER_ALIAS}:{broadcast_id}")
}

/// Project the `chat_id` column out of the roster rows, preserving order and
/// dropping blanks and duplicates.
pub fn chat_ids_from_matches(items: &[GoogleSheetsRowMatch]) -> Vec<String> {
    let mut seen = Vec::new();
    for item in items {
        let cell = item.values.get(ROSTER_HEADER);
        let value = match cell {
            Some(JsonValue::String(s)) => s.trim().to_string(),
            Some(JsonValue::Number(n)) => n.to_string(),
            _ => continue,
        };
        if !value.is_empty() && !seen.contains(&value) {
            seen.push(value);
        }
    }
    seen
}

fn node_error(err: impl std::fmt::Display) -> NodeError {
    NodeError::new(err.to_string())
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

/// Manual trigger: passes the typed broadcast request through.
#[def_node(
    trigger,
    name = "BroadcastTrigger",
    summary = "Manual ingress; receives the typed BroadcastRequest for this run",
    effects = "Pure",
    determinism = "Strict"
)]
async fn broadcast_trigger(request: BroadcastRequest) -> NodeResult<BroadcastRequest> {
    Ok(request)
}

/// Read the roster tab and project the chat-id column.
#[def_node(
    name = "LoadRoster",
    identifier = "connector.google.sheets.load_roster",
    summary = "Read the recipient roster via connector.google.sheets.find_rows",
    connector_ops(GoogleSheetsFindRows)
)]
async fn load_roster(request: BroadcastRequest) -> NodeResult<RosterPlan> {
    let found = GoogleSheetsFindRows::invoke(&GoogleSheetsFindRowsInput {
        spreadsheet_id: request.spreadsheet_id.clone(),
        sheet: request.sheet.clone(),
        filters: json!({}),
        limit: Some(MAX_RECIPIENTS),
        header_row: 1,
    })
    .await
    .map_err(|err| node_error(format!("connector.google.sheets.find_rows failed: {err}")))?;

    Ok(RosterPlan {
        chat_ids: chat_ids_from_matches(&found.items),
        request,
    })
}

/// Send the message to every recipient.
#[def_node(
    name = "BroadcastMessages",
    identifier = "connector.telegram.broadcast_messages",
    summary = "Send the message to each chat via connector.telegram.send_message",
    connector_ops(TelegramSendMessage)
)]
async fn broadcast_messages(plan: RosterPlan) -> NodeResult<BroadcastResult> {
    let mut deliveries = Vec::with_capacity(plan.chat_ids.len());
    for chat_id in &plan.chat_ids {
        let sent = TelegramSendMessage::invoke(&TelegramSendMessageInput {
            chat_id: chat_id.clone(),
            text: plan.request.message.clone(),
        })
        .await
        .map_err(|err| node_error(format!("connector.telegram.send_message failed: {err}")))?;
        deliveries.push(Delivery {
            chat_id: chat_id.clone(),
            message_id: sent.message_id,
        });
    }

    Ok(BroadcastResult {
        recipients: plan.chat_ids.len() as u32,
        request: plan.request,
        deliveries,
    })
}

/// Terminal KV upsert keyed on the broadcast id: redeliveries of one broadcast
/// collapse to a single record.
#[def_node(
    name = "RecordBroadcast",
    summary = "Upsert the broadcast record into KV, keyed on broadcast_id",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(
        kv_read(capabilities::kv::KeyValue),
        kv_write(capabilities::kv::KeyValue)
    )
)]
async fn record_broadcast(result: BroadcastResult) -> NodeResult<BroadcastRecord> {
    let key = broadcast_key(&result.request.broadcast_id);
    let record = BroadcastRecord {
        key: key.clone(),
        stored: true,
        broadcast_id: result.request.broadcast_id.clone(),
        recipients: result.recipients,
        delivered: result.deliveries.len() as u32,
    };

    let stored = context::with_current_async(|resources| {
        let key = key.clone();
        let record = record.clone();
        async move {
            let kv = resources.kv().ok_or_else(|| {
                NodeError::new(
                    "record_broadcast requires a KV capability (declare resource::kv::write)",
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
                .map_err(|err| node_error(format!("serialize broadcast record: {err}")))?;
            kv.put(&key, &row, None)
                .await
                .map_err(|err| node_error(format!("kv put failed: {err}")))?;
            Ok(true)
        }
    })
    .await
    .ok_or_else(|| NodeError::new("record_broadcast missing ResourceAccess context"))??;

    Ok(BroadcastRecord { stored, ..record })
}

/// Terminal capture: the durable effects are the Telegram sends and the KV
/// record.
#[def_node(
    name = "Capture",
    summary = "Capture the broadcast record",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture(record: BroadcastRecord) -> NodeResult<BroadcastRecord> {
    Ok(record)
}

dag_macros::flow! {
    name: s17_telegram_broadcast_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Clone: a manually triggered Telegram broadcast — read a roster of chat ids from a sheet and message each one; recorded idempotently under an operator broadcast_id";

    let broadcast_trigger = node!(broadcast_trigger);
    let load_roster = node!(load_roster);
    let broadcast_messages = node!(broadcast_messages);
    let record_broadcast = node!(record_broadcast);
    let capture = node!(capture);

    connect!(broadcast_trigger -> load_roster);
    connect!(load_roster -> broadcast_messages);
    connect!(broadcast_messages -> record_broadcast);
    connect!(record_broadcast -> capture);

    entrypoint!({
        trigger: "broadcast_trigger",
        capture: "capture",
        route_aliases: ["/telegram/broadcast"],
        method: "POST",
        deadline_ms: 60_000,
    });
}

#[cfg(test)]
mod tests;
