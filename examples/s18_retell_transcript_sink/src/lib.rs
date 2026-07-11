//! S18 — clone: a webhook-triggered analyzed-call transcript sink.
//!
//! Behavioral spec (independent Rust implementation of an audited workflow's
//! behavior; no third-party workflow content is embedded here):
//!
//! 1. An **HTTP webhook** (`POST /retell`) receives a voice-call event once the
//!    provider has finished analyzing the call.
//! 2. A pure **filter/normalize** node keeps only `call_analyzed` events and
//!    projects the useful fields: call id, start/end datetimes (derived from
//!    epoch-ms timestamps), duration, transcript, summary, sentiment, the
//!    call's phone number (the `to`/`from` number chosen by call direction),
//!    and the cost in dollars (cents / 100).
//! 3. The normalized record is written to **Airtable** (`record.create`).
//! 4. The same record is appended to a **Google Sheets** tab (`append_row`).
//! 5. The same record is written to a **Notion** database (`databasePage`
//!    create).
//! 6. A terminal **KV upsert** records the delivery keyed
//!    `<flow>:<trigger>:{call_id}` — the webhook's natural idempotency key.
//!
//! ## Idempotency
//!
//! Every effectful payload is a pure function of the inbound event: the three
//! sink writes and the terminal KV key all derive from `call_id` and the
//! normalized fields. A redelivery of the same `call_id` replays byte-identical
//! requests, and the terminal KV record dedupes to a single row. Provider-side
//! duplicate suppression for the three writes remains the delivery gate's job
//! (`Delivery::ExactlyOnce` composition is proven per-op in each connector's
//! honesty tests) — the same posture as the s16 pilot.

use capabilities::context;
use connector_airtable::AirtableCreateRecordInput;
use connector_airtable::ops::AirtableCreateRecord;
use connector_google_sheets::GoogleSheetsAppendRowInput;
use connector_google_sheets::ops::GoogleSheetsAppendRow;
use connector_notion::NotionCreatePageInput;
use connector_notion::ops::NotionCreatePage;
use dag_core::{NodeError, NodeResult};
use dag_macros::{def_node, node};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub const TRIGGER_ALIAS: &str = "intake";
pub const FLOW_NAME: &str = "s18_retell_transcript_sink_flow";

/// Only events of this kind carry a completed analysis; everything else is
/// dropped by the filter node. (Our own constant — not template text.)
pub const ANALYZED_EVENT: &str = "call_analyzed";

/// Fixed sink targets. Real deployments would source these from
/// config/bindings; constants keep the example self-contained.
pub const AIRTABLE_BASE_ID: &str = "appLatticePilot01";
pub const AIRTABLE_TABLE: &str = "Transcripts";
pub const SHEETS_SPREADSHEET_ID: &str = "retell-transcripts-log";
pub const SHEETS_TAB: &str = "Transcripts";
pub const NOTION_DATABASE_ID: &str = "db-lattice-pilot-transcripts";

// ---------------------------------------------------------------------------
// Webhook payload (our own schema; models an analyzed-call event)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CallEvent {
    pub event: String,
    pub call: CallPayload,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CallPayload {
    pub call_id: String,
    /// `inbound` or `outbound`; selects which phone number is "the" number.
    pub direction: String,
    pub from_number: String,
    pub to_number: String,
    pub start_timestamp_ms: u64,
    pub end_timestamp_ms: u64,
    pub duration_seconds: f64,
    pub transcript: String,
    pub summary: String,
    pub sentiment: String,
    /// Combined call cost, in cents.
    pub combined_cost_cents: f64,
}

/// The normalized record fanned out to every sink — a pure projection of the
/// inbound event.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TranscriptRecord {
    pub call_id: String,
    pub start_datetime: String,
    pub end_datetime: String,
    pub duration_seconds: f64,
    pub transcript: String,
    pub summary: String,
    pub sentiment: String,
    pub phone_number: String,
    pub cost_dollars: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AirtableStored {
    pub record: TranscriptRecord,
    pub airtable_record_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SheetsStored {
    pub record: TranscriptRecord,
    pub airtable_record_id: String,
    pub sheet_updated_range: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NotionStored {
    pub record: TranscriptRecord,
    pub airtable_record_id: String,
    pub sheet_updated_range: String,
    pub notion_page_id: String,
}

/// Terminal capture: what the KV write did for this delivery.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SinkRecord {
    /// `<flow>:<trigger>:{call_id}`.
    pub key: String,
    /// `false` on a redelivery of an already-recorded call.
    pub stored: bool,
    pub call_id: String,
    pub airtable_record_id: String,
    pub sheet_updated_range: String,
    pub notion_page_id: String,
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

/// The idempotency key for one call delivery (spec shape
/// `<flow>:<trigger>:{call_id}`).
pub fn sink_key(call_id: &str) -> String {
    format!("{FLOW_NAME}:{TRIGGER_ALIAS}:{call_id}")
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

/// RFC 3339 timestamp (second precision, UTC) for an epoch-ms instant.
pub fn utc_rfc3339(epoch_ms: u64) -> String {
    let (year, month, day) = civil_from_days((epoch_ms / 86_400_000) as i64);
    let secs_of_day = (epoch_ms / 1_000) % 86_400;
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        secs_of_day / 3_600,
        (secs_of_day / 60) % 60,
        secs_of_day % 60
    )
}

/// Project a filtered `call_analyzed` event into the normalized record. The
/// phone number is the call's `to`/`from` number chosen by direction; the cost
/// is converted from cents to dollars.
pub fn normalize(call: &CallPayload) -> TranscriptRecord {
    let phone_number = if call.direction.eq_ignore_ascii_case("outbound") {
        call.to_number.clone()
    } else {
        call.from_number.clone()
    };
    TranscriptRecord {
        call_id: call.call_id.clone(),
        start_datetime: utc_rfc3339(call.start_timestamp_ms),
        end_datetime: utc_rfc3339(call.end_timestamp_ms),
        duration_seconds: call.duration_seconds,
        transcript: call.transcript.clone(),
        summary: call.summary.clone(),
        sentiment: call.sentiment.clone(),
        phone_number,
        cost_dollars: call.combined_cost_cents / 100.0,
    }
}

/// The flat column map shared by the Airtable record and the Sheets row.
pub fn record_columns(record: &TranscriptRecord) -> serde_json::Value {
    json!({
        "Call ID": record.call_id,
        "Start Datetime": record.start_datetime,
        "End Datetime": record.end_datetime,
        "Duration in seconds": record.duration_seconds,
        "Phone Number": record.phone_number,
        "Transcript": record.transcript,
        "Call Summary": record.summary,
        "User Sentiment": record.sentiment,
        "Total Cost in Dollars": record.cost_dollars,
    })
}

/// Notion property values for the record (title = summary; the rest are typed
/// Notion property objects).
pub fn notion_properties(record: &TranscriptRecord) -> serde_json::Value {
    let rich_text = |value: &str| json!([{ "text": { "content": value } }]);
    json!({
        "Call Summary": { "title": rich_text(&record.summary) },
        "Call ID": { "rich_text": rich_text(&record.call_id) },
        "Transcript": { "rich_text": rich_text(&record.transcript) },
        "User Sentiment": { "rich_text": rich_text(&record.sentiment) },
        "Phone Number": { "phone_number": record.phone_number },
        "Start Datetime": { "date": { "start": record.start_datetime } },
        "End Datetime": { "date": { "start": record.end_datetime } },
        "Duration in seconds": { "number": record.duration_seconds },
        "Total Cost in Dollars": { "number": record.cost_dollars },
    })
}

fn node_error(err: impl std::fmt::Display) -> NodeError {
    NodeError::new(err.to_string())
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

/// Webhook ingress: passes the typed call event through.
#[def_node(
    trigger,
    name = "IntakeTrigger",
    summary = "HTTP webhook ingress; receives the analyzed-call event",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn intake(event: CallEvent) -> NodeResult<CallEvent> {
    Ok(event)
}

/// Filter + normalize: keep only `call_analyzed` events, project the fields.
#[def_node(
    name = "NormalizeCall",
    summary = "Keep only call_analyzed events and project the transcript record",
    effects = "Pure",
    determinism = "Strict"
)]
async fn normalize_call(event: CallEvent) -> NodeResult<TranscriptRecord> {
    if event.event != ANALYZED_EVENT {
        return Err(NodeError::new(format!(
            "ignoring event `{}`: only `{ANALYZED_EVENT}` events are stored",
            event.event
        )));
    }
    Ok(normalize(&event.call))
}

/// Write the record to Airtable.
#[def_node(
    name = "StoreInAirtable",
    identifier = "connector.airtable.store_transcript",
    summary = "Create the transcript record via connector.airtable.create_record",
    connector_ops(AirtableCreateRecord)
)]
async fn store_in_airtable(record: TranscriptRecord) -> NodeResult<AirtableStored> {
    let created = AirtableCreateRecord::invoke(&AirtableCreateRecordInput {
        base_id: AIRTABLE_BASE_ID.to_string(),
        table: AIRTABLE_TABLE.to_string(),
        fields: record_columns(&record),
        typecast: None,
    })
    .await
    .map_err(|err| node_error(format!("connector.airtable.create_record failed: {err}")))?;

    Ok(AirtableStored {
        record,
        airtable_record_id: created.id,
    })
}

/// Append the record to the Google Sheets tab.
#[def_node(
    name = "StoreInSheets",
    identifier = "connector.google.sheets.append_transcript_row",
    summary = "Append the transcript row via connector.google.sheets.append_row",
    connector_ops(GoogleSheetsAppendRow)
)]
async fn store_in_sheets(stored: AirtableStored) -> NodeResult<SheetsStored> {
    let appended = GoogleSheetsAppendRow::invoke(&GoogleSheetsAppendRowInput {
        spreadsheet_id: SHEETS_SPREADSHEET_ID.to_string(),
        sheet: SHEETS_TAB.to_string(),
        row: record_columns(&stored.record),
        header_row: 1,
        value_input_option: None,
    })
    .await
    .map_err(|err| node_error(format!("connector.google.sheets.append_row failed: {err}")))?;

    Ok(SheetsStored {
        record: stored.record,
        airtable_record_id: stored.airtable_record_id,
        sheet_updated_range: appended.updated_range,
    })
}

/// Write the record to the Notion database.
#[def_node(
    name = "StoreInNotion",
    identifier = "connector.notion.store_transcript_page",
    summary = "Create the transcript page via connector.notion.create_page",
    connector_ops(NotionCreatePage)
)]
async fn store_in_notion(stored: SheetsStored) -> NodeResult<NotionStored> {
    let page = NotionCreatePage::invoke(&NotionCreatePageInput {
        database_id: NOTION_DATABASE_ID.to_string(),
        properties: notion_properties(&stored.record),
        icon: None,
    })
    .await
    .map_err(|err| node_error(format!("connector.notion.create_page failed: {err}")))?;

    Ok(NotionStored {
        record: stored.record,
        airtable_record_id: stored.airtable_record_id,
        sheet_updated_range: stored.sheet_updated_range,
        notion_page_id: page.id,
    })
}

/// Terminal KV upsert keyed on the call id: redeliveries of one call collapse
/// to a single record.
#[def_node(
    name = "RecordSink",
    summary = "Upsert the delivery record into KV, keyed on call_id",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(
        kv_read(capabilities::kv::KeyValue),
        kv_write(capabilities::kv::KeyValue)
    )
)]
async fn record_sink(stored: NotionStored) -> NodeResult<SinkRecord> {
    let key = sink_key(&stored.record.call_id);
    let record = SinkRecord {
        key: key.clone(),
        stored: true,
        call_id: stored.record.call_id.clone(),
        airtable_record_id: stored.airtable_record_id,
        sheet_updated_range: stored.sheet_updated_range,
        notion_page_id: stored.notion_page_id,
    };

    let stored_now = context::with_current_async(|resources| {
        let key = key.clone();
        let record = record.clone();
        async move {
            let kv = resources.kv().ok_or_else(|| {
                NodeError::new("record_sink requires a KV capability (declare resource::kv::write)")
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
                .map_err(|err| node_error(format!("serialize sink record: {err}")))?;
            kv.put(&key, &row, None)
                .await
                .map_err(|err| node_error(format!("kv put failed: {err}")))?;
            Ok(true)
        }
    })
    .await
    .ok_or_else(|| NodeError::new("record_sink missing ResourceAccess context"))??;

    Ok(SinkRecord {
        stored: stored_now,
        ..record
    })
}

/// Terminal capture: the durable effects are the three sink writes and the KV
/// record.
#[def_node(
    name = "Capture",
    summary = "Capture the delivery's sink record",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture(record: SinkRecord) -> NodeResult<SinkRecord> {
    Ok(record)
}

dag_macros::flow! {
    name: s18_retell_transcript_sink_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Clone: webhook-triggered analyzed-call transcript sink — normalize the event and fan it into Airtable, Google Sheets, and Notion; payloads keyed on call_id";

    let intake = node!(intake);
    let normalize_call = node!(normalize_call);
    let store_in_airtable = node!(store_in_airtable);
    let store_in_sheets = node!(store_in_sheets);
    let store_in_notion = node!(store_in_notion);
    let record_sink = node!(record_sink);
    let capture = node!(capture);

    connect!(intake -> normalize_call);
    connect!(normalize_call -> store_in_airtable);
    connect!(store_in_airtable -> store_in_sheets);
    connect!(store_in_sheets -> store_in_notion);
    connect!(store_in_notion -> record_sink);
    connect!(record_sink -> capture);

    entrypoint!({
        trigger: "intake",
        capture: "capture",
        route_aliases: ["/retell"],
        method: "POST",
        deadline_ms: 15_000,
    });
}

#[cfg(test)]
mod tests;
