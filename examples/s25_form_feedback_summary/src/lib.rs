//! S25 — clone (packet N4-T5): a manually triggered "summarize form feedback"
//! flow over Google Sheets + an LLM + Gmail.
//!
//! Behavioral spec (independent Rust implementation of an audited workflow's
//! *behavior*; no third-party workflow text, parameter strings, or prose are
//! embedded here):
//!
//! 1. A **manual (HTTP) trigger** starts the flow with a `FeedbackRequest`
//!    (an operator-supplied `run_id`, the responses spreadsheet + tab, the model
//!    to summarize with, the feedback question columns to analyze, and the
//!    recipient + subject for the report email).
//! 2. `connector.google.sheets.find_rows` reads the form-response rows.
//! 3. A pure node **aggregates** the responses: for each requested question
//!    column it collects that column's answers across every row into an array
//!    (the n8n Aggregate node's "combine answers per question" step), then
//!    builds a single user prompt from those arrays.
//! 4. `connector.llm.complete` summarizes the aggregated feedback into a
//!    Markdown report (single-turn: a fixed system instruction + the aggregated
//!    user prompt; `openai_compat` dialect, the model the operator supplied).
//! 5. A pure node renders the report into the email body.
//! 6. `connector.google.gmail.send_message` emails the report to the recipient.
//! 7. A terminal KV upsert records the run, keyed `<flow>:<trigger>:{run_id}`.
//!
//! ## Idempotency
//!
//! The KV record is keyed on the operator's `run_id`, so a redelivery of the
//! same run dedupes to a single row (the manual-trigger analogue of s15/s16's
//! `scheduled_time_ms` keying, following the s17 precedent). The sheet read and
//! the aggregated prompt are pure functions of the request; the LLM completion
//! is `Nondeterministic` (sampled output differs across identical inputs), so a
//! genuine replay may email a differently-worded summary. Provider-side
//! duplicate suppression of the email send across redeliveries remains the
//! delivery gate's job (same documented posture as s17); the terminal record is
//! what dedupes here.

use capabilities::context;
use connector_google_gmail::GoogleGmailSendMessageInput;
use connector_google_gmail::ops::GoogleGmailSendMessage;
use connector_google_sheets::ops::GoogleSheetsFindRows;
use connector_google_sheets::{GoogleSheetsFindRowsInput, GoogleSheetsRowMatch};
use connector_llm::ops::LlmComplete;
use connector_llm::{LlmCompleteInput, LlmProvider};
use dag_core::{NodeError, NodeResult};
use dag_macros::{def_node, node};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

pub const TRIGGER_ALIAS: &str = "feedback_trigger";
pub const FLOW_NAME: &str = "s25_form_feedback_summary_flow";

/// System instruction for the summarizer (our own words — not copied from any
/// third-party workflow). Describes the task and the desired report shape.
pub const SUMMARY_SYSTEM_PROMPT: &str = "\
You summarize event feedback survey responses. You receive several questions, \
each followed by the list of answers respondents gave, with individual answers \
separated by ' | '. Produce a concise report: first an overall sentiment read \
across the responses, then the concrete, actionable suggestions for improvement. \
Reply in Markdown.";

/// Sampling temperature for the summary (a behavioral parameter, not text).
const SUMMARY_TEMPERATURE: f64 = 0.3;
/// Output token cap for the summary.
const MAX_SUMMARY_TOKENS: u64 = 1024;
/// Bound on how many response rows one run will read.
const MAX_RESPONSE_ROWS: u32 = 1000;

// ---------------------------------------------------------------------------
// Flow data types
// ---------------------------------------------------------------------------

/// The operator-supplied summarization request (manual trigger input).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FeedbackRequest {
    /// Stable id for this run; the KV record is keyed on it.
    pub run_id: String,
    /// Spreadsheet holding the form responses.
    pub spreadsheet_id: String,
    /// Tab (sheet) name holding the responses; row 1 is the header row.
    pub sheet: String,
    /// Provider model identifier used for the summary.
    pub model: String,
    /// The feedback question columns to aggregate and summarize, in order.
    pub questions: Vec<String>,
    /// Destination address for the report email.
    pub recipient: String,
    /// Subject line for the report email.
    pub subject: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FeedbackScan {
    pub request: FeedbackRequest,
    pub rows: Vec<GoogleSheetsRowMatch>,
}

/// One question column and every answer given to it, in row order.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct QuestionAnswers {
    pub question: String,
    pub answers: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AggregatedFeedback {
    pub request: FeedbackRequest,
    pub questions: Vec<QuestionAnswers>,
    /// Number of response rows read.
    pub response_count: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SummaryDraft {
    pub request: FeedbackRequest,
    pub summary_text: String,
    pub model: String,
    pub response_count: u32,
    pub total_tokens: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ReportEmail {
    pub request: FeedbackRequest,
    pub subject: String,
    pub body: String,
    pub response_count: u32,
    pub total_tokens: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SendOutcome {
    pub request: FeedbackRequest,
    pub message_id: String,
    pub response_count: u32,
    pub total_tokens: u64,
}

/// Terminal capture: what this run recorded.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SummaryRecord {
    /// `<flow>:<trigger>:{run_id}`.
    pub key: String,
    /// `false` on a redelivery of an already-recorded run.
    pub stored: bool,
    pub run_id: String,
    pub recipient: String,
    pub message_id: String,
    pub response_count: u32,
    pub total_tokens: u64,
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

/// The idempotency key for one run (spec shape `<flow>:<trigger>:{run_id}`).
pub fn feedback_key(run_id: &str) -> String {
    format!("{FLOW_NAME}:{TRIGGER_ALIAS}:{run_id}")
}

/// Read a cell as text from a sheet row's `header -> value` object. String and
/// number cells are kept (numbers stringified); anything else is empty.
fn cell_text(values: &JsonValue, column: &str) -> String {
    match values.get(column) {
        Some(JsonValue::String(s)) => s.trim().to_string(),
        Some(JsonValue::Number(n)) => n.to_string(),
        _ => String::new(),
    }
}

/// Aggregate the response rows into per-question answer arrays. For each
/// requested question column (in request order), collect that column's
/// non-empty answers across every row, preserving row order.
pub fn aggregate_answers(
    rows: &[GoogleSheetsRowMatch],
    questions: &[String],
) -> Vec<QuestionAnswers> {
    questions
        .iter()
        .map(|question| {
            let answers = rows
                .iter()
                .map(|row| cell_text(&row.values, question))
                .filter(|answer| !answer.is_empty())
                .collect();
            QuestionAnswers {
                question: question.clone(),
                answers,
            }
        })
        .collect()
}

/// Build the single-turn user prompt from the aggregated answers: a numbered
/// list, each question followed by its answers joined with ` | ` inside a code
/// fence so the model sees the answer boundaries.
pub fn build_prompt(aggregated: &[QuestionAnswers]) -> String {
    let mut out = String::new();
    for (index, qa) in aggregated.iter().enumerate() {
        out.push_str(&format!(
            "{}. {}: ```{}```\n",
            index + 1,
            qa.question,
            qa.answers.join(" | ")
        ));
    }
    out
}

fn node_error(err: impl std::fmt::Display) -> NodeError {
    NodeError::new(err.to_string())
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

/// Manual trigger: passes the typed feedback request through.
#[def_node(
    trigger,
    name = "FeedbackTrigger",
    summary = "Manual ingress; receives the typed FeedbackRequest for this run",
    effects = "Pure",
    determinism = "Strict"
)]
async fn feedback_trigger(request: FeedbackRequest) -> NodeResult<FeedbackRequest> {
    Ok(request)
}

/// Read the form-response rows.
#[def_node(
    name = "LoadFeedback",
    identifier = "connector.google.sheets.load_feedback",
    summary = "Read the form-response rows via connector.google.sheets.find_rows",
    connector_ops(GoogleSheetsFindRows)
)]
async fn load_feedback(request: FeedbackRequest) -> NodeResult<FeedbackScan> {
    let found = GoogleSheetsFindRows::invoke(&GoogleSheetsFindRowsInput {
        spreadsheet_id: request.spreadsheet_id.clone(),
        sheet: request.sheet.clone(),
        filters: json!({}),
        limit: Some(MAX_RESPONSE_ROWS),
        header_row: 1,
    })
    .await
    .map_err(|err| node_error(format!("connector.google.sheets.find_rows failed: {err}")))?;

    Ok(FeedbackScan {
        request,
        rows: found.items,
    })
}

/// Pure aggregation: per-question answer arrays.
#[def_node(
    name = "AggregateFeedback",
    summary = "Combine each question column's answers across all responses into arrays",
    effects = "Pure",
    determinism = "Strict"
)]
async fn aggregate_feedback(scan: FeedbackScan) -> NodeResult<AggregatedFeedback> {
    let questions = aggregate_answers(&scan.rows, &scan.request.questions);
    Ok(AggregatedFeedback {
        response_count: scan.rows.len() as u32,
        questions,
        request: scan.request,
    })
}

/// Summarize the aggregated feedback via the LLM connector.
#[def_node(
    name = "SummarizeFeedback",
    identifier = "connector.llm.summarize_feedback",
    summary = "Summarize the aggregated feedback via connector.llm.complete",
    connector_ops(LlmComplete)
)]
async fn summarize_feedback(aggregated: AggregatedFeedback) -> NodeResult<SummaryDraft> {
    let prompt = build_prompt(&aggregated.questions);
    let completion = LlmComplete::invoke(&LlmCompleteInput {
        provider: LlmProvider::OpenaiCompat,
        model: aggregated.request.model.clone(),
        prompt,
        system: Some(SUMMARY_SYSTEM_PROMPT.to_string()),
        temperature: Some(SUMMARY_TEMPERATURE),
        max_tokens: Some(MAX_SUMMARY_TOKENS),
        output_schema: None,
    })
    .await
    .map_err(|err| node_error(format!("connector.llm.complete failed: {err}")))?;

    Ok(SummaryDraft {
        summary_text: completion.text,
        model: completion.model,
        response_count: aggregated.response_count,
        total_tokens: completion.usage.total_tokens,
        request: aggregated.request,
    })
}

/// Pure render: turn the summary into the email body.
///
/// Behavioral cut: the audited workflow ran the Markdown through a
/// Markdown→HTML node before sending an HTML email. The Gmail connector's
/// `send_message` op exposes only a plain-text body (`text_body`), so the report
/// is delivered as its Markdown source. Rendering to an HTML body is a genuine
/// gap in the connector surface (no `html_body` field), not a shortcut — see the
/// report's "what was cut".
#[def_node(
    name = "RenderReport",
    summary = "Render the summary into the report email body",
    effects = "Pure",
    determinism = "Strict"
)]
async fn render_report(draft: SummaryDraft) -> NodeResult<ReportEmail> {
    Ok(ReportEmail {
        subject: draft.request.subject.clone(),
        body: draft.summary_text,
        response_count: draft.response_count,
        total_tokens: draft.total_tokens,
        request: draft.request,
    })
}

/// Email the report to the recipient.
#[def_node(
    name = "SendReport",
    identifier = "connector.google.gmail.send_report",
    summary = "Email the report via connector.google.gmail.send_message",
    connector_ops(GoogleGmailSendMessage)
)]
async fn send_report(email: ReportEmail) -> NodeResult<SendOutcome> {
    let sent = GoogleGmailSendMessage::invoke(&GoogleGmailSendMessageInput {
        to: email.request.recipient.clone(),
        cc: None,
        bcc: None,
        subject: email.subject.clone(),
        text_body: email.body.clone(),
    })
    .await
    .map_err(|err| node_error(format!("connector.google.gmail.send_message failed: {err}")))?;

    Ok(SendOutcome {
        message_id: sent.id,
        response_count: email.response_count,
        total_tokens: email.total_tokens,
        request: email.request,
    })
}

/// Terminal KV upsert keyed on the run id: redeliveries of one run collapse to a
/// single record.
#[def_node(
    name = "RecordSummary",
    summary = "Upsert the run's summary record into KV, keyed on run_id",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(
        kv_read(capabilities::kv::KeyValue),
        kv_write(capabilities::kv::KeyValue)
    )
)]
async fn record_summary(outcome: SendOutcome) -> NodeResult<SummaryRecord> {
    let key = feedback_key(&outcome.request.run_id);
    let record = SummaryRecord {
        key: key.clone(),
        stored: true,
        run_id: outcome.request.run_id.clone(),
        recipient: outcome.request.recipient.clone(),
        message_id: outcome.message_id.clone(),
        response_count: outcome.response_count,
        total_tokens: outcome.total_tokens,
    };

    let stored = context::with_current_async(|resources| {
        let key = key.clone();
        let record = record.clone();
        async move {
            let kv = resources.kv().ok_or_else(|| {
                NodeError::new(
                    "record_summary requires a KV capability (declare resource::kv::write)",
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
                .map_err(|err| node_error(format!("serialize summary record: {err}")))?;
            kv.put(&key, &row, None)
                .await
                .map_err(|err| node_error(format!("kv put failed: {err}")))?;
            Ok(true)
        }
    })
    .await
    .ok_or_else(|| NodeError::new("record_summary missing ResourceAccess context"))??;

    Ok(SummaryRecord { stored, ..record })
}

/// Terminal capture: the durable effects are the sent email and the KV record.
#[def_node(
    name = "Capture",
    summary = "Capture the run's summary record",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture(record: SummaryRecord) -> NodeResult<SummaryRecord> {
    Ok(record)
}

dag_macros::flow! {
    name: s25_form_feedback_summary_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Clone: a manually triggered form-feedback summary — read form responses from a sheet, aggregate answers per question, summarize with an LLM, and email the report; recorded idempotently under an operator run_id";

    let feedback_trigger = node!(feedback_trigger);
    let load_feedback = node!(load_feedback);
    let aggregate_feedback = node!(aggregate_feedback);
    let summarize_feedback = node!(summarize_feedback);
    let render_report = node!(render_report);
    let send_report = node!(send_report);
    let record_summary = node!(record_summary);
    let capture = node!(capture);

    connect!(feedback_trigger -> load_feedback);
    connect!(load_feedback -> aggregate_feedback);
    connect!(aggregate_feedback -> summarize_feedback);
    connect!(summarize_feedback -> render_report);
    connect!(render_report -> send_report);
    connect!(send_report -> record_summary);
    connect!(record_summary -> capture);

    entrypoint!({
        trigger: "feedback_trigger",
        capture: "capture",
        route_aliases: ["/feedback/summary"],
        method: "POST",
        deadline_ms: 60_000,
    });
}

#[cfg(test)]
mod tests;
