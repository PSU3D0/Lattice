//! S21 — clone of n8n shortlist template #2 ("AI CV Screening").
//!
//! Behavioral spec (independent Rust implementation of an audited workflow's
//! behavior; no third-party workflow content is embedded here):
//!
//! 1. A **webhook (application form) trigger** receives a candidate submission:
//!    name, email, salary expectation, LinkedIn URL, the CV file name, and the
//!    a host-staged `cv: Artifact<Exact>` produced by bounded multipart ingress.
//! 2. `connector.llm.complete` scores the resume against a fixed job
//!    description and returns a plain-text compatibility rating + hire
//!    recommendation (the op this clone is the first flow to consume).
//! 3. The candidate row (name, email, expectation, LinkedIn, CV file name, AI
//!    rating) is appended to a fixed Google Sheet via
//!    `connector.google.sheets.append_row`.
//! 4. A confirmation email is sent to the candidate via
//!    `connector.google.gmail.send_message`.
//! 5. A notification email carrying the candidate details + AI rating is sent
//!    to HR via a second `connector.google.gmail.send_message`.
//! 6. A terminal **KV upsert** records the delivery keyed
//!    `<flow>:<trigger>:{email}` — the application's natural idempotency key.
//!
//! ## Checked PDF boundary
//!
//! The public request boundary carries application text fields plus `cv: Artifact<Exact>`.
//! S21's inline `extract_cv_text` node bounded-reads and hash-compares the staged PDF before
//! invoking the host-provided, capability-less sandboxed transform. PDF bytes never enter
//! invocation JSON, node output, checkpoints, or logs. The node remains application-local;
//! only the byte plane and transform runtimes are reusable infrastructure.
//!
//! ## Idempotency posture
//!
//! Every effectful payload is a pure function of the submission. After successful
//! extraction, a sequential webhook redelivery reads the terminal KV record and returns it
//! without replaying LLM, Sheets, or Gmail effects. This is the established sequential
//! redelivery posture; concurrent deliveries are not claimed to be atomically suppressed.
//! The LLM call is `Nondeterministic`; its first result is retained in the terminal record.

use capabilities::artifact::{Artifact, Exact};
use capabilities::context;
use connector_google_gmail::GoogleGmailSendMessageInput;
use connector_google_gmail::ops::GoogleGmailSendMessage;
use connector_google_sheets::GoogleSheetsAppendRowInput;
use connector_google_sheets::ops::GoogleSheetsAppendRow;
use connector_llm::ops::LlmComplete;
use connector_llm::{LlmCompleteInput, LlmProvider};
use dag_core::{NodeError, NodeResult};
use dag_macros::{def_node, node};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub mod pdf_extraction;
#[cfg(feature = "host-bundle")]
pub use pdf_extraction::extract_cv_text_register;
pub use pdf_extraction::{
    extract_cv_text, extract_cv_text_Input, extract_cv_text_Output, extract_cv_text_node_spec,
};

pub const FLOW_NAME: &str = "s21_ai_cv_screening_flow";
pub const TRIGGER_ALIAS: &str = "screening_trigger";

/// The role the fixed job description screens for. Our own constant — not
/// template text.
pub const JOB_TITLE: &str = "Software Engineer";
/// The HR mailbox that receives the review notification (our own test domain).
pub const HR_RECIPIENT: &str = "hiring@lattice-pilot.test";
/// The fixed spreadsheet + tab collecting screened candidates. Real
/// deployments would source these from config/bindings; constants keep the
/// example self-contained.
pub const CV_SPREADSHEET_ID: &str = "cv-screening-candidates";
pub const CV_SHEET: &str = "Candidates";
/// The Gemini model, reached through an OpenAI-compatible endpoint (the lock
/// selects the base URL; the dialect is `openai_compat`).
pub const LLM_MODEL: &str = "gemini-1.5-flash";

// ---------------------------------------------------------------------------
// Flow data types
// ---------------------------------------------------------------------------

/// The inbound webhook application submission. PDF bytes remain in the
/// workspace byte plane; only this exact artifact handle enters graph JSON.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CvApplication {
    pub full_name: String,
    pub email: String,
    #[serde(default)]
    pub expectation: String,
    #[serde(default)]
    pub linkedin: String,
    #[serde(default)]
    pub cv_filename: String,
    pub cv: Artifact<Exact>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExtractedCvApplication {
    pub application: CvApplication,
    pub resume_text: String,
    pub page_count: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScreeningDispatch {
    pub route: String,
    pub application: Option<ExtractedCvApplication>,
    pub existing: Option<ScreeningRecord>,
}

/// After the LLM screening: the application plus the rating text.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RatedCandidate {
    pub application: CvApplication,
    pub ai_rating: String,
    pub model: String,
}

/// After the sheet append.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordedCandidate {
    pub application: CvApplication,
    pub ai_rating: String,
    pub appended_range: String,
}

/// After the candidate confirmation email.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfirmedCandidate {
    pub application: CvApplication,
    pub ai_rating: String,
    pub appended_range: String,
    pub candidate_message_id: String,
}

/// After the HR notification email.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NotifiedScreening {
    pub application: CvApplication,
    pub ai_rating: String,
    pub appended_range: String,
    pub candidate_message_id: String,
    pub hr_message_id: String,
}

/// Terminal record of the whole fire.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScreeningRecord {
    /// `<flow>:<trigger>:{email}`.
    pub key: String,
    /// `false` on a redelivery of an already-recorded application.
    pub stored: bool,
    pub email: String,
    pub ai_rating: String,
    pub appended_range: String,
    pub candidate_message_id: String,
    pub hr_message_id: String,
}

// ---------------------------------------------------------------------------
// Pure helpers (all effectful payloads are pure functions of the submission)
// ---------------------------------------------------------------------------

/// The idempotency key for one application delivery.
pub fn screening_key(email: &str) -> String {
    format!("{FLOW_NAME}:{TRIGGER_ALIAS}:{email}")
}

/// System instructions for the screening model — our own wording.
pub fn analyst_system() -> String {
    "You are a senior technical recruiter. Rate how well the candidate fits the \
     role on a scale of 1 (poor) to 10 (excellent), then give a one-line \
     interview recommendation with a brief reason. Reply in plain text only, no \
     markdown, under 75 words."
        .to_string()
}

/// The screening prompt — a pure function of the role and the resume text.
pub fn screening_prompt(application: &ExtractedCvApplication) -> String {
    format!(
        "Role under consideration: {JOB_TITLE}\n\nCandidate resume:\n{}\n\nAssess the candidate's fit for the role.",
        application.resume_text
    )
}

/// The candidate confirmation email `(subject, body)` — our own wording.
pub fn confirmation_email(application: &CvApplication) -> (String, String) {
    let subject = "We received your application".to_string();
    let body = format!(
        "Hello {},\n\nThank you for applying for the {JOB_TITLE} role. We have received your \
         CV and will review it shortly.\n\nBest regards,\nRecruiting",
        application.full_name
    );
    (subject, body)
}

/// The HR notification email `(subject, body)` — our own wording. Carries the
/// candidate details plus the AI rating.
pub fn hr_email(application: &CvApplication, ai_rating: &str) -> (String, String) {
    let subject = format!("New {JOB_TITLE} candidate for review");
    let body = format!(
        "A new application has been received.\n\nName: {}\nEmail: {}\nExpectation: {}\nLinkedIn: {}\nCV file: {}\n\nAI screening:\n{ai_rating}\n",
        application.full_name,
        application.email,
        application.expectation,
        application.linkedin,
        application.cv_filename,
    );
    (subject, body)
}

/// The candidate row appended to the sheet — our own snake_case column keys
/// (not template field strings).
pub fn candidate_row(application: &CvApplication, ai_rating: &str) -> serde_json::Value {
    json!({
        "full_name": application.full_name,
        "email": application.email,
        "expectation": application.expectation,
        "linkedin": application.linkedin,
        "cv_filename": application.cv_filename,
        "ai_rating": ai_rating,
    })
}

fn node_error(err: impl std::fmt::Display) -> NodeError {
    NodeError::new(err.to_string())
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

/// Webhook ingress: passes the typed application through.
#[def_node(
    trigger,
    name = "ScreeningTrigger",
    summary = "Webhook ingress; receives application fields plus a host-staged PDF artifact",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(workspace_write(capabilities::workspace::Workspace))
)]
async fn screening_trigger(application: CvApplication) -> NodeResult<CvApplication> {
    Ok(application)
}

/// Check the natural terminal key only after the PDF passes checked extraction.
#[def_node(
    name = "CheckRedelivery",
    summary = "Return an existing terminal record for sequential redelivery",
    effects = "ReadOnly",
    determinism = "BestEffort",
    resources(kv_read(capabilities::kv::KeyValue))
)]
async fn check_redelivery(application: ExtractedCvApplication) -> NodeResult<ScreeningDispatch> {
    let key = screening_key(&application.application.email);
    let existing = context::with_current_async(move |resources| async move {
        let kv = resources
            .kv()
            .ok_or_else(|| NodeError::new("check_redelivery requires KV"))?;
        let value = kv
            .get(&key)
            .await
            .map_err(|err| node_error(format!("kv get failed: {err}")))?;
        value
            .map(|bytes| serde_json::from_slice::<ScreeningRecord>(&bytes))
            .transpose()
            .map_err(|err| node_error(format!("decode screening record: {err}")))
    })
    .await
    .ok_or_else(|| NodeError::new("check_redelivery missing ResourceAccess context"))??;

    if let Some(mut record) = existing {
        record.stored = false;
        Ok(ScreeningDispatch {
            route: "existing".to_string(),
            application: None,
            existing: Some(record),
        })
    } else {
        Ok(ScreeningDispatch {
            route: "new".to_string(),
            application: Some(application),
            existing: None,
        })
    }
}

/// Score the resume via `connector.llm.complete`.
#[def_node(
    name = "RateCandidate",
    identifier = "connector.llm.rate_candidate",
    summary = "Screen the resume against the job description via connector.llm.complete",
    connector_ops(LlmComplete)
)]
async fn rate_candidate(dispatch: ScreeningDispatch) -> NodeResult<RatedCandidate> {
    let extracted = dispatch
        .application
        .ok_or_else(|| NodeError::new("new screening dispatch is missing application"))?;
    let completion = LlmComplete::invoke(&LlmCompleteInput {
        provider: LlmProvider::OpenaiCompat,
        model: LLM_MODEL.to_string(),
        prompt: screening_prompt(&extracted),
        system: Some(analyst_system()),
        temperature: Some(0.2),
        max_tokens: Some(256),
        output_schema: None,
    })
    .await
    .map_err(|err| node_error(format!("connector.llm.complete failed: {err}")))?;

    Ok(RatedCandidate {
        application: extracted.application,
        ai_rating: completion.text,
        model: completion.model,
    })
}

/// Append the candidate row to the sheet.
#[def_node(
    name = "RecordCandidate",
    identifier = "connector.google.sheets.record_candidate",
    summary = "Append the candidate row via connector.google.sheets.append_row",
    connector_ops(GoogleSheetsAppendRow)
)]
async fn record_candidate(rated: RatedCandidate) -> NodeResult<RecordedCandidate> {
    let appended = GoogleSheetsAppendRow::invoke(&GoogleSheetsAppendRowInput {
        spreadsheet_id: CV_SPREADSHEET_ID.to_string(),
        sheet: CV_SHEET.to_string(),
        row: candidate_row(&rated.application, &rated.ai_rating),
        header_row: 1,
        value_input_option: None,
    })
    .await
    .map_err(|err| node_error(format!("connector.google.sheets.append_row failed: {err}")))?;

    Ok(RecordedCandidate {
        application: rated.application,
        ai_rating: rated.ai_rating,
        appended_range: appended.updated_range,
    })
}

/// Email the candidate a confirmation of receipt.
#[def_node(
    name = "ConfirmCandidate",
    identifier = "connector.google.gmail.confirm_candidate",
    summary = "Email the candidate a receipt via connector.google.gmail.send_message",
    connector_ops(GoogleGmailSendMessage)
)]
async fn confirm_candidate(recorded: RecordedCandidate) -> NodeResult<ConfirmedCandidate> {
    let (subject, text_body) = confirmation_email(&recorded.application);
    let sent = GoogleGmailSendMessage::invoke(&GoogleGmailSendMessageInput {
        to: recorded.application.email.clone(),
        cc: None,
        bcc: None,
        subject,
        text_body,
    })
    .await
    .map_err(|err| node_error(format!("connector.google.gmail.send_message failed: {err}")))?;

    Ok(ConfirmedCandidate {
        application: recorded.application,
        ai_rating: recorded.ai_rating,
        appended_range: recorded.appended_range,
        candidate_message_id: sent.id,
    })
}

/// Email HR the candidate details plus the AI rating.
#[def_node(
    name = "NotifyHr",
    identifier = "connector.google.gmail.notify_hr",
    summary = "Email HR the candidate + rating via connector.google.gmail.send_message",
    connector_ops(GoogleGmailSendMessage)
)]
async fn notify_hr(confirmed: ConfirmedCandidate) -> NodeResult<NotifiedScreening> {
    let (subject, text_body) = hr_email(&confirmed.application, &confirmed.ai_rating);
    let sent = GoogleGmailSendMessage::invoke(&GoogleGmailSendMessageInput {
        to: HR_RECIPIENT.to_string(),
        cc: None,
        bcc: None,
        subject,
        text_body,
    })
    .await
    .map_err(|err| node_error(format!("connector.google.gmail.send_message failed: {err}")))?;

    Ok(NotifiedScreening {
        application: confirmed.application,
        ai_rating: confirmed.ai_rating,
        appended_range: confirmed.appended_range,
        candidate_message_id: confirmed.candidate_message_id,
        hr_message_id: sent.id,
    })
}

/// Terminal KV upsert keyed on the applicant email: redeliveries of one
/// application collapse to a single record.
#[def_node(
    name = "RecordScreening",
    summary = "Upsert the delivery record into KV, keyed on the applicant email",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(
        kv_read(capabilities::kv::KeyValue),
        kv_write(capabilities::kv::KeyValue)
    )
)]
async fn record_screening(notified: NotifiedScreening) -> NodeResult<ScreeningRecord> {
    let key = screening_key(&notified.application.email);
    let record = ScreeningRecord {
        key: key.clone(),
        stored: true,
        email: notified.application.email.clone(),
        ai_rating: notified.ai_rating.clone(),
        appended_range: notified.appended_range.clone(),
        candidate_message_id: notified.candidate_message_id.clone(),
        hr_message_id: notified.hr_message_id.clone(),
    };

    let stored_now = context::with_current_async(|resources| {
        let key = key.clone();
        let record = record.clone();
        async move {
            let kv = resources.kv().ok_or_else(|| {
                NodeError::new(
                    "record_screening requires a KV capability (declare resource::kv::write)",
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
                .map_err(|err| node_error(format!("serialize screening record: {err}")))?;
            kv.put(&key, &row, None)
                .await
                .map_err(|err| node_error(format!("kv put failed: {err}")))?;
            Ok(true)
        }
    })
    .await
    .ok_or_else(|| NodeError::new("record_screening missing ResourceAccess context"))??;

    Ok(ScreeningRecord {
        stored: stored_now,
        ..record
    })
}

#[def_node(
    name = "CaptureRedelivery",
    summary = "Return the existing terminal record without replaying provider effects",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture_redelivery(dispatch: ScreeningDispatch) -> NodeResult<ScreeningRecord> {
    dispatch
        .existing
        .ok_or_else(|| NodeError::new("existing screening dispatch is missing record"))
}

/// Terminal capture: the durable effects are the sheet row, the two emails, and
/// the KV record.
#[def_node(
    name = "Capture",
    summary = "Capture the screening record",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture(record: ScreeningRecord) -> NodeResult<ScreeningRecord> {
    Ok(record)
}

dag_macros::flow! {
    name: s21_ai_cv_screening_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Clone of n8n template #2 (AI CV Screening): a webhook application form that scores the resume via an LLM, appends the candidate to a Google Sheet, emails the candidate a receipt, and emails HR the candidate details + rating; keyed on the applicant email";

    let screening_trigger = node!(screening_trigger);
    let extract_cv_text = node!(extract_cv_text);
    let check_redelivery = node!(check_redelivery);
    let capture_redelivery = node!(capture_redelivery);
    let rate_candidate = node!(rate_candidate);
    let record_candidate = node!(record_candidate);
    let confirm_candidate = node!(confirm_candidate);
    let notify_hr = node!(notify_hr);
    let record_screening = node!(record_screening);
    let capture = node!(capture);

    connect!(screening_trigger -> extract_cv_text);
    connect!(extract_cv_text -> check_redelivery);
    connect!(check_redelivery -> rate_candidate);
    connect!(check_redelivery -> capture_redelivery);
    switch!(
        source = check_redelivery,
        selector_pointer = "/route",
        cases = { "new" => rate_candidate, "existing" => capture_redelivery }
    );
    connect!(rate_candidate -> record_candidate);
    connect!(record_candidate -> confirm_candidate);
    connect!(confirm_candidate -> notify_hr);
    connect!(notify_hr -> record_screening);
    connect!(record_screening -> capture);
    connect!(capture_redelivery -> capture);

    entrypoint!({
        trigger: "screening_trigger",
        capture: "capture",
        route_aliases: ["/cv-screening"],
        method: "POST",
        deadline_ms: 20_000,
    });
}

#[cfg(test)]
mod tests;
