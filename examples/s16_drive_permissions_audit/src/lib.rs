//! S16 — pilot clone: daily Google Drive sharing audit.
//!
//! Behavioral spec (independent Rust implementation of an audited workflow's
//! behavior; no third-party workflow content is embedded here):
//!
//! 1. A **cron trigger** fires once a day at 06:00 UTC.
//! 2. A new tab named `audit-YYYYMMDD` (date derived from the fire's
//!    `scheduled_time_ms`, never wall clock) is added to a fixed audit
//!    spreadsheet via `connector.google.sheets.create_sheet`.
//! 3. `connector.google.drive.search_files` fetches files modified in the 24h
//!    window before the fire, including each file's sharing metadata.
//! 4. A pure node flags risk: files granted to `anyone` (public link) or
//!    shared with accounts outside the internal domain, then flattens each
//!    non-owner grant into one audit row.
//! 5. Rows are appended to the day's tab via
//!    `connector.google.sheets.append_row`.
//! 6. A plain-text summary email is sent via
//!    `connector.google.gmail.send_message`.
//! 7. A terminal KV upsert records the fire, keyed
//!    `<flow>:<trigger>:{scheduled_time_ms}` (the s15 idempotency pattern).
//!
//! ## Idempotency
//!
//! Every effectful payload is a pure function of `scheduled_time_ms`: the tab
//! title, the Drive query window, the email subject/body, and the terminal KV
//! key. A redelivery of the same fire therefore replays byte-identical
//! requests (and the KV record dedupes to one row). Cross-fire, distinct
//! scheduled times produce distinct tabs/keys. Provider-side dedupe of the
//! sheet/email writes is the delivery gate's job (`Delivery::ExactlyOnce`
//! composition is proven per-op in each connector's honesty tests).

use capabilities::context;
use connector_google_drive::ops::GoogleDriveSearchFiles;
use connector_google_drive::{GoogleDriveFileHit, GoogleDriveSearchFilesInput};
use connector_google_gmail::GoogleGmailSendMessageInput;
use connector_google_gmail::ops::GoogleGmailSendMessage;
use connector_google_sheets::ops::{GoogleSheetsAppendRow, GoogleSheetsCreateSheet};
use connector_google_sheets::{GoogleSheetsAppendRowInput, GoogleSheetsCreateSheetInput};
use dag_core::{NodeError, NodeResult, ScheduledEvent};
use dag_macros::{def_node, node};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Cadence: daily at 06:00 UTC (Cloudflare cron dialect).
pub const SCHEDULE_CRON: &str = "0 6 * * *";
pub const TRIGGER_ALIAS: &str = "audit_trigger";
pub const FLOW_NAME: &str = "s16_drive_permissions_audit_flow";

/// The fixed spreadsheet collecting one tab per daily audit. Real deployments
/// would source this from config/bindings; a constant keeps the example
/// self-contained.
pub const AUDIT_SPREADSHEET_ID: &str = "drive-audit-log";
/// Where the daily report goes.
pub const REPORT_RECIPIENT: &str = "secops@lattice-pilot.test";
/// Grants to addresses outside this domain count as external shares.
pub const INTERNAL_DOMAIN: &str = "lattice-pilot.test";
/// Bound page size for the Drive search so one fire stays bounded.
const MAX_FILES: u32 = 100;

// ---------------------------------------------------------------------------
// Flow data types
// ---------------------------------------------------------------------------

/// Everything downstream nodes need, derived once from the fire.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AuditPlan {
    pub scheduled_time_ms: u64,
    pub cron: String,
    /// `YYYY-MM-DD`, derived from `scheduled_time_ms`.
    pub date: String,
    /// `audit-YYYYMMDD` — stable across redeliveries of one fire.
    pub sheet_title: String,
    /// Drive query for the 24h window preceding the fire.
    pub drive_query: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AuditSheet {
    pub plan: AuditPlan,
    pub spreadsheet_id: String,
    pub sheet_id: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DriveScan {
    pub sheet: AuditSheet,
    pub files: Vec<GoogleDriveFileHit>,
}

/// One flattened audit row: a single non-owner grant on a flagged file.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AuditRow {
    pub file_id: String,
    pub file_name: String,
    /// Grant audience: `anyone`, `user`, `group`, `domain`, or `unknown`.
    pub share_type: String,
    pub user_id: String,
    pub user: String,
    pub role: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AuditFindings {
    pub sheet: AuditSheet,
    pub rows: Vec<AuditRow>,
    pub files_flagged: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AppendedAudit {
    pub findings: AuditFindings,
    pub rows_appended: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ReportedAudit {
    pub scheduled_time_ms: u64,
    pub date: String,
    pub files_flagged: u32,
    pub rows_appended: u32,
    pub message_id: String,
}

/// Terminal capture: what the KV write did for this fire.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AuditRecord {
    /// `<flow>:<trigger>:{scheduled_time_ms}`.
    pub key: String,
    /// `false` on a redelivery of an already-recorded fire.
    pub stored: bool,
    pub scheduled_time_ms: u64,
    pub files_flagged: u32,
    pub rows_appended: u32,
    pub message_id: String,
}

// ---------------------------------------------------------------------------
// Pure helpers (all deterministic in scheduled_time_ms)
// ---------------------------------------------------------------------------

/// The idempotency key for one scheduled fire (spec shape
/// `<flow>:<trigger>:{scheduled_time_ms}`).
pub fn audit_key(scheduled_time_ms: u64) -> String {
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

fn is_external_address(address: &str) -> bool {
    let domain = address.rsplit('@').next().unwrap_or("");
    !domain.eq_ignore_ascii_case(INTERNAL_DOMAIN)
}

/// Risk rule: a file is flagged when any grant is to `anyone` (public link),
/// or the file is marked shared and any grant names an address outside the
/// internal domain.
pub fn file_is_flagged(file: &GoogleDriveFileHit) -> bool {
    let public = file
        .permissions
        .iter()
        .any(|p| p.grantee_type.as_deref() == Some("anyone"));
    let external = file.shared == Some(true)
        && file
            .permissions
            .iter()
            .any(|p| p.email_address.as_deref().is_some_and(is_external_address));
    public || external
}

/// Flatten a flagged file into rows: one per non-owner grant.
pub fn rows_for_file(file: &GoogleDriveFileHit) -> Vec<AuditRow> {
    file.permissions
        .iter()
        .filter(|p| p.role.as_deref() != Some("owner"))
        .map(|p| AuditRow {
            file_id: file.id.clone(),
            file_name: file.name.clone().unwrap_or_default(),
            share_type: p.grantee_type.clone().unwrap_or_else(|| "unknown".into()),
            user_id: p.permission_id.clone().unwrap_or_default(),
            user: p.email_address.clone().unwrap_or_else(|| "unknown".into()),
            role: p.role.clone().unwrap_or_else(|| "unknown".into()),
        })
        .collect()
}

/// Report body — a pure function of the findings (and thus of the fire).
pub fn report_text(findings: &AuditFindings) -> String {
    let date = &findings.sheet.plan.date;
    let mut text = format!(
        "Automated Drive sharing audit for {date}.\n\n\
         Flagged files: {}\nAudit rows written: {}\n",
        findings.files_flagged,
        findings.rows.len()
    );

    let public: Vec<&AuditRow> = findings
        .rows
        .iter()
        .filter(|row| row.share_type == "anyone")
        .collect();
    if !public.is_empty() {
        text.push_str("\nOpen to anyone with the link:\n");
        for row in public {
            text.push_str(&format!("- {} ({})\n", row.file_name, row.file_id));
        }
    }

    let external: Vec<&AuditRow> = findings
        .rows
        .iter()
        .filter(|row| row.share_type == "user" && is_external_address(&row.user))
        .collect();
    if !external.is_empty() {
        text.push_str("\nShared with external accounts:\n");
        for row in external {
            text.push_str(&format!(
                "- {} ({}) -> {}\n",
                row.file_name, row.file_id, row.user
            ));
        }
    }

    text.push_str(&format!(
        "\nFull detail: spreadsheet `{}`, tab `{}`.\n",
        findings.sheet.spreadsheet_id, findings.sheet.plan.sheet_title
    ));
    text
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
    name = "AuditTrigger",
    summary = "Cron ingress; receives the typed ScheduledEvent for this fire",
    effects = "Pure",
    determinism = "Strict"
)]
async fn audit_trigger(event: ScheduledEvent) -> NodeResult<ScheduledEvent> {
    Ok(event)
}

/// Derive the whole audit plan from the fire — every downstream payload is a
/// pure function of `scheduled_time_ms`.
#[def_node(
    name = "PlanAudit",
    summary = "Derive date, tab title, and Drive query window from the fire",
    effects = "Pure",
    determinism = "Strict"
)]
async fn plan_audit(event: ScheduledEvent) -> NodeResult<AuditPlan> {
    let date = utc_date(event.scheduled_time_ms);
    let window_start = utc_rfc3339(event.scheduled_time_ms.saturating_sub(86_400_000));
    Ok(AuditPlan {
        sheet_title: format!("audit-{}", date.replace('-', "")),
        drive_query: format!("modifiedTime > '{window_start}' and trashed = false"),
        date,
        scheduled_time_ms: event.scheduled_time_ms,
        cron: event.cron,
    })
}

/// Add the day's tab to the audit spreadsheet. The title is derived from the
/// fire's scheduled time, so a redelivery replays the identical request.
#[def_node(
    name = "CreateAuditSheet",
    identifier = "connector.google.sheets.create_audit_sheet",
    summary = "Add the dated audit tab via connector.google.sheets.create_sheet",
    connector_ops(GoogleSheetsCreateSheet)
)]
async fn create_audit_sheet(plan: AuditPlan) -> NodeResult<AuditSheet> {
    let created = GoogleSheetsCreateSheet::invoke(&GoogleSheetsCreateSheetInput {
        spreadsheet_id: AUDIT_SPREADSHEET_ID.to_string(),
        title: plan.sheet_title.clone(),
        index: None,
        row_count: None,
        column_count: None,
    })
    .await
    .map_err(|err| {
        node_error(format!(
            "connector.google.sheets.create_sheet failed: {err}"
        ))
    })?;

    Ok(AuditSheet {
        plan,
        spreadsheet_id: created.spreadsheet_id,
        sheet_id: created.sheet.sheet_id,
    })
}

/// Fetch recently modified files (with sharing metadata) for the fire's 24h
/// window.
#[def_node(
    name = "FetchRecentFiles",
    identifier = "connector.google.drive.fetch_recent_files",
    summary = "Search the last day's modified files via connector.google.drive.search_files",
    connector_ops(GoogleDriveSearchFiles)
)]
async fn fetch_recent_files(sheet: AuditSheet) -> NodeResult<DriveScan> {
    let found = GoogleDriveSearchFiles::invoke(&GoogleDriveSearchFilesInput {
        query: sheet.plan.drive_query.clone(),
        page_size: Some(MAX_FILES),
    })
    .await
    .map_err(|err| node_error(format!("connector.google.drive.search_files failed: {err}")))?;

    Ok(DriveScan {
        sheet,
        files: found.items,
    })
}

/// Pure risk filter + row flattening.
#[def_node(
    name = "FlagExternalShares",
    summary = "Flag public/externally shared files and flatten non-owner grants into rows",
    effects = "Pure",
    determinism = "Strict"
)]
async fn flag_external_shares(scan: DriveScan) -> NodeResult<AuditFindings> {
    let flagged: Vec<&GoogleDriveFileHit> = scan
        .files
        .iter()
        .filter(|file| file_is_flagged(file))
        .collect();
    let rows: Vec<AuditRow> = flagged
        .iter()
        .flat_map(|file| rows_for_file(file))
        .collect();

    Ok(AuditFindings {
        files_flagged: flagged.len() as u32,
        sheet: scan.sheet,
        rows,
    })
}

/// Append one row per flagged grant to the day's tab.
#[def_node(
    name = "AppendAuditRows",
    identifier = "connector.google.sheets.append_audit_rows",
    summary = "Append the audit rows via connector.google.sheets.append_row",
    connector_ops(GoogleSheetsAppendRow)
)]
async fn append_audit_rows(findings: AuditFindings) -> NodeResult<AppendedAudit> {
    let mut rows_appended = 0u32;
    for row in &findings.rows {
        GoogleSheetsAppendRow::invoke(&GoogleSheetsAppendRowInput {
            spreadsheet_id: findings.sheet.spreadsheet_id.clone(),
            sheet: findings.sheet.plan.sheet_title.clone(),
            row: json!({
                "file_id": row.file_id,
                "file_name": row.file_name,
                "share_type": row.share_type,
                "user_id": row.user_id,
                "user": row.user,
                "role": row.role,
            }),
            header_row: 1,
            value_input_option: None,
        })
        .await
        .map_err(|err| node_error(format!("connector.google.sheets.append_row failed: {err}")))?;
        rows_appended += 1;
    }

    Ok(AppendedAudit {
        findings,
        rows_appended,
    })
}

/// Email the day's report. Subject and body are pure functions of the fire.
#[def_node(
    name = "SendAuditReport",
    identifier = "connector.google.gmail.send_audit_report",
    summary = "Send the audit summary via connector.google.gmail.send_message",
    connector_ops(GoogleGmailSendMessage)
)]
async fn send_audit_report(appended: AppendedAudit) -> NodeResult<ReportedAudit> {
    let plan = &appended.findings.sheet.plan;
    let sent = GoogleGmailSendMessage::invoke(&GoogleGmailSendMessageInput {
        to: REPORT_RECIPIENT.to_string(),
        cc: None,
        bcc: None,
        subject: format!("Drive sharing audit — {}", plan.date),
        text_body: report_text(&appended.findings),
    })
    .await
    .map_err(|err| node_error(format!("connector.google.gmail.send_message failed: {err}")))?;

    Ok(ReportedAudit {
        scheduled_time_ms: plan.scheduled_time_ms,
        date: plan.date.clone(),
        files_flagged: appended.findings.files_flagged,
        rows_appended: appended.rows_appended,
        message_id: sent.id,
    })
}

/// Terminal KV upsert keyed on the fire's scheduled time (s15 pattern):
/// redeliveries of one fire collapse to a single record.
#[def_node(
    name = "RecordAudit",
    summary = "Upsert the fire's audit record into KV, keyed on scheduled_time_ms",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(
        kv_read(capabilities::kv::KeyValue),
        kv_write(capabilities::kv::KeyValue)
    )
)]
async fn record_audit(report: ReportedAudit) -> NodeResult<AuditRecord> {
    let key = audit_key(report.scheduled_time_ms);
    let record = AuditRecord {
        key: key.clone(),
        stored: true,
        scheduled_time_ms: report.scheduled_time_ms,
        files_flagged: report.files_flagged,
        rows_appended: report.rows_appended,
        message_id: report.message_id,
    };

    let stored = context::with_current_async(|resources| {
        let key = key.clone();
        let record = record.clone();
        async move {
            let kv = resources.kv().ok_or_else(|| {
                NodeError::new(
                    "record_audit requires a KV capability (declare resource::kv::write)",
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
                .map_err(|err| node_error(format!("serialize audit record: {err}")))?;
            kv.put(&key, &row, None)
                .await
                .map_err(|err| node_error(format!("kv put failed: {err}")))?;
            Ok(true)
        }
    })
    .await
    .ok_or_else(|| NodeError::new("record_audit missing ResourceAccess context"))??;

    Ok(AuditRecord { stored, ..record })
}

/// Terminal capture: logged by the scheduler; the durable effects are the
/// sheet rows, the email, and the KV record.
#[def_node(
    name = "Capture",
    summary = "Capture the fire's audit record",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture(record: AuditRecord) -> NodeResult<AuditRecord> {
    Ok(record)
}

dag_macros::flow! {
    name: s16_drive_permissions_audit_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Pilot clone: daily 06:00 UTC Drive sharing audit — flag public/external shares, log rows to a dated sheet tab, email a report; payloads keyed on scheduled_time_ms";

    let audit_trigger = node!(audit_trigger);
    let plan_audit = node!(plan_audit);
    let create_audit_sheet = node!(create_audit_sheet);
    let fetch_recent_files = node!(fetch_recent_files);
    let flag_external_shares = node!(flag_external_shares);
    let append_audit_rows = node!(append_audit_rows);
    let send_audit_report = node!(send_audit_report);
    let record_audit = node!(record_audit);
    let capture = node!(capture);

    connect!(audit_trigger -> plan_audit);
    connect!(plan_audit -> create_audit_sheet);
    connect!(create_audit_sheet -> fetch_recent_files);
    connect!(fetch_recent_files -> flag_external_shares);
    connect!(flag_external_shares -> append_audit_rows);
    connect!(append_audit_rows -> send_audit_report);
    connect!(send_audit_report -> record_audit);
    connect!(record_audit -> capture);

    entrypoint!({
        trigger: "audit_trigger",
        capture: "capture",
        schedule: "0 6 * * *",
        deadline_ms: 60_000,
    });
}

#[cfg(test)]
mod tests;
