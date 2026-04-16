use anyhow::{Context, anyhow};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::config::TranscriptSyncConfig;
use crate::domain::TranscriptSyncRequest;

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ScheduledTick {
    pub scheduled_at: String,
    pub cron: String,
}

impl ScheduledTick {
    pub fn new(scheduled_at: impl Into<String>, cron: impl Into<String>) -> Self {
        Self {
            scheduled_at: scheduled_at.into(),
            cron: cron.into(),
        }
    }

    pub fn source_label(&self) -> String {
        let cron = self.cron.trim();
        if cron.is_empty() {
            "scheduled".to_string()
        } else {
            format!("cron:{cron}")
        }
    }
}

pub fn request_for_scheduled_tick(
    config: &TranscriptSyncConfig,
    tick: &ScheduledTick,
) -> anyhow::Result<TranscriptSyncRequest> {
    let scheduled_at = OffsetDateTime::parse(&tick.scheduled_at, &Rfc3339)
        .with_context(|| format!("parse scheduled tick timestamp `{}`", tick.scheduled_at))?;
    let window_start =
        scheduled_at - time::Duration::minutes(i64::from(config.sync_lookback_minutes));

    Ok(TranscriptSyncRequest {
        org_scope: config.org_scope.clone(),
        window_start: window_start
            .format(&Rfc3339)
            .map_err(|error| anyhow!("format scheduled tick window_start: {error}"))?,
        window_end: scheduled_at
            .format(&Rfc3339)
            .map_err(|error| anyhow!("format scheduled tick window_end: {error}"))?,
        source: tick.source_label(),
        backfill_reason: None,
    })
}
