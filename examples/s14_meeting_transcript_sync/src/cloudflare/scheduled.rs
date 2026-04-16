#![cfg(target_arch = "wasm32")]

use anyhow::{Context, anyhow};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::TranscriptSyncSummary;
use crate::execution::TranscriptSyncExecutor;
use crate::scheduled::ScheduledTick;

pub fn scheduled_tick_from_event(event: &worker::ScheduledEvent) -> anyhow::Result<ScheduledTick> {
    let scheduled_ms = event.schedule();
    if !scheduled_ms.is_finite() {
        return Err(anyhow!("scheduled event time is not finite"));
    }

    let scheduled_at = OffsetDateTime::from_unix_timestamp((scheduled_ms / 1000.0).floor() as i64)
        .context("convert scheduled event timestamp")?
        .format(&Rfc3339)
        .map_err(|error| anyhow!("format scheduled event timestamp: {error}"))?;

    Ok(ScheduledTick::new(scheduled_at, event.cron()))
}

pub async fn execute_scheduled_event(
    executor: &TranscriptSyncExecutor,
    event: &worker::ScheduledEvent,
) -> anyhow::Result<TranscriptSyncSummary> {
    let tick = scheduled_tick_from_event(event)?;
    executor.execute_scheduled_tick(&tick).await
}
