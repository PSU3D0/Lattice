use crate::config::TranscriptSyncConfig;
use crate::domain::{TranscriptSyncRequest, TranscriptSyncSummary};
use crate::engine::{self, TranscriptSyncServices};
use crate::scheduled::{ScheduledTick, request_for_scheduled_tick};

#[derive(Clone)]
pub struct TranscriptSyncExecutor {
    config: TranscriptSyncConfig,
    services: TranscriptSyncServices,
}

impl TranscriptSyncExecutor {
    pub fn new(config: TranscriptSyncConfig, services: TranscriptSyncServices) -> Self {
        Self { config, services }
    }

    pub fn config(&self) -> &TranscriptSyncConfig {
        &self.config
    }

    pub async fn execute(
        &self,
        request: TranscriptSyncRequest,
    ) -> anyhow::Result<TranscriptSyncSummary> {
        engine::run_reconcile(&self.services, &self.config, &request).await
    }

    pub fn request_for_scheduled_tick(
        &self,
        tick: &ScheduledTick,
    ) -> anyhow::Result<TranscriptSyncRequest> {
        request_for_scheduled_tick(&self.config, tick)
    }

    pub async fn execute_scheduled_tick(
        &self,
        tick: &ScheduledTick,
    ) -> anyhow::Result<TranscriptSyncSummary> {
        let request = self.request_for_scheduled_tick(tick)?;
        self.execute(request).await
    }
}
