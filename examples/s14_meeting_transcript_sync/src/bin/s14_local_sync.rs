#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    native::run().await
}

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::path::Path;
    use std::sync::Arc;

    use anyhow::Context;
    use cap_http_reqwest::ReqwestHttpClient;
    use cap_sql_sqlx_sqlite::SqlxSqlite;
    use example_s14_meeting_transcript_sync::live::{
        GoogleCalendarMeetingSource, GoogleDriveTranscriptResolver, GoogleDriveTranscriptUploader,
        GoogleOAuthRefreshTokenProvider, LiveTranscriptFetcher, LocalSyncConfig,
        StaticBearerTokenProvider, ZoomServerToServerTokenProvider,
    };
    use example_s14_meeting_transcript_sync::sql_store::SqlTranscriptJobStore;
    use example_s14_meeting_transcript_sync::{
        ScheduledTick, TranscriptSyncConfig, TranscriptSyncExecutor, TranscriptSyncServices,
    };
    use time::format_description::well_known::Rfc3339;
    use time::{Duration, OffsetDateTime};

    pub async fn run() -> anyhow::Result<()> {
        let local = LocalSyncConfig::from_env();
        if let Some(parent) = Path::new(&local.ledger_path).parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create ledger parent directory {}", parent.display()))?;
        }

        let http = Arc::new(
            ReqwestHttpClient::with_default_tls().context("build reqwest HTTP capability")?,
        );
        let http_read = http.clone();
        let http_write = http.clone();

        let google_auth: Arc<dyn example_s14_meeting_transcript_sync::live::BearerTokenProvider> =
            if let Ok(token) = std::env::var("GOOGLE_ACCESS_TOKEN") {
                Arc::new(StaticBearerTokenProvider::new(token))
            } else {
                Arc::new(GoogleOAuthRefreshTokenProvider::from_env(
                    http_write.clone(),
                )?)
            };
        let zoom_auth = Arc::new(ZoomServerToServerTokenProvider::from_env(
            http_write.clone(),
        )?);

        let mut config = TranscriptSyncConfig::default();
        config.org_scope = local.org_scope.clone();
        config.calendar_ids = local.calendar_ids.clone();
        config.sync_batch_limit = local.batch_limit;
        // Destination is Google Drive in this local-live path; keep the existing string-shaped
        // config field as an operator-readable destination hint.
        config.destination_prefix = format!("gdrive://{}", local.destination_folder_name);

        let ledger = Arc::new(
            SqlxSqlite::connect(&local.ledger_path)
                .with_context(|| format!("open local SQL ledger at {}", local.ledger_path))?,
        );
        let store =
            SqlTranscriptJobStore::new_with_setup(ledger.clone(), ledger.clone(), ledger.as_ref())
                .await
                .with_context(|| format!("initialize local SQL ledger at {}", local.ledger_path))?;
        let services = TranscriptSyncServices::new(
            Arc::new(GoogleCalendarMeetingSource::new(
                http_read.clone(),
                google_auth.clone(),
            )),
            Arc::new(GoogleDriveTranscriptResolver::new(
                http_read.clone(),
                google_auth.clone(),
            )),
            Arc::new(LiveTranscriptFetcher::new(
                http_read.clone(),
                google_auth.clone(),
                zoom_auth,
            )),
            Arc::new(GoogleDriveTranscriptUploader::new(
                http_read,
                http_write,
                google_auth,
                local.destination_folder_name.clone(),
                local.destination_parent_folder_id.clone(),
            )),
            Arc::new(store),
        );
        let executor = TranscriptSyncExecutor::new(config, services);

        let now = OffsetDateTime::now_utc();
        let tick = ScheduledTick::new(
            now.format(&Rfc3339).context("format scheduled tick time")?,
            "local-manual",
        );
        let mut request = executor.request_for_scheduled_tick(&tick)?;
        request.window_start = (now - Duration::days(local.lookback_days))
            .format(&Rfc3339)
            .context("format local lookback start")?;
        request.window_end = now.format(&Rfc3339).context("format local window end")?;
        request.source = "local_live".to_string();

        let summary = executor.execute(request).await?;
        println!("{}", serde_json::to_string_pretty(&summary)?);
        Ok(())
    }
}
