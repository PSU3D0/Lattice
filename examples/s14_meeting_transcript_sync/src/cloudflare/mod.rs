#[cfg(not(target_arch = "wasm32"))]
mod d1_store;
mod r2_uploader;
#[cfg(target_arch = "wasm32")]
mod scheduled;

/// On Cloudflare Workers (wasm32) the D1 transcript job store is provided by
/// the generic SQL-backed [`SqlTranscriptJobStore`] wired through the
/// `cap-sql-workers-d1` capability provider. On native targets we keep the
/// rusqlite-based implementation under `d1_store` for round-trip tests.
#[cfg(target_arch = "wasm32")]
pub use crate::sql_store::SqlTranscriptJobStore as D1TranscriptJobStore;
#[cfg(not(target_arch = "wasm32"))]
pub use d1_store::D1TranscriptJobStore;
pub use r2_uploader::R2TranscriptUploader;
#[cfg(target_arch = "wasm32")]
pub use scheduled::{execute_scheduled_event, scheduled_tick_from_event};
