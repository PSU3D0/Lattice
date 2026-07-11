//! Live smoke — env-gated, NEVER part of default CI.
//!
//! Both gates must be opened deliberately:
//! - the test is `#[ignore]`d, so `cargo test -p connector_google_drive`
//!   skips it even when credentials are present;
//! - it asserts `LATTICE_LIVE_SMOKE=1`, so `--ignored` sweeps cannot hit the
//!   real API by accident.
//!
//! Run:
//! ```sh
//! LATTICE_LIVE_SMOKE=1 \
//! LATTICE_CONNECTOR_AUTH_GOOGLE_WORKSPACE_AUTH=ya29... \
//!   cargo test -p connector_google_drive --test live_smoke -- --ignored --nocapture
//! ```
//!
//! Read-only by design: `search_files` proves auth + endpoint + decode against
//! the real API without touching any file.

use std::sync::Arc;

use cap_http_reqwest::ReqwestHttpClient;
use capabilities::{ResourceBag, context};
use connector_google_drive::runtime::transport::EnvConnectorRuntime;
use connector_google_drive::{GoogleDriveSearchFilesInput, google_drive_search_files};

fn live_resources() -> Arc<ResourceBag> {
    let client = Arc::new(ReqwestHttpClient::default());
    Arc::new(
        ResourceBag::default()
            .with_http_read(Arc::clone(&client))
            .with_http_write(client)
            .with_connector_runtime(Arc::new(EnvConnectorRuntime))
            .with_connector_scope(capabilities::connector::ConnectorBindingScope::new(
                "flow://live-smoke",
                "live_smoke",
                "connector.google.drive.search_files",
                "connector.google.drive",
            )),
    )
}

#[tokio::test]
#[ignore = "live smoke: requires LATTICE_LIVE_SMOKE=1 and network access"]
async fn search_files_against_live_api() {
    assert_eq!(
        std::env::var("LATTICE_LIVE_SMOKE").as_deref(),
        Ok("1"),
        "set LATTICE_LIVE_SMOKE=1 to run live smoke deliberately"
    );

    let output = context::with_resources(live_resources(), async {
        google_drive_search_files(GoogleDriveSearchFilesInput {
            query: "trashed = false".to_string(),
            page_size: Some(5),
        })
        .await
        .expect("live search succeeds")
    })
    .await;

    println!("live smoke: search returned {} files", output.items.len());
}
