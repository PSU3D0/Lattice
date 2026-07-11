use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;

use crate::generated::types::{GoogleDriveSearchFilesInput, GoogleDriveSearchFilesOutput};
use crate::ops::GoogleDriveSearchFiles;

#[def_node(
    name = "GoogleDriveSearchFiles",
    summary = "Search files by Drive query string, returning sharing metadata per file",
    identifier = "connector.google.drive.search_files",
    connector_ops(crate::ops::GoogleDriveSearchFiles)
)]
pub async fn google_drive_search_files(
    input: GoogleDriveSearchFilesInput,
) -> NodeResult<GoogleDriveSearchFilesOutput> {
    GoogleDriveSearchFiles::invoke(&input)
        .await
        .map_err(|err| NodeError::new(format!("connector.google.drive.search_files failed: {err}")))
}
