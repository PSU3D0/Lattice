use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GoogleDrivePermissionSummary {
    pub permission_id: Option<String>,
    /// Who the permission is granted to: `user`, `group`, `domain`, or
    /// `anyone` (Drive API `permissions[].type`).
    pub grantee_type: Option<String>,
    pub role: Option<String>,
    pub email_address: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GoogleDriveFileHit {
    pub id: String,
    pub name: Option<String>,
    pub mime_type: Option<String>,
    pub shared: Option<bool>,
    pub web_view_link: Option<String>,
    pub permissions: Vec<GoogleDrivePermissionSummary>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GoogleDriveSearchFilesInput {
    /// Drive search query string (the `q` parameter of `files.list`).
    pub query: String,
    pub page_size: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GoogleDriveSearchFilesOutput {
    pub items: Vec<GoogleDriveFileHit>,
    /// Present when the search matched more files than one page returned.
    pub next_page_token: Option<String>,
}
