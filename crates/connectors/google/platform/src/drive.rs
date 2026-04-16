use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const GOOGLE_DRIVE_BASE_URL: &str = "https://www.googleapis.com";
pub const GOOGLE_DRIVE_READONLY_SCOPE: &str = "https://www.googleapis.com/auth/drive.readonly";
pub const GOOGLE_DOCS_DOCUMENT_MIME_TYPE: &str = "application/vnd.google-apps.document";
pub const GOOGLE_DRIVE_EXPORT_TEXT_PLAIN_MIME_TYPE: &str = "text/plain";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GoogleDriveQuery {
    clauses: Vec<String>,
}

impl GoogleDriveQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn full_text_contains(mut self, value: &str) -> Self {
        self.clauses.push(format!(
            "fullText contains '{}'",
            escape_drive_query_literal(value)
        ));
        self
    }

    pub fn mime_type_eq(mut self, value: &str) -> Self {
        self.clauses.push(format!(
            "mimeType = '{}'",
            escape_drive_query_literal(value)
        ));
        self
    }

    pub fn modified_time_gte(mut self, value: &str) -> Self {
        self.clauses.push(format!(
            "modifiedTime >= '{}'",
            escape_drive_query_literal(value)
        ));
        self
    }

    pub fn modified_time_lte(mut self, value: &str) -> Self {
        self.clauses.push(format!(
            "modifiedTime <= '{}'",
            escape_drive_query_literal(value)
        ));
        self
    }

    pub fn name_contains(mut self, value: &str) -> Self {
        self.clauses.push(format!(
            "name contains '{}'",
            escape_drive_query_literal(value)
        ));
        self
    }

    pub fn parent_in(mut self, parent_id: &str) -> Self {
        self.clauses.push(format!(
            "'{}' in parents",
            escape_drive_query_literal(parent_id)
        ));
        self
    }

    pub fn trashed(mut self, value: bool) -> Self {
        self.clauses.push(format!(
            "trashed = {}",
            if value { "true" } else { "false" }
        ));
        self
    }

    pub fn render(&self) -> String {
        self.clauses.join(" and ")
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GoogleDriveFilesListQuery {
    pub corpora: Option<String>,
    pub drive_id: Option<String>,
    pub fields: Option<String>,
    pub include_items_from_all_drives: Option<bool>,
    pub order_by: Option<String>,
    pub page_size: Option<u32>,
    pub page_token: Option<String>,
    pub q: Option<String>,
    pub spaces: Option<String>,
    pub supports_all_drives: Option<bool>,
}

impl GoogleDriveFilesListQuery {
    pub fn to_query_pairs(&self) -> Vec<(String, String)> {
        let mut pairs = Vec::new();
        push_optional_string(&mut pairs, "q", self.q.as_deref());
        push_optional_string(&mut pairs, "fields", self.fields.as_deref());
        push_optional_u32(&mut pairs, "pageSize", self.page_size);
        push_optional_string(&mut pairs, "pageToken", self.page_token.as_deref());
        push_optional_string(&mut pairs, "orderBy", self.order_by.as_deref());
        push_optional_string(&mut pairs, "spaces", self.spaces.as_deref());
        push_optional_string(&mut pairs, "corpora", self.corpora.as_deref());
        push_optional_string(&mut pairs, "driveId", self.drive_id.as_deref());
        push_optional_bool(
            &mut pairs,
            "includeItemsFromAllDrives",
            self.include_items_from_all_drives,
        );
        push_optional_bool(&mut pairs, "supportsAllDrives", self.supports_all_drives);
        pairs
    }
}

pub const fn drive_files_path() -> &'static str {
    "/drive/v3/files"
}

pub fn drive_file_export_path(file_id: &str) -> String {
    format!("/drive/v3/files/{}/export", encode_path_segment(file_id))
}

pub fn drive_file_path(file_id: &str) -> String {
    format!("/drive/v3/files/{}", encode_path_segment(file_id))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoogleDriveFilesListResponse {
    #[serde(default)]
    pub files: Vec<GoogleDriveFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoogleDriveFile {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_time: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub export_links: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default)]
    pub owners: Vec<GoogleDriveUser>,
    #[serde(default)]
    pub parents: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shortcut_details: Option<GoogleDriveShortcutDetails>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trashed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub web_view_link: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoogleDriveShortcutDetails {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_mime_type: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoogleDriveUser {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email_address: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub me: Option<bool>,
}

fn escape_drive_query_literal(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

fn push_optional_bool(pairs: &mut Vec<(String, String)>, name: &str, value: Option<bool>) {
    if let Some(value) = value {
        pairs.push((name.to_string(), value.to_string()));
    }
}

fn push_optional_string(pairs: &mut Vec<(String, String)>, name: &str, value: Option<&str>) {
    if let Some(value) = value {
        pairs.push((name.to_string(), value.to_string()));
    }
}

fn push_optional_u32(pairs: &mut Vec<(String, String)>, name: &str, value: Option<u32>) {
    if let Some(value) = value {
        pairs.push((name.to_string(), value.to_string()));
    }
}

fn encode_path_segment(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drive_files_list_query_has_no_default_posture() {
        assert!(GoogleDriveFilesListQuery::default()
            .to_query_pairs()
            .is_empty());
    }

    #[test]
    fn drive_query_builder_escapes_literals_and_renders_clauses() {
        let query = GoogleDriveQuery::new()
            .trashed(false)
            .mime_type_eq(GOOGLE_DOCS_DOCUMENT_MIME_TYPE)
            .name_contains("team's meeting notes")
            .full_text_contains(r#"Sprint \\ retrospective"#)
            .modified_time_gte("2026-04-16T00:00:00Z")
            .modified_time_lte("2026-04-17T00:00:00Z")
            .parent_in("folder-123")
            .render();

        assert_eq!(
            query,
            "trashed = false and mimeType = 'application/vnd.google-apps.document' and name contains 'team\\'s meeting notes' and fullText contains 'Sprint \\\\\\\\ retrospective' and modifiedTime >= '2026-04-16T00:00:00Z' and modifiedTime <= '2026-04-17T00:00:00Z' and 'folder-123' in parents"
        );
    }

    #[test]
    fn drive_files_list_query_serializes_only_explicit_options() {
        let query = GoogleDriveFilesListQuery {
            corpora: Some("allDrives".to_string()),
            drive_id: Some("drive-123".to_string()),
            fields: Some("files(id,name,mimeType)".to_string()),
            include_items_from_all_drives: Some(true),
            order_by: Some("modifiedTime desc".to_string()),
            page_size: Some(10),
            page_token: Some("page-3".to_string()),
            q: Some(
                GoogleDriveQuery::new()
                    .trashed(false)
                    .mime_type_eq(GOOGLE_DOCS_DOCUMENT_MIME_TYPE)
                    .render(),
            ),
            spaces: Some("drive".to_string()),
            supports_all_drives: Some(true),
        };

        assert_eq!(
            query.to_query_pairs(),
            vec![
                (
                    "q".to_string(),
                    "trashed = false and mimeType = 'application/vnd.google-apps.document'"
                        .to_string(),
                ),
                (
                    "fields".to_string(),
                    "files(id,name,mimeType)".to_string(),
                ),
                ("pageSize".to_string(), "10".to_string()),
                ("pageToken".to_string(), "page-3".to_string()),
                ("orderBy".to_string(), "modifiedTime desc".to_string()),
                ("spaces".to_string(), "drive".to_string()),
                ("corpora".to_string(), "allDrives".to_string()),
                ("driveId".to_string(), "drive-123".to_string()),
                (
                    "includeItemsFromAllDrives".to_string(),
                    "true".to_string(),
                ),
                ("supportsAllDrives".to_string(), "true".to_string()),
            ]
        );
    }

    #[test]
    fn drive_paths_encode_file_ids() {
        assert_eq!(drive_file_path("doc/123"), "/drive/v3/files/doc%2F123");
        assert_eq!(
            drive_file_export_path("doc/123"),
            "/drive/v3/files/doc%2F123/export"
        );
    }

    #[test]
    fn drive_files_response_deserializes_realistic_payload() {
        let response: GoogleDriveFilesListResponse = serde_json::from_str(
            r#"{
                "files": [
                    {
                        "id": "drive-doc-1",
                        "name": "Weekly staff sync transcript",
                        "mimeType": "application/vnd.google-apps.document",
                        "webViewLink": "https://docs.google.com/document/d/drive-doc-1/edit",
                        "createdTime": "2026-04-16T18:55:10.123Z",
                        "modifiedTime": "2026-04-16T19:02:44.000Z",
                        "trashed": false,
                        "parents": ["folder-abc"],
                        "owners": [
                            {
                                "displayName": "Meeting Bot",
                                "emailAddress": "meeting-bot@example.com",
                                "me": true
                            }
                        ],
                        "exportLinks": {
                            "text/plain": "https://www.googleapis.com/drive/v3/files/drive-doc-1/export?mimeType=text/plain",
                            "application/pdf": "https://www.googleapis.com/drive/v3/files/drive-doc-1/export?mimeType=application/pdf"
                        }
                    },
                    {
                        "id": "drive-shortcut-1",
                        "name": "Transcript shortcut",
                        "mimeType": "application/vnd.google-apps.shortcut",
                        "shortcutDetails": {
                            "targetId": "drive-doc-1",
                            "targetMimeType": "application/vnd.google-apps.document"
                        }
                    }
                ],
                "nextPageToken": "next-drive-page"
            }"#,
        )
        .expect("drive response should deserialize");

        assert_eq!(response.next_page_token.as_deref(), Some("next-drive-page"));
        assert_eq!(response.files.len(), 2);

        let document = &response.files[0];
        assert_eq!(document.id, "drive-doc-1");
        assert_eq!(document.parents, vec!["folder-abc".to_string()]);
        assert_eq!(
            document.export_links.get(GOOGLE_DRIVE_EXPORT_TEXT_PLAIN_MIME_TYPE),
            Some(&"https://www.googleapis.com/drive/v3/files/drive-doc-1/export?mimeType=text/plain".to_string())
        );

        let shortcut = &response.files[1];
        assert_eq!(shortcut.id, "drive-shortcut-1");
        assert_eq!(
            shortcut
                .shortcut_details
                .as_ref()
                .and_then(|details| details.target_id.as_deref()),
            Some("drive-doc-1")
        );
    }
}
