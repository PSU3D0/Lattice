#![cfg(not(target_arch = "wasm32"))]

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, anyhow, bail};
use async_trait::async_trait;
use capabilities::http::{HttpMethod, HttpRead, HttpRequest, HttpResponse, HttpWrite};
use connector_google_platform::calendar::{
    GOOGLE_CALENDAR_BASE_URL, GoogleCalendarEvent, GoogleCalendarEventOrderBy,
    GoogleCalendarEventsListQuery, GoogleCalendarEventsListResponse, calendar_events_path,
};
use connector_google_platform::drive::{
    GOOGLE_DOCS_DOCUMENT_MIME_TYPE, GOOGLE_DRIVE_BASE_URL,
    GOOGLE_DRIVE_EXPORT_TEXT_PLAIN_MIME_TYPE, GoogleDriveFile, GoogleDriveFilesListQuery,
    GoogleDriveFilesListResponse, GoogleDriveQuery, drive_file_export_path, drive_files_path,
};
use connector_zoom_platform::ZoomRequestConfig;
use connector_zoom_platform::auth::server_to_server_token_request_with_config;
use connector_zoom_platform::transcripts::{
    ZoomMeetingRecordings, meeting_recordings_request_with_config,
    transcript_download_request_with_config,
};
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};

use crate::adapters::{
    FetchOutcome, MeetingSource, SourceResolution, TranscriptFetcher, TranscriptSourceResolver,
    TranscriptUploader,
};
use crate::config::TranscriptSyncConfig;
use crate::domain::{
    CompletedMeeting, ConferenceKind, ConferenceLocator, TranscriptArtifact, TranscriptSourceKind,
    TranscriptSourceRef, UploadedTranscript,
};

const FORM_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'&')
    .add(b'+')
    .add(b'/')
    .add(b'=')
    .add(b'?');

const QUERY_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'&')
    .add(b'+')
    .add(b'=');

const GOOGLE_DRIVE_FOLDER_MIME_TYPE: &str = "application/vnd.google-apps.folder";
const GOOGLE_OAUTH_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";

#[async_trait]
pub trait BearerTokenProvider: Send + Sync {
    async fn bearer_token(&self) -> anyhow::Result<String>;
}

#[derive(Clone, Debug)]
pub struct StaticBearerTokenProvider {
    token: String,
}

impl StaticBearerTokenProvider {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }
}

#[async_trait]
impl BearerTokenProvider for StaticBearerTokenProvider {
    async fn bearer_token(&self) -> anyhow::Result<String> {
        Ok(self.token.clone())
    }
}

#[derive(Clone)]
pub struct GoogleOAuthRefreshTokenProvider {
    http_write: Arc<dyn HttpWrite>,
    client_id: String,
    client_secret: String,
    refresh_token: String,
}

impl GoogleOAuthRefreshTokenProvider {
    pub fn new(
        http_write: Arc<dyn HttpWrite>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        refresh_token: impl Into<String>,
    ) -> Self {
        Self {
            http_write,
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            refresh_token: refresh_token.into(),
        }
    }

    pub fn from_env(http_write: Arc<dyn HttpWrite>) -> anyhow::Result<Self> {
        Ok(Self::new(
            http_write,
            require_env("GOOGLE_OAUTH_CLIENT_ID")?,
            require_env("GOOGLE_OAUTH_CLIENT_SECRET")?,
            require_env("GOOGLE_OAUTH_REFRESH_TOKEN")?,
        ))
    }
}

#[derive(Debug, Deserialize)]
struct GoogleOAuthTokenResponse {
    access_token: String,
}

#[async_trait]
impl BearerTokenProvider for GoogleOAuthRefreshTokenProvider {
    async fn bearer_token(&self) -> anyhow::Result<String> {
        let body = form_urlencode(&[
            ("grant_type", "refresh_token"),
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
            ("refresh_token", &self.refresh_token),
        ]);
        let request = HttpRequest::new(HttpMethod::Post, GOOGLE_OAUTH_TOKEN_URL)
            .with_header("content-type", "application/x-www-form-urlencoded")
            .with_header("accept", "application/json")
            .with_body(body.into_bytes());
        let response = self
            .http_write
            .send(request)
            .await
            .map_err(|err| anyhow!("exchange Google refresh token: {err}"))?;
        ensure_success(&response, "Google OAuth token exchange")?;
        let token: GoogleOAuthTokenResponse =
            serde_json::from_slice(&response.body).context("decode Google OAuth token response")?;
        Ok(token.access_token)
    }
}

#[derive(Clone)]
pub struct ZoomServerToServerTokenProvider {
    http_write: Arc<dyn HttpWrite>,
    account_id: String,
    client_id: String,
    client_secret: String,
    request_config: ZoomRequestConfig,
}

impl ZoomServerToServerTokenProvider {
    pub fn new(
        http_write: Arc<dyn HttpWrite>,
        account_id: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Self {
        Self {
            http_write,
            account_id: account_id.into(),
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            request_config: ZoomRequestConfig::default(),
        }
    }

    pub fn from_env(http_write: Arc<dyn HttpWrite>) -> anyhow::Result<Self> {
        Ok(Self::new(
            http_write,
            require_env("ZOOM_ACCOUNT_ID")?,
            require_env("ZOOM_CLIENT_ID")?,
            require_env("ZOOM_CLIENT_SECRET")?,
        ))
    }
}

#[derive(Debug, Deserialize)]
struct ZoomTokenResponse {
    access_token: String,
}

#[async_trait]
impl BearerTokenProvider for ZoomServerToServerTokenProvider {
    async fn bearer_token(&self) -> anyhow::Result<String> {
        let request = server_to_server_token_request_with_config(
            &self.account_id,
            &self.client_id,
            &self.client_secret,
            &self.request_config,
        );
        let response = self
            .http_write
            .send(request)
            .await
            .map_err(|err| anyhow!("exchange Zoom server-to-server token: {err}"))?;
        ensure_success(&response, "Zoom OAuth token exchange")?;
        let token: ZoomTokenResponse =
            serde_json::from_slice(&response.body).context("decode Zoom OAuth token response")?;
        Ok(token.access_token)
    }
}

#[derive(Clone)]
pub struct GoogleCalendarMeetingSource {
    http_read: Arc<dyn HttpRead>,
    auth: Arc<dyn BearerTokenProvider>,
}

impl GoogleCalendarMeetingSource {
    pub fn new(http_read: Arc<dyn HttpRead>, auth: Arc<dyn BearerTokenProvider>) -> Self {
        Self { http_read, auth }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl MeetingSource for GoogleCalendarMeetingSource {
    async fn fetch_recent_completed_meetings(
        &self,
        request: &crate::domain::TranscriptSyncRequest,
        config: &TranscriptSyncConfig,
    ) -> anyhow::Result<Vec<CompletedMeeting>> {
        let mut meetings = Vec::new();
        for calendar_id in &config.calendar_ids {
            let query = GoogleCalendarEventsListQuery {
                max_results: Some(config.sync_batch_limit.max(1).min(2500)),
                order_by: Some(GoogleCalendarEventOrderBy::StartTime),
                single_events: Some(true),
                time_min: Some(request.window_start.clone()),
                time_max: Some(request.window_end.clone()),
                ..GoogleCalendarEventsListQuery::default()
            };
            let url = google_api_url(
                GOOGLE_CALENDAR_BASE_URL,
                &calendar_events_path(calendar_id),
                &query.to_query_pairs(),
            );
            let response = self.google_get(&url).await?;
            let listed: GoogleCalendarEventsListResponse = serde_json::from_slice(&response.body)
                .with_context(|| {
                format!("decode Google Calendar events for `{calendar_id}`")
            })?;
            meetings.extend(
                listed
                    .items
                    .into_iter()
                    .filter_map(|event| completed_meeting_from_google_event(calendar_id, event)),
            );
        }
        Ok(meetings)
    }
}

impl GoogleCalendarMeetingSource {
    async fn google_get(&self, url: &str) -> anyhow::Result<HttpResponse> {
        let token = self.auth.bearer_token().await?;
        let request = HttpRequest::new(HttpMethod::Get, url)
            .with_header("accept", "application/json")
            .with_header("authorization", format!("Bearer {token}"));
        let response = self
            .http_read
            .send(request)
            .await
            .map_err(|err| anyhow!("Google Calendar request failed: {err}"))?;
        ensure_success(&response, "Google Calendar request")?;
        Ok(response)
    }
}

#[derive(Clone)]
pub struct GoogleDriveTranscriptResolver {
    http_read: Arc<dyn HttpRead>,
    auth: Arc<dyn BearerTokenProvider>,
}

impl GoogleDriveTranscriptResolver {
    pub fn new(http_read: Arc<dyn HttpRead>, auth: Arc<dyn BearerTokenProvider>) -> Self {
        Self { http_read, auth }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl TranscriptSourceResolver for GoogleDriveTranscriptResolver {
    async fn resolve(
        &self,
        meeting: &CompletedMeeting,
        locator: &ConferenceLocator,
        config: &TranscriptSyncConfig,
    ) -> anyhow::Result<SourceResolution> {
        match locator.kind {
            ConferenceKind::GoogleMeet => self.resolve_google_meet(meeting, config).await,
            ConferenceKind::Zoom => {
                let Some(meeting_id) = locator.zoom_meeting_id.clone() else {
                    return Ok(SourceResolution::PermanentFailure {
                        code: "zoom_meeting_id_missing".to_string(),
                        message: "Zoom meeting was classified but no numeric meeting id was found"
                            .to_string(),
                    });
                };
                Ok(SourceResolution::Resolved(
                    TranscriptSourceRef::zoom_transcript(
                        meeting_id.clone(),
                        format!("zoom://meeting/{meeting_id}/transcript"),
                    ),
                ))
            }
            ConferenceKind::Unknown => Ok(SourceResolution::PermanentFailure {
                code: "unsupported_conference_kind".to_string(),
                message: "cannot resolve transcript source for unknown conference kind".to_string(),
            }),
        }
    }
}

impl GoogleDriveTranscriptResolver {
    async fn resolve_google_meet(
        &self,
        meeting: &CompletedMeeting,
        config: &TranscriptSyncConfig,
    ) -> anyhow::Result<SourceResolution> {
        let attachment_candidates = google_doc_attachments(meeting);
        if attachment_candidates.len() == 1 {
            let file = &attachment_candidates[0];
            return Ok(SourceResolution::Resolved(TranscriptSourceRef::google_doc(
                file.id.clone(),
                file.web_view_link
                    .clone()
                    .unwrap_or_else(|| format!("https://drive.google.com/open?id={}", file.id)),
            )));
        }
        if attachment_candidates.len() > 1 {
            return Ok(SourceResolution::Ambiguous {
                candidates: attachment_candidates
                    .into_iter()
                    .map(|file| {
                        TranscriptSourceRef::google_doc(
                            file.id,
                            file.web_view_link
                                .unwrap_or_else(|| "drive://unknown".to_string()),
                        )
                    })
                    .collect(),
            });
        }

        let mut candidates = Vec::new();
        for pattern in &config.gmeet_doc_title_patterns {
            let query = GoogleDriveQuery::new()
                .trashed(false)
                .mime_type_eq(GOOGLE_DOCS_DOCUMENT_MIME_TYPE)
                .name_contains(pattern)
                .modified_time_gte(&date_shift(&meeting.scheduled_end_at, -1)?)
                .modified_time_lte(&date_shift(&meeting.scheduled_end_at, 3)?)
                .render();
            let response = self
                .drive_files_list(GoogleDriveFilesListQuery {
                    q: Some(query),
                    fields: Some(
                        "files(id,name,mimeType,modifiedTime,webViewLink),nextPageToken"
                            .to_string(),
                    ),
                    page_size: Some(10),
                    order_by: Some("modifiedTime desc".to_string()),
                    supports_all_drives: Some(true),
                    include_items_from_all_drives: Some(true),
                    ..GoogleDriveFilesListQuery::default()
                })
                .await?;
            candidates.extend(response.files.into_iter().filter(|file| {
                let name = file.name.as_deref().unwrap_or_default().to_lowercase();
                name.contains(&meeting.title.to_lowercase()) || name.contains("notes by gemini")
            }));
            if !candidates.is_empty() {
                break;
            }
        }

        match candidates.len() {
            0 => Ok(SourceResolution::NotFoundYet),
            1 => {
                let file = candidates.remove(0);
                Ok(SourceResolution::Resolved(TranscriptSourceRef::google_doc(
                    file.id,
                    file.web_view_link
                        .unwrap_or_else(|| "drive://unknown".to_string()),
                )))
            }
            _ => Ok(SourceResolution::Ambiguous {
                candidates: candidates
                    .into_iter()
                    .map(|file| {
                        TranscriptSourceRef::google_doc(
                            file.id,
                            file.web_view_link
                                .unwrap_or_else(|| "drive://unknown".to_string()),
                        )
                    })
                    .collect(),
            }),
        }
    }

    async fn drive_files_list(
        &self,
        query: GoogleDriveFilesListQuery,
    ) -> anyhow::Result<GoogleDriveFilesListResponse> {
        let response = self
            .google_get(&google_api_url(
                GOOGLE_DRIVE_BASE_URL,
                drive_files_path(),
                &query.to_query_pairs(),
            ))
            .await?;
        serde_json::from_slice(&response.body).context("decode Google Drive files list response")
    }

    async fn google_get(&self, url: &str) -> anyhow::Result<HttpResponse> {
        let token = self.auth.bearer_token().await?;
        let request = HttpRequest::new(HttpMethod::Get, url)
            .with_header("accept", "application/json")
            .with_header("authorization", format!("Bearer {token}"));
        let response = self
            .http_read
            .send(request)
            .await
            .map_err(|err| anyhow!("Google Drive request failed: {err}"))?;
        ensure_success(&response, "Google Drive request")?;
        Ok(response)
    }
}

#[derive(Clone)]
pub struct LiveTranscriptFetcher {
    http_read: Arc<dyn HttpRead>,
    google_auth: Arc<dyn BearerTokenProvider>,
    zoom_auth: Arc<dyn BearerTokenProvider>,
    zoom_config: ZoomRequestConfig,
}

impl LiveTranscriptFetcher {
    pub fn new(
        http_read: Arc<dyn HttpRead>,
        google_auth: Arc<dyn BearerTokenProvider>,
        zoom_auth: Arc<dyn BearerTokenProvider>,
    ) -> Self {
        Self {
            http_read,
            google_auth,
            zoom_auth,
            zoom_config: ZoomRequestConfig::default(),
        }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl TranscriptFetcher for LiveTranscriptFetcher {
    async fn fetch(
        &self,
        source: &TranscriptSourceRef,
        _config: &TranscriptSyncConfig,
    ) -> anyhow::Result<FetchOutcome> {
        match source.kind {
            TranscriptSourceKind::GoogleDocTranscript => self.fetch_google_doc(source).await,
            TranscriptSourceKind::ZoomTranscript => self.fetch_zoom_transcript(source).await,
            TranscriptSourceKind::Unknown => Ok(FetchOutcome::PermanentFailure {
                code: "unknown_transcript_source".to_string(),
                message: "cannot fetch unknown transcript source".to_string(),
            }),
        }
    }
}

impl LiveTranscriptFetcher {
    async fn fetch_google_doc(&self, source: &TranscriptSourceRef) -> anyhow::Result<FetchOutcome> {
        let Some(file_id) = source.source_id.as_deref() else {
            return Ok(FetchOutcome::PermanentFailure {
                code: "google_doc_id_missing".to_string(),
                message: "Google Doc transcript source omitted source_id".to_string(),
            });
        };
        let token = self.google_auth.bearer_token().await?;
        let url = google_api_url(
            GOOGLE_DRIVE_BASE_URL,
            &drive_file_export_path(file_id),
            &[(
                "mimeType".to_string(),
                GOOGLE_DRIVE_EXPORT_TEXT_PLAIN_MIME_TYPE.to_string(),
            )],
        );
        let request = HttpRequest::new(HttpMethod::Get, url)
            .with_header("authorization", format!("Bearer {token}"));
        let response = self
            .http_read
            .send(request)
            .await
            .map_err(|err| anyhow!("export Google Doc transcript: {err}"))?;
        if response.status == 403 || response.status == 404 {
            return Ok(FetchOutcome::PermanentFailure {
                code: "google_doc_export_unavailable".to_string(),
                message: format!(
                    "Google Doc `{file_id}` could not be exported with current credentials (HTTP {})",
                    response.status
                ),
            });
        }
        ensure_success(&response, "Google Doc export")?;
        let text = String::from_utf8(response.body).context("Google Doc export was not UTF-8")?;
        Ok(FetchOutcome::Ready(TranscriptArtifact {
            text,
            normalized: json!({
                "source_kind": "google_doc",
                "source_id": source.source_id,
                "source_uri": source.source_uri,
            }),
            source_ref: source.clone(),
        }))
    }

    async fn fetch_zoom_transcript(
        &self,
        source: &TranscriptSourceRef,
    ) -> anyhow::Result<FetchOutcome> {
        let Some(meeting_id) = source.source_id.as_deref() else {
            return Ok(FetchOutcome::PermanentFailure {
                code: "zoom_meeting_id_missing".to_string(),
                message: "Zoom transcript source omitted source_id".to_string(),
            });
        };
        let access_token = self.zoom_auth.bearer_token().await?;
        let request =
            meeting_recordings_request_with_config(meeting_id, &access_token, &self.zoom_config);
        let response = self
            .http_read
            .send(request)
            .await
            .map_err(|err| anyhow!("request Zoom meeting recordings: {err}"))?;
        if response.status == 404 {
            return Ok(FetchOutcome::NotReady);
        }
        ensure_success(&response, "Zoom meeting recordings")?;
        let recordings: ZoomMeetingRecordings =
            serde_json::from_slice(&response.body).context("decode Zoom meeting recordings")?;
        let Some(transcript_file) = recordings.transcript_file() else {
            return Ok(FetchOutcome::NotReady);
        };
        let Some(download_url) = transcript_file.download_url.as_deref() else {
            return Ok(FetchOutcome::PermanentFailure {
                code: "zoom_transcript_download_url_missing".to_string(),
                message: "Zoom recording transcript file did not include a download_url"
                    .to_string(),
            });
        };
        let download_token = recordings
            .download_access_token
            .as_deref()
            .unwrap_or(&access_token);
        let request = transcript_download_request_with_config(
            download_url,
            download_token,
            &self.zoom_config,
        );
        let response = self
            .http_read
            .send(request)
            .await
            .map_err(|err| anyhow!("download Zoom transcript: {err}"))?;
        ensure_success(&response, "Zoom transcript download")?;
        let text =
            String::from_utf8(response.body).context("Zoom transcript download was not UTF-8")?;
        Ok(FetchOutcome::Ready(TranscriptArtifact {
            text,
            normalized: json!({
                "source_kind": "zoom_recording_transcript",
                "source_id": source.source_id,
                "download_url": download_url,
                "recording_file_id": transcript_file.id,
                "recording_file_type": transcript_file.file_type,
                "recording_type": transcript_file.recording_type,
            }),
            source_ref: source.clone(),
        }))
    }
}

#[derive(Clone)]
pub struct GoogleDriveTranscriptUploader {
    http_read: Arc<dyn HttpRead>,
    http_write: Arc<dyn HttpWrite>,
    auth: Arc<dyn BearerTokenProvider>,
    root_folder_name: String,
    parent_folder_id: Option<String>,
}

impl GoogleDriveTranscriptUploader {
    pub fn new(
        http_read: Arc<dyn HttpRead>,
        http_write: Arc<dyn HttpWrite>,
        auth: Arc<dyn BearerTokenProvider>,
        root_folder_name: impl Into<String>,
        parent_folder_id: Option<String>,
    ) -> Self {
        Self {
            http_read,
            http_write,
            auth,
            root_folder_name: root_folder_name.into(),
            parent_folder_id,
        }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl TranscriptUploader for GoogleDriveTranscriptUploader {
    async fn upload(
        &self,
        meeting: &CompletedMeeting,
        artifact: &TranscriptArtifact,
        config: &TranscriptSyncConfig,
    ) -> anyhow::Result<UploadedTranscript> {
        let root = self
            .ensure_folder(&self.root_folder_name, self.parent_folder_id.as_deref())
            .await?;
        let ended_at = OffsetDateTime::parse(&meeting.scheduled_end_at, &Rfc3339)
            .with_context(|| format!("parse meeting end `{}`", meeting.scheduled_end_at))?;
        let year = ended_at.year().to_string();
        let month = format!("{:02}", u8::from(ended_at.month()));
        let org = normalize_drive_name(&config.org_scope);
        let org_folder = self.ensure_folder(&org, Some(&root.id)).await?;
        let year_folder = self.ensure_folder(&year, Some(&org_folder.id)).await?;
        let month_folder = self.ensure_folder(&month, Some(&year_folder.id)).await?;

        let file_name = transcript_file_name(meeting)?;
        let markdown = transcript_markdown(meeting, artifact);
        let existing = self
            .find_child_by_exact_name(
                &file_name,
                Some(&month_folder.id),
                Some(GOOGLE_DOCS_DOCUMENT_MIME_TYPE),
            )
            .await?;
        let uploaded = if let Some(existing) = existing {
            self.update_google_doc(&existing.id, &file_name, &markdown)
                .await?
        } else {
            self.create_google_doc(&month_folder.id, &file_name, &markdown)
                .await?
        };
        Ok(UploadedTranscript {
            destination_uri: uploaded
                .web_view_link
                .unwrap_or_else(|| format!("gdrive://{}", uploaded.id)),
            checksum: format!("sha256:{}", sha256_hex(markdown.as_bytes())),
            size_bytes: markdown.len() as u64,
        })
    }
}

impl GoogleDriveTranscriptUploader {
    async fn ensure_folder(
        &self,
        name: &str,
        parent_id: Option<&str>,
    ) -> anyhow::Result<GoogleDriveFile> {
        if let Some(folder) = self
            .find_child_by_exact_name(name, parent_id, Some(GOOGLE_DRIVE_FOLDER_MIME_TYPE))
            .await?
        {
            return Ok(folder);
        }
        self.create_folder(name, parent_id).await
    }

    async fn find_child_by_exact_name(
        &self,
        name: &str,
        parent_id: Option<&str>,
        mime_type: Option<&str>,
    ) -> anyhow::Result<Option<GoogleDriveFile>> {
        let mut query = GoogleDriveQuery::new().trashed(false).name_contains(name);
        if let Some(mime_type) = mime_type {
            query = query.mime_type_eq(mime_type);
        }
        if let Some(parent_id) = parent_id {
            query = query.parent_in(parent_id);
        }
        let response = self
            .drive_files_list(GoogleDriveFilesListQuery {
                q: Some(query.render()),
                fields: Some(
                    "files(id,name,mimeType,webViewLink,parents),nextPageToken".to_string(),
                ),
                page_size: Some(10),
                supports_all_drives: Some(true),
                include_items_from_all_drives: Some(true),
                ..GoogleDriveFilesListQuery::default()
            })
            .await?;
        Ok(response
            .files
            .into_iter()
            .find(|file| file.name.as_deref() == Some(name)))
    }

    async fn drive_files_list(
        &self,
        query: GoogleDriveFilesListQuery,
    ) -> anyhow::Result<GoogleDriveFilesListResponse> {
        let response = self
            .google_get(&google_api_url(
                GOOGLE_DRIVE_BASE_URL,
                drive_files_path(),
                &query.to_query_pairs(),
            ))
            .await?;
        serde_json::from_slice(&response.body).context("decode Google Drive files list response")
    }

    async fn create_folder(
        &self,
        name: &str,
        parent_id: Option<&str>,
    ) -> anyhow::Result<GoogleDriveFile> {
        let mut metadata = json!({
            "name": name,
            "mimeType": GOOGLE_DRIVE_FOLDER_MIME_TYPE,
        });
        if let Some(parent_id) = parent_id {
            metadata["parents"] = json!([parent_id]);
        }
        let response = self
            .google_json_request(
                HttpMethod::Post,
                &google_api_url(
                    GOOGLE_DRIVE_BASE_URL,
                    drive_files_path(),
                    &[(
                        "fields".to_string(),
                        "id,name,mimeType,webViewLink,parents".to_string(),
                    )],
                ),
                metadata,
            )
            .await?;
        serde_json::from_slice(&response.body).context("decode created Drive folder")
    }

    async fn create_google_doc(
        &self,
        parent_id: &str,
        name: &str,
        markdown: &str,
    ) -> anyhow::Result<GoogleDriveFile> {
        let metadata = json!({
            "name": name,
            "mimeType": GOOGLE_DOCS_DOCUMENT_MIME_TYPE,
            "parents": [parent_id],
        });
        self.multipart_upload(HttpMethod::Post, None, metadata, markdown)
            .await
    }

    async fn update_google_doc(
        &self,
        file_id: &str,
        name: &str,
        markdown: &str,
    ) -> anyhow::Result<GoogleDriveFile> {
        let metadata = json!({
            "name": name,
            "mimeType": GOOGLE_DOCS_DOCUMENT_MIME_TYPE,
        });
        self.multipart_upload(HttpMethod::Patch, Some(file_id), metadata, markdown)
            .await
    }

    async fn multipart_upload(
        &self,
        method: HttpMethod,
        file_id: Option<&str>,
        metadata: JsonValue,
        markdown: &str,
    ) -> anyhow::Result<GoogleDriveFile> {
        let boundary = format!(
            "lattice-s14-{}",
            sha256_hex(metadata.to_string().as_bytes())
        );
        let metadata_bytes = serde_json::to_vec(&metadata).context("serialize Drive metadata")?;
        let mut body = Vec::new();
        body.extend_from_slice(
            format!("--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n")
                .as_bytes(),
        );
        body.extend_from_slice(&metadata_bytes);
        body.extend_from_slice(
            format!("\r\n--{boundary}\r\nContent-Type: text/markdown; charset=UTF-8\r\n\r\n")
                .as_bytes(),
        );
        body.extend_from_slice(markdown.as_bytes());
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

        let path = match file_id {
            Some(file_id) => format!("/upload/drive/v3/files/{}", encode_path_segment(file_id)),
            None => "/upload/drive/v3/files".to_string(),
        };
        let url = google_api_url(
            "https://www.googleapis.com",
            &path,
            &[
                ("uploadType".to_string(), "multipart".to_string()),
                (
                    "fields".to_string(),
                    "id,name,mimeType,webViewLink,parents".to_string(),
                ),
            ],
        );
        let token = self.auth.bearer_token().await?;
        let request = HttpRequest::new(method, url)
            .with_header("authorization", format!("Bearer {token}"))
            .with_header(
                "content-type",
                format!("multipart/related; boundary={boundary}"),
            )
            .with_header("accept", "application/json")
            .with_body(body);
        let response = self
            .http_write
            .send(request)
            .await
            .map_err(|err| anyhow!("Drive multipart upload failed: {err}"))?;
        ensure_success(&response, "Google Drive multipart upload")?;
        serde_json::from_slice(&response.body).context("decode Drive upload response")
    }

    async fn google_json_request(
        &self,
        method: HttpMethod,
        url: &str,
        body: JsonValue,
    ) -> anyhow::Result<HttpResponse> {
        let token = self.auth.bearer_token().await?;
        let request = HttpRequest::new(method, url)
            .with_header("authorization", format!("Bearer {token}"))
            .with_header("content-type", "application/json")
            .with_header("accept", "application/json")
            .with_body(serde_json::to_vec(&body).context("serialize Google JSON body")?);
        let response = self
            .http_write
            .send(request)
            .await
            .map_err(|err| anyhow!("Google JSON request failed: {err}"))?;
        ensure_success(&response, "Google JSON request")?;
        Ok(response)
    }

    async fn google_get(&self, url: &str) -> anyhow::Result<HttpResponse> {
        let token = self.auth.bearer_token().await?;
        let request = HttpRequest::new(HttpMethod::Get, url)
            .with_header("accept", "application/json")
            .with_header("authorization", format!("Bearer {token}"));
        let response = self
            .http_read
            .send(request)
            .await
            .map_err(|err| anyhow!("Google Drive request failed: {err}"))?;
        ensure_success(&response, "Google Drive request")?;
        Ok(response)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct LocalSyncConfig {
    pub org_scope: String,
    pub calendar_ids: Vec<String>,
    pub lookback_days: i64,
    pub batch_limit: u32,
    pub destination_folder_name: String,
    pub destination_parent_folder_id: Option<String>,
    pub ledger_path: String,
}

impl Default for LocalSyncConfig {
    fn default() -> Self {
        Self {
            org_scope: "studio".to_string(),
            calendar_ids: vec!["primary".to_string()],
            lookback_days: 30,
            batch_limit: 10,
            destination_folder_name: "Lattice Meeting Transcripts".to_string(),
            destination_parent_folder_id: None,
            ledger_path: "../scratch/s14-meeting-transcript-sync-local/ledger.sqlite".to_string(),
        }
    }
}

impl LocalSyncConfig {
    pub fn from_env() -> Self {
        let defaults = Self::default();
        Self {
            org_scope: std::env::var("S14_ORG_SCOPE").unwrap_or(defaults.org_scope),
            calendar_ids: std::env::var("S14_CALENDAR_IDS")
                .ok()
                .map(|value| split_csv(&value))
                .filter(|values| !values.is_empty())
                .unwrap_or(defaults.calendar_ids),
            lookback_days: std::env::var("S14_LOOKBACK_DAYS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(defaults.lookback_days),
            batch_limit: std::env::var("S14_SYNC_BATCH_LIMIT")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(defaults.batch_limit),
            destination_folder_name: std::env::var("S14_DESTINATION_FOLDER_NAME")
                .unwrap_or(defaults.destination_folder_name),
            destination_parent_folder_id: std::env::var("S14_DESTINATION_PARENT_FOLDER_ID")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            ledger_path: std::env::var("S14_LEDGER_PATH").unwrap_or(defaults.ledger_path),
        }
    }
}

fn completed_meeting_from_google_event(
    calendar_id: &str,
    event: GoogleCalendarEvent,
) -> Option<CompletedMeeting> {
    if event.status.as_deref() == Some("cancelled") {
        return None;
    }
    let scheduled_end_at = event.end.as_ref()?.date_time.clone().or(event
        .end
        .as_ref()?
        .date
        .clone())?;
    let calendar_event_id = event.id.clone();
    let title = event
        .summary
        .clone()
        .unwrap_or_else(|| "Untitled meeting".to_string());
    let mut meeting =
        CompletedMeeting::new(calendar_id, calendar_event_id, title, scheduled_end_at);
    meeting.scheduled_start_at = event
        .start
        .as_ref()
        .and_then(|start| start.date_time.clone().or_else(|| start.date.clone()));
    meeting.description = event.description.clone();
    meeting.location = event.location.clone();
    let google_meet_join_url = event
        .hangout_link
        .clone()
        .or_else(|| google_meet_entrypoint_uri(&event));
    meeting.organizer_email = event.organizer.and_then(|person| person.email);
    meeting.attendees = event
        .attendees
        .into_iter()
        .filter_map(|person| person.email)
        .collect();
    meeting.join_url = first_zoom_url(meeting.description.as_deref())
        .or_else(|| first_zoom_url(meeting.location.as_deref()))
        .or(google_meet_join_url);
    meeting.metadata = json!({
        "google_calendar": {
            "html_link": event.html_link,
            "hangout_link": event.hangout_link,
            "attachments": event.attachments,
            "conference_data": event.conference_data,
        }
    });
    Some(meeting)
}

fn google_meet_entrypoint_uri(event: &GoogleCalendarEvent) -> Option<String> {
    event
        .conference_data
        .as_ref()?
        .entry_points
        .iter()
        .find_map(|entry| {
            if entry.entry_point_type.as_deref() == Some("video") {
                entry.uri.clone()
            } else {
                None
            }
        })
}

fn first_zoom_url(value: Option<&str>) -> Option<String> {
    let value = value?;
    value.split_whitespace().find_map(|candidate| {
        let trimmed = candidate.trim_matches(|ch: char| {
            ch == '<' || ch == '>' || ch == ')' || ch == '(' || ch == ',' || ch == '.'
        });
        if trimmed.contains("zoom.us/") || trimmed.contains("zoom.com/") {
            Some(trimmed.to_string())
        } else {
            None
        }
    })
}

fn google_doc_attachments(meeting: &CompletedMeeting) -> Vec<GoogleDriveFile> {
    let attachments = meeting
        .metadata
        .pointer("/google_calendar/attachments")
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default();
    attachments
        .into_iter()
        .filter_map(|value| {
            let file_id = value.get("fileId")?.as_str()?.to_string();
            let title = value
                .get("title")
                .and_then(JsonValue::as_str)
                .map(str::to_string);
            let mime_type = value
                .get("mimeType")
                .and_then(JsonValue::as_str)
                .map(str::to_string);
            if mime_type.as_deref() != Some(GOOGLE_DOCS_DOCUMENT_MIME_TYPE) {
                return None;
            }
            let title_lower = title.as_deref().unwrap_or_default().to_lowercase();
            if !(title_lower.contains("notes by gemini") || title_lower.contains("transcript")) {
                return None;
            }
            Some(GoogleDriveFile {
                id: file_id,
                created_time: None,
                export_links: BTreeMap::new(),
                mime_type,
                modified_time: None,
                name: title,
                owners: Vec::new(),
                parents: Vec::new(),
                shortcut_details: None,
                trashed: None,
                web_view_link: value
                    .get("fileUrl")
                    .and_then(JsonValue::as_str)
                    .map(str::to_string),
            })
        })
        .collect()
}

fn transcript_file_name(meeting: &CompletedMeeting) -> anyhow::Result<String> {
    let ended_at = OffsetDateTime::parse(&meeting.scheduled_end_at, &Rfc3339)
        .with_context(|| format!("parse meeting end `{}`", meeting.scheduled_end_at))?;
    let date = ended_at.date();
    Ok(format!(
        "{} - {} - Transcript",
        date,
        normalize_drive_name(&meeting.title)
    ))
}

fn transcript_markdown(meeting: &CompletedMeeting, artifact: &TranscriptArtifact) -> String {
    format!(
        "# {}\n\n- Meeting key: `{}`\n- Calendar event: `{}`\n- Scheduled end: `{}`\n- Source kind: `{:?}`\n\n---\n\n{}\n",
        meeting.title,
        meeting.meeting_key,
        meeting.calendar_event_id,
        meeting.scheduled_end_at,
        artifact.source_ref.kind,
        artifact.text.trim()
    )
}

fn normalize_drive_name(value: &str) -> String {
    let mut out = String::new();
    let mut last_space = false;
    for ch in value.chars() {
        let replacement = match ch {
            '/' | '\\' | '\n' | '\r' | '\t' => ' ',
            '|' => '-',
            _ => ch,
        };
        if replacement.is_whitespace() {
            if !last_space {
                out.push(' ');
                last_space = true;
            }
        } else {
            out.push(replacement);
            last_space = false;
        }
    }
    out.trim().to_string()
}

fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

fn require_env(name: &str) -> anyhow::Result<String> {
    std::env::var(name).with_context(|| format!("missing required env var `{name}`"))
}

fn form_urlencode(pairs: &[(&str, &str)]) -> String {
    pairs
        .iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                utf8_percent_encode(key, FORM_ENCODE_SET),
                utf8_percent_encode(value, FORM_ENCODE_SET)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn google_api_url(base: &str, path: &str, query: &[(String, String)]) -> String {
    if query.is_empty() {
        return format!("{}{}", base.trim_end_matches('/'), path);
    }
    let rendered = query
        .iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                utf8_percent_encode(key, QUERY_ENCODE_SET),
                utf8_percent_encode(value, QUERY_ENCODE_SET)
            )
        })
        .collect::<Vec<_>>()
        .join("&");
    format!("{}{}?{}", base.trim_end_matches('/'), path, rendered)
}

fn encode_path_segment(value: &str) -> String {
    utf8_percent_encode(value, QUERY_ENCODE_SET).to_string()
}

fn ensure_success(response: &HttpResponse, context: &str) -> anyhow::Result<()> {
    if response.is_success() {
        return Ok(());
    }
    let body = String::from_utf8_lossy(&response.body);
    bail!(
        "{context} returned HTTP {}: {}",
        response.status,
        body.chars().take(500).collect::<String>()
    )
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(bytes))
}

fn date_shift(value: &str, days: i64) -> anyhow::Result<String> {
    let parsed = OffsetDateTime::parse(value, &Rfc3339)
        .with_context(|| format!("parse RFC3339 timestamp `{value}`"))?;
    (parsed + Duration::days(days))
        .format(&Rfc3339)
        .map_err(|error| anyhow!("format shifted timestamp: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_drive_name_removes_path_separators_and_collapses_space() {
        assert_eq!(
            normalize_drive_name("LeAP / Client\n Kickoff | Notes"),
            "LeAP Client Kickoff - Notes"
        );
    }

    #[test]
    fn form_urlencode_encodes_refresh_body_parts() {
        assert_eq!(
            form_urlencode(&[("grant_type", "refresh_token"), ("refresh_token", "a/b+c")]),
            "grant_type=refresh_token&refresh_token=a%2Fb%2Bc"
        );
    }
}
