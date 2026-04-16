#[cfg(not(target_arch = "wasm32"))]
use std::fs;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
#[cfg(target_arch = "wasm32")]
use worker::{Bucket, HttpMetadata};

use crate::adapters::TranscriptUploader;
use crate::config::TranscriptSyncConfig;
use crate::domain::{CompletedMeeting, TranscriptArtifact, UploadedTranscript};

pub struct R2TranscriptUploader {
    #[cfg(not(target_arch = "wasm32"))]
    root: PathBuf,
    #[cfg(target_arch = "wasm32")]
    bucket: BucketHandle,
}

#[cfg(target_arch = "wasm32")]
#[derive(Clone)]
struct BucketHandle(Bucket);

#[cfg(target_arch = "wasm32")]
// SAFETY: Cloudflare Workers runs this wasm code on a single-threaded event loop.
unsafe impl Send for BucketHandle {}
#[cfg(target_arch = "wasm32")]
// SAFETY: Cloudflare Workers runs this wasm code on a single-threaded event loop.
unsafe impl Sync for BucketHandle {}

impl R2TranscriptUploader {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_local_root(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    #[cfg(target_arch = "wasm32")]
    pub fn from_bucket(bucket: Bucket) -> Self {
        Self {
            bucket: BucketHandle(bucket),
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub fn from_env(env: &worker::Env, binding: &str) -> Result<Self, worker::Error> {
        Ok(Self {
            bucket: BucketHandle(env.bucket(binding)?),
        })
    }

    async fn write_bytes(
        &self,
        key: &str,
        bytes: &[u8],
        content_type: &str,
        _sha256_bytes: Option<&[u8]>,
    ) -> anyhow::Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let path = self.root.join(key);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("create local R2 path {}", parent.display()))?;
            }
            fs::write(&path, bytes)
                .with_context(|| format!("write local R2 object {}", path.display()))?;
            let metadata_path = self.root.join(format!("{key}.content_type"));
            if let Some(parent) = metadata_path.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("create metadata path {}", parent.display()))?;
            }
            fs::write(&metadata_path, content_type.as_bytes()).with_context(|| {
                format!(
                    "write local R2 content type sidecar {}",
                    metadata_path.display()
                )
            })?;
            Ok(())
        }

        #[cfg(target_arch = "wasm32")]
        {
            let mut http_metadata = HttpMetadata::default();
            http_metadata.content_type = Some(content_type.to_string());
            let mut builder = self
                .bucket
                .0
                .put(key.to_string(), bytes.to_vec())
                .http_metadata(http_metadata);
            if let Some(sha256_bytes) = _sha256_bytes {
                builder = builder.sha256(sha256_bytes.to_vec());
            }
            builder
                .execute()
                .await
                .map_err(|error| anyhow!("put R2 object `{key}`: {error}"))?;
            Ok(())
        }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl TranscriptUploader for R2TranscriptUploader {
    async fn upload(
        &self,
        meeting: &CompletedMeeting,
        artifact: &TranscriptArtifact,
        config: &TranscriptSyncConfig,
    ) -> anyhow::Result<UploadedTranscript> {
        let destination = destination_for(meeting, config)?;
        let transcript_bytes = artifact.text.as_bytes().to_vec();
        let sha256_bytes = Sha256::digest(&transcript_bytes).to_vec();
        let checksum = format!("sha256:{}", hex::encode(&sha256_bytes));
        let source_bytes = serde_json::to_vec_pretty(&artifact.source_ref)
            .context("serialize transcript source snapshot")?;
        let normalized_bytes = serde_json::to_vec_pretty(&artifact.normalized)
            .context("serialize normalized transcript payload")?;

        self.write_bytes(
            &destination.source_key,
            &source_bytes,
            "application/json",
            None,
        )
        .await?;
        self.write_bytes(
            &destination.transcript_key,
            &transcript_bytes,
            "text/plain; charset=utf-8",
            Some(&sha256_bytes),
        )
        .await?;
        self.write_bytes(
            &destination.normalized_key,
            &normalized_bytes,
            "application/json",
            None,
        )
        .await?;

        Ok(UploadedTranscript {
            destination_uri: destination.transcript_uri,
            checksum,
            size_bytes: transcript_bytes.len() as u64,
        })
    }
}

struct DestinationLayout {
    transcript_key: String,
    source_key: String,
    normalized_key: String,
    transcript_uri: String,
}

#[derive(Debug)]
struct ParsedR2Destination {
    bucket: String,
    prefix: String,
}

fn destination_for(
    meeting: &CompletedMeeting,
    config: &TranscriptSyncConfig,
) -> anyhow::Result<DestinationLayout> {
    let parsed_destination = parse_destination_prefix(&config.destination_prefix)?;
    let ended_at =
        OffsetDateTime::parse(&meeting.scheduled_end_at, &Rfc3339).with_context(|| {
            format!(
                "parse meeting scheduled_end_at `{}`",
                meeting.scheduled_end_at
            )
        })?;
    let year = ended_at.year();
    let month = u8::from(ended_at.month());
    let scoped_prefix = join_key_segments(&[
        parsed_destination.prefix.as_str(),
        sanitize_segment(&config.org_scope).as_str(),
        format!("{year:04}").as_str(),
        format!("{month:02}").as_str(),
        sanitize_segment(&meeting.meeting_key).as_str(),
    ]);

    let transcript_key = join_key_segments(&[scoped_prefix.as_str(), "transcript.txt"]);
    let source_key = join_key_segments(&[scoped_prefix.as_str(), "source.json"]);
    let normalized_key = join_key_segments(&[scoped_prefix.as_str(), "transcript.normalized.json"]);
    let transcript_uri = format!("r2://{}/{}", parsed_destination.bucket, transcript_key);

    Ok(DestinationLayout {
        transcript_key,
        source_key,
        normalized_key,
        transcript_uri,
    })
}

fn parse_destination_prefix(value: &str) -> anyhow::Result<ParsedR2Destination> {
    let trimmed = value.trim();
    let without_scheme = trimmed
        .strip_prefix("r2://")
        .ok_or_else(|| anyhow!("destination_prefix must start with `r2://`, got `{trimmed}`"))?;
    let mut parts = without_scheme.splitn(2, '/');
    let bucket = parts
        .next()
        .filter(|bucket| !bucket.is_empty())
        .ok_or_else(|| anyhow!("destination_prefix is missing an R2 bucket name: `{trimmed}`"))?;
    let prefix = parts
        .next()
        .unwrap_or_default()
        .trim_matches('/')
        .to_string();

    Ok(ParsedR2Destination {
        bucket: bucket.to_string(),
        prefix,
    })
}

fn sanitize_segment(value: &str) -> String {
    let mut sanitized = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch == '/' || ch == '\\' {
            sanitized.push('_');
        } else {
            sanitized.push(ch);
        }
    }
    sanitized.trim_matches('/').trim_matches('\\').to_string()
}

fn join_key_segments(segments: &[&str]) -> String {
    segments
        .iter()
        .filter_map(|segment| {
            let trimmed = segment.trim_matches('/');
            (!trimmed.is_empty()).then_some(trimmed)
        })
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::domain::{TranscriptSourceRef, meeting_key_from_event};

    fn sample_config() -> TranscriptSyncConfig {
        TranscriptSyncConfig {
            org_scope: "studio".to_string(),
            calendar_ids: vec!["primary".to_string()],
            sync_lookback_minutes: 30,
            sync_batch_limit: 20,
            transcript_ready_retry_minutes: 5,
            max_transcript_attempts: 3,
            destination_prefix: "r2://meeting-transcripts/transcripts".to_string(),
            gmeet_doc_title_patterns: vec!["Transcript".to_string()],
        }
    }

    fn sample_meeting() -> CompletedMeeting {
        let mut meeting =
            CompletedMeeting::new("primary", "evt-r2", "Weekly sync", "2026-04-16T10:00:00Z");
        meeting.meeting_key = meeting_key_from_event("evt-r2", "2026-04-16T10:00:00Z");
        meeting
    }

    #[tokio::test]
    async fn r2_uploader_writes_expected_object_layout() {
        let temp = tempdir().expect("create tempdir");
        let uploader = R2TranscriptUploader::from_local_root(temp.path());
        let meeting = sample_meeting();
        let artifact = TranscriptArtifact {
            text: "hello transcript".to_string(),
            normalized: serde_json::json!({"paragraphs": 1}),
            source_ref: TranscriptSourceRef::google_doc(
                "doc-1",
                "https://docs.google.com/document/d/doc-1/edit",
            ),
        };

        let uploaded = uploader
            .upload(&meeting, &artifact, &sample_config())
            .await
            .expect("upload transcript");

        assert_eq!(
            uploaded.destination_uri,
            "r2://meeting-transcripts/transcripts/studio/2026/04/evt-r2:2026-04-16T10:00:00Z/transcript.txt"
        );
        assert!(uploaded.checksum.starts_with("sha256:"));
        assert_eq!(uploaded.size_bytes, 16);

        let transcript_path = temp
            .path()
            .join("transcripts/studio/2026/04/evt-r2:2026-04-16T10:00:00Z/transcript.txt");
        let source_path = temp
            .path()
            .join("transcripts/studio/2026/04/evt-r2:2026-04-16T10:00:00Z/source.json");
        let normalized_path = temp.path().join(
            "transcripts/studio/2026/04/evt-r2:2026-04-16T10:00:00Z/transcript.normalized.json",
        );

        assert_eq!(
            fs::read_to_string(&transcript_path).expect("read transcript"),
            "hello transcript"
        );
        let source_json = fs::read_to_string(&source_path).expect("read source");
        assert!(source_json.contains("google_doc_transcript"));
        let normalized_json = fs::read_to_string(&normalized_path).expect("read normalized");
        assert!(normalized_json.contains("paragraphs"));
        assert_eq!(
            fs::read_to_string(format!("{}.content_type", transcript_path.display()))
                .expect("read content type"),
            "text/plain; charset=utf-8"
        );
    }

    #[test]
    fn parse_destination_prefix_rejects_non_r2_uris() {
        let error = parse_destination_prefix("https://example.com/transcripts").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("destination_prefix must start with `r2://`")
        );
    }
}
