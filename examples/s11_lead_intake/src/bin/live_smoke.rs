use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use cap_http_reqwest::ReqwestHttpClient;
use cap_workspace_fs::{FsWorkspaceConfig, FsWorkspaceFactory};
use capabilities::Capability;
use capabilities::ResourceBag;
use capabilities::workspace::{
    Workspace, WorkspaceCompletionDisposition, WorkspaceDeleteResult, WorkspaceEntry,
    WorkspaceFactory, WorkspaceListOptions, WorkspacePolicy, WorkspaceReadResult,
    WorkspaceRunScope, WorkspaceWriteOptions, WorkspaceWriteResult,
};
use example_s11_lead_intake::{EmailPackage, LeadSubmission, bundle};
use host_inproc::HostExecutionResult;
use serde_json::json;

const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com/v1";
const DEFAULT_OPENAI_TEXT_MODEL: &str = "gpt-5.4-mini";
const DEFAULT_OPENAI_IMAGE_MODEL: &str = "gpt-image-1.5";

#[derive(Clone)]
struct WorkspaceHandle(Arc<dyn Workspace>);

impl Capability for WorkspaceHandle {
    fn name(&self) -> &'static str {
        "workspace.live-smoke"
    }
}

#[async_trait::async_trait]
impl Workspace for WorkspaceHandle {
    async fn read_normalized(
        &self,
        normalized_path: &str,
    ) -> Result<Option<WorkspaceReadResult>, capabilities::workspace::WorkspaceError> {
        self.0.read_normalized(normalized_path).await
    }

    async fn write_normalized(
        &self,
        normalized_path: &str,
        data: &[u8],
        options: WorkspaceWriteOptions,
    ) -> Result<WorkspaceWriteResult, capabilities::workspace::WorkspaceError> {
        self.0
            .write_normalized(normalized_path, data, options)
            .await
    }

    async fn list_normalized(
        &self,
        options: WorkspaceListOptions,
    ) -> Result<Vec<WorkspaceEntry>, capabilities::workspace::WorkspaceError> {
        self.0.list_normalized(options).await
    }

    async fn delete_normalized(
        &self,
        normalized_path: &str,
    ) -> Result<WorkspaceDeleteResult, capabilities::workspace::WorkspaceError> {
        self.0.delete_normalized(normalized_path).await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let api_key = std::env::var("OPENAI_API_KEY")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("OPENAI_API_KEY must be set for live smoke"))?;
    let _ = api_key;

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let sample_path = manifest_dir.join("payloads/live-sample.json");
    let sample_bytes = fs::read(&sample_path)
        .with_context(|| format!("read live smoke sample at {}", sample_path.display()))?;
    let submission: LeadSubmission =
        serde_json::from_slice(&sample_bytes).context("decode live smoke sample payload")?;

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_secs();
    let output_root = std::env::var("S11_LIVE_OUTPUT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            manifest_dir
                .join("../../../scratch/s11-lead-intake-live")
                .join(timestamp.to_string())
        });
    fs::create_dir_all(&output_root)
        .with_context(|| format!("create output dir {}", output_root.display()))?;

    let workspace_root = output_root.join("workspace");
    fs::create_dir_all(&workspace_root)
        .with_context(|| format!("create workspace root {}", workspace_root.display()))?;

    fs::write(
        output_root.join("input.json"),
        serde_json::to_vec_pretty(&submission).context("serialize smoke input")?,
    )
    .context("write input artifact")?;

    let scope = WorkspaceRunScope::new("s11_lead_intake_flow", format!("live-smoke-{timestamp}"));
    let workspace_factory = FsWorkspaceFactory::new(FsWorkspaceConfig {
        root: workspace_root.clone(),
        policy: WorkspacePolicy {
            retain_completed_for: Some(Duration::from_secs(24 * 60 * 60)),
            ..WorkspacePolicy::default()
        },
    });
    let run_root = workspace_factory.run_root_path(&scope);
    let workspace = workspace_factory
        .open(scope.clone())
        .await
        .context("open fs workspace")?;

    let http = Arc::new(ReqwestHttpClient::default());
    let bundle = bundle();
    let payload = serde_json::to_value(&submission).context("encode invocation payload")?;
    let output = bundle
        .executor()
        .with_resource_bag(
            ResourceBag::new()
                .with_http_read(Arc::clone(&http))
                .with_http_write(http)
                .with_workspace(Arc::new(WorkspaceHandle(Arc::clone(&workspace)))),
        )
        .run_once(&bundle.validated_ir, "trigger", payload, "capture", None)
        .await
        .context("execute s11 lead-intake flow")?;

    let package: EmailPackage = match output {
        HostExecutionResult::Value(value) => {
            serde_json::from_value(value).context("decode email package output")?
        }
        HostExecutionResult::Stream(_) => {
            return Err(anyhow!("expected value output, got stream"));
        }
        HostExecutionResult::Halt { alias, .. } => {
            return Err(anyhow!("unexpected halt at {alias}"));
        }
    };

    let workspace_entries = workspace
        .list_normalized(WorkspaceListOptions::default())
        .await
        .context("list workspace contents")?;

    fs::write(
        output_root.join("email_package.json"),
        serde_json::to_vec_pretty(&package).context("serialize email package")?,
    )
    .context("write email package artifact")?;

    fs::write(
        output_root.join("workspace_entries.json"),
        serde_json::to_vec_pretty(&workspace_entries).context("serialize workspace entries")?,
    )
    .context("write workspace entries artifact")?;

    if let Some(image_path) = package.image_artifact_path.as_deref() {
        let image = workspace
            .read_normalized(image_path)
            .await
            .with_context(|| format!("read workspace image at {image_path}"))?;
        match image {
            Some(WorkspaceReadResult::Bytes(bytes)) => {
                fs::write(output_root.join("hero.png"), &bytes).context("write hero image")?;
            }
            Some(WorkspaceReadResult::BlobRef(blob_ref)) => {
                fs::write(output_root.join("hero.blobref.txt"), blob_ref)
                    .context("write blob ref artifact")?;
            }
            None => {
                return Err(anyhow!(
                    "email package referenced image artifact path `{image_path}` but no workspace file was found"
                ));
            }
        }
    }

    fs::write(
        output_root.join("summary.json"),
        serde_json::to_vec_pretty(&json!({
            "status": "ok",
            "base_url": std::env::var("OPENAI_BASE_URL").unwrap_or_else(|_| DEFAULT_OPENAI_BASE_URL.to_string()),
            "text_model": std::env::var("OPENAI_TEXT_MODEL").unwrap_or_else(|_| DEFAULT_OPENAI_TEXT_MODEL.to_string()),
            "image_model": std::env::var("OPENAI_IMAGE_MODEL").unwrap_or_else(|_| DEFAULT_OPENAI_IMAGE_MODEL.to_string()),
            "output_root": output_root,
            "workspace_run_root": run_root,
            "image_artifact_path": package.image_artifact_path,
        }))
        .context("serialize summary artifact")?,
    )
    .context("write summary artifact")?;

    workspace_factory
        .complete(scope, WorkspaceCompletionDisposition::Succeeded)
        .await
        .context("mark workspace complete")?;

    println!("Live smoke complete.");
    println!("Artifacts: {}", output_root.display());
    println!("Workspace run root: {}", run_root.display());
    println!(
        "Email package: {}",
        output_root.join("email_package.json").display()
    );
    if output_root.join("hero.png").exists() {
        println!("Hero image: {}", output_root.join("hero.png").display());
    }

    Ok(())
}
