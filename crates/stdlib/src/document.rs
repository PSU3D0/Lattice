use std::sync::Arc;

use capabilities::ResourceAccess;
use capabilities::artifact::{Artifact, ByteAccessError, Exact};
use capabilities::context;
use capabilities::transform::{TransformBeginError, TransformExecutionRecord, TransformFailure};
use capabilities::workspace::WorkspaceError;
use dag_core::{NodeError, NodeResult};
use dag_macros::def_node;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const EXTRACT_PDF_TEXT_IDENTIFIER: &str = "std.document.extract_pdf_text";
pub const PDF_EXTRACT_TRANSFORM_ID: &str = "lattice.pdf.extract_text.v1";
pub const ERR_DOCUMENT_INTEGRITY: &str = "STD-DOC-001";
pub const ERR_DOCUMENT_TRANSFORM: &str = "STD-DOC-002";
pub const ERR_DOCUMENT_ENVELOPE: &str = "STD-DOC-003";

const PDF_CONTENT_TYPE: &str = "application/pdf";
const PDF_MAGIC: &[u8] = b"%PDF-";
const MAX_PDF_PAGES: u32 = 200;
const MAX_TEXT_BYTES: usize = 512 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExtractPdfTextInput {
    pub artifact: Artifact<Exact>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtractPdfTextOutput {
    pub text: String,
    pub page_count: u32,
}

#[derive(Debug)]
struct CompositeExecution {
    output: ExtractPdfTextOutput,
    transform_record: TransformExecutionRecord,
    canonical_text_sha256: [u8; 32],
}

#[def_node(
    name = "ExtractPdfText",
    summary = "Extract canonical embedded text from a bounded PDF artifact",
    identifier = "std.document.extract_pdf_text",
    effects = "ReadOnly",
    determinism = "BestEffort",
    resources(workspace_read(capabilities::workspace::Workspace))
)]
pub async fn extract_pdf_text(input: ExtractPdfTextInput) -> NodeResult<ExtractPdfTextOutput> {
    let execution = context::with_current_async(|resources| async move {
        extract_pdf_text_with_resources(resources, input.artifact).await
    })
    .await
    .ok_or_else(|| document_error(ERR_DOCUMENT_INTEGRITY, "resource_context_unavailable"))??;
    let CompositeExecution {
        output,
        transform_record,
        canonical_text_sha256,
    } = execution;
    drop(transform_record);
    let _ = canonical_text_sha256;
    Ok(output)
}

async fn extract_pdf_text_with_resources(
    resources: Arc<dyn ResourceAccess>,
    artifact: Artifact<Exact>,
) -> NodeResult<CompositeExecution> {
    if artifact.content_type != PDF_CONTENT_TYPE {
        return Err(document_error(ERR_DOCUMENT_INTEGRITY, "mime_mismatch"));
    }

    // CAP110 authority is deliberately reached before the ungated transform service.
    let workspace = resources
        .workspace_read()
        .ok_or_else(|| document_error(ERR_DOCUMENT_INTEGRITY, "workspace_read_unavailable"))?;
    let runtime = resources
        .transform_runtime()
        .ok_or_else(|| transform_begin_error(TransformBeginError::RuntimeUnavailable))?;
    let lease = runtime
        .try_begin(PDF_EXTRACT_TRANSFORM_ID)
        .map_err(transform_begin_error)?;
    let max_input_bytes = lease.max_input_bytes();

    if artifact.len > max_input_bytes {
        return Err(document_error(
            ERR_DOCUMENT_INTEGRITY,
            "length_exceeds_limit",
        ));
    }

    let bytes = workspace
        .read_bounded(&artifact.handle, max_input_bytes)
        .await
        .map_err(map_workspace_error)?;
    let actual_len = u64::try_from(bytes.len())
        .map_err(|_| document_error(ERR_DOCUMENT_INTEGRITY, "actual_length_unrepresentable"))?;
    if actual_len != artifact.len {
        return Err(document_error(ERR_DOCUMENT_INTEGRITY, "length_mismatch"));
    }

    let input_sha256: [u8; 32] = Sha256::digest(&bytes).into();
    if let Some(expected) = artifact.content_hash.as_deref() {
        let matches = hash_matches_constant_shape(expected, input_sha256);
        metrics::counter!(
            "lattice.transform.input_hash_comparisons_total",
            "backend" => "native",
            "transform" => PDF_EXTRACT_TRANSFORM_ID,
            "outcome" => if matches { "matched" } else { "mismatch" }
        )
        .increment(1);
        if !matches {
            return Err(document_error(ERR_DOCUMENT_INTEGRITY, "hash_mismatch"));
        }
    }
    if !bytes.starts_with(PDF_MAGIC) {
        return Err(document_error(ERR_DOCUMENT_INTEGRITY, "magic_mismatch"));
    }

    let outcome = lease.run(bytes).await.map_err(transform_execution_error)?;
    let (output, record) = outcome.into_parts();
    validate_pdf_envelope(output, record)
}

fn validate_pdf_envelope(
    mut envelope: Vec<u8>,
    transform_record: TransformExecutionRecord,
) -> NodeResult<CompositeExecution> {
    if envelope.len() < 4 {
        return Err(document_error(ERR_DOCUMENT_ENVELOPE, "invalid_output"));
    }
    let page_count = u32::from_le_bytes(
        envelope[..4]
            .try_into()
            .expect("four-byte envelope prefix was checked"),
    );
    if !(1..=MAX_PDF_PAGES).contains(&page_count) {
        return Err(document_error(ERR_DOCUMENT_ENVELOPE, "invalid_output"));
    }

    let text_bytes = &envelope[4..];
    if text_bytes.is_empty() || text_bytes.len() > MAX_TEXT_BYTES {
        return Err(document_error(ERR_DOCUMENT_ENVELOPE, "invalid_output"));
    }
    let text_view = std::str::from_utf8(text_bytes)
        .map_err(|_| document_error(ERR_DOCUMENT_ENVELOPE, "invalid_output"))?;
    if text_view.contains('\r')
        || !text_view
            .chars()
            .all(|character| character == '\n' || character == '\t' || !character.is_control())
        || !text_view
            .chars()
            .any(|character| !character.is_whitespace())
    {
        return Err(document_error(ERR_DOCUMENT_ENVELOPE, "invalid_output"));
    }

    let canonical_text_sha256 = Sha256::digest(text_bytes).into();
    envelope.drain(..4);
    let text = String::from_utf8(envelope)
        .map_err(|_| document_error(ERR_DOCUMENT_ENVELOPE, "invalid_output"))?;
    Ok(CompositeExecution {
        output: ExtractPdfTextOutput { text, page_count },
        transform_record,
        canonical_text_sha256,
    })
}

fn hash_matches_constant_shape(expected_hex: &str, actual: [u8; 32]) -> bool {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let expected = expected_hex.as_bytes();
    let mut difference = (expected.len() ^ 64) as u64;
    for (index, byte) in actual.into_iter().enumerate() {
        let high = HEX[usize::from(byte >> 4)];
        let low = HEX[usize::from(byte & 0x0f)];
        difference |= u64::from(expected.get(index * 2).copied().unwrap_or(0) ^ high);
        difference |= u64::from(expected.get(index * 2 + 1).copied().unwrap_or(0) ^ low);
    }
    difference == 0
}

fn document_error(code: &'static str, class: &'static str) -> NodeError {
    NodeError::new(format!("{code}: document operation failed [{class}]"))
}

fn transform_begin_error(error: TransformBeginError) -> NodeError {
    document_error(ERR_DOCUMENT_TRANSFORM, error.as_str())
}

fn transform_execution_error(error: TransformFailure) -> NodeError {
    document_error(ERR_DOCUMENT_TRANSFORM, error.class().as_str())
}

fn map_workspace_error(error: ByteAccessError) -> NodeError {
    match error {
        ByteAccessError::Workspace(WorkspaceError::TooLarge { .. }) => NodeError::new(format!(
            "{}: bounded workspace entry exceeds the transform ceiling",
            capabilities::workspace::ERR_WORKSPACE_TOO_LARGE
        )),
        ByteAccessError::Workspace(WorkspaceError::Unsupported(_)) => {
            document_error(ERR_DOCUMENT_INTEGRITY, "workspace_unsupported")
        }
        ByteAccessError::Workspace(WorkspaceError::NotFound(_)) | ByteAccessError::NotFound(_) => {
            document_error(ERR_DOCUMENT_INTEGRITY, "not_found")
        }
        ByteAccessError::Workspace(WorkspaceError::InvalidPath(_))
        | ByteAccessError::Workspace(WorkspaceError::PathTraversal(_)) => {
            document_error(ERR_DOCUMENT_INTEGRITY, "invalid_path")
        }
        ByteAccessError::Workspace(WorkspaceError::Backend(_)) => {
            document_error(ERR_DOCUMENT_INTEGRITY, "workspace_backend")
        }
        ByteAccessError::Workspace(WorkspaceError::MissingWorkspaceRead(_))
        | ByteAccessError::Workspace(WorkspaceError::MissingWorkspaceWrite(_))
        | ByteAccessError::MintVerification
        | ByteAccessError::OutOfScope { .. }
        | ByteAccessError::StoreMismatch { .. } => {
            document_error(ERR_DOCUMENT_INTEGRITY, "invalid_handle")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use capabilities::artifact::{Handle, StoreRef, sha256_hex};
    use capabilities::transform::{
        MeteredTransformBudgets, MeteredTransformObservations, PlatformTransformBudgets,
        PlatformTransformObservations, TransformErrorClass, TransformInstanceModel, TransformLease,
        TransformOutcome, TransformTerminationClass,
    };
    use capabilities::workspace::{
        Workspace, WorkspaceDeleteResult, WorkspaceEntry, WorkspaceListOptions,
        WorkspaceReadResult, WorkspaceWriteOptions, WorkspaceWriteResult,
    };
    use capabilities::{Capability, ResourceBag};
    use dag_core::{Determinism, Effects, FlowBuilder, NodeSpec, Profile, SchemaSpec};
    use kernel_exec::{ExecutionResult, FlowExecutor, NodeRegistry};
    use kernel_plan::validate;
    use semver::Version;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    const ROOT_KEY: &[u8] = b"stdlib-document-test-root";

    #[derive(Clone)]
    enum ReadBehavior {
        Bytes(Vec<u8>),
        BlobRef,
        Missing,
        Backend,
    }

    struct RecordingWorkspace {
        behavior: Mutex<ReadBehavior>,
        bounded_calls: AtomicUsize,
        unbounded_calls: AtomicUsize,
        last_limit: Mutex<Option<u64>>,
    }

    impl RecordingWorkspace {
        fn bytes(bytes: Vec<u8>) -> Self {
            Self {
                behavior: Mutex::new(ReadBehavior::Bytes(bytes)),
                bounded_calls: AtomicUsize::new(0),
                unbounded_calls: AtomicUsize::new(0),
                last_limit: Mutex::new(None),
            }
        }

        fn with_behavior(behavior: ReadBehavior) -> Self {
            Self {
                behavior: Mutex::new(behavior),
                bounded_calls: AtomicUsize::new(0),
                unbounded_calls: AtomicUsize::new(0),
                last_limit: Mutex::new(None),
            }
        }
    }

    impl Capability for RecordingWorkspace {
        fn name(&self) -> &'static str {
            "workspace.document-test"
        }
    }

    #[async_trait]
    impl Workspace for RecordingWorkspace {
        async fn read_normalized(
            &self,
            _normalized_path: &str,
        ) -> Result<Option<WorkspaceReadResult>, WorkspaceError> {
            self.unbounded_calls.fetch_add(1, Ordering::SeqCst);
            panic!("document composite must never use unbounded workspace read")
        }

        async fn read_bounded_normalized(
            &self,
            _normalized_path: &str,
            max_bytes: u64,
        ) -> Result<Option<WorkspaceReadResult>, WorkspaceError> {
            self.bounded_calls.fetch_add(1, Ordering::SeqCst);
            *self.last_limit.lock().expect("limit") = Some(max_bytes);
            match self.behavior.lock().expect("behavior").clone() {
                ReadBehavior::Bytes(bytes) => {
                    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > max_bytes {
                        Err(WorkspaceError::TooLarge { max_bytes })
                    } else {
                        Ok(Some(WorkspaceReadResult::Bytes(bytes)))
                    }
                }
                ReadBehavior::BlobRef => Ok(Some(WorkspaceReadResult::BlobRef(
                    "private://blob-reference".to_string(),
                ))),
                ReadBehavior::Missing => Ok(None),
                ReadBehavior::Backend => {
                    Err(WorkspaceError::Backend("private /host/path".to_string()))
                }
            }
        }

        async fn write_normalized(
            &self,
            normalized_path: &str,
            data: &[u8],
            _options: WorkspaceWriteOptions,
        ) -> Result<WorkspaceWriteResult, WorkspaceError> {
            Ok(WorkspaceWriteResult {
                path: normalized_path.to_string(),
                size_bytes: data.len() as u64,
                updated_at_ms: 0,
            })
        }

        async fn list_normalized(
            &self,
            _options: WorkspaceListOptions,
        ) -> Result<Vec<WorkspaceEntry>, WorkspaceError> {
            Ok(Vec::new())
        }

        async fn delete_normalized(
            &self,
            _normalized_path: &str,
        ) -> Result<WorkspaceDeleteResult, WorkspaceError> {
            Ok(WorkspaceDeleteResult { deleted: false })
        }
    }

    #[cfg(target_os = "linux")]
    struct WorkspaceDelegate(Arc<dyn Workspace>);

    #[cfg(target_os = "linux")]
    impl Capability for WorkspaceDelegate {
        fn name(&self) -> &'static str {
            "workspace.fs-delegate"
        }
    }

    #[cfg(target_os = "linux")]
    #[async_trait]
    impl Workspace for WorkspaceDelegate {
        async fn read_normalized(
            &self,
            path: &str,
        ) -> Result<Option<WorkspaceReadResult>, WorkspaceError> {
            self.0.read_normalized(path).await
        }

        async fn read_bounded_normalized(
            &self,
            path: &str,
            max_bytes: u64,
        ) -> Result<Option<WorkspaceReadResult>, WorkspaceError> {
            self.0.read_bounded_normalized(path, max_bytes).await
        }

        async fn write_normalized(
            &self,
            path: &str,
            data: &[u8],
            options: WorkspaceWriteOptions,
        ) -> Result<WorkspaceWriteResult, WorkspaceError> {
            self.0.write_normalized(path, data, options).await
        }

        async fn list_normalized(
            &self,
            options: WorkspaceListOptions,
        ) -> Result<Vec<WorkspaceEntry>, WorkspaceError> {
            self.0.list_normalized(options).await
        }

        async fn delete_normalized(
            &self,
            path: &str,
        ) -> Result<WorkspaceDeleteResult, WorkspaceError> {
            self.0.delete_normalized(path).await
        }
    }

    #[derive(Clone)]
    enum RunBehavior {
        Output(Vec<u8>),
        Failure(TransformErrorClass),
    }

    struct RecordingRuntime {
        max_input_bytes: u64,
        behavior: RunBehavior,
        begins: Arc<AtomicUsize>,
        runs: Arc<AtomicUsize>,
        begin_error: Option<TransformBeginError>,
    }

    impl RecordingRuntime {
        fn output(max_input_bytes: u64, output: Vec<u8>) -> Arc<Self> {
            Arc::new(Self {
                max_input_bytes,
                behavior: RunBehavior::Output(output),
                begins: Arc::new(AtomicUsize::new(0)),
                runs: Arc::new(AtomicUsize::new(0)),
                begin_error: None,
            })
        }

        fn failure(class: TransformErrorClass) -> Arc<Self> {
            Arc::new(Self {
                max_input_bytes: 1024,
                behavior: RunBehavior::Failure(class),
                begins: Arc::new(AtomicUsize::new(0)),
                runs: Arc::new(AtomicUsize::new(0)),
                begin_error: None,
            })
        }
    }

    impl capabilities::transform::TransformRuntime for RecordingRuntime {
        fn try_begin(
            &self,
            transform_id: &str,
        ) -> Result<Box<dyn TransformLease>, TransformBeginError> {
            self.begins.fetch_add(1, Ordering::SeqCst);
            assert_eq!(transform_id, PDF_EXTRACT_TRANSFORM_ID);
            if let Some(error) = self.begin_error {
                return Err(error);
            }
            Ok(Box::new(RecordingLease {
                max_input_bytes: self.max_input_bytes,
                behavior: self.behavior.clone(),
                runs: Arc::clone(&self.runs),
            }))
        }
    }

    struct RecordingLease {
        max_input_bytes: u64,
        behavior: RunBehavior,
        runs: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TransformLease for RecordingLease {
        fn max_input_bytes(&self) -> u64 {
            self.max_input_bytes
        }

        async fn run(
            self: Box<Self>,
            input: Vec<u8>,
        ) -> Result<TransformOutcome, TransformFailure> {
            self.runs.fetch_add(1, Ordering::SeqCst);
            let input_hash = Sha256::digest(&input).into();
            match self.behavior {
                RunBehavior::Output(output) => {
                    let output_hash = Sha256::digest(&output).into();
                    Ok(TransformOutcome::new(
                        output,
                        record(
                            Some(input_hash),
                            Some(output_hash),
                            TransformTerminationClass::Success,
                        ),
                    )
                    .expect("test outcome record is successful"))
                }
                RunBehavior::Failure(class) => {
                    let record = if class == TransformErrorClass::PlatformTerminated {
                        platform_record(
                            Some(input_hash),
                            None,
                            TransformTerminationClass::Failure(class),
                        )
                    } else {
                        record(
                            Some(input_hash),
                            None,
                            TransformTerminationClass::Failure(class),
                        )
                    };
                    Err(TransformFailure::new(
                        class,
                        record,
                        Some("private trap / parser diagnostic".to_string()),
                    )
                    .expect("test failure class matches record"))
                }
            }
        }
    }

    fn record(
        input_sha256: Option<[u8; 32]>,
        output_sha256: Option<[u8; 32]>,
        termination_class: TransformTerminationClass,
    ) -> TransformExecutionRecord {
        TransformExecutionRecord::metered(
            PDF_EXTRACT_TRANSFORM_ID,
            [3; 32],
            "lattice.transform.v1",
            "test-runtime",
            MeteredTransformBudgets::new(
                1,
                1024,
                2048,
                1,
                1,
                1,
                1,
                (4 + MAX_TEXT_BYTES) as u64,
                Duration::from_secs(2),
                Duration::from_millis(10),
                Duration::from_millis(250),
                100,
                10,
                2,
            ),
            input_sha256,
            output_sha256,
            termination_class,
            MeteredTransformObservations::new(
                Duration::from_millis(1),
                input_sha256.map_or(0, |_| 1),
                output_sha256.map_or(0, |_| 1),
                Some(1),
                Some(65_536),
            ),
        )
        .expect("valid metered test record")
    }

    fn platform_record(
        input_sha256: Option<[u8; 32]>,
        output_sha256: Option<[u8; 32]>,
        termination_class: TransformTerminationClass,
    ) -> TransformExecutionRecord {
        TransformExecutionRecord::platform(
            PDF_EXTRACT_TRANSFORM_ID,
            [3; 32],
            "lattice.transform.v1",
            "2026-07-15",
            PlatformTransformBudgets::new(
                50,
                128 * 1024 * 1024,
                64 * 1024 * 1024,
                1024,
                (4 + MAX_TEXT_BYTES) as u64,
                1,
                TransformInstanceModel::FreshPerInvocation,
            )
            .expect("valid platform test budgets"),
            input_sha256,
            output_sha256,
            termination_class,
            PlatformTransformObservations::new(
                Duration::from_millis(1),
                input_sha256.map_or(0, |_| 1),
                output_sha256.map_or(0, |_| 1),
            ),
        )
        .expect("valid platform test record")
    }

    fn envelope(page_count: u32, text: &[u8]) -> Vec<u8> {
        let mut output = page_count.to_le_bytes().to_vec();
        output.extend_from_slice(text);
        output
    }

    fn artifact(bytes: &[u8]) -> Artifact<Exact> {
        artifact_at(bytes, "ingress/resume.pdf")
    }

    fn artifact_at(bytes: &[u8], path: &str) -> Artifact<Exact> {
        Artifact {
            handle: Handle::host_mint_exact(
                StoreRef::new("workspace"),
                ROOT_KEY,
                "test-root",
                path,
            ),
            content_type: PDF_CONTENT_TYPE.to_string(),
            len: bytes.len() as u64,
            content_hash: Some(sha256_hex(bytes)),
        }
    }

    fn resources(
        workspace: Arc<RecordingWorkspace>,
        runtime: Option<Arc<RecordingRuntime>>,
    ) -> Arc<dyn ResourceAccess> {
        let mut bag = ResourceBag::new()
            .with_workspace(workspace)
            .with_workspace_root_key(ROOT_KEY.to_vec(), "test-root");
        if let Some(runtime) = runtime {
            bag = bag.with_transform_runtime(runtime);
        }
        Arc::new(bag)
    }

    #[test]
    fn node_metadata_has_only_workspace_read_effect_floor() {
        let spec = extract_pdf_text_node_spec();
        assert_eq!(spec.identifier, EXTRACT_PDF_TEXT_IDENTIFIER);
        assert_eq!(spec.effects, Effects::ReadOnly);
        assert_eq!(spec.determinism, Determinism::BestEffort);
        assert_eq!(
            spec.effect_hints,
            [capabilities::workspace::HINT_WORKSPACE_READ]
        );
        assert!(spec.connector_ops.is_empty());
    }

    #[tokio::test]
    async fn successful_composite_preserves_record_and_hashes_exact_graph_text() {
        let pdf = b"%PDF-fake bounded input".to_vec();
        let workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let runtime = RecordingRuntime::output(64, envelope(2, b"first\nsecond"));
        let mut input = artifact(&pdf);
        input.content_hash = None;

        let execution = extract_pdf_text_with_resources(
            resources(workspace.clone(), Some(runtime.clone())),
            input,
        )
        .await
        .expect("composite succeeds");

        assert_eq!(
            execution.output,
            ExtractPdfTextOutput {
                text: "first\nsecond".to_string(),
                page_count: 2,
            }
        );
        let expected_text_hash: [u8; 32] = Sha256::digest(b"first\nsecond").into();
        assert_eq!(execution.canonical_text_sha256, expected_text_hash);
        assert_eq!(
            execution.transform_record.input_sha256(),
            Some(Sha256::digest(&pdf).into())
        );
        assert_eq!(workspace.bounded_calls.load(Ordering::SeqCst), 1);
        assert_eq!(workspace.unbounded_calls.load(Ordering::SeqCst), 0);
        assert_eq!(*workspace.last_limit.lock().expect("limit"), Some(64));
        assert_eq!(runtime.begins.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.runs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn execution_order_fails_before_forbidden_stages() {
        let pdf = b"%PDF-order".to_vec();

        let mime_workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let mime_runtime = RecordingRuntime::output(64, envelope(1, b"text"));
        let mut mime = artifact(&pdf);
        mime.content_type = "Application/PDF".to_string();
        let error = extract_pdf_text_with_resources(
            resources(mime_workspace.clone(), Some(mime_runtime.clone())),
            mime,
        )
        .await
        .expect_err("MIME mismatch");
        assert!(error.to_string().contains("mime_mismatch"));
        assert_eq!(mime_workspace.bounded_calls.load(Ordering::SeqCst), 0);
        assert_eq!(mime_runtime.begins.load(Ordering::SeqCst), 0);

        let absent_workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let error = extract_pdf_text_with_resources(
            resources(absent_workspace.clone(), None),
            artifact(&pdf),
        )
        .await
        .expect_err("runtime absent");
        assert!(error.to_string().contains(ERR_DOCUMENT_TRANSFORM));
        assert_eq!(absent_workspace.bounded_calls.load(Ordering::SeqCst), 0);

        let busy_workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let busy_runtime = Arc::new(RecordingRuntime {
            max_input_bytes: 64,
            behavior: RunBehavior::Output(envelope(1, b"text")),
            begins: Arc::new(AtomicUsize::new(0)),
            runs: Arc::new(AtomicUsize::new(0)),
            begin_error: Some(TransformBeginError::Busy),
        });
        let error = extract_pdf_text_with_resources(
            resources(busy_workspace.clone(), Some(busy_runtime.clone())),
            artifact(&pdf),
        )
        .await
        .expect_err("busy admission");
        assert!(error.to_string().contains("busy"));
        assert_eq!(busy_workspace.bounded_calls.load(Ordering::SeqCst), 0);
        assert_eq!(busy_runtime.begins.load(Ordering::SeqCst), 1);
        assert_eq!(busy_runtime.runs.load(Ordering::SeqCst), 0);

        let limit_workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let limit_runtime = RecordingRuntime::output(4, envelope(1, b"text"));
        let error = extract_pdf_text_with_resources(
            resources(limit_workspace.clone(), Some(limit_runtime.clone())),
            artifact(&pdf),
        )
        .await
        .expect_err("length hint over ceiling");
        assert!(error.to_string().contains("length_exceeds_limit"));
        assert_eq!(limit_workspace.bounded_calls.load(Ordering::SeqCst), 0);
        assert_eq!(limit_runtime.begins.load(Ordering::SeqCst), 1);
        assert_eq!(limit_runtime.runs.load(Ordering::SeqCst), 0);

        let hash_workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let hash_runtime = RecordingRuntime::output(64, envelope(1, b"text"));
        let mut hash_mismatch = artifact(&pdf);
        hash_mismatch.content_hash = Some("00".repeat(32));
        let error = extract_pdf_text_with_resources(
            resources(hash_workspace.clone(), Some(hash_runtime.clone())),
            hash_mismatch,
        )
        .await
        .expect_err("hash mismatch");
        assert!(error.to_string().contains("hash_mismatch"));
        assert_eq!(hash_workspace.bounded_calls.load(Ordering::SeqCst), 1);
        assert_eq!(hash_runtime.runs.load(Ordering::SeqCst), 0);

        let magic = b"NOT-PDF!!!".to_vec();
        let magic_workspace = Arc::new(RecordingWorkspace::bytes(magic.clone()));
        let magic_runtime = RecordingRuntime::output(64, envelope(1, b"text"));
        let error = extract_pdf_text_with_resources(
            resources(magic_workspace.clone(), Some(magic_runtime.clone())),
            artifact(&magic),
        )
        .await
        .expect_err("magic mismatch");
        assert!(error.to_string().contains("magic_mismatch"));
        assert_eq!(magic_runtime.runs.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn handle_integrity_and_provider_failures_are_sanitized_before_run() {
        let pdf = b"%PDF-handle".to_vec();
        let runtime = RecordingRuntime::output(64, envelope(1, b"text"));

        let forged_workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let mut wire = serde_json::to_value(artifact(&pdf)).expect("artifact json");
        wire["handle"]["mint"]["tag"][0] = serde_json::json!(255);
        let forged: Artifact<Exact> = serde_json::from_value(wire).expect("forged wire shape");
        let error = extract_pdf_text_with_resources(
            resources(forged_workspace.clone(), Some(runtime.clone())),
            forged,
        )
        .await
        .expect_err("forged handle");
        assert!(error.to_string().contains("invalid_handle"));
        assert!(!error.to_string().contains("ingress/resume.pdf"));
        assert_eq!(forged_workspace.bounded_calls.load(Ordering::SeqCst), 0);

        let scope_workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let mut wire = serde_json::to_value(artifact(&pdf)).expect("artifact json");
        wire["handle"]["scope"]["Exact"] = serde_json::json!("ingress/other.pdf");
        let out_of_scope: Artifact<Exact> =
            serde_json::from_value(wire).expect("out-of-scope wire shape");
        let error = extract_pdf_text_with_resources(
            resources(scope_workspace.clone(), Some(runtime.clone())),
            out_of_scope,
        )
        .await
        .expect_err("out-of-scope handle");
        assert!(error.to_string().contains("invalid_handle"));
        assert_eq!(scope_workspace.bounded_calls.load(Ordering::SeqCst), 0);

        let foreign_workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let mut foreign = artifact(&pdf);
        foreign.handle = Handle::host_mint_exact(
            StoreRef::new("workspace"),
            b"foreign-run-root",
            "foreign",
            "ingress/resume.pdf",
        );
        let error = extract_pdf_text_with_resources(
            resources(foreign_workspace.clone(), Some(runtime.clone())),
            foreign,
        )
        .await
        .expect_err("foreign root");
        assert!(error.to_string().contains("invalid_handle"));
        assert_eq!(foreign_workspace.bounded_calls.load(Ordering::SeqCst), 0);

        let store_workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let mut wrong_store = artifact(&pdf);
        wrong_store.handle = Handle::host_mint_exact(
            StoreRef::new("other"),
            ROOT_KEY,
            "test-root",
            "ingress/resume.pdf",
        );
        let error = extract_pdf_text_with_resources(
            resources(store_workspace.clone(), Some(runtime.clone())),
            wrong_store,
        )
        .await
        .expect_err("wrong store");
        assert!(error.to_string().contains("invalid_handle"));
        assert_eq!(store_workspace.bounded_calls.load(Ordering::SeqCst), 0);

        let missing = Arc::new(RecordingWorkspace::with_behavior(ReadBehavior::Missing));
        let error = extract_pdf_text_with_resources(
            resources(missing, Some(runtime.clone())),
            artifact(&pdf),
        )
        .await
        .expect_err("missing");
        assert!(error.to_string().contains("not_found"));

        let blob = Arc::new(RecordingWorkspace::with_behavior(ReadBehavior::BlobRef));
        let error =
            extract_pdf_text_with_resources(resources(blob, Some(runtime.clone())), artifact(&pdf))
                .await
                .expect_err("blob ref");
        assert!(error.to_string().contains("workspace_unsupported"));

        let backend = Arc::new(RecordingWorkspace::with_behavior(ReadBehavior::Backend));
        let error = extract_pdf_text_with_resources(
            resources(backend, Some(runtime.clone())),
            artifact(&pdf),
        )
        .await
        .expect_err("backend");
        assert!(error.to_string().contains("workspace_backend"));
        assert!(!error.to_string().contains("/host/path"));
        assert_eq!(runtime.runs.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn actual_size_and_length_mismatches_are_distinct_and_pre_parser() {
        let too_large = b"%PDF-actual-too-large".to_vec();
        let too_large_workspace = Arc::new(RecordingWorkspace::bytes(too_large.clone()));
        let runtime = RecordingRuntime::output(8, envelope(1, b"text"));
        let mut lying = artifact(&too_large);
        lying.len = 8;
        lying.content_hash = None;
        let error = extract_pdf_text_with_resources(
            resources(too_large_workspace.clone(), Some(runtime.clone())),
            lying,
        )
        .await
        .expect_err("provider catches actual oversize");
        assert!(
            error
                .to_string()
                .contains(capabilities::workspace::ERR_WORKSPACE_TOO_LARGE)
        );
        assert_eq!(runtime.runs.load(Ordering::SeqCst), 0);

        let within = b"%PDF-short".to_vec();
        let mismatch_workspace = Arc::new(RecordingWorkspace::bytes(within.clone()));
        let mismatch_runtime = RecordingRuntime::output(64, envelope(1, b"text"));
        let mut mismatch = artifact(&within);
        mismatch.len -= 1;
        let error = extract_pdf_text_with_resources(
            resources(mismatch_workspace, Some(mismatch_runtime.clone())),
            mismatch,
        )
        .await
        .expect_err("actual length mismatch");
        assert!(error.to_string().contains("length_mismatch"));
        assert_eq!(mismatch_runtime.runs.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn envelope_validation_rejects_every_noncanonical_shape_without_normalizing() {
        let mut cases = vec![
            Vec::new(),
            vec![0, 0, 0],
            envelope(0, b"text"),
            envelope(201, b"text"),
            envelope(1, b""),
            envelope(1, b" \n\t"),
            envelope(1, b"has\rreturn"),
            envelope(1, b"has\x01control"),
            envelope(1, &[0xff]),
        ];
        cases.push(envelope(1, &vec![b'x'; MAX_TEXT_BYTES + 1]));
        for output in cases {
            let error = validate_pdf_envelope(
                output,
                record(None, Some([1; 32]), TransformTerminationClass::Success),
            )
            .expect_err("invalid envelope");
            assert!(error.to_string().contains(ERR_DOCUMENT_ENVELOPE));
        }
    }

    #[tokio::test]
    async fn every_transform_failure_class_is_stable_and_sanitized() {
        let classes = [
            TransformErrorClass::InvalidModule,
            TransformErrorClass::InvalidAbi,
            TransformErrorClass::InputTooLarge,
            TransformErrorClass::Busy,
            TransformErrorClass::RuntimeUnavailable,
            TransformErrorClass::FuelExhausted,
            TransformErrorClass::MemoryExhausted,
            TransformErrorClass::WallTimeExceeded,
            TransformErrorClass::PlatformTerminated,
            TransformErrorClass::Cancelled,
            TransformErrorClass::OutputTooLarge,
            TransformErrorClass::GuestFailed,
            TransformErrorClass::InvalidOutput,
            TransformErrorClass::UnsupportedDocument,
        ];
        let pdf = b"%PDF-transform-failure".to_vec();
        for class in classes {
            let runtime = RecordingRuntime::failure(class);
            let workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
            let error = extract_pdf_text_with_resources(
                resources(workspace, Some(runtime)),
                artifact(&pdf),
            )
            .await
            .expect_err("transform failure");
            let message = error.to_string();
            assert!(message.contains(ERR_DOCUMENT_TRANSFORM));
            assert!(message.contains(class.as_str()));
            assert!(!message.contains("private trap"));
            assert!(!message.contains("parser diagnostic"));
        }
    }

    fn validated_document_flow(with_declaration: bool) -> kernel_plan::ValidatedIR {
        let mut builder = FlowBuilder::new("document-test", Version::new(1, 0, 0), Profile::Dev);
        let trigger = builder
            .add_node(
                "trigger",
                &NodeSpec::inline(
                    "tests.document.trigger",
                    "Trigger",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .expect("trigger");
        let dishonest_spec;
        let document_spec = if with_declaration {
            extract_pdf_text_node_spec()
        } else {
            dishonest_spec = NodeSpec::inline(
                EXTRACT_PDF_TEXT_IDENTIFIER,
                "DishonestDocument",
                SchemaSpec::Opaque,
                SchemaSpec::Opaque,
                Effects::ReadOnly,
                Determinism::BestEffort,
                None,
            );
            &dishonest_spec
        };
        let document = builder
            .add_node("document", document_spec)
            .expect("document");
        builder.connect(&trigger, &document);
        validate(&builder.build()).expect("valid document flow")
    }

    fn document_registry() -> NodeRegistry {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests.document.trigger",
                |value: ExtractPdfTextInput| async move { Ok(value) },
            )
            .expect("register trigger");
        registry
            .register_fn(EXTRACT_PDF_TEXT_IDENTIFIER, extract_pdf_text)
            .expect("register document node");
        registry
    }

    async fn execute_document(
        artifact: Artifact<Exact>,
        resources: Arc<dyn ResourceAccess>,
        declared: bool,
    ) -> Result<ExtractPdfTextOutput, String> {
        let executor =
            FlowExecutor::new(Arc::new(document_registry())).with_resource_access(resources);
        let result = executor
            .run_once(
                &validated_document_flow(declared),
                "trigger",
                serde_json::to_value(ExtractPdfTextInput { artifact }).expect("artifact payload"),
                "document",
                None,
            )
            .await
            .map_err(|error| error.to_string())?;
        let ExecutionResult::Value(value) = result else {
            return Err("document flow returned a non-value result".to_string());
        };
        serde_json::from_value(value).map_err(|error| error.to_string())
    }

    #[tokio::test]
    async fn executor_path_passes_transform_through_both_resource_wrappers() {
        let pdf = b"%PDF-executor".to_vec();
        let workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let runtime = RecordingRuntime::output(64, envelope(1, b"executor text"));
        let executor = FlowExecutor::new(Arc::new(document_registry()))
            .with_resource_access(resources(workspace.clone(), Some(runtime.clone())));
        let payload = serde_json::to_value(ExtractPdfTextInput {
            artifact: artifact(&pdf),
        })
        .expect("artifact payload");
        assert!(!payload.to_string().contains("PDF-executor"));

        let result = executor
            .run_once(
                &validated_document_flow(true),
                "trigger",
                payload,
                "document",
                None,
            )
            .await
            .expect("executor document success");
        let ExecutionResult::Value(value) = result else {
            panic!("expected value output")
        };
        let output: ExtractPdfTextOutput = serde_json::from_value(value).expect("typed output");
        assert_eq!(output.text, "executor text");
        assert_eq!(workspace.bounded_calls.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.begins.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.runs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn executor_underdeclared_node_emits_cap110_before_transform_or_provider() {
        let pdf = b"%PDF-denial".to_vec();
        let workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let runtime = RecordingRuntime::output(64, envelope(1, b"must not run"));
        let executor = FlowExecutor::new(Arc::new(document_registry()))
            .with_resource_access(resources(workspace.clone(), Some(runtime.clone())));
        let error = match executor
            .run_once(
                &validated_document_flow(false),
                "trigger",
                serde_json::to_value(ExtractPdfTextInput {
                    artifact: artifact(&pdf),
                })
                .expect("payload"),
                "document",
                None,
            )
            .await
        {
            Ok(_) => panic!("underdeclared node must be denied"),
            Err(error) => error,
        };
        let message = error.to_string();
        assert_eq!(message.matches("CAP110:").count(), 1, "{message}");
        assert_eq!(workspace.bounded_calls.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.begins.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.runs.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn executor_negative_matrix_preserves_order_without_spurious_cap110() {
        type Case = (
            &'static str,
            Artifact<Exact>,
            Arc<RecordingWorkspace>,
            Option<Arc<RecordingRuntime>>,
            usize,
            usize,
            usize,
            &'static str,
        );

        let pdf = b"%PDF-executor-negative".to_vec();
        let mut cases: Vec<Case> = Vec::new();

        cases.push((
            "missing_runtime",
            artifact(&pdf),
            Arc::new(RecordingWorkspace::bytes(pdf.clone())),
            None,
            0,
            0,
            0,
            "runtime_unavailable",
        ));

        let mut mime = artifact(&pdf);
        mime.content_type = "Application/PDF".to_string();
        cases.push((
            "mime",
            mime,
            Arc::new(RecordingWorkspace::bytes(pdf.clone())),
            Some(RecordingRuntime::output(64, envelope(1, b"unused"))),
            0,
            0,
            0,
            "mime_mismatch",
        ));

        let stale = Arc::new(RecordingRuntime {
            max_input_bytes: 64,
            behavior: RunBehavior::Output(envelope(1, b"unused")),
            begins: Arc::new(AtomicUsize::new(0)),
            runs: Arc::new(AtomicUsize::new(0)),
            begin_error: Some(TransformBeginError::RuntimeUnavailable),
        });
        cases.push((
            "runtime_unavailable",
            artifact(&pdf),
            Arc::new(RecordingWorkspace::bytes(pdf.clone())),
            Some(stale),
            1,
            0,
            0,
            "runtime_unavailable",
        ));

        let busy = Arc::new(RecordingRuntime {
            max_input_bytes: 64,
            behavior: RunBehavior::Output(envelope(1, b"unused")),
            begins: Arc::new(AtomicUsize::new(0)),
            runs: Arc::new(AtomicUsize::new(0)),
            begin_error: Some(TransformBeginError::Busy),
        });
        cases.push((
            "busy",
            artifact(&pdf),
            Arc::new(RecordingWorkspace::bytes(pdf.clone())),
            Some(busy),
            1,
            0,
            0,
            "busy",
        ));

        let mut forged_wire = serde_json::to_value(artifact(&pdf)).expect("artifact json");
        let tag_byte = forged_wire["handle"]["mint"]["tag"][0]
            .as_u64()
            .expect("tag byte");
        forged_wire["handle"]["mint"]["tag"][0] = serde_json::json!(tag_byte ^ 0xff);
        let forged = serde_json::from_value(forged_wire).expect("forged artifact");
        cases.push((
            "forged",
            forged,
            Arc::new(RecordingWorkspace::bytes(pdf.clone())),
            Some(RecordingRuntime::output(64, envelope(1, b"unused"))),
            1,
            0,
            0,
            "invalid_handle",
        ));

        let mut foreign = artifact(&pdf);
        foreign.handle = Handle::host_mint_exact(
            StoreRef::new("workspace"),
            b"foreign-root",
            "foreign",
            "ingress/resume.pdf",
        );
        cases.push((
            "foreign",
            foreign,
            Arc::new(RecordingWorkspace::bytes(pdf.clone())),
            Some(RecordingRuntime::output(64, envelope(1, b"unused"))),
            1,
            0,
            0,
            "invalid_handle",
        ));

        let mut store = artifact(&pdf);
        store.handle = Handle::host_mint_exact(
            StoreRef::new("other-store"),
            ROOT_KEY,
            "test-root",
            "ingress/resume.pdf",
        );
        cases.push((
            "store",
            store,
            Arc::new(RecordingWorkspace::bytes(pdf.clone())),
            Some(RecordingRuntime::output(64, envelope(1, b"unused"))),
            1,
            0,
            0,
            "invalid_handle",
        ));

        let mut scope_wire = serde_json::to_value(artifact(&pdf)).expect("artifact json");
        scope_wire["handle"]["scope"]["Exact"] = serde_json::json!("ingress/sibling.pdf");
        let scope = serde_json::from_value(scope_wire).expect("scope artifact");
        cases.push((
            "scope",
            scope,
            Arc::new(RecordingWorkspace::bytes(pdf.clone())),
            Some(RecordingRuntime::output(64, envelope(1, b"unused"))),
            1,
            0,
            0,
            "invalid_handle",
        ));

        let mut hash = artifact(&pdf);
        hash.content_hash = Some("00".repeat(32));
        cases.push((
            "hash",
            hash,
            Arc::new(RecordingWorkspace::bytes(pdf.clone())),
            Some(RecordingRuntime::output(64, envelope(1, b"unused"))),
            1,
            1,
            0,
            "hash_mismatch",
        ));

        let bad_magic = b"not-a-pdf-but-bounded".to_vec();
        cases.push((
            "magic",
            artifact(&bad_magic),
            Arc::new(RecordingWorkspace::bytes(bad_magic)),
            Some(RecordingRuntime::output(64, envelope(1, b"unused"))),
            1,
            1,
            0,
            "magic_mismatch",
        ));

        cases.push((
            "hint_oversize",
            artifact(&pdf),
            Arc::new(RecordingWorkspace::bytes(pdf.clone())),
            Some(RecordingRuntime::output(4, envelope(1, b"unused"))),
            1,
            0,
            0,
            "length_exceeds_limit",
        ));

        let actual_large = b"%PDF-real-bytes-exceed-limit".to_vec();
        let mut lying = artifact(&actual_large);
        lying.len = 8;
        lying.content_hash = None;
        cases.push((
            "actual_oversize",
            lying,
            Arc::new(RecordingWorkspace::bytes(actual_large)),
            Some(RecordingRuntime::output(8, envelope(1, b"unused"))),
            1,
            1,
            0,
            capabilities::workspace::ERR_WORKSPACE_TOO_LARGE,
        ));

        for (name, artifact, workspace, runtime, begins, reads, runs, class) in cases {
            let error = execute_document(
                artifact,
                resources(workspace.clone(), runtime.clone()),
                true,
            )
            .await
            .expect_err(name);
            assert!(error.contains(class), "{name}: {error}");
            assert!(!error.contains("CAP110"), "{name}: {error}");
            assert_eq!(
                workspace.bounded_calls.load(Ordering::SeqCst),
                reads,
                "{name}"
            );
            assert_eq!(
                workspace.unbounded_calls.load(Ordering::SeqCst),
                0,
                "{name}"
            );
            if let Some(runtime) = runtime {
                assert_eq!(runtime.begins.load(Ordering::SeqCst), begins, "{name}");
                assert_eq!(runtime.runs.load(Ordering::SeqCst), runs, "{name}");
            } else {
                assert_eq!(begins, 0, "{name}");
                assert_eq!(runs, 0, "{name}");
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn fs_resources(
        workspace: Arc<dyn Workspace>,
        runtime: Arc<RecordingRuntime>,
    ) -> Arc<dyn ResourceAccess> {
        Arc::new(
            ResourceBag::new()
                .with_workspace(Arc::new(WorkspaceDelegate(workspace)))
                .with_workspace_root_key(ROOT_KEY.to_vec(), "test-root")
                .with_transform_runtime(runtime),
        )
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn executor_composite_uses_real_openat2_for_valid_oversize_and_symlink_cases() {
        use cap_workspace_fs::{FsWorkspaceConfig, FsWorkspaceFactory};
        use capabilities::workspace::{WorkspaceFactory, WorkspacePolicy, WorkspaceRunScope};
        use std::fs;
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir");
        let factory = FsWorkspaceFactory::new(FsWorkspaceConfig {
            root: temp.path().join("workspace"),
            policy: WorkspacePolicy::default(),
        });

        let valid_scope = WorkspaceRunScope::new("flow", "valid");
        let valid_workspace = factory
            .open(valid_scope.clone())
            .await
            .expect("valid workspace");
        let valid_pdf = b"%PDF-real-openat2-valid".to_vec();
        valid_workspace
            .write(
                "ingress/resume.pdf",
                &valid_pdf,
                WorkspaceWriteOptions::default(),
            )
            .await
            .expect("seed valid PDF");
        let valid_runtime = RecordingRuntime::output(64, envelope(1, b"real fs text"));
        let output = execute_document(
            artifact(&valid_pdf),
            fs_resources(valid_workspace, valid_runtime.clone()),
            true,
        )
        .await
        .expect("real filesystem composite succeeds");
        assert_eq!(
            output,
            ExtractPdfTextOutput {
                text: "real fs text".to_string(),
                page_count: 1,
            }
        );
        assert_eq!(valid_runtime.runs.load(Ordering::SeqCst), 1);

        let oversize_scope = WorkspaceRunScope::new("flow", "oversize");
        let oversize_workspace = factory
            .open(oversize_scope)
            .await
            .expect("oversize workspace");
        let oversize_pdf = b"%PDF-real-openat2-actual-oversize".to_vec();
        oversize_workspace
            .write(
                "ingress/resume.pdf",
                &oversize_pdf,
                WorkspaceWriteOptions::default(),
            )
            .await
            .expect("seed oversize PDF");
        let mut oversize_artifact = artifact(&oversize_pdf);
        oversize_artifact.len = 8;
        oversize_artifact.content_hash = None;
        let oversize_runtime = RecordingRuntime::output(8, envelope(1, b"unused"));
        let error = execute_document(
            oversize_artifact,
            fs_resources(oversize_workspace, oversize_runtime.clone()),
            true,
        )
        .await
        .expect_err("real provider rejects actual oversize");
        assert!(error.contains(capabilities::workspace::ERR_WORKSPACE_TOO_LARGE));
        assert!(!error.contains("CAP110"));
        assert_eq!(oversize_runtime.runs.load(Ordering::SeqCst), 0);

        let outside = temp.path().join("outside");
        fs::create_dir_all(&outside).expect("outside directory");
        fs::write(outside.join("secret.pdf"), b"%PDF-outside").expect("outside file");

        let final_scope = WorkspaceRunScope::new("flow", "final-symlink");
        let final_workspace = factory
            .open(final_scope.clone())
            .await
            .expect("final workspace");
        let final_root = factory.run_root_path(&final_scope);
        symlink(outside.join("secret.pdf"), final_root.join("final.pdf")).expect("final symlink");
        let final_runtime = RecordingRuntime::output(64, envelope(1, b"unused"));
        let error = execute_document(
            artifact_at(b"%PDF-outside", "final.pdf"),
            fs_resources(final_workspace, final_runtime.clone()),
            true,
        )
        .await
        .expect_err("final symlink rejected");
        assert!(error.contains("workspace_backend"), "{error}");
        assert!(!error.contains("CAP110"));
        assert_eq!(final_runtime.runs.load(Ordering::SeqCst), 0);

        let intermediate_scope = WorkspaceRunScope::new("flow", "intermediate-symlink");
        let intermediate_workspace = factory
            .open(intermediate_scope.clone())
            .await
            .expect("intermediate workspace");
        let intermediate_root = factory.run_root_path(&intermediate_scope);
        symlink(&outside, intermediate_root.join("linked")).expect("intermediate symlink");
        let intermediate_runtime = RecordingRuntime::output(64, envelope(1, b"unused"));
        let error = execute_document(
            artifact_at(b"%PDF-outside", "linked/secret.pdf"),
            fs_resources(intermediate_workspace, intermediate_runtime.clone()),
            true,
        )
        .await
        .expect_err("intermediate symlink rejected");
        assert!(error.contains("workspace_backend"), "{error}");
        assert!(!error.contains("CAP110"));
        assert_eq!(intermediate_runtime.runs.load(Ordering::SeqCst), 0);
    }

    fn escape_pdf_literal(text: &[u8]) -> Vec<u8> {
        let mut escaped = Vec::with_capacity(text.len());
        for byte in text {
            match byte {
                b'(' | b')' | b'\\' => {
                    escaped.push(b'\\');
                    escaped.push(*byte);
                }
                _ => escaped.push(*byte),
            }
        }
        escaped
    }

    fn finish_pdf(objects: Vec<Vec<u8>>, trailer_extra: &str) -> Vec<u8> {
        let mut pdf = b"%PDF-1.4\n% independent stdlib fixture\n".to_vec();
        let mut offsets = vec![0];
        for (index, object) in objects.iter().enumerate() {
            offsets.push(pdf.len());
            pdf.extend_from_slice(format!("{} 0 obj\n", index + 1).as_bytes());
            pdf.extend_from_slice(object);
            pdf.extend_from_slice(b"\nendobj\n");
        }
        let xref = pdf.len();
        pdf.extend_from_slice(format!("xref\n0 {}\n", objects.len() + 1).as_bytes());
        pdf.extend_from_slice(b"0000000000 65535 f \n");
        for offset in offsets.into_iter().skip(1) {
            pdf.extend_from_slice(format!("{offset:010} 00000 n \n").as_bytes());
        }
        pdf.extend_from_slice(
            format!(
                "trailer\n<< /Size {} /Root 1 0 R{trailer_extra} >>\nstartxref\n{xref}\n%%EOF\n",
                objects.len() + 1
            )
            .as_bytes(),
        );
        pdf
    }

    fn synthetic_pdf(page_text: &[Vec<u8>], encrypted: bool) -> Vec<u8> {
        let page_count = page_text.len();
        let font_id = 3 + page_count * 2;
        let encrypt_id = font_id + 1;
        let mut objects = vec![b"<< /Type /Catalog /Pages 2 0 R >>".to_vec()];
        let kids = (0..page_count)
            .map(|index| format!("{} 0 R", 3 + index * 2))
            .collect::<Vec<_>>()
            .join(" ");
        objects.push(format!("<< /Type /Pages /Kids [{kids}] /Count {page_count} >>").into_bytes());
        for (index, text) in page_text.iter().enumerate() {
            let page_id = 3 + index * 2;
            let content_id = page_id + 1;
            objects.push(format!("<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 {font_id} 0 R >> >> /Contents {content_id} 0 R >>").into_bytes());
            let escaped = escape_pdf_literal(text);
            let mut stream = b"BT /F1 12 Tf 72 720 Td (".to_vec();
            stream.extend_from_slice(&escaped);
            stream.extend_from_slice(b") Tj ET");
            let mut object = format!("<< /Length {} >>\nstream\n", stream.len()).into_bytes();
            object.extend_from_slice(&stream);
            object.extend_from_slice(b"\nendstream");
            objects.push(object);
        }
        objects.push(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>".to_vec());
        if encrypted {
            objects.push(b"<< /Filter /Standard /V 1 /R 2 /O <0000000000000000000000000000000000000000000000000000000000000000> /U <0000000000000000000000000000000000000000000000000000000000000000> /P -4 >>".to_vec());
        }
        let trailer_extra = if encrypted {
            format!(
                " /Encrypt {encrypt_id} 0 R /ID [<00112233445566778899aabbccddeeff><00112233445566778899aabbccddeeff>]"
            )
        } else {
            String::new()
        };
        finish_pdf(objects, &trailer_extra)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn run_real_pdf(
        service: Arc<processing_context::PdfTransformRuntime>,
        pdf: Vec<u8>,
    ) -> NodeResult<CompositeExecution> {
        let workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let bag = ResourceBag::new()
            .with_workspace(workspace)
            .with_workspace_root_key(ROOT_KEY.to_vec(), "test-root")
            .with_transform_runtime(service);
        extract_pdf_text_with_resources(Arc::new(bag), artifact(&pdf)).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn assert_exact_checked_provenance(execution: &CompositeExecution, input: &[u8]) {
        let record = &execution.transform_record;
        assert_eq!(
            record.backend(),
            capabilities::transform::TransformBackend::Wasmtime
        );
        assert_eq!(
            record.transform_id(),
            processing_context::PDF_EXTRACT_TRANSFORM_ID
        );
        assert_eq!(
            record.module_identity(),
            capabilities::transform::TransformModuleIdentity::RuntimeVerifiedSha256(
                processing_context::PDF_EXTRACT_MODULE.module_sha256()
            )
        );
        assert_eq!(record.abi_version(), processing_context::ABI_VERSION);
        assert_eq!(
            record.runtime_version(),
            Some(processing_context::RUNTIME_VERSION)
        );
        assert_eq!(record.input_sha256(), Some(Sha256::digest(input).into()));
        let output_envelope = envelope(
            execution.output.page_count,
            execution.output.text.as_bytes(),
        );
        assert_eq!(
            record.output_sha256(),
            Some(Sha256::digest(&output_envelope).into())
        );
        assert_eq!(
            record.termination_class(),
            TransformTerminationClass::Success
        );
        let canonical_text_sha256: [u8; 32] =
            Sha256::digest(execution.output.text.as_bytes()).into();
        assert_eq!(execution.canonical_text_sha256, canonical_text_sha256);
        let policy = processing_context::pdf_extract_processing_budgets();
        let budgets = record
            .metered_budgets()
            .expect("native provenance has metered budgets");
        assert_eq!(budgets.input_bytes(), policy.input_bytes as u64);
        assert_eq!(budgets.output_bytes(), policy.output_bytes as u64);
        assert_eq!(budgets.memory_bytes(), policy.memory_bytes as u64);
        assert_eq!(budgets.wall_time(), policy.wall_time);
        assert_eq!(budgets.fuel(), policy.fuel);
        assert_eq!(budgets.concurrency(), policy.concurrency as u64);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn checked_pdf_adapter_handles_valid_hostile_and_recovery_cases() {
        assert_eq!(
            PDF_EXTRACT_TRANSFORM_ID,
            processing_context::PDF_EXTRACT_TRANSFORM_ID
        );
        let service =
            Arc::new(processing_context::PdfTransformRuntime::new().expect("checked PDF service"));

        let one_pdf = synthetic_pdf(&[b"one page".to_vec()], false);
        let one = run_real_pdf(service.clone(), one_pdf.clone())
            .await
            .expect("one page");
        assert_eq!(
            one.output,
            ExtractPdfTextOutput {
                text: "\n\none page".to_string(),
                page_count: 1,
            }
        );
        assert_exact_checked_provenance(&one, &one_pdf);

        let multiple_pdf = synthetic_pdf(&[b"first".to_vec(), b"second".to_vec()], false);
        let multiple = run_real_pdf(service.clone(), multiple_pdf.clone())
            .await
            .expect("multiple pages");
        assert_eq!(
            multiple.output,
            ExtractPdfTextOutput {
                text: "\n\nfirstsecond".to_string(),
                page_count: 2,
            }
        );
        assert_exact_checked_provenance(&multiple, &multiple_pdf);

        let qpdf_encrypted =
            include_bytes!("../../processing-context/tests/fixtures/qpdf-encrypted.pdf").to_vec();
        let qpdf_error = run_real_pdf(service.clone(), qpdf_encrypted)
            .await
            .expect_err("genuine qpdf encryption is unsupported");
        assert_eq!(
            qpdf_error.to_string(),
            format!("{ERR_DOCUMENT_TRANSFORM}: document operation failed [unsupported_document]")
        );

        let pages_201 = (0..201)
            .map(|index| format!("page {index}").into_bytes())
            .collect::<Vec<_>>();
        let hostile = [
            b"%PDF-malformed".to_vec(),
            synthetic_pdf(&[b"secret".to_vec()], true),
            synthetic_pdf(&[], false),
            synthetic_pdf(&[Vec::new()], false),
            synthetic_pdf(&pages_201, false),
            include_bytes!("../../processing-context/tests/fixtures/flate-output-expansion.pdf")
                .to_vec(),
            include_bytes!("../../processing-context/tests/fixtures/type0-missing-descendants.pdf")
                .to_vec(),
        ];
        for fixture in hostile {
            let error = run_real_pdf(service.clone(), fixture)
                .await
                .expect_err("hostile PDF contained");
            assert!(error.to_string().contains(ERR_DOCUMENT_TRANSFORM));
            assert!(!error.to_string().contains("parser"));
            assert!(!error.to_string().contains("wasm"));
        }

        let recovered = run_real_pdf(service, synthetic_pdf(&[b"after failures".to_vec()], false))
            .await
            .expect("fresh valid invocation after failures");
        assert_eq!(recovered.output.page_count, 1);
        assert!(recovered.output.text.contains("after failures"));
    }
}
