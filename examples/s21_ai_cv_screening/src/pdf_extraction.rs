use std::sync::Arc;

use capabilities::ResourceAccess;
use capabilities::artifact::{ByteAccessError, Exact};
use capabilities::context;
use capabilities::transform::{
    TransformBackend, TransformBeginError, TransformExecutionRecord, TransformFailure,
};
use capabilities::workspace::WorkspaceError;
use dag_core::{ImplementationDependencySpec, NodeError, NodeResult};
use dag_macros::def_node;
use sha2::{Digest, Sha256};

use crate::{CvApplication, ExtractedCvApplication};

pub const PDF_EXTRACT_TRANSFORM_ID: &str = "lattice.pdf.extract_text.v1";
pub const PDF_EXTRACT_DEPENDENCY: ImplementationDependencySpec =
    ImplementationDependencySpec::sandboxed_transform(PDF_EXTRACT_TRANSFORM_ID);
pub const ERR_PDF_INTEGRITY: &str = "S21-PDF-001";
pub const ERR_PDF_TRANSFORM: &str = "S21-PDF-002";
pub const ERR_PDF_ENVELOPE: &str = "S21-PDF-003";

const PDF_CONTENT_TYPE: &str = "application/pdf";
const PDF_MAGIC: &[u8] = b"%PDF-";
const MAX_PDF_PAGES: u32 = 200;
const MAX_TEXT_BYTES: usize = 512 * 1024;

#[derive(Debug)]
struct Extraction {
    text: String,
    page_count: u32,
    transform_record: TransformExecutionRecord,
    canonical_text_sha256: [u8; 32],
}

/// S21-owned PDF extraction node. The reusable pieces are the artifact byte
/// plane and host-provided transform runtime; this graph handler is deliberately
/// application-local until a second real flow demonstrates a shared node contract.
#[def_node(
    name = "ExtractCvText",
    summary = "Bounded PDF artifact dereference and sandboxed embedded-text extraction",
    effects = "ReadOnly",
    determinism = "BestEffort",
    resources(workspace_read(capabilities::workspace::Workspace)),
    implementation_dependencies(crate::pdf_extraction::PDF_EXTRACT_DEPENDENCY)
)]
pub async fn extract_cv_text(application: CvApplication) -> NodeResult<ExtractedCvApplication> {
    let artifact = application.cv.clone();
    let extraction = context::with_current_async(|resources| async move {
        extract_with_resources(resources, artifact).await
    })
    .await
    .ok_or_else(|| pdf_error(ERR_PDF_INTEGRITY, "resource_context_unavailable"))??;
    let Extraction {
        text,
        page_count,
        transform_record,
        canonical_text_sha256,
    } = extraction;
    drop(transform_record);
    let _ = canonical_text_sha256;
    Ok(ExtractedCvApplication {
        application,
        resume_text: text,
        page_count,
    })
}

async fn extract_with_resources(
    resources: Arc<dyn ResourceAccess>,
    artifact: capabilities::artifact::Artifact<Exact>,
) -> NodeResult<Extraction> {
    if artifact.content_type != PDF_CONTENT_TYPE {
        return Err(pdf_error(ERR_PDF_INTEGRITY, "mime_mismatch"));
    }

    // CAP110 authority is deliberately reached before the ungated transform service.
    let workspace = resources
        .workspace_read()
        .ok_or_else(|| pdf_error(ERR_PDF_INTEGRITY, "workspace_read_unavailable"))?;
    let runtime = resources
        .transform_runtime()
        .ok_or_else(|| transform_begin_error(TransformBeginError::RuntimeUnavailable))?;
    let runtime_backend = runtime.backend();
    let backend = runtime_backend.metric_label();
    let lease = runtime
        .try_begin(PDF_EXTRACT_TRANSFORM_ID)
        .map_err(transform_begin_error)?;
    let max_input_bytes = lease.max_input_bytes();

    if artifact.len > max_input_bytes {
        return Err(pdf_error(ERR_PDF_INTEGRITY, "length_exceeds_limit"));
    }

    let bytes = workspace
        .read_bounded(&artifact.handle, max_input_bytes)
        .await
        .map_err(map_workspace_error)?;
    let actual_len = u64::try_from(bytes.len())
        .map_err(|_| pdf_error(ERR_PDF_INTEGRITY, "actual_length_unrepresentable"))?;
    if actual_len != artifact.len {
        return Err(pdf_error(ERR_PDF_INTEGRITY, "length_mismatch"));
    }

    let input_sha256: [u8; 32] = Sha256::digest(&bytes).into();
    if let Some(expected) = artifact.content_hash.as_deref() {
        let matches = hash_matches_constant_shape(expected, input_sha256);
        metrics::counter!(
            "lattice.transform.input_hash_comparisons_total",
            "backend" => backend,
            "transform" => PDF_EXTRACT_TRANSFORM_ID,
            "outcome" => if matches { "matched" } else { "mismatch" }
        )
        .increment(1);
        if !matches {
            return Err(pdf_error(ERR_PDF_INTEGRITY, "hash_mismatch"));
        }
    }
    if !bytes.starts_with(PDF_MAGIC) {
        return Err(pdf_error(ERR_PDF_INTEGRITY, "magic_mismatch"));
    }

    let outcome = lease.run(bytes).await.map_err(transform_execution_error)?;
    let (output, record) = outcome.into_parts();
    validate_pdf_envelope(output, record, runtime_backend)
}

fn validate_pdf_envelope(
    mut envelope: Vec<u8>,
    transform_record: TransformExecutionRecord,
    expected_backend: TransformBackend,
) -> NodeResult<Extraction> {
    if transform_record.backend() != expected_backend
        || transform_record.transform_id() != PDF_EXTRACT_TRANSFORM_ID
    {
        return Err(pdf_error(ERR_PDF_ENVELOPE, "invalid_output"));
    }
    if envelope.len() < 4 {
        return Err(pdf_error(ERR_PDF_ENVELOPE, "invalid_output"));
    }
    let page_count = u32::from_le_bytes(
        envelope[..4]
            .try_into()
            .expect("four-byte envelope prefix was checked"),
    );
    if !(1..=MAX_PDF_PAGES).contains(&page_count) {
        return Err(pdf_error(ERR_PDF_ENVELOPE, "invalid_output"));
    }

    let text_bytes = &envelope[4..];
    if text_bytes.is_empty() || text_bytes.len() > MAX_TEXT_BYTES {
        return Err(pdf_error(ERR_PDF_ENVELOPE, "invalid_output"));
    }
    let text_view = std::str::from_utf8(text_bytes)
        .map_err(|_| pdf_error(ERR_PDF_ENVELOPE, "invalid_output"))?;
    if text_view.contains('\r')
        || !text_view
            .chars()
            .all(|character| character == '\n' || character == '\t' || !character.is_control())
        || !text_view
            .chars()
            .any(|character| !character.is_whitespace())
    {
        return Err(pdf_error(ERR_PDF_ENVELOPE, "invalid_output"));
    }

    let canonical_text_sha256 = Sha256::digest(text_bytes).into();
    envelope.drain(..4);
    let text =
        String::from_utf8(envelope).map_err(|_| pdf_error(ERR_PDF_ENVELOPE, "invalid_output"))?;
    Ok(Extraction {
        text,
        page_count,
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

fn pdf_error(code: &'static str, class: &'static str) -> NodeError {
    NodeError::new(format!("{code}: PDF extraction failed [{class}]"))
}

fn transform_begin_error(error: TransformBeginError) -> NodeError {
    pdf_error(ERR_PDF_TRANSFORM, error.as_str())
}

fn transform_execution_error(error: TransformFailure) -> NodeError {
    pdf_error(ERR_PDF_TRANSFORM, error.class().as_str())
}

fn map_workspace_error(error: ByteAccessError) -> NodeError {
    match error {
        ByteAccessError::Workspace(WorkspaceError::TooLarge { .. }) => NodeError::new(format!(
            "{}: bounded workspace entry exceeds the transform ceiling",
            capabilities::workspace::ERR_WORKSPACE_TOO_LARGE
        )),
        ByteAccessError::Workspace(WorkspaceError::Unsupported(_)) => {
            pdf_error(ERR_PDF_INTEGRITY, "workspace_unsupported")
        }
        ByteAccessError::Workspace(WorkspaceError::NotFound(_)) | ByteAccessError::NotFound(_) => {
            pdf_error(ERR_PDF_INTEGRITY, "not_found")
        }
        ByteAccessError::Workspace(WorkspaceError::InvalidPath(_))
        | ByteAccessError::Workspace(WorkspaceError::PathTraversal(_)) => {
            pdf_error(ERR_PDF_INTEGRITY, "invalid_path")
        }
        ByteAccessError::Workspace(WorkspaceError::Backend(_)) => {
            pdf_error(ERR_PDF_INTEGRITY, "workspace_backend")
        }
        ByteAccessError::Workspace(WorkspaceError::MissingWorkspaceRead(_))
        | ByteAccessError::Workspace(WorkspaceError::MissingWorkspaceWrite(_))
        | ByteAccessError::MintVerification
        | ByteAccessError::OutOfScope { .. }
        | ByteAccessError::StoreMismatch { .. } => pdf_error(ERR_PDF_INTEGRITY, "invalid_handle"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use capabilities::artifact::{Artifact, Handle, StoreRef, sha256_hex};
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
    use dag_core::{Determinism, Effects};
    #[cfg(feature = "host-bundle")]
    use dag_core::{FlowBuilder, NodeSpec, Profile, SchemaSpec};
    #[cfg(feature = "host-bundle")]
    use kernel_exec::{FlowExecutor, NodeRegistry};
    #[cfg(feature = "host-bundle")]
    use kernel_plan::validate;
    #[cfg(feature = "host-bundle")]
    use semver::Version;

    use super::*;

    const ROOT_KEY: &[u8] = b"s21-pdf-test-root";

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
        fn with_behavior(behavior: ReadBehavior) -> Self {
            Self {
                behavior: Mutex::new(behavior),
                bounded_calls: AtomicUsize::new(0),
                unbounded_calls: AtomicUsize::new(0),
                last_limit: Mutex::new(None),
            }
        }

        fn bytes(bytes: Vec<u8>) -> Self {
            Self::with_behavior(ReadBehavior::Bytes(bytes))
        }
    }

    impl Capability for RecordingWorkspace {
        fn name(&self) -> &'static str {
            "workspace.s21-pdf-test"
        }
    }

    #[async_trait]
    impl Workspace for RecordingWorkspace {
        async fn read_normalized(
            &self,
            _normalized_path: &str,
        ) -> Result<Option<WorkspaceReadResult>, WorkspaceError> {
            self.unbounded_calls.fetch_add(1, Ordering::SeqCst);
            panic!("S21 extraction must never use unbounded workspace read")
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
            "workspace.s21-fs-delegate"
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
        fn backend(&self) -> capabilities::transform::TransformBackend {
            capabilities::transform::TransformBackend::Wasmtime
        }

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
                        success_record_with_hashes(Some(input_hash), Some(output_hash)),
                    )
                    .expect("successful record"))
                }
                RunBehavior::Failure(class) => Err(TransformFailure::new(
                    class,
                    failure_record(class, Some(input_hash)),
                    Some("private trap / parser diagnostic".to_string()),
                )
                .expect("failure record")),
            }
        }
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

    fn metered_record(
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
        .expect("metered record")
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

    fn success_record_with_hashes(
        input_sha256: Option<[u8; 32]>,
        output_sha256: Option<[u8; 32]>,
    ) -> TransformExecutionRecord {
        metered_record(
            input_sha256,
            output_sha256,
            TransformTerminationClass::Success,
        )
    }

    fn failure_record(
        class: TransformErrorClass,
        input_sha256: Option<[u8; 32]>,
    ) -> TransformExecutionRecord {
        if class == TransformErrorClass::PlatformTerminated {
            TransformExecutionRecord::platform(
                PDF_EXTRACT_TRANSFORM_ID,
                [7; 32],
                "test.abi.v1",
                "2026-07-16",
                PlatformTransformBudgets::new(
                    1,
                    2,
                    1,
                    8 * 1024 * 1024,
                    4 + 512 * 1024,
                    1,
                    TransformInstanceModel::FreshPerInvocation,
                )
                .expect("budgets"),
                input_sha256,
                None,
                TransformTerminationClass::Failure(class),
                PlatformTransformObservations::new(
                    Duration::from_millis(1),
                    input_sha256.map_or(0, |_| 1),
                    0,
                ),
            )
            .expect("platform failure record")
        } else {
            metered_record(
                input_sha256,
                None,
                TransformTerminationClass::Failure(class),
            )
        }
    }

    fn success_record(output: &[u8]) -> TransformExecutionRecord {
        TransformExecutionRecord::platform(
            PDF_EXTRACT_TRANSFORM_ID,
            [7; 32],
            "test.abi.v1",
            "2026-07-16",
            PlatformTransformBudgets::new(
                1,
                2,
                1,
                8 * 1024 * 1024,
                4 + 512 * 1024,
                1,
                TransformInstanceModel::FreshPerInvocation,
            )
            .expect("budgets"),
            Some([1; 32]),
            Some(Sha256::digest(output).into()),
            TransformTerminationClass::Success,
            PlatformTransformObservations::new(Duration::from_millis(1), 1, output.len() as u64),
        )
        .expect("record")
    }

    fn envelope(page_count: u32, text: &[u8]) -> Vec<u8> {
        let mut output = page_count.to_le_bytes().to_vec();
        output.extend_from_slice(text);
        output
    }

    #[test]
    fn inline_node_owns_only_a_generic_transform_dependency() {
        let spec = extract_cv_text_node_spec();
        assert!(spec.identifier.contains("example_s21_ai_cv_screening"));
        assert_eq!(spec.effects, Effects::ReadOnly);
        assert_eq!(spec.determinism, Determinism::BestEffort);
        assert_eq!(spec.implementation_dependencies, &[PDF_EXTRACT_DEPENDENCY]);
        assert_eq!(
            PDF_EXTRACT_DEPENDENCY.kind,
            dag_core::ImplementationDependencyKind::SandboxedTransform
        );
    }

    #[cfg(feature = "host-bundle")]
    fn validated_extraction_flow(declared: bool) -> kernel_plan::ValidatedIR {
        let mut builder = FlowBuilder::new("s21-pdf-test", Version::new(1, 0, 0), Profile::Dev);
        let trigger = builder
            .add_node(
                "trigger",
                &NodeSpec::inline(
                    "tests.s21_pdf.trigger",
                    "Trigger",
                    SchemaSpec::Opaque,
                    SchemaSpec::Opaque,
                    Effects::Pure,
                    Determinism::Strict,
                    None,
                ),
            )
            .expect("trigger");
        let dishonest;
        let extraction_spec = if declared {
            extract_cv_text_node_spec()
        } else {
            dishonest = NodeSpec::inline(
                extract_cv_text_node_spec().identifier,
                "UnderdeclaredS21Extraction",
                SchemaSpec::Opaque,
                SchemaSpec::Opaque,
                Effects::ReadOnly,
                Determinism::BestEffort,
                None,
            );
            &dishonest
        };
        let extraction = builder
            .add_node("extract", extraction_spec)
            .expect("extraction");
        builder.connect(&trigger, &extraction);
        validate(&builder.build()).expect("valid extraction flow")
    }

    #[cfg(feature = "host-bundle")]
    fn extraction_registry() -> NodeRegistry {
        let mut registry = NodeRegistry::new();
        registry
            .register_fn(
                "tests.s21_pdf.trigger",
                |application: CvApplication| async move { Ok(application) },
            )
            .expect("register trigger");
        extract_cv_text_register(&mut registry).expect("register S21 extraction");
        registry
    }

    #[cfg(feature = "host-bundle")]
    fn application(bytes: &[u8]) -> CvApplication {
        CvApplication {
            full_name: "Test Candidate".to_string(),
            email: "candidate@example.test".to_string(),
            expectation: String::new(),
            linkedin: String::new(),
            cv_filename: "resume.pdf".to_string(),
            cv: artifact(bytes),
        }
    }

    #[cfg(feature = "host-bundle")]
    #[tokio::test]
    async fn cap110_denies_underdeclared_adapter_before_transform_or_provider() {
        let pdf = b"%PDF-cap110".to_vec();
        let workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let runtime = RecordingRuntime::output(64, envelope(1, b"must not run"));
        let executor = FlowExecutor::new(Arc::new(extraction_registry())).with_resource_access(
            resources(Arc::clone(&workspace), Some(Arc::clone(&runtime))),
        );

        let error = match executor
            .run_once(
                &validated_extraction_flow(false),
                "trigger",
                serde_json::to_value(application(&pdf)).expect("payload"),
                "extract",
                None,
            )
            .await
        {
            Ok(_) => panic!("underdeclared node must fail"),
            Err(error) => error,
        };
        let message = error.to_string();
        assert_eq!(message.matches("CAP110:").count(), 1, "{message}");
        assert_eq!(workspace.bounded_calls.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.begins.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.runs.load(Ordering::SeqCst), 0);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn adapter_uses_real_openat2_and_rejects_oversize_and_symlinks() {
        use std::fs;
        use std::os::unix::fs::symlink;

        use cap_workspace_fs::{FsWorkspaceConfig, FsWorkspaceFactory};
        use capabilities::workspace::{WorkspaceFactory, WorkspacePolicy, WorkspaceRunScope};

        let temp = tempfile::tempdir().expect("tempdir");
        let factory = FsWorkspaceFactory::new(FsWorkspaceConfig {
            root: temp.path().join("workspace"),
            policy: WorkspacePolicy::default(),
        });

        let valid_scope = WorkspaceRunScope::new("flow", "valid");
        let valid_workspace = factory.open(valid_scope).await.expect("valid workspace");
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
        let extraction = extract_with_resources(
            fs_resources(valid_workspace, Arc::clone(&valid_runtime)),
            artifact(&valid_pdf),
        )
        .await
        .expect("real filesystem extraction");
        assert_eq!(extraction.text, "real fs text");
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
        let error = extract_with_resources(
            fs_resources(oversize_workspace, Arc::clone(&oversize_runtime)),
            oversize_artifact,
        )
        .await
        .expect_err("real provider rejects actual oversize");
        assert!(
            error
                .to_string()
                .contains(capabilities::workspace::ERR_WORKSPACE_TOO_LARGE)
        );
        assert_eq!(oversize_runtime.runs.load(Ordering::SeqCst), 0);

        let outside = temp.path().join("outside");
        fs::create_dir_all(&outside).expect("outside directory");
        fs::write(outside.join("secret.pdf"), b"%PDF-outside").expect("outside file");

        let final_scope = WorkspaceRunScope::new("flow", "final-symlink");
        let final_workspace = factory
            .open(final_scope.clone())
            .await
            .expect("final workspace");
        symlink(
            outside.join("secret.pdf"),
            factory.run_root_path(&final_scope).join("final.pdf"),
        )
        .expect("final symlink");
        let final_runtime = RecordingRuntime::output(64, envelope(1, b"unused"));
        let error = extract_with_resources(
            fs_resources(final_workspace, Arc::clone(&final_runtime)),
            artifact_at(b"%PDF-outside", "final.pdf"),
        )
        .await
        .expect_err("final symlink rejected");
        assert!(error.to_string().contains("workspace_backend"));
        assert!(
            !error
                .to_string()
                .contains(outside.to_string_lossy().as_ref())
        );
        assert_eq!(final_runtime.runs.load(Ordering::SeqCst), 0);

        let intermediate_scope = WorkspaceRunScope::new("flow", "intermediate-symlink");
        let intermediate_workspace = factory
            .open(intermediate_scope.clone())
            .await
            .expect("intermediate workspace");
        symlink(
            &outside,
            factory.run_root_path(&intermediate_scope).join("linked"),
        )
        .expect("intermediate symlink");
        let intermediate_runtime = RecordingRuntime::output(64, envelope(1, b"unused"));
        let error = extract_with_resources(
            fs_resources(intermediate_workspace, Arc::clone(&intermediate_runtime)),
            artifact_at(b"%PDF-outside", "linked/secret.pdf"),
        )
        .await
        .expect_err("intermediate symlink rejected");
        assert!(error.to_string().contains("workspace_backend"));
        assert!(
            !error
                .to_string()
                .contains(outside.to_string_lossy().as_ref())
        );
        assert_eq!(intermediate_runtime.runs.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn successful_adapter_uses_only_bounded_read_and_preserves_provenance() {
        let pdf = b"%PDF-fake bounded input".to_vec();
        let workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let runtime = RecordingRuntime::output(64, envelope(2, b"first\nsecond"));
        let mut input = artifact(&pdf);
        input.content_hash = None;

        let extraction = extract_with_resources(
            resources(Arc::clone(&workspace), Some(Arc::clone(&runtime))),
            input,
        )
        .await
        .expect("adapter succeeds");

        assert_eq!(extraction.text, "first\nsecond");
        assert_eq!(extraction.page_count, 2);
        assert_eq!(
            extraction.transform_record.input_sha256(),
            Some(Sha256::digest(&pdf).into())
        );
        assert_eq!(workspace.bounded_calls.load(Ordering::SeqCst), 1);
        assert_eq!(workspace.unbounded_calls.load(Ordering::SeqCst), 0);
        assert_eq!(*workspace.last_limit.lock().expect("limit"), Some(64));
        assert_eq!(runtime.begins.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.runs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn admission_and_integrity_checks_precede_materialization_and_parser() {
        let pdf = b"%PDF-order".to_vec();

        let workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let runtime = RecordingRuntime::output(64, envelope(1, b"text"));
        let mut wrong_mime = artifact(&pdf);
        wrong_mime.content_type = "Application/PDF".to_string();
        let error = extract_with_resources(
            resources(Arc::clone(&workspace), Some(Arc::clone(&runtime))),
            wrong_mime,
        )
        .await
        .expect_err("MIME mismatch");
        assert!(error.to_string().contains("mime_mismatch"));
        assert_eq!(workspace.bounded_calls.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.begins.load(Ordering::SeqCst), 0);

        let workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let error = extract_with_resources(resources(Arc::clone(&workspace), None), artifact(&pdf))
            .await
            .expect_err("runtime unavailable");
        assert!(error.to_string().contains("runtime_unavailable"));
        assert_eq!(workspace.bounded_calls.load(Ordering::SeqCst), 0);

        let workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let runtime = Arc::new(RecordingRuntime {
            max_input_bytes: 64,
            behavior: RunBehavior::Output(envelope(1, b"text")),
            begins: Arc::new(AtomicUsize::new(0)),
            runs: Arc::new(AtomicUsize::new(0)),
            begin_error: Some(TransformBeginError::Busy),
        });
        let error = extract_with_resources(
            resources(Arc::clone(&workspace), Some(Arc::clone(&runtime))),
            artifact(&pdf),
        )
        .await
        .expect_err("busy");
        assert!(error.to_string().contains("busy"));
        assert_eq!(workspace.bounded_calls.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.runs.load(Ordering::SeqCst), 0);

        let workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let runtime = RecordingRuntime::output(4, envelope(1, b"text"));
        let error = extract_with_resources(
            resources(Arc::clone(&workspace), Some(Arc::clone(&runtime))),
            artifact(&pdf),
        )
        .await
        .expect_err("declared length over ceiling");
        assert!(error.to_string().contains("length_exceeds_limit"));
        assert_eq!(workspace.bounded_calls.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.runs.load(Ordering::SeqCst), 0);

        let workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let runtime = RecordingRuntime::output(64, envelope(1, b"text"));
        let mut mismatch = artifact(&pdf);
        mismatch.content_hash = Some("00".repeat(32));
        let error = extract_with_resources(
            resources(Arc::clone(&workspace), Some(Arc::clone(&runtime))),
            mismatch,
        )
        .await
        .expect_err("hash mismatch");
        assert!(error.to_string().contains("hash_mismatch"));
        assert_eq!(workspace.bounded_calls.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.runs.load(Ordering::SeqCst), 0);

        let not_pdf = b"NOT-PDF!!!".to_vec();
        let workspace = Arc::new(RecordingWorkspace::bytes(not_pdf.clone()));
        let runtime = RecordingRuntime::output(64, envelope(1, b"text"));
        let error = extract_with_resources(
            resources(workspace, Some(Arc::clone(&runtime))),
            artifact(&not_pdf),
        )
        .await
        .expect_err("magic mismatch");
        assert!(error.to_string().contains("magic_mismatch"));
        assert_eq!(runtime.runs.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn forged_authority_and_provider_failures_are_sanitized_before_parser() {
        let pdf = b"%PDF-handle".to_vec();
        let runtime = RecordingRuntime::output(64, envelope(1, b"text"));

        let workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let mut wire = serde_json::to_value(artifact(&pdf)).expect("artifact JSON");
        wire["handle"]["mint"]["tag"][0] = serde_json::json!(255);
        let forged: Artifact<Exact> = serde_json::from_value(wire).expect("wire shape");
        let error = extract_with_resources(
            resources(Arc::clone(&workspace), Some(Arc::clone(&runtime))),
            forged,
        )
        .await
        .expect_err("forged handle");
        assert!(error.to_string().contains("invalid_handle"));
        assert!(!error.to_string().contains("ingress/resume.pdf"));
        assert_eq!(workspace.bounded_calls.load(Ordering::SeqCst), 0);

        let workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let mut foreign_root = artifact(&pdf);
        foreign_root.handle = Handle::host_mint_exact(
            StoreRef::new("workspace"),
            b"foreign-run-root",
            "foreign-root",
            "ingress/resume.pdf",
        );
        let error = extract_with_resources(
            resources(Arc::clone(&workspace), Some(Arc::clone(&runtime))),
            foreign_root,
        )
        .await
        .expect_err("foreign run root");
        assert!(error.to_string().contains("invalid_handle"));
        assert_eq!(workspace.bounded_calls.load(Ordering::SeqCst), 0);

        let workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let mut scope_wire = serde_json::to_value(artifact(&pdf)).expect("artifact JSON");
        scope_wire["handle"]["scope"]["Exact"] = serde_json::json!("ingress/sibling.pdf");
        let altered_scope: Artifact<Exact> =
            serde_json::from_value(scope_wire).expect("altered exact scope wire shape");
        let error = extract_with_resources(
            resources(Arc::clone(&workspace), Some(Arc::clone(&runtime))),
            altered_scope,
        )
        .await
        .expect_err("altered exact scope");
        assert!(error.to_string().contains("invalid_handle"));
        assert_eq!(workspace.bounded_calls.load(Ordering::SeqCst), 0);

        let workspace = Arc::new(RecordingWorkspace::bytes(pdf.clone()));
        let mut wrong_store = artifact(&pdf);
        wrong_store.handle = Handle::host_mint_exact(
            StoreRef::new("other"),
            ROOT_KEY,
            "test-root",
            "ingress/resume.pdf",
        );
        let error = extract_with_resources(
            resources(Arc::clone(&workspace), Some(Arc::clone(&runtime))),
            wrong_store,
        )
        .await
        .expect_err("wrong store");
        assert!(error.to_string().contains("invalid_handle"));
        assert_eq!(workspace.bounded_calls.load(Ordering::SeqCst), 0);

        for (behavior, class) in [
            (ReadBehavior::Missing, "not_found"),
            (ReadBehavior::BlobRef, "workspace_unsupported"),
            (ReadBehavior::Backend, "workspace_backend"),
        ] {
            let workspace = Arc::new(RecordingWorkspace::with_behavior(behavior));
            let error = extract_with_resources(
                resources(workspace, Some(Arc::clone(&runtime))),
                artifact(&pdf),
            )
            .await
            .expect_err("provider failure");
            assert!(error.to_string().contains(class));
            assert!(!error.to_string().contains("/host/path"));
        }
        assert_eq!(runtime.runs.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn actual_size_and_declared_length_mismatch_fail_before_parser() {
        let too_large = b"%PDF-actual-too-large".to_vec();
        let workspace = Arc::new(RecordingWorkspace::bytes(too_large.clone()));
        let runtime = RecordingRuntime::output(8, envelope(1, b"text"));
        let mut lying = artifact(&too_large);
        lying.len = 8;
        lying.content_hash = None;
        let error = extract_with_resources(resources(workspace, Some(Arc::clone(&runtime))), lying)
            .await
            .expect_err("actual oversize");
        assert!(
            error
                .to_string()
                .contains(capabilities::workspace::ERR_WORKSPACE_TOO_LARGE)
        );
        assert_eq!(runtime.runs.load(Ordering::SeqCst), 0);

        let within = b"%PDF-short".to_vec();
        let workspace = Arc::new(RecordingWorkspace::bytes(within.clone()));
        let runtime = RecordingRuntime::output(64, envelope(1, b"text"));
        let mut mismatch = artifact(&within);
        mismatch.len -= 1;
        let error =
            extract_with_resources(resources(workspace, Some(Arc::clone(&runtime))), mismatch)
                .await
                .expect_err("length mismatch");
        assert!(error.to_string().contains("length_mismatch"));
        assert_eq!(runtime.runs.load(Ordering::SeqCst), 0);
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
            let error = extract_with_resources(resources(workspace, Some(runtime)), artifact(&pdf))
                .await
                .expect_err("transform failure");
            let message = error.to_string();
            assert!(message.contains(ERR_PDF_TRANSFORM));
            assert!(message.contains(class.as_str()));
            assert!(!message.contains("private trap"));
            assert!(!message.contains("parser diagnostic"));
        }
    }

    #[test]
    fn canonical_envelope_is_accepted_without_normalization() {
        let output = envelope(2, b"first\nsecond");
        let extraction = validate_pdf_envelope(
            output.clone(),
            success_record(&output),
            TransformBackend::CloudflareWorkers,
        )
        .expect("canonical output");
        assert_eq!(extraction.page_count, 2);
        assert_eq!(extraction.text, "first\nsecond");
        let expected_hash: [u8; 32] = Sha256::digest(b"first\nsecond").into();
        assert_eq!(extraction.canonical_text_sha256, expected_hash);
    }

    #[test]
    fn backend_specific_records_cannot_be_mixed() {
        let output = envelope(1, b"text");
        let error = validate_pdf_envelope(
            output.clone(),
            success_record(&output),
            TransformBackend::Wasmtime,
        )
        .expect_err("platform record under native backend");
        assert_eq!(
            error.to_string(),
            "S21-PDF-003: PDF extraction failed [invalid_output]"
        );
    }

    #[test]
    fn every_noncanonical_envelope_shape_is_rejected() {
        let cases = [
            envelope(0, b"text"),
            envelope(201, b"text"),
            envelope(1, b""),
            envelope(1, b"   \n\t"),
            envelope(1, b"carriage\rreturn"),
            envelope(1, &[0xff]),
            envelope(1, &[0x00]),
        ];
        for output in cases {
            let error = validate_pdf_envelope(
                output.clone(),
                success_record(&output),
                TransformBackend::CloudflareWorkers,
            )
            .expect_err("invalid envelope");
            assert_eq!(
                error.to_string(),
                "S21-PDF-003: PDF extraction failed [invalid_output]"
            );
        }

        let oversized = envelope(1, &vec![b'x'; MAX_TEXT_BYTES + 1]);
        assert!(
            validate_pdf_envelope(
                oversized.clone(),
                success_record(&oversized),
                TransformBackend::CloudflareWorkers,
            )
            .is_err()
        );
    }

    #[test]
    fn expected_hash_comparison_has_constant_shape() {
        let actual: [u8; 32] = Sha256::digest(b"payload").into();
        let expected = actual
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert!(hash_matches_constant_shape(&expected, actual));
        assert!(!hash_matches_constant_shape("short", actual));
        assert!(!hash_matches_constant_shape(&"0".repeat(64), actual));
    }

    #[test]
    fn private_workspace_details_collapse_to_stable_classes() {
        let error = map_workspace_error(ByteAccessError::Workspace(WorkspaceError::Backend(
            "private/object/key".to_string(),
        )));
        assert_eq!(
            error.to_string(),
            "S21-PDF-001: PDF extraction failed [workspace_backend]"
        );
        assert!(!error.to_string().contains("private/object/key"));
    }
}
