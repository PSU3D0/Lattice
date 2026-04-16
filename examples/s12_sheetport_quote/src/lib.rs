#[cfg(all(feature = "host-bundle", not(target_arch = "wasm32")))]
use std::sync::Arc;

use connector_formualizer_sheetport::types::{
    ManifestSourceRef, SheetPortEvaluateInput, SheetPortEvaluateOutput, SheetPortInputPayload,
    SheetPortModelSelector, SheetPortValue, WorkbookSourceRef,
};
use dag_core::{FlowIR, NodeError, NodeResult};
use dag_macros::def_node;
use kernel_plan::{ValidatedIR, validate};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub const QUOTE_WORKBOOK_BLOB_KEY: &str = "models/quote_model.xlsx";
pub const QUOTE_MANIFEST_YAML: &str = include_str!("../assets/quote_model.fio.yaml");
pub const QUOTE_WORKBOOK_BYTES: &[u8] = include_bytes!("../assets/quote_model.xlsx");

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QuoteRequest {
    pub base_price: f64,
    pub quantity: u32,
    pub discount: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QuoteResponse {
    pub total: f64,
    pub manifest_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_name: Option<String>,
    pub mode: String,
}

#[def_node(
    trigger,
    name = "QuoteTrigger",
    summary = "Ingress trigger for the SheetPort quote example",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn quote_trigger(input: QuoteRequest) -> NodeResult<QuoteRequest> {
    Ok(input)
}

#[def_node(
    name = "AdaptQuoteToSheetPortInput",
    summary = "Map quote request fields into the SheetPort connector input surface",
    effects = "Pure",
    determinism = "Strict"
)]
async fn adapt_quote_to_sheetport_input(input: QuoteRequest) -> NodeResult<SheetPortEvaluateInput> {
    Ok(sheetport_request(input, None, false))
}

#[def_node(
    name = "CanonicalSheetPortEvaluate",
    summary = "Local wrapper exposing the SheetPort connector as a discrete topology-visible node",
    identifier = "connector.formualizer.sheetport.evaluate",
    connector_ops(connector_formualizer_sheetport::ops::SheetPortEvaluate)
)]
async fn canonical_sheetport_evaluate(
    input: SheetPortEvaluateInput,
) -> NodeResult<SheetPortEvaluateOutput> {
    connector_formualizer_sheetport::actions::sheetport_evaluate(input).await
}

#[def_node(
    name = "ExtractQuoteResponse",
    summary = "Project the SheetPort connector output into a typed quote response",
    effects = "Pure",
    determinism = "Strict"
)]
async fn extract_quote_response(input: SheetPortEvaluateOutput) -> NodeResult<QuoteResponse> {
    sheetport_output_to_quote_response(input, "bound")
}

#[def_node(
    name = "EvaluateQuoteInternal",
    summary = "Custom node that internally invokes the SheetPort connector using explicit late-bound refs",
    effects = "Effectful",
    determinism = "BestEffort",
    connector_resolution = "late_bound_refs",
    connector_ops(connector_formualizer_sheetport::ops::SheetPortEvaluate)
)]
async fn evaluate_quote_internal(input: QuoteRequest) -> NodeResult<QuoteResponse> {
    let request = sheetport_request(
        input,
        Some(SheetPortModelSelector::LateBoundSources {
            workbook_source: WorkbookSourceRef::Blob {
                key: QUOTE_WORKBOOK_BLOB_KEY.to_string(),
            },
            manifest_source: ManifestSourceRef::InlineYaml {
                value: QUOTE_MANIFEST_YAML.to_string(),
            },
            eval_defaults: None,
        }),
        false,
    );

    let output = connector_formualizer_sheetport::ops::SheetPortEvaluate::invoke(&request)
        .await
        .map_err(|err| NodeError::new(format!("internal SheetPort evaluate failed: {err}")))?;

    sheetport_output_to_quote_response(output, "late_bound")
}

#[def_node(
    name = "Capture",
    summary = "Capture the typed quote response",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture(input: QuoteResponse) -> NodeResult<QuoteResponse> {
    Ok(input)
}

mod bound_bundle_def {
    #[cfg(feature = "host-bundle")]
    use super::{
        adapt_quote_to_sheetport_input_register, canonical_sheetport_evaluate_register,
        capture_register, extract_quote_response_register, quote_trigger_register,
    };
    use dag_macros::node;

    dag_macros::flow! {
        name: s12_sheetport_quote_flow,
        version: "1.0.0",
        profile: Web,
        summary: "Representative SheetPort quote flow using a canonical bound connector node";

        let trigger = node!(quote_trigger);
        let adapt = node!(adapt_quote_to_sheetport_input);
        let evaluate = node!(canonical_sheetport_evaluate);
        let extract = node!(extract_quote_response);
        let capture = node!(capture);

        connect!(trigger -> adapt);
        connect!(adapt -> evaluate);
        connect!(evaluate -> extract);
        connect!(extract -> capture);

        entrypoint!({
            trigger: "trigger",
            capture: "capture",
            route_aliases: ["/quote"],
            method: "POST",
            deadline_ms: 2_000,
        });
    }
}

mod internal_bundle_def {
    #[cfg(feature = "host-bundle")]
    use super::{capture_register, evaluate_quote_internal_register, quote_trigger_register};
    use dag_macros::node;

    dag_macros::flow! {
        name: s12_sheetport_quote_internal_flow,
        version: "1.0.0",
        profile: Web,
        summary: "Representative SheetPort quote flow using an internal custom node with late-bound refs";

        let trigger = node!(quote_trigger);
        let evaluate = node!(evaluate_quote_internal);
        let capture = node!(capture);

        connect!(trigger -> evaluate);
        connect!(evaluate -> capture);

        entrypoint!({
            trigger: "trigger",
            capture: "capture",
            route_aliases: ["/quote/internal"],
            method: "POST",
            deadline_ms: 2_000,
        });
    }
}

pub fn bound_flow() -> FlowIR {
    bound_bundle_def::flow()
}

pub fn internal_flow() -> FlowIR {
    internal_bundle_def::flow()
}

pub fn validated_bound_ir() -> ValidatedIR {
    validate(&bound_flow()).expect("s12 bound quote flow should validate")
}

pub fn validated_internal_ir() -> ValidatedIR {
    validate(&internal_flow()).expect("s12 internal quote flow should validate")
}

#[cfg(all(feature = "host-bundle", not(target_arch = "wasm32")))]
pub fn bound_bundle() -> host_inproc::FlowBundle {
    bundle_for(validated_bound_ir(), bound_flow(), "/quote", || {
        let mut registry = kernel_exec::NodeRegistry::new();
        quote_trigger_register(&mut registry).expect("register trigger");
        adapt_quote_to_sheetport_input_register(&mut registry).expect("register adapt");
        canonical_sheetport_evaluate_register(&mut registry)
            .expect("register canonical sheetport connector wrapper");
        extract_quote_response_register(&mut registry).expect("register extract");
        capture_register(&mut registry).expect("register capture");
        registry
    })
}

#[cfg(all(feature = "host-bundle", not(target_arch = "wasm32")))]
pub fn internal_bundle() -> host_inproc::FlowBundle {
    bundle_for(
        validated_internal_ir(),
        internal_flow(),
        "/quote/internal",
        || {
            let mut registry = kernel_exec::NodeRegistry::new();
            quote_trigger_register(&mut registry).expect("register trigger");
            evaluate_quote_internal_register(&mut registry).expect("register internal evaluator");
            capture_register(&mut registry).expect("register capture");
            registry
        },
    )
}

#[cfg(all(feature = "host-bundle", not(target_arch = "wasm32")))]
fn bundle_for(
    validated_ir: ValidatedIR,
    flow: FlowIR,
    route_path: &str,
    registry_builder: impl FnOnce() -> kernel_exec::NodeRegistry,
) -> host_inproc::FlowBundle {
    use std::time::Duration;

    use host_inproc::{FlowBundle, FlowEntrypoint, NodeContract, NodeSource};
    use kernel_exec::RegistryResolver;

    let registry = registry_builder();
    let node_contracts = flow
        .nodes
        .iter()
        .map(|node| NodeContract {
            identifier: node.identifier.clone(),
            contract_hash: None,
            source: NodeSource::Local,
        })
        .collect();

    FlowBundle {
        validated_ir,
        entrypoints: vec![FlowEntrypoint {
            trigger_alias: "trigger".to_string(),
            capture_alias: "capture".to_string(),
            route_path: Some(route_path.to_string()),
            method: Some("POST".to_string()),
            deadline: Some(Duration::from_millis(2_000)),
            route_aliases: vec![route_path.to_string()],
        }],
        resolver: Arc::new(RegistryResolver::new(Arc::new(registry))),
        node_contracts,
        environment_plugins: Vec::new(),
    }
}

fn sheetport_request(
    input: QuoteRequest,
    model: Option<SheetPortModelSelector>,
    emit_debug_artifacts: bool,
) -> SheetPortEvaluateInput {
    SheetPortEvaluateInput {
        model,
        inputs: SheetPortInputPayload {
            ports: [
                (
                    "base_price".to_string(),
                    SheetPortValue::Scalar {
                        value: json!(input.base_price),
                    },
                ),
                (
                    "quantity".to_string(),
                    SheetPortValue::Scalar {
                        value: json!(input.quantity),
                    },
                ),
                (
                    "discount".to_string(),
                    SheetPortValue::Scalar {
                        value: json!(input.discount),
                    },
                ),
            ]
            .into_iter()
            .collect(),
        },
        eval: None,
        emit_debug_artifacts,
    }
}

fn sheetport_output_to_quote_response(
    output: SheetPortEvaluateOutput,
    mode: &str,
) -> NodeResult<QuoteResponse> {
    let Some(SheetPortValue::Scalar { value }) = output.outputs.ports.get("total") else {
        return Err(NodeError::new(
            "missing `total` output from SheetPort quote model",
        ));
    };
    let total = value
        .as_f64()
        .ok_or_else(|| NodeError::new(format!("expected numeric `total`, got `{value}`")))?;

    Ok(QuoteResponse {
        total,
        manifest_id: output.manifest_id,
        connection_name: output.connection_name,
        mode: mode.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use capabilities::ResourceBag;
    use capabilities::blob::{BlobStore, MemoryBlobStore};
    use capabilities::connector::{
        ConnectorBindingScope, ConnectorRuntime, ConnectorRuntimeError, EndpointProfileDescriptor,
        OutboundAuthProfileDescriptor, ResolvedConnectorConnection, ResolvedEndpointProfile,
    };
    use capabilities::durability::{
        CheckpointError, CheckpointFilter, CheckpointHandle, CheckpointRecord, CheckpointStore,
        Lease,
    };
    use capabilities::workspace::{
        Workspace, WorkspaceDeleteResult, WorkspaceEntry, WorkspaceListOptions,
        WorkspaceReadResult, WorkspaceWriteOptions, WorkspaceWriteResult,
    };
    use host_inproc::{HostRuntime, Invocation};
    use kernel_exec::ExecutionError;
    use serde_json::json;

    use super::*;

    #[derive(Clone)]
    struct MockRuntime {
        connection: Option<ResolvedConnectorConnection>,
    }

    #[derive(Default)]
    struct MemoryWorkspace {
        files: std::sync::Mutex<std::collections::BTreeMap<String, Vec<u8>>>,
        clock: std::sync::Mutex<u64>,
    }

    impl capabilities::Capability for MemoryWorkspace {
        fn name(&self) -> &'static str {
            "workspace.memory"
        }
    }

    #[async_trait]
    impl Workspace for MemoryWorkspace {
        async fn read_normalized(
            &self,
            normalized_path: &str,
        ) -> Result<Option<WorkspaceReadResult>, capabilities::workspace::WorkspaceError> {
            Ok(self
                .files
                .lock()
                .expect("workspace lock")
                .get(normalized_path)
                .cloned()
                .map(WorkspaceReadResult::Bytes))
        }

        async fn write_normalized(
            &self,
            normalized_path: &str,
            data: &[u8],
            _options: WorkspaceWriteOptions,
        ) -> Result<WorkspaceWriteResult, capabilities::workspace::WorkspaceError> {
            self.files
                .lock()
                .expect("workspace lock")
                .insert(normalized_path.to_string(), data.to_vec());
            let mut clock = self.clock.lock().expect("clock lock");
            *clock += 1;
            Ok(WorkspaceWriteResult {
                path: normalized_path.to_string(),
                size_bytes: data.len() as u64,
                updated_at_ms: *clock,
            })
        }

        async fn list_normalized(
            &self,
            options: WorkspaceListOptions,
        ) -> Result<Vec<WorkspaceEntry>, capabilities::workspace::WorkspaceError> {
            let prefix = options.prefix;
            let files = self.files.lock().expect("workspace lock");
            Ok(files
                .iter()
                .filter(|(path, _)| {
                    prefix
                        .as_deref()
                        .map(|prefix| path.starts_with(prefix))
                        .unwrap_or(true)
                })
                .enumerate()
                .map(|(index, (path, bytes))| WorkspaceEntry {
                    path: path.clone(),
                    size_bytes: bytes.len() as u64,
                    updated_at_ms: index as u64,
                    content_hash: Some(format!("sha256:{}", bytes.len())),
                })
                .collect())
        }

        async fn delete_normalized(
            &self,
            normalized_path: &str,
        ) -> Result<WorkspaceDeleteResult, capabilities::workspace::WorkspaceError> {
            let deleted = self
                .files
                .lock()
                .expect("workspace lock")
                .remove(normalized_path)
                .is_some();
            Ok(WorkspaceDeleteResult { deleted })
        }
    }

    struct NoopCheckpointStore;

    impl capabilities::Capability for NoopCheckpointStore {
        fn name(&self) -> &'static str {
            "checkpoint_store.noop"
        }
    }

    #[async_trait]
    impl CheckpointStore for NoopCheckpointStore {
        async fn put(
            &self,
            _record: CheckpointRecord,
        ) -> Result<CheckpointHandle, CheckpointError> {
            Err(CheckpointError::Storage(
                "noop checkpoint store".to_string(),
            ))
        }

        async fn get(
            &self,
            _handle: &CheckpointHandle,
        ) -> Result<CheckpointRecord, CheckpointError> {
            Err(CheckpointError::NotFound)
        }

        async fn ack(&self, _handle: &CheckpointHandle) -> Result<(), CheckpointError> {
            Ok(())
        }

        async fn lease(
            &self,
            _handle: &CheckpointHandle,
            _ttl: std::time::Duration,
        ) -> Result<Lease, CheckpointError> {
            Err(CheckpointError::LeaseConflict)
        }

        async fn release_lease(&self, _lease: Lease) -> Result<(), CheckpointError> {
            Ok(())
        }

        async fn list(
            &self,
            _filter: CheckpointFilter,
        ) -> Result<Vec<CheckpointHandle>, CheckpointError> {
            Ok(Vec::new())
        }
    }

    fn connector_scope() -> ConnectorBindingScope {
        ConnectorBindingScope::new(
            "flow.sheetport.quote",
            "quote_node",
            "tests::quote_node",
            connector_formualizer_sheetport::CONNECTOR_FAMILY,
        )
    }

    #[async_trait]
    impl ConnectorRuntime for MockRuntime {
        async fn apply_outbound_auth(
            &self,
            _scope: &ConnectorBindingScope,
            _profile: &OutboundAuthProfileDescriptor,
            _request: &mut capabilities::http::HttpRequest,
        ) -> Result<(), ConnectorRuntimeError> {
            unreachable!("auth not used in SheetPort quote example")
        }

        async fn resolve_endpoint_profile(
            &self,
            _scope: &ConnectorBindingScope,
            _profile: &EndpointProfileDescriptor,
        ) -> Result<ResolvedEndpointProfile, ConnectorRuntimeError> {
            unreachable!("endpoint profiles not used in SheetPort quote example")
        }

        async fn resolve_connection(
            &self,
            _scope: &ConnectorBindingScope,
        ) -> Result<Option<ResolvedConnectorConnection>, ConnectorRuntimeError> {
            Ok(self.connection.clone())
        }

        async fn resolve_required_effect_hints(
            &self,
            _scope: &ConnectorBindingScope,
            selected_mode: dag_core::ConnectorResolutionModeDecl,
        ) -> Result<Vec<String>, ConnectorRuntimeError> {
            if selected_mode != dag_core::ConnectorResolutionModeDecl::BoundConnection {
                return Ok(Vec::new());
            }
            let Some(connection) = &self.connection else {
                return Ok(Vec::new());
            };
            let mut hints = Vec::new();
            if connection.config["workbook_source"]["kind"] == json!("blob") {
                hints.push(capabilities::blob::HINT_BLOB_READ.to_string());
            }
            if connection.config["manifest_source"]["kind"] == json!("blob") {
                hints.push(capabilities::blob::HINT_BLOB_READ.to_string());
            }
            hints.sort();
            hints.dedup();
            Ok(hints)
        }
    }

    async fn quote_blob_store() -> Arc<MemoryBlobStore> {
        let blob = Arc::new(MemoryBlobStore::default());
        blob.put(QUOTE_WORKBOOK_BLOB_KEY, QUOTE_WORKBOOK_BYTES)
            .await
            .expect("store quote workbook bytes");
        blob
    }

    fn bound_runtime() -> Arc<MockRuntime> {
        Arc::new(MockRuntime {
            connection: Some(ResolvedConnectorConnection {
                connection_name: Some("quote_model_v1".to_string()),
                connector_id: connector_formualizer_sheetport::CONNECTOR_FAMILY.to_string(),
                config: json!({
                    "workbook_source": {
                        "kind": "blob",
                        "key": QUOTE_WORKBOOK_BLOB_KEY
                    },
                    "manifest_source": {
                        "kind": "inline_yaml",
                        "value": QUOTE_MANIFEST_YAML
                    },
                    "eval_defaults": {
                        "freeze_volatile": true,
                        "rng_seed": 7
                    }
                }),
            }),
        })
    }

    fn sample_request() -> QuoteRequest {
        QuoteRequest {
            base_price: 100.0,
            quantity: 2,
            discount: 0.1,
        }
    }

    fn export_runtime() -> Arc<MockRuntime> {
        Arc::new(MockRuntime {
            connection: Some(ResolvedConnectorConnection {
                connection_name: Some("quote_model_v1".to_string()),
                connector_id: connector_formualizer_sheetport::CONNECTOR_FAMILY.to_string(),
                config: json!({
                    "workbook_source": {
                        "kind": "blob",
                        "key": QUOTE_WORKBOOK_BLOB_KEY
                    },
                    "manifest_source": {
                        "kind": "inline_yaml",
                        "value": QUOTE_MANIFEST_YAML
                    },
                    "artifact_policy": {
                        "allow_workspace_export": true
                    }
                }),
            }),
        })
    }

    async fn execute_bound(request: QuoteRequest) -> QuoteResponse {
        let bundle = bound_bundle();
        let payload = serde_json::to_value(&request).expect("serialize request");
        let bag = ResourceBag::new()
            .with_blob(quote_blob_store().await)
            .with_connector_runtime(bound_runtime());
        let output = bundle
            .executor()
            .with_resource_bag(bag)
            .run_once(&bundle.validated_ir, "trigger", payload, "capture", None)
            .await
            .expect("bound flow should execute");
        match output {
            host_inproc::HostExecutionResult::Value(value) => {
                serde_json::from_value(value).expect("decode bound response")
            }
            _ => panic!("expected value output from bound flow"),
        }
    }

    async fn execute_internal(request: QuoteRequest) -> QuoteResponse {
        let bundle = internal_bundle();
        let payload = serde_json::to_value(&request).expect("serialize request");
        let bag = ResourceBag::new().with_blob(quote_blob_store().await);
        let output = bundle
            .executor()
            .with_resource_bag(bag)
            .run_once(&bundle.validated_ir, "trigger", payload, "capture", None)
            .await
            .expect("internal flow should execute");
        match output {
            host_inproc::HostExecutionResult::Value(value) => {
                serde_json::from_value(value).expect("decode internal response")
            }
            _ => panic!("expected value output from internal flow"),
        }
    }

    #[test]
    fn flows_validate_and_internal_node_declares_sheetport_connector_op() {
        let _bound_validated = validated_bound_ir();
        let _internal_validated = validated_internal_ir();

        let bound = bound_flow();
        let internal = internal_flow();

        assert_eq!(bound.nodes.len(), 5);
        let internal_node = internal.node("evaluate").expect("internal evaluator node");
        assert_eq!(internal_node.connector_ops.len(), 1);
        assert_eq!(
            internal_node.connector_ops[0].operation_id,
            connector_formualizer_sheetport::SHEETPORT_EVALUATE_IDENTIFIER
        );
        assert_eq!(
            internal_node.connector_ops[0].default_resolution_mode,
            dag_core::ConnectorResolutionModeDecl::BoundConnection
        );
        assert_eq!(
            internal_node.connector_ops[0].selected_resolution_mode,
            dag_core::ConnectorResolutionModeDecl::LateBoundRefs
        );
        assert_eq!(
            internal_node.connector_ops[0].supported_resolution_modes,
            vec![
                dag_core::ConnectorResolutionModeDecl::BoundConnection,
                dag_core::ConnectorResolutionModeDecl::LateBoundRefs,
                dag_core::ConnectorResolutionModeDecl::InlinePayload,
            ]
        );
    }

    #[tokio::test]
    async fn canonical_bound_flow_executes_quote_model() {
        let response = execute_bound(sample_request()).await;
        assert_eq!(response.manifest_id, "quote-model");
        assert_eq!(response.connection_name.as_deref(), Some("quote_model_v1"));
        assert_eq!(response.mode, "bound");
        assert!((response.total - 180.0).abs() < 0.0001);
    }

    #[tokio::test]
    async fn internal_late_bound_flow_executes_quote_model() {
        let response = execute_internal(sample_request()).await;
        assert_eq!(response.manifest_id, "quote-model");
        assert_eq!(response.connection_name, None);
        assert_eq!(response.mode, "late_bound");
        assert!((response.total - 180.0).abs() < 0.0001);
    }

    #[tokio::test]
    async fn canonical_and_internal_flows_produce_same_quote() {
        let bound = execute_bound(sample_request()).await;
        let internal = execute_internal(sample_request()).await;

        assert!((bound.total - internal.total).abs() < 0.0001);
        assert_eq!(bound.manifest_id, internal.manifest_id);
    }

    #[tokio::test]
    async fn canonical_wrapper_can_export_evaluated_workbook_to_workspace() {
        let blob = quote_blob_store().await;
        let workspace = Arc::new(MemoryWorkspace::default());
        let bag = ResourceBag::new()
            .with_blob(blob)
            .with_workspace(Arc::clone(&workspace))
            .with_connector_runtime(export_runtime())
            .with_connector_scope(connector_scope());

        let request = sheetport_request(sample_request(), None, true);
        let output = capabilities::context::with_resources(Arc::new(bag), async {
            canonical_sheetport_evaluate(request)
                .await
                .expect("canonical wrapper export succeeds")
        })
        .await;

        let path = output
            .debug_artifacts
            .as_ref()
            .and_then(|artifacts| artifacts.evaluated_workbook_workspace_path.as_deref())
            .expect("workspace artifact path");
        assert_eq!(
            path,
            "artifacts/sheetport/quote-model/quote_model_v1/evaluated.xlsx"
        );
        assert!(
            workspace
                .files
                .lock()
                .expect("workspace lock")
                .contains_key(path)
        );
    }

    #[tokio::test]
    async fn canonical_bound_flow_preflight_requires_blob_via_resolution_aware_check() {
        let bundle = bound_bundle();
        let payload = serde_json::to_value(sample_request()).expect("serialize request");
        let bag = ResourceBag::new()
            .with_connector_runtime(bound_runtime())
            .with_checkpoint_store(Arc::new(NoopCheckpointStore));
        let err = match HostRuntime::new(bundle.executor(), Arc::new(bundle.validated_ir.clone()))
            .with_resource_bag(bag)
            .execute(Invocation::new("trigger", "capture", payload))
            .await
        {
            Ok(_) => panic!("missing blob should fail at preflight"),
            Err(err) => err,
        };

        match err {
            ExecutionError::MissingCapabilities { hints } => {
                assert_eq!(hints, vec![capabilities::blob::HINT_BLOB_READ.to_string()]);
            }
            other => panic!("expected MissingCapabilities, got {other}"),
        }
    }

    #[tokio::test]
    async fn internal_late_bound_flow_defers_blob_requirement_to_runtime() {
        let bundle = internal_bundle();
        let payload = serde_json::to_value(sample_request()).expect("serialize request");
        let err = match bundle
            .executor()
            .with_resource_bag(ResourceBag::new())
            .run_once(&bundle.validated_ir, "trigger", payload, "capture", None)
            .await
        {
            Ok(_) => panic!("runtime should fail without blob for late-bound refs"),
            Err(err) => err,
        };

        match err {
            ExecutionError::NodeFailed { source, .. } => {
                let message = source.to_string();
                assert!(message.contains("missing blob capability"), "{message}");
            }
            other => panic!("expected NodeFailed, got {other}"),
        }
    }
}
