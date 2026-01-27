use std::sync::Arc;
use std::time::Duration;

use capabilities::context;
use dag_core::{FlowIR, IdempotencyScope, NodeError, NodeResult};
use dag_macros::{node, trigger};
use kernel_plan::{ValidatedIR, validate};
use serde::{Deserialize, Serialize};

#[allow(dead_code)]
const SPILL_THRESHOLD_BYTES: u64 = 512;
const IDEMPOTENCY_TTL_MS: u64 = 300_000;
const ACK_DELAY_MS: u64 = 120;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchRequest {
    pub batch_id: String,
    pub items: Vec<String>,
    #[serde(default)]
    pub lf_burst_index: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlobPayload {
    pub batch_id: String,
    pub item: String,
    pub blob_key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ack {
    pub batch_id: String,
    pub item: String,
    pub stored: bool,
}

#[trigger(name = "BatchTrigger", summary = "Ingress trigger for batch ingest")]
async fn batch_trigger(request: BatchRequest) -> NodeResult<BatchRequest> {
    Ok(request)
}

#[node(
    name = "PreparePayload",
    summary = "Package each item with its blob key",
    effects = "Pure",
    determinism = "Strict"
)]
async fn prepare_payload(request: BatchRequest) -> NodeResult<Vec<BlobPayload>> {
    let index_suffix = request
        .lf_burst_index
        .map(|idx| format!("-{idx}"))
        .unwrap_or_default();
    let batch_id = format!("{}{}", request.batch_id, index_suffix);

    let payloads = request
        .items
        .into_iter()
        .enumerate()
        .map(|(idx, item)| BlobPayload {
            batch_id: batch_id.clone(),
            item,
            blob_key: format!("batch/{batch_id}/payload/{idx}.json"),
        })
        .collect();

    Ok(payloads)
}

#[node(
    name = "StoreBlob",
    summary = "Persist payload to blob storage",
    effects = "Effectful",
    determinism = "BestEffort",
    resources(blob_store(capabilities::blob::BlobStore))
)]
async fn store_blob(payloads: Vec<BlobPayload>) -> NodeResult<Vec<Ack>> {
    let mut acks = Vec::with_capacity(payloads.len());
    let stored_payloads = payloads.clone();
    context::with_current_async(move |resources| async move {
        let blob = resources
            .blob()
            .ok_or_else(|| NodeError::new("blob capability missing"))?;
        for payload in &stored_payloads {
            let body = serde_json::to_vec(payload)
                .map_err(|err| NodeError::new(format!("serialize payload: {err}")))?;
            blob.put(&payload.blob_key, &body)
                .await
                .map_err(|err| NodeError::new(format!("blob put failed: {err}")))?;
        }
        Ok::<_, NodeError>(())
    })
    .await
    .ok_or_else(|| NodeError::new("resource context missing"))??;

    for payload in payloads {
        acks.push(Ack {
            batch_id: payload.batch_id,
            item: payload.item,
            stored: true,
        });
    }

    Ok(acks)
}

#[node(
    name = "SlowAck",
    summary = "Simulate slow downstream acknowledgement",
    effects = "Pure",
    determinism = "Strict"
)]
async fn slow_ack(acks: Vec<Ack>) -> NodeResult<Vec<Ack>> {
    tokio::time::sleep(Duration::from_millis(ACK_DELAY_MS)).await;
    Ok(acks)
}

#[node(
    name = "Capture",
    summary = "Capture acknowledgements",
    effects = "Pure",
    determinism = "Strict"
)]
async fn capture(acks: Vec<Ack>) -> NodeResult<Vec<Ack>> {
    Ok(acks)
}

mod bundle_def {
    use super::{
        batch_trigger_node_spec, batch_trigger_register, capture_node_spec, capture_register,
        prepare_payload_node_spec, prepare_payload_register, slow_ack_node_spec,
        slow_ack_register, store_blob_node_spec, store_blob_register,
    };

    dag_macros::workflow_bundle! {
        name: s6_spill_flow,
        version: "1.0.0",
        profile: Web,
        summary: "Demonstrates buffer and spill on batch ingest workflows";

        let trigger = batch_trigger_node_spec();
        let prepare = prepare_payload_node_spec();
        let store = store_blob_node_spec();
        let ack = slow_ack_node_spec();
        let capture = capture_node_spec();

        connect!(trigger -> prepare);
        connect!(prepare -> store);
        connect!(store -> ack);
        connect!(ack -> capture);

        buffer!(prepare -> store, max_items = 1);
        spill!(prepare -> store, tier = "local", threshold_bytes = 512);

        entrypoint!({
            trigger: "trigger",
            capture: "capture",
            route: "/spill",
            method: "POST",
            deadline_ms: 2000,
        });
    }
}

pub fn flow() -> FlowIR {
    let mut flow = bundle_def::flow();
    if let Some(node) = flow.nodes.iter_mut().find(|node| node.alias == "store") {
        node.idempotency.key = Some("batch_id".to_string());
        node.idempotency.scope = Some(IdempotencyScope::Node);
        node.idempotency.ttl_ms = Some(IDEMPOTENCY_TTL_MS);
    }
    flow
}

pub fn validated_ir() -> ValidatedIR {
    validate(&flow()).expect("S6 flow should validate")
}

pub fn bundle() -> host_inproc::FlowBundle {
    let validated_ir = validated_ir();
    let mut registry = kernel_exec::NodeRegistry::new();
    batch_trigger_register(&mut registry).expect("register batch_trigger");
    prepare_payload_register(&mut registry).expect("register prepare_payload");
    store_blob_register(&mut registry).expect("register store_blob");
    slow_ack_register(&mut registry).expect("register slow_ack");
    capture_register(&mut registry).expect("register capture");
    let resolver: Arc<dyn host_inproc::NodeResolver> = Arc::new(registry);
    let entrypoints = vec![host_inproc::FlowEntrypoint {
        trigger_alias: "trigger".to_string(),
        capture_alias: "capture".to_string(),
        route_path: Some("/spill".to_string()),
        method: Some("POST".to_string()),
        deadline: Some(Duration::from_millis(2000)),
    }];
    let node_contracts = vec![
        host_inproc::NodeContract {
            identifier: batch_trigger_node_spec().identifier.to_string(),
            contract_hash: None,
            source: host_inproc::NodeSource::Local,
        },
        host_inproc::NodeContract {
            identifier: prepare_payload_node_spec().identifier.to_string(),
            contract_hash: None,
            source: host_inproc::NodeSource::Local,
        },
        host_inproc::NodeContract {
            identifier: store_blob_node_spec().identifier.to_string(),
            contract_hash: None,
            source: host_inproc::NodeSource::Local,
        },
        host_inproc::NodeContract {
            identifier: slow_ack_node_spec().identifier.to_string(),
            contract_hash: None,
            source: host_inproc::NodeSource::Local,
        },
        host_inproc::NodeContract {
            identifier: capture_node_spec().identifier.to_string(),
            contract_hash: None,
            source: host_inproc::NodeSource::Local,
        },
    ];
    host_inproc::FlowBundle {
        validated_ir,
        entrypoints,
        resolver,
        node_contracts,
        environment_plugins: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kernel_exec::ExecutionResult;
    use std::sync::Arc;
    use serde_json::json;

    #[tokio::test]
    async fn spill_flow_runs_end_to_end() {
        let bundle = bundle();
        let entrypoint = bundle.entrypoints.first().expect("entrypoint");
        let batch = BatchRequest {
            batch_id: "b-1".to_string(),
            items: vec!["alpha".into(), "beta".into(), "gamma".into()],
            lf_burst_index: None,
        };
        let payload = serde_json::to_value(&batch).expect("serialize batch");
        let resources = capabilities::ResourceBag::new()
            .with_blob(Arc::new(capabilities::blob::MemoryBlobStore::new()));
        let result = bundle
            .executor()
            .with_resource_bag(resources)
            .with_edge_capacity(1)
            .run_once(
                &bundle.validated_ir,
                entrypoint.trigger_alias.as_str(),
                payload,
                entrypoint.capture_alias.as_str(),
                entrypoint.deadline,
            )
            .await
            .expect("spill flow executes");

        match result {
            ExecutionResult::Value(value) => {
                let acked: Vec<Ack> = serde_json::from_value(value).expect("decode ack");
                assert_eq!(acked.len(), 3);
            }
            ExecutionResult::Stream(_) => panic!("expected value response"),
        }
    }

    #[test]
    fn binding_requires_blob_resource_hint() {
        let ir = flow();
        assert!(ir.nodes.iter().any(|node| {
            node.effect_hints
                .iter()
                .any(|hint| hint == capabilities::blob::HINT_BLOB_WRITE)
        }));
    }

    #[test]
    fn flow_contains_spill_policy() {
        let ir = flow();
        assert_eq!(ir.edges.len(), 4);
        let edge = ir
            .edges
            .iter()
            .find(|edge| edge.from == "prepare" && edge.to == "store")
            .expect("spill edge");
        assert_eq!(edge.buffer.max_items, Some(1));
        assert_eq!(edge.buffer.spill_tier.as_deref(), Some("local"));
        assert_eq!(
            edge.buffer.spill_threshold_bytes,
            Some(SPILL_THRESHOLD_BYTES)
        );
    }

    #[test]
    fn sample_payload_is_large_enough() {
        let batch = BatchRequest {
            batch_id: "example".to_string(),
            items: vec!["alpha".repeat(200)],
            lf_burst_index: None,
        };
        let payload = json!(batch);
        let size = serde_json::to_vec(&payload)
            .expect("serialize payload")
            .len();
        assert!(
            size >= SPILL_THRESHOLD_BYTES as usize,
            "payload size {size} < {SPILL_THRESHOLD_BYTES} bytes"
        );
    }
}
