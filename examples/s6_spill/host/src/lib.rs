use std::sync::Arc;
use std::time::Duration;

use example_s6_spill as flow;

pub fn bundle() -> host_inproc::FlowBundle {
    let validated_ir = flow::validated_ir();
    let mut registry = kernel_exec::NodeRegistry::new();
    flow::batch_trigger_register(&mut registry).expect("register batch_trigger");
    flow::prepare_payload_register(&mut registry).expect("register prepare_payload");
    flow::store_blob_register(&mut registry).expect("register store_blob");
    flow::slow_ack_register(&mut registry).expect("register slow_ack");
    flow::capture_register(&mut registry).expect("register capture");
    let registry = Arc::new(registry);
    let resolver: Arc<dyn kernel_exec::NodeResolver> =
        Arc::new(kernel_exec::RegistryResolver::new(Arc::clone(&registry)));
    let entrypoints = vec![host_inproc::FlowEntrypoint {
        trigger_alias: "trigger".to_string(),
        capture_alias: "capture".to_string(),
        route_path: Some("/spill".to_string()),
        method: Some("POST".to_string()),
        deadline: Some(Duration::from_millis(2000)),
        route_aliases: vec!["/spill".to_string()],
    }];
    let node_contracts = validated_ir
        .flow()
        .nodes
        .iter()
        .map(|node| host_inproc::NodeContract {
            identifier: node.identifier.clone(),
            contract_hash: None,
            source: host_inproc::NodeSource::Local,
        })
        .collect();

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
    use capabilities::ResourceBag;
    use example_s6_spill::{Ack, BatchRequest};
    use kernel_exec::ExecutionResult;
    use std::sync::Arc;

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
        let resources = ResourceBag::new()
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
}
