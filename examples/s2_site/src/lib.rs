use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};
use std::time::Duration;

use async_stream::stream;
use dag_core::NodeResult;
use dag_macros::{node, trigger};
use futures::Stream;
use kernel_exec::{NodeRegistry, RegistryError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SiteRequest {
    pub site: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SiteSnapshot {
    pub site: String,
    pub status: String,
    pub latency_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SiteEvent {
    pub site: String,
    pub stage: String,
    pub status: String,
    pub latency_ms: u64,
}

impl SiteEvent {
    fn snapshot(snapshot: &SiteSnapshot) -> Self {
        Self {
            site: snapshot.site.clone(),
            stage: "snapshot".to_string(),
            status: snapshot.status.clone(),
            latency_ms: snapshot.latency_ms,
        }
    }

    fn update(site: &str, idx: usize, latency_ms: u64) -> Self {
        let status = if idx.is_multiple_of(2) {
            "degraded"
        } else {
            "operational"
        };
        Self {
            site: site.to_string(),
            stage: format!("update_{idx}"),
            status: status.to_string(),
            latency_ms,
        }
    }
}

struct SiteEventStream {
    inner: Mutex<Pin<Box<dyn Stream<Item = NodeResult<SiteEvent>> + Send>>>,
}

impl SiteEventStream {
    fn new(stream: impl Stream<Item = NodeResult<SiteEvent>> + Send + 'static) -> Self {
        Self {
            inner: Mutex::new(Box::pin(stream)),
        }
    }
}

impl Stream for SiteEventStream {
    type Item = NodeResult<SiteEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut guard = self
            .inner
            .lock()
            .expect("site event stream lock poisoned");
        guard.as_mut().poll_next(cx)
    }
}

impl serde::Serialize for SiteEventStream {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_unit()
    }
}

#[trigger(
    name = "SiteHttpTrigger",
    summary = "Ingress trigger for site status requests"
)]
async fn http_trigger(request: SiteRequest) -> NodeResult<SiteRequest> {
    Ok(request)
}

#[node(
    name = "BuildSnapshot",
    summary = "Construct the initial site snapshot prior to streaming",
    effects = "ReadOnly",
    determinism = "Stable"
)]
async fn build_snapshot(request: SiteRequest) -> NodeResult<SiteSnapshot> {
    // Pretend to fetch existing telemetry for the requested site.
    let snapshot = SiteSnapshot {
        site: request.site,
        status: "operational".to_string(),
        latency_ms: 120,
    };
    Ok(snapshot)
}

#[node(
    name = "StreamTelemetry",
    summary = "Emit incremental status updates as an SSE stream",
    effects = "ReadOnly",
    determinism = "BestEffort",
    out = "SiteEvent"
)]
async fn stream_telemetry(snapshot: SiteSnapshot) -> NodeResult<SiteEventStream> {
    let site = snapshot.site.clone();
    let mut latency = snapshot.latency_ms;
    let stream = stream! {
        yield Ok(SiteEvent::snapshot(&snapshot));

        for idx in 0..5 {
            tokio::time::sleep(Duration::from_millis(120)).await;
            latency += 12;
            yield Ok(SiteEvent::update(&site, idx as usize, latency));
        }
    };

    Ok(SiteEventStream::new(stream))
}

fn stream_telemetry_stream_node_spec() -> &'static dag_core::NodeSpec {
    stream_telemetry_node_spec()
}

fn stream_telemetry_stream_register(
    registry: &mut NodeRegistry,
) -> Result<(), RegistryError> {
    registry.register_stream_fn(
        concat!(module_path!(), "::", stringify!(stream_telemetry)),
        stream_telemetry,
    )
}

dag_macros::workflow_bundle! {
    name: s2_site_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Implements the S2 streaming site-status example with SSE";
    let trigger = http_trigger_node_spec();
    let snapshot = build_snapshot_node_spec();
    let stream = stream_telemetry_stream_node_spec();
    connect!(trigger -> snapshot);
    connect!(snapshot -> stream);
    entrypoint!({
        trigger: "trigger",
        capture: "stream",
        route: "/site/stream",
        method: "POST",
        deadline_ms: 5000,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn flow_contains_expected_nodes() {
        let ir = flow();
        let aliases: Vec<_> = ir.nodes.iter().map(|node| node.alias.as_str()).collect();
        assert_eq!(aliases, vec!["trigger", "snapshot", "stream"]);
        assert_eq!(ir.edges.len(), 2);
    }

    #[test]
    fn workflow_serialises_to_json() {
        let ir = flow();
        let json = serde_json::to_value(&ir).expect("serialise flow");
        assert_eq!(json["profile"], json!("web"));
        assert_eq!(json["nodes"].as_array().unwrap().len(), 3);
    }
}
