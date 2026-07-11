//! The normative §6 mechanism, in full: an `http_post` node behind a real
//! **exactly-once edge** (`Delivery::ExactlyOnce`) with an **idempotency key**,
//! a **TTL**, and a **dedupe binding** (`resource::dedupe::write` +
//! `DedupeStore`). kernel-plan's EXACT001–003 checks pass only when all four
//! are present, so this flow is the compile-checked reference for the rule the
//! clone playbook mandates.
//!
//! The main [`super`] flow uses the KV idempotency pattern (s16/s18) because
//! the CLI's lock-provisioned resource bag does not mint a dedupe store; this
//! submodule is exercised in-crate with a `MemoryDedupeStore`, proving the real
//! `Delivery::ExactlyOnce` wiring end-to-end (a redelivery does not
//! double-fire).

use connector_http::ops::HttpPost;
use connector_http::{HttpJsonOutput, HttpWriteInput};
use dag_core::{NodeError, NodeResult};
use dag_macros::{def_node, node};
use serde::{Deserialize, Serialize};

/// Minimum TTL kernel-plan requires on an exactly-once edge target
/// (`MIN_EXACTLY_ONCE_TTL_MS`). Anything shorter is EXACT003.
pub const EXACTLY_ONCE_TTL_MS: u64 = 300_000;

/// A write request: the idempotency key plus the body to POST.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct WriteRequest {
    pub idempotency_key: String,
    pub path: String,
    pub body: serde_json::Value,
}

/// What the exactly-once write did.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CommitResult {
    pub idempotency_key: String,
    /// `false` on a redelivery whose reservation was already taken.
    pub applied: bool,
    pub remote_id: String,
}

fn node_error(err: impl std::fmt::Display) -> NodeError {
    NodeError::new(err.to_string())
}

/// Seeds the write request.
#[def_node(
    trigger,
    name = "ExactlyOnceSeed",
    summary = "Seed the exactly-once write request",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn seed(request: WriteRequest) -> NodeResult<WriteRequest> {
    Ok(request)
}

/// The exactly-once write target: `connector.http.post` behind a
/// `DedupeStore.put_if_absent` reservation with a TTL. The `idempotency(...)`
/// attribute + `resource::dedupe::write` + the `delivery!(... exactly_once)`
/// edge in the flow satisfy EXACT001–003.
#[def_node(
    name = "ExactlyOnceCommit",
    identifier = "connector.http.exactly_once_commit",
    summary = "POST via connector.http.post behind an exactly-once dedupe reservation",
    effects = "Effectful",
    determinism = "BestEffort",
    connector_ops(connector_http::ops::HttpPost),
    resources(dedupe_write(capabilities::dedupe::DedupeStore)),
    idempotency(key = "idempotency_key", scope = "Edge", ttl_ms = 300_000)
)]
async fn commit(request: WriteRequest) -> NodeResult<CommitResult> {
    use std::time::Duration;

    let key = request.idempotency_key.clone();
    let first = capabilities::context::with_current_async(|resources| {
        let key = key.clone();
        async move {
            let store = resources.dedupe_store().ok_or_else(|| {
                NodeError::new("commit requires a dedupe capability (resource::dedupe::write)")
            })?;
            store
                .put_if_absent(key.as_bytes(), Duration::from_millis(EXACTLY_ONCE_TTL_MS))
                .await
                .map_err(|err| node_error(format!("dedupe reservation failed: {err}")))
        }
    })
    .await
    .ok_or_else(|| NodeError::new("commit missing ResourceAccess context"))??;

    if !first {
        return Ok(CommitResult {
            idempotency_key: key,
            applied: false,
            remote_id: String::new(),
        });
    }

    let out: HttpJsonOutput = HttpPost::invoke(&HttpWriteInput {
        path: request.path.clone(),
        body: Some(request.body.clone()),
        ..Default::default()
    })
    .await
    .map_err(|err| node_error(format!("connector.http.post failed: {err}")))?;

    let remote_id = out
        .body
        .get("id")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string();

    Ok(CommitResult {
        idempotency_key: key,
        applied: true,
        remote_id,
    })
}

/// Terminal capture.
#[def_node(
    name = "ExactlyOnceCapture",
    summary = "Return the commit result",
    effects = "Pure",
    determinism = "Strict"
)]
async fn commit_capture(result: CommitResult) -> NodeResult<CommitResult> {
    Ok(result)
}

dag_macros::flow! {
    name: s26_http_write_exactly_once_flow,
    version: "1.0.0",
    profile: Web,
    summary: "Normative §6 reference: an http_post behind a real exactly-once edge with an idempotency key, TTL, and dedupe binding";

    let seed = node!(crate::exactly_once::seed);
    let commit = node!(crate::exactly_once::commit);
    let commit_capture = node!(crate::exactly_once::commit_capture);

    connect!(seed -> commit);
    connect!(commit -> commit_capture);

    // The exactly-once edge (spec §6). kernel-plan enforces that `commit`
    // carries a dedupe binding, an idempotency key, and a TTL >= 300000ms.
    delivery!(seed -> commit, mode = exactly_once);

    entrypoint!({
        trigger: "seed",
        capture: "commit_capture",
        route_aliases: ["/commit"],
        method: "POST",
        deadline_ms: 10_000,
    });
}
