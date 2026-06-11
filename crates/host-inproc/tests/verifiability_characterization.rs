//! Characterization tests for TODAY'S capability-enforcement gaps (packet W0-2).
//!
//! Every test in this file PASSES against the current code on purpose: each one
//! pins a hole in the verifiability story so that the Wave-2 enforcement packets
//! (A1: typed effect hints, A2: scoped per-node resource views) can flip them
//! into fail-closed regression tests. If one of these tests starts failing, it
//! means enforcement landed — flip the assertion per the packet comment instead
//! of "fixing" the test back.
//!
//! The enforcement model TODAY, as discovered empirically while writing this file:
//!
//! 1. AMBIENT AUTHORITY. kernel-exec wraps every node handler invocation in
//!    `capabilities::context::with_resources(<full bag>)` (kernel-exec run_node,
//!    `context::with_resources(resources_for_invoke, ...)`). The only per-node
//!    scoping that exists is `NodeScopedResources`, which rewrites the CONNECTOR
//!    scope and forwards every capability accessor straight to the shared bag.
//!    `capabilities::context::with_current_async` therefore hands ANY node the
//!    full `Arc<dyn ResourceAccess>` regardless of its `effects`/hint declarations.
//!
//! 2. DECLARATIONS ARE ONLY USED FOR PROVISIONING, NEVER FOR RESTRICTION.
//!    host-inproc preflight collects `node.effect_hints` strings that start with
//!    `resource::` and checks that each is satisfiable by the bag
//!    (`is_hint_satisfied_by_resources`). It checks "declared => present"; it
//!    never checks "accessed => declared". A node may declare nothing (even
//!    `Effects::Pure`) and still use anything in the bag.
//!
//! 3. ~~HINTS ARE BARE STRINGS.~~ CLOSED by packet A1: hints are now typed
//!    (`dag_core::EffectHint`); kernel-plan validation rejects unknown hint
//!    strings fail-closed with EFFECT202. Test 3 below was flipped from
//!    `typo_effect_hint_passes_preflight_today` into
//!    `typo_effect_hint_fails_closed` and is now an enforcement regression
//!    test.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use capabilities::http::{HttpMethod, HttpRequest, HttpResponse, HttpResult};
use capabilities::{ResourceBag, context};
use dag_core::prelude::*;
use dag_core::{DurabilityMode, FlowIR};
use host_inproc::{HostRuntime, Invocation};
use kernel_exec::{ExecutionResult, FlowExecutor, NodeRegistry};
use kernel_plan::validate;
use serde_json::{Value as JsonValue, json};

// ---------------------------------------------------------------------------
// Stub capabilities (no real IO anywhere in this file)
// ---------------------------------------------------------------------------

/// Canned in-memory HttpRead. Never touches the network.
struct CannedHttpRead;

#[async_trait]
impl capabilities::http::HttpRead for CannedHttpRead {
    async fn send(&self, request: HttpRequest) -> HttpResult<HttpResponse> {
        Ok(HttpResponse {
            status: 200,
            headers: Default::default(),
            body: format!("canned-response-for:{}", request.url).into_bytes(),
        })
    }
}

/// Fixed clock so assertions stay deterministic.
struct FixedClock;

const FIXED_EPOCH_SECS: u64 = 1_700_000_000;

impl capabilities::Capability for FixedClock {
    fn name(&self) -> &'static str {
        "clock.fixed.characterization"
    }
}

impl capabilities::clock::Clock for FixedClock {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(FIXED_EPOCH_SECS)
    }
}

// ---------------------------------------------------------------------------
// Flow plumbing helpers (mirrors the idiom of host-inproc's in-module tests)
// ---------------------------------------------------------------------------

/// Build a two-node flow `trigger -> node` where `node` carries the given
/// declarations. Durability is forced Off so these tests exercise ONLY the
/// capability-enforcement surface (no checkpoint-store stubs required).
fn two_node_flow(
    flow_id: &str,
    node_identifier: &'static str,
    effects: Effects,
    determinism: Determinism,
    effect_hints: &'static [&'static str],
) -> FlowIR {
    let mut builder = FlowBuilder::new(flow_id, Version::new(1, 0, 0), Profile::Dev);
    let trigger = builder
        .add_node(
            "trigger",
            &NodeSpec::inline(
                "tests::trigger",
                "Trigger",
                SchemaSpec::Opaque,
                SchemaSpec::Opaque,
                Effects::Pure,
                Determinism::Strict,
                None,
            ),
        )
        .expect("trigger added");
    let node = builder
        .add_node(
            "node",
            &NodeSpec::inline_with_hints(
                node_identifier,
                "Node",
                SchemaSpec::Opaque,
                SchemaSpec::Opaque,
                effects,
                determinism,
                None,
                &[],
                effect_hints,
            ),
        )
        .expect("node added");
    builder.connect(&trigger, &node);

    let mut flow = builder.build();
    flow.policies.durability.mode = DurabilityMode::Off;
    flow
}

fn passthrough_registry_with(
    node_identifier: &'static str,
    handler: impl Fn(JsonValue) -> futures_free::NodeFuture + Send + Sync + 'static,
) -> NodeRegistry {
    let mut registry = NodeRegistry::new();
    registry
        .register_fn(
            "tests::trigger",
            |value: JsonValue| async move { Ok(value) },
        )
        .expect("trigger registered");
    registry
        .register_fn(node_identifier, move |value: JsonValue| handler(value))
        .expect("node registered");
    registry
}

/// Tiny shim so helper closures can return boxed futures without pulling in
/// the `futures` crate as a new dev-dependency.
mod futures_free {
    use dag_core::NodeResult;
    use serde_json::Value as JsonValue;
    use std::future::Future;
    use std::pin::Pin;

    pub type NodeFuture = Pin<Box<dyn Future<Output = NodeResult<JsonValue>> + Send>>;
}

async fn execute_single(runtime: &HostRuntime, payload: JsonValue) -> JsonValue {
    let invocation = Invocation::new("trigger", "node", payload);
    match runtime
        .execute(invocation)
        .await
        .expect("execution succeeds")
    {
        ExecutionResult::Value(value) => value,
        ExecutionResult::Stream(_) => panic!("expected value result, got stream"),
        ExecutionResult::Halt { .. } => panic!("expected value result, got halt"),
    }
}

// ---------------------------------------------------------------------------
// Test 1
// ---------------------------------------------------------------------------

/// CHARACTERIZATION: flips in packet A2 (scoped resource views).
///
/// HOLE: a node declared `effects = Pure` with NO resource declarations of any
/// kind can reach into the ambient task-local context via
/// `capabilities::context::with_current_async` and successfully use the
/// flow-wide `http_read()` capability. Nothing — not kernel-plan validation,
/// not host-inproc preflight, not the executor — relates what a node DECLARES
/// to what it can ACCESS. "Pure" is a label, not a sandbox.
///
/// After A2, the node's view must be empty (Pure => empty view) and this access
/// must fail closed with a structured denial (CAP11x); flip the assertions to
/// expect that denial.
#[tokio::test]
async fn pure_node_reads_http_via_ambient_context_today() {
    let registry = passthrough_registry_with("tests::pure_http_node", |_value| {
        Box::pin(async move {
            // This is the over-reach: a Pure node grabbing the FULL resource
            // bag from ambient context and using HTTP.
            let outcome = context::with_current_async(|resources| async move {
                let Some(http) = resources.http_read() else {
                    return Err(NodeError::new(
                        "http_read absent from ambient context (enforcement landed?)",
                    ));
                };
                let request =
                    HttpRequest::new(HttpMethod::Get, "https://example.invalid/characterization");
                let response = http
                    .send(request)
                    .await
                    .map_err(|err| NodeError::new(err.to_string()))?;
                Ok(String::from_utf8_lossy(&response.body).into_owned())
            })
            .await;

            match outcome {
                Some(Ok(body)) => Ok(json!({ "ambient_http_body": body })),
                Some(Err(err)) => Err(err),
                None => Err(NodeError::new("no ambient resource context was scoped")),
            }
        })
    });

    // Node declares Pure / Strict and an EMPTY hint list.
    let flow = two_node_flow(
        "w02_pure_ambient_http",
        "tests::pure_http_node",
        Effects::Pure,
        Determinism::Strict,
        &[],
    );
    let ir = Arc::new(validate(&flow).expect("flow validates"));

    let bag = ResourceBag::new().with_http_read(Arc::new(CannedHttpRead));
    let runtime =
        HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir).with_resource_bag(bag);

    // Preflight does not object: it only checks declared hints are satisfiable,
    // and this node declared nothing.
    runtime
        .preflight()
        .expect("preflight passes for an undeclared-everything Pure node");

    let value = execute_single(&runtime, json!({"ok": true})).await;

    // The Pure node successfully performed an HTTP read it never declared.
    assert_eq!(
        value,
        json!({
            "ambient_http_body":
                "canned-response-for:https://example.invalid/characterization"
        }),
        "TODAY a Pure node with zero declarations can use http_read from ambient context",
    );
}

// ---------------------------------------------------------------------------
// Test 2
// ---------------------------------------------------------------------------

/// CHARACTERIZATION: flips in packet A2 (scoped resource views).
///
/// HOLE: declaring ONE capability does not restrict access to OTHERS. This
/// node honestly declares `resource::kv::read` (the macro-level analog of
/// `resources(kv)`), and preflight verifies kv is present — but at runtime the
/// node freely uses `clock()` from the ambient context, which it never
/// declared. Declarations gate provisioning ("declared => must be in the
/// bag"), never access ("accessed => must be declared").
///
/// After A2, the scoped view built from the node's declared hint set must
/// return None for clock() and surface a structured denial; flip this test to
/// assert that denial.
#[tokio::test]
async fn undeclared_capability_access_is_unrestricted_today() {
    const DECLARED_HINTS: [&str; 1] = [capabilities::kv::HINT_KV_READ];

    let registry = passthrough_registry_with("tests::kv_declared_node", |_value| {
        Box::pin(async move {
            let outcome = context::with_current_async(|resources| async move {
                // Sanity: the declared capability is there, as preflight promised.
                if resources.kv().is_none() {
                    return Err(NodeError::new("declared kv capability missing"));
                }
                // The over-reach: clock() was never declared by this node,
                // yet the ambient bag serves it without complaint.
                let Some(clock) = resources.clock() else {
                    return Err(NodeError::new(
                        "clock absent from ambient context (enforcement landed?)",
                    ));
                };
                let epoch_secs = clock
                    .now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("fixed clock is post-epoch")
                    .as_secs();
                Ok(epoch_secs)
            })
            .await;

            match outcome {
                Some(Ok(epoch_secs)) => Ok(json!({ "undeclared_clock_secs": epoch_secs })),
                Some(Err(err)) => Err(err),
                None => Err(NodeError::new("no ambient resource context was scoped")),
            }
        })
    });

    let flow = two_node_flow(
        "w02_undeclared_clock",
        "tests::kv_declared_node",
        Effects::ReadOnly,
        Determinism::BestEffort,
        &DECLARED_HINTS,
    );
    let ir = Arc::new(validate(&flow).expect("flow validates"));

    let bag = ResourceBag::new()
        .with_kv(Arc::new(capabilities::kv::MemoryKv::new()))
        .with_clock(Arc::new(FixedClock));
    let runtime =
        HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir).with_resource_bag(bag);

    runtime
        .preflight()
        .expect("preflight passes: the one declared hint (kv) is satisfied");

    let value = execute_single(&runtime, json!({"ok": true})).await;

    assert_eq!(
        value,
        json!({ "undeclared_clock_secs": FIXED_EPOCH_SECS }),
        "TODAY a node that declared only kv can freely use the undeclared clock capability",
    );
}

// ---------------------------------------------------------------------------
// Test 3
// ---------------------------------------------------------------------------

/// CHARACTERIZATION (FLIPPED by packet A1 — typed `dag_core::EffectHint`).
///
/// Originally (`typo_effect_hint_passes_preflight_today`) this test pinned the
/// two ASYMMETRIC holes of stringly-typed hints:
///
/// (a) Suffix typo (`resource::http_raed` — typo in the operation): kernel-plan validation was
///     silent; host-inproc preflight failed closed but MISLEADINGLY, with
///     `MissingCapabilities` naming the typo string itself — an error no
///     resource bag could ever satisfy, pointing away from the real problem.
///
/// (b) Prefix typo (`"resorce::http::read"`): validation silent AND preflight
///     passed silently — the hint did not start with `resource::`, so the
///     node's intended http requirement simply evaporated.
///
/// After A1, hints are typed: kernel-plan validation parses every hint via
/// `dag_core::EffectHint` and rejects BOTH spellings fail-closed with the
/// dedicated EFFECT202 diagnostic naming the offending string. A HostRuntime
/// can no longer even be constructed for such a flow (it requires a
/// ValidatedIR).
#[tokio::test]
async fn typo_effect_hint_fails_closed() {
    // Typo literals built via concat so this file stays honest under the
    // scripts/check-hint-literals.sh grep gate (the prefix typo doesn't
    // contain `resource::` at all, so it can stay a plain literal).
    let suffix_typo: &'static str =
        Box::leak(["resource", "::http_raed"].concat().into_boxed_str());
    let prefix_typo: &'static str = "resorce::http::read";

    for (case, typo_hint) in [("suffix typo", suffix_typo), ("prefix typo", prefix_typo)] {
        let hints: &'static [&'static str] = Box::leak(vec![typo_hint].into_boxed_slice());
        let flow = two_node_flow(
            "w02_typo",
            "tests::typo_node",
            Effects::ReadOnly,
            Determinism::BestEffort,
            hints,
        );

        let diagnostics = match validate(&flow) {
            Err(diags) => diags,
            Ok(_) => panic!("{case}: validation must reject the typo'd hint fail-closed"),
        };
        let effect202 = diagnostics
            .iter()
            .find(|diag| diag.code.code == "EFFECT202")
            .unwrap_or_else(|| panic!("{case}: expected EFFECT202, got: {diagnostics:?}"));
        assert!(
            effect202.message.contains(typo_hint),
            "{case}: EFFECT202 must name the offending hint so the fix is obvious, got: {}",
            effect202.message,
        );

        // And therefore no HostRuntime/preflight stage exists for this flow:
        // the misleading MissingCapabilities path and the silent-evaporation
        // path are both unreachable for unknown hints.
    }
}
