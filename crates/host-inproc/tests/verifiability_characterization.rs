//! Enforcement regression tests for the capability declaration contract.
//!
//! HISTORY: this file began as packet W0-2's characterization suite, pinning
//! the holes in the verifiability story so the Wave-2 enforcement packets
//! could flip them into fail-closed regression tests. All three original
//! holes are now CLOSED:
//!
//! 1. ~~AMBIENT AUTHORITY.~~ CLOSED by packet A2 (scoped per-node resource
//!    views): kernel-exec now wraps every node's resource handle — BOTH the
//!    ambient task-local context (`context::with_resources`) AND the direct
//!    `NodeContext::resources()` handle, which share one per-node
//!    `ScopedResources` view — in a grant set built from the node's declared
//!    `effect_hints` (plus connector-resolved hints). Undeclared accessors
//!    return `None` and surface a structured CAP110 denial naming the node,
//!    the capability, and the declaration to add. Test 1 was flipped from
//!    `pure_node_reads_http_via_ambient_context_today` into
//!    `pure_node_ambient_http_access_is_denied`.
//!
//! 2. ~~DECLARATIONS WERE ONLY USED FOR PROVISIONING, NEVER FOR
//!    RESTRICTION.~~ CLOSED by packet A2: "accessed => declared" is now
//!    enforced at runtime (CAP110), in addition to the historical
//!    "declared => present" preflight (CAP101). Test 2 was flipped from
//!    `undeclared_capability_access_is_unrestricted_today` into
//!    `undeclared_capability_access_is_denied`.
//!
//! 3. ~~HINTS ARE BARE STRINGS.~~ CLOSED by packet A1: hints are typed
//!    (`dag_core::EffectHint`); kernel-plan validation rejects unknown hint
//!    strings fail-closed with EFFECT202. Test 3 was flipped from
//!    `typo_effect_hint_passes_preflight_today` into
//!    `typo_effect_hint_fails_closed`.
//!
//! The remaining tests pin the positive paths of A2 enforcement: declared
//! capabilities still flow through both access paths, and connector-resolved
//! hints (bound connections) extend a node's grant set.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use capabilities::connector::{
    ConnectorBindingScope, ConnectorRuntime, ConnectorRuntimeError, EndpointProfileDescriptor,
    OutboundAuthProfileDescriptor, ResolvedEndpointProfile,
};
use capabilities::http::{HttpMethod, HttpRequest, HttpResponse, HttpResult};
use capabilities::{ResourceBag, context};
use dag_core::prelude::*;
use dag_core::{ConnectorOpRefIR, ConnectorResolutionModeDecl, DurabilityMode, FlowIR};
use host_inproc::{HostRuntime, Invocation};
use kernel_exec::{
    ExecutionResult, FlowExecutor, NodeContext, NodeHandler, NodeOutput, NodeRegistry,
    NodeResolver,
};
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

/// Run a single invocation and return the execution error message; panics if
/// execution unexpectedly succeeds (i.e. enforcement regressed).
async fn execute_expect_failure(runtime: &HostRuntime, payload: JsonValue) -> String {
    let invocation = Invocation::new("trigger", "node", payload);
    match runtime.execute(invocation).await {
        Err(err) => err.to_string(),
        Ok(_) => panic!("expected execution to fail closed under A2 enforcement"),
    }
}

// ---------------------------------------------------------------------------
// Test 1
// ---------------------------------------------------------------------------

/// ENFORCEMENT REGRESSION (flipped by packet A2 from
/// `pure_node_reads_http_via_ambient_context_today`).
///
/// Originally this test pinned the ambient-authority hole: a node declared
/// `effects = Pure` with NO resource declarations could reach into the
/// ambient task-local context via `capabilities::context::with_current_async`
/// and successfully use the flow-wide `http_read()` capability — "Pure" was a
/// label, not a sandbox.
///
/// After A2, a Pure node's scoped view is EMPTY (Pure means pure): the
/// ambient context hands the node its per-node `ScopedResources`, whose
/// `http_read()` returns `None` and records a CAP110 denial. The node's
/// failure must carry the structured CAP110 message naming the node, the
/// capability, and the declaration to add.
#[tokio::test]
async fn pure_node_ambient_http_access_is_denied() {
    let registry = passthrough_registry_with("tests::pure_http_node", |_value| {
        Box::pin(async move {
            // The over-reach: a Pure node grabbing the ambient context and
            // attempting HTTP.
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

    // Preflight still passes: it checks declared hints are satisfiable, and
    // this node declared nothing. Enforcement happens at access time.
    runtime
        .preflight()
        .expect("preflight passes for an undeclared-everything Pure node");

    let message = execute_expect_failure(&runtime, json!({"ok": true})).await;

    assert!(
        message.contains("CAP110"),
        "denial must carry the CAP110 code, got: {message}"
    );
    assert!(
        message.contains("`node`"),
        "denial must name the offending node alias, got: {message}"
    );
    assert!(
        message.contains("http_read()"),
        "denial must name the denied capability, got: {message}"
    );
    assert!(
        message.contains("resources("),
        "denial must tell the author what declaration to add, got: {message}"
    );
    assert!(
        message.contains(capabilities::http::HINT_HTTP_READ),
        "denial must name the granting effect hint, got: {message}"
    );
}

// ---------------------------------------------------------------------------
// Test 2
// ---------------------------------------------------------------------------

/// ENFORCEMENT REGRESSION (flipped by packet A2 from
/// `undeclared_capability_access_is_unrestricted_today`).
///
/// Originally this test pinned the over-reach hole: declaring ONE capability
/// did not restrict access to OTHERS. This node honestly declares
/// `resource::kv::read` (the macro-level analog of `resources(kv)`), and
/// preflight verifies kv is present — but pre-A2 the node could freely use
/// `clock()` from the ambient context, which it never declared.
///
/// After A2, the scoped view built from the node's declared hint set serves
/// the DECLARED capability (kv) and returns `None` for the undeclared one
/// (clock), surfacing a structured CAP110 denial. This also proves the
/// declared-capability positive path through the ambient context.
#[tokio::test]
async fn undeclared_capability_access_is_denied() {
    const DECLARED_HINTS: [&str; 1] = [capabilities::kv::HINT_KV_READ];

    let registry = passthrough_registry_with("tests::kv_declared_node", |_value| {
        Box::pin(async move {
            let outcome = context::with_current_async(|resources| async move {
                // The declared capability must still be served: declarations
                // grant exactly what they say.
                if resources.kv().is_none() {
                    return Err(NodeError::new("declared kv capability missing"));
                }
                // The over-reach: clock() was never declared by this node.
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

    let message = execute_expect_failure(&runtime, json!({"ok": true})).await;

    // The failure must be the structured clock denial — NOT the "declared kv
    // capability missing" branch, which would mean declared grants broke.
    assert!(
        !message.contains("declared kv capability missing"),
        "declared kv capability must still be served, got: {message}"
    );
    assert!(
        message.contains("CAP110"),
        "denial must carry the CAP110 code, got: {message}"
    );
    assert!(
        message.contains("clock()"),
        "denial must name the denied capability, got: {message}"
    );
    assert!(
        message.contains(capabilities::clock::HINT_CLOCK),
        "denial must name the granting effect hint, got: {message}"
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

// ---------------------------------------------------------------------------
// Tests 4-6: A2 positive paths (added with the A2 flip)
// ---------------------------------------------------------------------------

/// Trigger handler used by the resolver-based tests below.
struct PassthroughHandler;

#[async_trait]
impl NodeHandler for PassthroughHandler {
    async fn invoke(&self, input: JsonValue, _ctx: &NodeContext) -> NodeResult<NodeOutput> {
        Ok(NodeOutput::Value(input))
    }
}

/// Resolver that serves the trigger as a passthrough and `node` via the
/// supplied handler — used to exercise the `NodeContext` access path, which
/// `NodeRegistry::register_fn` closures cannot reach.
struct ProbeResolver {
    node_identifier: &'static str,
    node_handler: Arc<dyn NodeHandler>,
}

impl NodeResolver for ProbeResolver {
    fn resolve(&self, identifier: &str) -> Option<Arc<dyn NodeHandler>> {
        if identifier == "tests::trigger" {
            return Some(Arc::new(PassthroughHandler));
        }
        if identifier == self.node_identifier {
            return Some(self.node_handler.clone());
        }
        None
    }
}

/// A2 POSITIVE PATH: a node WITH declared hints still receives its
/// capabilities through BOTH access paths — the direct
/// `NodeContext::resources()` handle and the ambient task-local context —
/// because kernel-exec hands both paths the same per-node `ScopedResources`.
#[tokio::test]
async fn declared_capabilities_are_granted_through_both_access_paths() {
    struct BothPathsHandler;

    #[async_trait]
    impl NodeHandler for BothPathsHandler {
        async fn invoke(&self, _input: JsonValue, ctx: &NodeContext) -> NodeResult<NodeOutput> {
            // Path (b): direct NodeContext access.
            let direct_clock_secs = {
                let resources = ctx.resources();
                if resources.kv().is_none() {
                    return Err(NodeError::new("declared kv missing via NodeContext"));
                }
                let Some(clock) = resources.clock() else {
                    return Err(NodeError::new("declared clock missing via NodeContext"));
                };
                clock
                    .now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("fixed clock is post-epoch")
                    .as_secs()
            };

            // Path (a): ambient task-local context.
            let ambient_clock_secs = context::with_current_async(|resources| async move {
                if resources.kv().is_none() {
                    return Err(NodeError::new("declared kv missing via ambient context"));
                }
                let Some(clock) = resources.clock() else {
                    return Err(NodeError::new("declared clock missing via ambient context"));
                };
                Ok(clock
                    .now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("fixed clock is post-epoch")
                    .as_secs())
            })
            .await
            .ok_or_else(|| NodeError::new("no ambient resource context was scoped"))??;

            Ok(NodeOutput::Value(json!({
                "direct_clock_secs": direct_clock_secs,
                "ambient_clock_secs": ambient_clock_secs,
            })))
        }
    }

    const DECLARED_HINTS: [&str; 2] = [
        capabilities::kv::HINT_KV_READ,
        capabilities::clock::HINT_CLOCK,
    ];

    let flow = two_node_flow(
        "a2_both_paths_positive",
        "tests::both_paths_node",
        Effects::ReadOnly,
        Determinism::BestEffort,
        &DECLARED_HINTS,
    );
    let ir = Arc::new(validate(&flow).expect("flow validates"));

    let resolver = ProbeResolver {
        node_identifier: "tests::both_paths_node",
        node_handler: Arc::new(BothPathsHandler),
    };
    let bag = ResourceBag::new()
        .with_kv(Arc::new(capabilities::kv::MemoryKv::new()))
        .with_clock(Arc::new(FixedClock));
    let runtime = HostRuntime::new(FlowExecutor::new_with_resolver(Arc::new(resolver)), ir)
        .with_resource_bag(bag);
    runtime.preflight().expect("preflight passes");

    let value = execute_single(&runtime, json!({"ok": true})).await;
    assert_eq!(
        value,
        json!({
            "direct_clock_secs": FIXED_EPOCH_SECS,
            "ambient_clock_secs": FIXED_EPOCH_SECS,
        }),
        "a node with declared hints must receive its capabilities through both access paths",
    );
}

/// A2 ENFORCEMENT: the direct `NodeContext::resources()` path is scoped too —
/// not just the ambient context. A node that declared only kv is denied
/// clock() on the context handle, and the denial is attributed with CAP110.
#[tokio::test]
async fn node_context_direct_access_is_scoped() {
    struct DirectOverreachHandler;

    #[async_trait]
    impl NodeHandler for DirectOverreachHandler {
        async fn invoke(&self, _input: JsonValue, ctx: &NodeContext) -> NodeResult<NodeOutput> {
            let resources = ctx.resources();
            if resources.kv().is_none() {
                return Err(NodeError::new("declared kv missing via NodeContext"));
            }
            let Some(clock) = resources.clock() else {
                return Err(NodeError::new(
                    "clock absent from NodeContext (enforcement landed?)",
                ));
            };
            let _ = clock.now();
            Ok(NodeOutput::Value(json!({"undeclared_clock": "served"})))
        }
    }

    const DECLARED_HINTS: [&str; 1] = [capabilities::kv::HINT_KV_READ];

    let flow = two_node_flow(
        "a2_direct_path_denial",
        "tests::direct_overreach_node",
        Effects::ReadOnly,
        Determinism::BestEffort,
        &DECLARED_HINTS,
    );
    let ir = Arc::new(validate(&flow).expect("flow validates"));

    let resolver = ProbeResolver {
        node_identifier: "tests::direct_overreach_node",
        node_handler: Arc::new(DirectOverreachHandler),
    };
    let bag = ResourceBag::new()
        .with_kv(Arc::new(capabilities::kv::MemoryKv::new()))
        .with_clock(Arc::new(FixedClock));
    let runtime = HostRuntime::new(FlowExecutor::new_with_resolver(Arc::new(resolver)), ir)
        .with_resource_bag(bag);
    runtime.preflight().expect("preflight passes");

    let message = execute_expect_failure(&runtime, json!({"ok": true})).await;
    assert!(
        message.contains("CAP110") && message.contains("clock()"),
        "NodeContext path denial must surface CAP110 for clock, got: {message}"
    );
}

/// A2 POSITIVE PATH: connector-resolved hints (bound connections) extend the
/// node's grant set. The node declares NO http hint statically; the mock
/// connector runtime resolves `resource::http::read` for its bound-connection
/// op during preflight, and that grant lets the node use `http_read()`.
#[tokio::test]
async fn connector_resolved_hints_extend_node_grants() {
    struct HintGrantingConnectorRuntime;

    #[async_trait]
    impl ConnectorRuntime for HintGrantingConnectorRuntime {
        async fn apply_outbound_auth(
            &self,
            _scope: &ConnectorBindingScope,
            _profile: &OutboundAuthProfileDescriptor,
            _request: &mut HttpRequest,
        ) -> Result<(), ConnectorRuntimeError> {
            Ok(())
        }

        async fn resolve_endpoint_profile(
            &self,
            _scope: &ConnectorBindingScope,
            profile: &EndpointProfileDescriptor,
        ) -> Result<ResolvedEndpointProfile, ConnectorRuntimeError> {
            Ok(ResolvedEndpointProfile {
                base_url: profile.base_url.to_string(),
                default_headers: Vec::new(),
            })
        }

        async fn resolve_required_effect_hints(
            &self,
            scope: &ConnectorBindingScope,
            _selected_mode: ConnectorResolutionModeDecl,
        ) -> Result<Vec<String>, ConnectorRuntimeError> {
            // Grant http::read to the connector node only.
            if scope.node_alias == "node" {
                Ok(vec![capabilities::http::HINT_HTTP_READ.to_string()])
            } else {
                Ok(Vec::new())
            }
        }
    }

    let registry = passthrough_registry_with("tests::bound_connector_node", |_value| {
        Box::pin(async move {
            let outcome = context::with_current_async(|resources| async move {
                let Some(http) = resources.http_read() else {
                    return Err(NodeError::new(
                        "http_read absent despite connector-resolved grant",
                    ));
                };
                let request =
                    HttpRequest::new(HttpMethod::Get, "https://example.invalid/bound-connection");
                let response = http
                    .send(request)
                    .await
                    .map_err(|err| NodeError::new(err.to_string()))?;
                Ok(String::from_utf8_lossy(&response.body).into_owned())
            })
            .await;

            match outcome {
                Some(Ok(body)) => Ok(json!({ "connector_http_body": body })),
                Some(Err(err)) => Err(err),
                None => Err(NodeError::new("no ambient resource context was scoped")),
            }
        })
    });

    // The node's STATIC declarations carry no http hint; the http requirement
    // is resolved from the bound connection at preflight time.
    let mut flow = two_node_flow(
        "a2_connector_grants",
        "tests::bound_connector_node",
        Effects::ReadOnly,
        Determinism::BestEffort,
        &[],
    );
    flow.nodes
        .iter_mut()
        .find(|node| node.alias == "node")
        .expect("node exists")
        .connector_ops
        .push(ConnectorOpRefIR {
            operation_id: "connector.test.read".to_string(),
            connector_id: "connector.test".to_string(),
            roles: Vec::new(),
            default_resolution_mode: ConnectorResolutionModeDecl::BoundConnection,
            selected_resolution_mode: ConnectorResolutionModeDecl::BoundConnection,
            supported_resolution_modes: vec![ConnectorResolutionModeDecl::BoundConnection],
        });
    let ir = Arc::new(validate(&flow).expect("flow validates"));

    let bag = ResourceBag::new()
        .with_http_read(Arc::new(CannedHttpRead))
        .with_connector_runtime(Arc::new(HintGrantingConnectorRuntime));
    let runtime =
        HostRuntime::new(FlowExecutor::new(Arc::new(registry)), ir).with_resource_bag(bag);

    // Preflight resolves the bound connection's hints and verifies the bag
    // satisfies them; execution then grants them to the node's scoped view.
    runtime
        .preflight()
        .expect("preflight resolves and satisfies the connector hints");

    let value = execute_single(&runtime, json!({"ok": true})).await;
    assert_eq!(
        value,
        json!({
            "connector_http_body":
                "canned-response-for:https://example.invalid/bound-connection"
        }),
        "connector-resolved hints must be included in the node's grant set",
    );
}
