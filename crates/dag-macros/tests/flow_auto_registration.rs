//! Packet D1: `flow!` derives node registration from the `node!(...)` bindings
//! it already sees, so authors never maintain a manual register list that can
//! drift from the flow definition.
//!
//! Run with: `cargo test -p dag-macros --features host-bundle --test flow_auto_registration`
//! (the generated `bundle()` / `__register_nodes` items are only emitted when
//! the calling crate enables `host-bundle`, mirroring how example crates build).
#![cfg(feature = "host-bundle")]

use dag_macros::def_node;
use serde::{Deserialize, Serialize};

// Same trick as tests/flow_macro.rs: satisfy the macro's `::host_inproc::*`
// references with structurally-identical local mocks. Registration and
// execution below go through the REAL kernel-exec registry/executor.
extern crate self as host_inproc;

use std::sync::Arc;
use std::time::Duration;

pub trait EnvironmentPlugin {}

#[derive(Clone, Debug)]
pub enum NodeSource {
    Local,
}

#[derive(Clone, Debug)]
pub struct NodeContract {
    pub identifier: String,
    pub contract_hash: Option<String>,
    pub source: NodeSource,
}

#[derive(Clone, Debug)]
pub struct FlowEntrypoint {
    pub trigger_alias: String,
    pub capture_alias: String,
    pub route_path: Option<String>,
    pub method: Option<String>,
    pub deadline: Option<Duration>,
    pub route_aliases: Vec<String>,
}

pub struct FlowBundle {
    pub validated_ir: kernel_plan::ValidatedIR,
    pub entrypoints: Vec<FlowEntrypoint>,
    pub resolver: Arc<dyn kernel_exec::NodeResolver>,
    pub node_contracts: Vec<NodeContract>,
    pub environment_plugins: Vec<Arc<dyn EnvironmentPlugin>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Payload {
    pub value: u32,
}

#[def_node(
    trigger,
    name = "AutoTrigger",
    summary = "Trigger for the auto-registration test flow",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn auto_trigger(input: Payload) -> dag_core::NodeResult<Payload> {
    Ok(input)
}

#[def_node(
    name = "AddOne",
    summary = "Increment the payload",
    effects = "Pure",
    determinism = "Strict"
)]
async fn add_one(input: Payload) -> dag_core::NodeResult<Payload> {
    Ok(Payload {
        value: input.value + 1,
    })
}

#[def_node(
    name = "AutoCapture",
    summary = "Capture the payload",
    effects = "Pure",
    determinism = "Strict"
)]
async fn auto_capture(input: Payload) -> dag_core::NodeResult<Payload> {
    Ok(input)
}

/// The canary shape: `flow!` nested inside a module with NO manual
/// `<name>_register` imports. Registration paths must be derived from the
/// `node!(...)` bindings and resolve from the crate root, exactly like the
/// `<name>_node_spec` paths `node!` already emits.
mod bundle_def {
    use dag_macros::node;

    dag_macros::flow! {
        name: auto_registration_flow,
        version: "1.0.0",
        profile: Web;
        let trigger = node!(auto_trigger);
        let add = node!(add_one);
        let capture = node!(auto_capture);
        connect!(trigger -> add);
        connect!(add -> capture);
        entrypoint!({
            trigger: "trigger",
            capture: "capture",
        });
    }
}

/// Double-registration shape: the same node fn bound under two aliases. The
/// registry is keyed by node identifier, so the derived registration list is
/// deduped at expansion time — building the bundle must be benign, not a
/// `RegistryError::Duplicate` panic.
mod double_binding_def {
    use dag_macros::node;

    dag_macros::flow! {
        name: double_binding_flow,
        version: "1.0.0",
        profile: Web;
        let trigger = node!(auto_trigger);
        let first = node!(add_one);
        let second = node!(add_one);
        let capture = node!(auto_capture);
        connect!(trigger -> first);
        connect!(first -> second);
        connect!(second -> capture);
        entrypoint!({
            trigger: "trigger",
            capture: "capture",
        });
    }
}

#[test]
fn auto_registration_covers_every_node_in_the_flow_ir() {
    let bundle = bundle_def::bundle();
    for node in bundle_def::flow().nodes {
        assert!(
            bundle.resolver.resolve(&node.identifier).is_some(),
            "node `{}` referenced by the IR was not auto-registered",
            node.identifier
        );
    }
}

#[tokio::test]
async fn flow_executes_with_zero_manual_registration() {
    let bundle = bundle_def::bundle();
    let executor = kernel_exec::FlowExecutor::new_with_resolver(bundle.resolver.clone());
    let result = executor
        .run_once(
            &bundle.validated_ir,
            "trigger",
            serde_json::json!({ "value": 1 }),
            "capture",
            Some(Duration::from_secs(5)),
        )
        .await
        .expect("flow should execute with derived registration only");

    match result {
        kernel_exec::ExecutionResult::Value(value) => {
            assert_eq!(value, serde_json::json!({ "value": 2 }));
        }
        _ => panic!("expected value output"),
    }
}

#[tokio::test]
async fn binding_the_same_node_twice_registers_once_and_executes() {
    // Before expansion-time dedupe this panicked: `register add_one:
    // node `...::add_one` already registered`.
    let bundle = double_binding_def::bundle();
    let executor = kernel_exec::FlowExecutor::new_with_resolver(bundle.resolver.clone());
    let result = executor
        .run_once(
            &bundle.validated_ir,
            "trigger",
            serde_json::json!({ "value": 1 }),
            "capture",
            Some(Duration::from_secs(5)),
        )
        .await
        .expect("double-bound flow should execute");

    match result {
        kernel_exec::ExecutionResult::Value(value) => {
            assert_eq!(value, serde_json::json!({ "value": 3 }));
        }
        _ => panic!("expected value output"),
    }
}
