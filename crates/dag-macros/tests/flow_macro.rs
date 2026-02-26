use dag_macros::{def_node, flow, node, subflow, workflow};
use serde::{Deserialize, Serialize};

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
pub struct EntryInput;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EntryOutput;

#[def_node(
    trigger,
    name = "EntryTrigger",
    summary = "Test trigger",
    effects = "ReadOnly",
    determinism = "Strict"
)]
async fn entry_trigger(input: EntryInput) -> dag_core::NodeResult<EntryInput> {
    Ok(input)
}

#[def_node(
    name = "EntryCapture",
    summary = "Test capture",
    effects = "Pure",
    determinism = "Strict"
)]
async fn entry_capture(_input: EntryInput) -> dag_core::NodeResult<EntryOutput> {
    Ok(EntryOutput)
}

flow! {
    name: s1_echo,
    version: "1.0.0",
    profile: Web;
}

mod typed_entry_flow_module {
    use super::*;

    flow! {
        name: typed_entry_flow,
        version: "1.0.0",
        profile: Web;
        let entry = node!(crate::entry_trigger);
        let capture = node!(crate::entry_capture);
        connect!(entry -> capture);
        entrypoint!({
            trigger: "entry",
            capture: "capture",
        });
    }
}

mod external_type_flow_module {
    use super::*;

    #[def_node(
        trigger,
        name = "StringTrigger",
        summary = "String trigger",
        effects = "ReadOnly",
        determinism = "Strict"
    )]
    async fn string_trigger(
        input: std::string::String,
    ) -> dag_core::NodeResult<std::string::String> {
        Ok(input)
    }

    #[def_node(
        name = "StringCapture",
        summary = "String capture",
        effects = "Pure",
        determinism = "Strict"
    )]
    async fn string_capture(
        input: std::string::String,
    ) -> dag_core::NodeResult<std::string::String> {
        Ok(input)
    }

    flow! {
        name: external_type_flow,
        version: "1.0.0",
        profile: Web;
        let entry = node!(crate::external_type_flow_module::string_trigger);
        let capture = node!(crate::external_type_flow_module::string_capture);
        connect!(entry -> capture);
        entrypoint!({
            trigger: "entry",
            capture: "capture",
        });
    }
}

mod multi_entrypoint_flow_module {
    use super::*;

    #[def_node(
        trigger,
        name = "PrimaryTrigger",
        summary = "Primary trigger",
        effects = "ReadOnly",
        determinism = "Strict"
    )]
    async fn primary_trigger(input: EntryInput) -> dag_core::NodeResult<EntryInput> {
        Ok(input)
    }

    #[def_node(
        name = "PrimaryCapture",
        summary = "Primary capture",
        effects = "Pure",
        determinism = "Strict"
    )]
    async fn primary_capture(_input: EntryInput) -> dag_core::NodeResult<EntryOutput> {
        Ok(EntryOutput)
    }

    #[def_node(
        trigger,
        name = "SecondaryTrigger",
        summary = "Secondary trigger",
        effects = "ReadOnly",
        determinism = "Strict"
    )]
    async fn secondary_trigger(input: EntryInput) -> dag_core::NodeResult<EntryInput> {
        Ok(input)
    }

    #[def_node(
        name = "SecondaryCapture",
        summary = "Secondary capture",
        effects = "Pure",
        determinism = "Strict"
    )]
    async fn secondary_capture(_input: EntryInput) -> dag_core::NodeResult<EntryOutput> {
        Ok(EntryOutput)
    }

    flow! {
        name: multi_entrypoint_flow,
        version: "1.0.0",
        profile: Web;
        let primary_trigger = node!(crate::multi_entrypoint_flow_module::primary_trigger);
        let primary_capture = node!(crate::multi_entrypoint_flow_module::primary_capture);
        let secondary_trigger = node!(crate::multi_entrypoint_flow_module::secondary_trigger);
        let secondary_capture = node!(crate::multi_entrypoint_flow_module::secondary_capture);
        connect!(primary_trigger -> primary_capture);
        connect!(secondary_trigger -> secondary_capture);
        entrypoint!({
            trigger: "primary_trigger",
            capture: "primary_capture",
            route_aliases: ["/primary", "/primary-alias"],
            method: "GET",
        });
        entrypoint!({
            trigger: "secondary_trigger",
            capture: "secondary_capture",
            route_aliases: ["/secondary"],
            method: "POST",
        });
    }
}

mod subflow_entry_flow_module {
    use super::*;

    #[def_node(
        trigger,
        name = "SubflowTrigger",
        summary = "Subflow trigger",
        effects = "ReadOnly",
        determinism = "Strict",
        checkpointable = false
    )]
    async fn subflow_trigger(input: EntryInput) -> dag_core::NodeResult<EntryInput> {
        Ok(input)
    }

    #[def_node(
        name = "SubflowCapture",
        summary = "Subflow capture",
        effects = "Effectful",
        determinism = "Nondeterministic",
        replayable = false,
        halts = true
    )]
    async fn subflow_capture(_input: EntryInput) -> dag_core::NodeResult<EntryOutput> {
        Ok(EntryOutput)
    }

    flow! {
        name: subflow_flow,
        version: "1.2.3",
        profile: Web;
        let trigger = node!(crate::subflow_entry_flow_module::subflow_trigger);
        let capture = node!(crate::subflow_entry_flow_module::subflow_capture);
        connect!(trigger -> capture);
        entrypoint!({
            trigger: "trigger",
            capture: "capture",
        });
    }
}

mod root_alias_subflow_module {
    use super::*;

    #[def_node(
        trigger,
        name = "RootAliasTrigger",
        summary = "Root alias trigger",
        effects = "Effectful",
        determinism = "Nondeterministic",
        checkpointable = false,
        replayable = false,
        halts = true
    )]
    async fn root_alias_trigger(input: EntryInput) -> dag_core::NodeResult<EntryInput> {
        Ok(input)
    }

    #[def_node(
        name = "RootAliasCapture",
        summary = "Root alias capture",
        effects = "Effectful",
        determinism = "Nondeterministic",
        checkpointable = false,
        replayable = false,
        halts = true
    )]
    async fn root_alias_capture(_input: EntryInput) -> dag_core::NodeResult<EntryOutput> {
        Ok(EntryOutput)
    }

    flow! {
        name: root_alias_flow,
        version: "9.9.9",
        profile: Web;
        let trigger = node!(crate::root_alias_subflow_module::root_alias_trigger);
        let capture = node!(crate::root_alias_subflow_module::root_alias_capture);
        connect!(trigger -> capture);
        entrypoint!({
            trigger: "trigger",
            capture: "capture",
        });
    }
}

workflow! {
    name: subflow_parent_flow,
    version: "1.0.0",
    profile: Web;
    let trigger = node!(crate::entry_trigger);
    let sub = subflow!(subflow_entry_flow_module::subflow_flow::trigger);
    connect!(trigger -> sub);
}

mod single_segment_subflow_module {
    use super::*;

    #[def_node(
        trigger,
        name = "SingleTrigger",
        summary = "Single trigger",
        effects = "ReadOnly",
        determinism = "Strict"
    )]
    async fn single_trigger(input: EntryInput) -> dag_core::NodeResult<EntryInput> {
        Ok(input)
    }

    #[def_node(
        name = "SingleCapture",
        summary = "Single capture",
        effects = "Pure",
        determinism = "Strict"
    )]
    async fn single_capture(_input: EntryInput) -> dag_core::NodeResult<EntryOutput> {
        Ok(EntryOutput)
    }

    flow! {
        name: single_segment_flow,
        version: "0.1.0",
        profile: Web;
        let trigger = node!(crate::single_segment_subflow_module::single_trigger);
        let capture = node!(crate::single_segment_subflow_module::single_capture);
        connect!(trigger -> capture);
        entrypoint!({
            trigger: "trigger",
            capture: "capture",
        });
    }

    workflow! {
        name: single_segment_parent_flow,
        version: "0.1.0",
        profile: Web;
        let trigger = node!(crate::entry_trigger);
        let sub = subflow!(single_segment_flow);
        connect!(trigger -> sub);
    }
}

mod aggregated_effects_subflow_module {
    use super::*;

    #[def_node(
        trigger,
        name = "EffectsTrigger",
        summary = "Effects trigger",
        effects = "Pure",
        determinism = "Strict"
    )]
    async fn effects_trigger(input: EntryInput) -> dag_core::NodeResult<EntryInput> {
        Ok(input)
    }

    #[def_node(
        name = "EffectsWriter",
        summary = "Effects writer",
        effects = "Effectful",
        determinism = "Strict"
    )]
    async fn effects_writer(input: EntryInput) -> dag_core::NodeResult<EntryOutput> {
        let _ = input;
        Ok(EntryOutput)
    }

    flow! {
        name: effects_aggregate_flow,
        version: "0.2.0",
        profile: Web;
        let trigger = node!(crate::aggregated_effects_subflow_module::effects_trigger);
        let writer = node!(crate::aggregated_effects_subflow_module::effects_writer);
        connect!(trigger -> writer);
        entrypoint!({
            trigger: "trigger",
            capture: "writer",
        });
    }
}

mod aggregated_determinism_subflow_module {
    use super::*;

    #[def_node(
        trigger,
        name = "DeterminismTrigger",
        summary = "Determinism trigger",
        effects = "Pure",
        determinism = "Strict"
    )]
    async fn determinism_trigger(input: EntryInput) -> dag_core::NodeResult<EntryInput> {
        Ok(input)
    }

    #[def_node(
        name = "DeterminismBestEffort",
        summary = "Determinism best-effort",
        effects = "Pure",
        determinism = "BestEffort"
    )]
    async fn determinism_best_effort(input: EntryInput) -> dag_core::NodeResult<EntryOutput> {
        let _ = input;
        Ok(EntryOutput)
    }

    flow! {
        name: determinism_aggregate_flow,
        version: "0.3.0",
        profile: Web;
        let trigger = node!(crate::aggregated_determinism_subflow_module::determinism_trigger);
        let best_effort =
            node!(crate::aggregated_determinism_subflow_module::determinism_best_effort);
        connect!(trigger -> best_effort);
        entrypoint!({
            trigger: "trigger",
            capture: "best_effort",
        });
    }
}

mod aggregated_determinism_nondet_subflow_module {
    use super::*;

    #[def_node(
        trigger,
        name = "NondeterminismTrigger",
        summary = "Nondeterminism trigger",
        effects = "Pure",
        determinism = "Strict"
    )]
    async fn nondeterminism_trigger(input: EntryInput) -> dag_core::NodeResult<EntryInput> {
        Ok(input)
    }

    #[def_node(
        name = "NondeterminismNode",
        summary = "Nondeterminism node",
        effects = "Pure",
        determinism = "Nondeterministic"
    )]
    async fn nondeterminism_node(input: EntryInput) -> dag_core::NodeResult<EntryOutput> {
        let _ = input;
        Ok(EntryOutput)
    }

    flow! {
        name: nondeterministic_aggregate_flow,
        version: "0.3.1",
        profile: Web;
        let trigger =
            node!(crate::aggregated_determinism_nondet_subflow_module::nondeterminism_trigger);
        let nondet =
            node!(crate::aggregated_determinism_nondet_subflow_module::nondeterminism_node);
        connect!(trigger -> nondet);
        entrypoint!({
            trigger: "trigger",
            capture: "nondet",
        });
    }
}

mod aggregated_durability_subflow_module {
    use super::*;

    #[def_node(
        trigger,
        name = "DurabilityTrigger",
        summary = "Durability trigger",
        effects = "Pure",
        determinism = "Strict",
        checkpointable = false
    )]
    async fn durability_trigger(input: EntryInput) -> dag_core::NodeResult<EntryInput> {
        Ok(input)
    }

    #[def_node(
        name = "DurabilityMid",
        summary = "Durability mid",
        effects = "Pure",
        determinism = "Strict",
        replayable = false
    )]
    async fn durability_mid(input: EntryInput) -> dag_core::NodeResult<EntryInput> {
        Ok(input)
    }

    #[def_node(
        name = "DurabilityHalt",
        summary = "Durability halt",
        effects = "Pure",
        determinism = "Strict",
        halts = true
    )]
    async fn durability_halt(input: EntryInput) -> dag_core::NodeResult<EntryOutput> {
        let _ = input;
        Ok(EntryOutput)
    }

    flow! {
        name: durability_aggregate_flow,
        version: "0.4.0",
        profile: Web;
        let trigger = node!(crate::aggregated_durability_subflow_module::durability_trigger);
        let mid = node!(crate::aggregated_durability_subflow_module::durability_mid);
        let halt = node!(crate::aggregated_durability_subflow_module::durability_halt);
        connect!(trigger -> mid);
        connect!(mid -> halt);
        entrypoint!({
            trigger: "trigger",
            capture: "halt",
        });
    }
}

mod aggregated_hints_subflow_module {
    use super::*;

    #[allow(dead_code)]
    struct HttpRead;
    #[allow(dead_code)]
    struct HttpWrite;
    #[allow(dead_code)]
    struct RngSource;

    #[def_node(
        trigger,
        name = "HintsTrigger",
        summary = "Hints trigger",
        effects = "ReadOnly",
        determinism = "BestEffort",
        resources(http(HttpRead))
    )]
    async fn hints_trigger(input: EntryInput) -> dag_core::NodeResult<EntryInput> {
        Ok(input)
    }

    #[def_node(
        name = "HintsWriter",
        summary = "Hints writer",
        effects = "Effectful",
        determinism = "BestEffort",
        resources(http(HttpWrite))
    )]
    async fn hints_writer(input: EntryInput) -> dag_core::NodeResult<EntryInput> {
        Ok(input)
    }

    #[def_node(
        name = "HintsRng",
        summary = "Hints rng",
        effects = "Pure",
        determinism = "BestEffort",
        resources(rng(RngSource))
    )]
    async fn hints_rng(input: EntryInput) -> dag_core::NodeResult<EntryOutput> {
        let _ = input;
        Ok(EntryOutput)
    }

    flow! {
        name: hints_aggregate_flow,
        version: "0.5.0",
        profile: Web;
        let trigger = node!(crate::aggregated_hints_subflow_module::hints_trigger);
        let writer = node!(crate::aggregated_hints_subflow_module::hints_writer);
        let rng = node!(crate::aggregated_hints_subflow_module::hints_rng);
        connect!(trigger -> writer);
        connect!(writer -> rng);
        entrypoint!({
            trigger: "trigger",
            capture: "rng",
        });
    }
}

#[test]
fn flow_macro_builds_bundle() {
    let ir = flow();
    assert_eq!(ir.name, "s1_echo");
}

#[test]
fn flow_generates_entrypoint_consts() {
    let _: dag_core::FlowEntrypoint<EntryInput, EntryOutput> =
        typed_entry_flow_module::typed_entry_flow::entry;
    let _: dag_core::FlowEntrypoint<EntryInput, EntryOutput> =
        typed_entry_flow_module::typed_entry_flow;
}

#[test]
fn flow_persists_multi_entrypoint_metadata() {
    let ir = multi_entrypoint_flow_module::flow();
    assert_eq!(ir.metadata.entrypoints.len(), 2);

    let primary = &ir.metadata.entrypoints[0];
    assert_eq!(primary.trigger_alias, "primary_trigger");
    assert_eq!(primary.capture_alias, "primary_capture");
    assert_eq!(primary.route_path.as_deref(), Some("/primary"));
    assert_eq!(primary.method.as_deref(), Some("GET"));
    assert_eq!(
        primary.route_aliases,
        vec!["/primary".to_string(), "/primary-alias".to_string()]
    );

    let secondary = &ir.metadata.entrypoints[1];
    assert_eq!(secondary.trigger_alias, "secondary_trigger");
    assert_eq!(secondary.capture_alias, "secondary_capture");
    assert_eq!(secondary.route_path.as_deref(), Some("/secondary"));
    assert_eq!(secondary.method.as_deref(), Some("POST"));
    assert_eq!(secondary.route_aliases, vec!["/secondary".to_string()]);
}

#[derive(Clone, Debug)]
struct ExternalEntryInput;

impl From<ExternalEntryInput> for typed_entry_flow_module::typed_entry_flow::contract::entry::In {
    fn from(_: ExternalEntryInput) -> Self {
        Self(EntryInput)
    }
}

#[test]
fn flow_contract_exports_nominal_adapter_types() {
    let _: dag_core::FlowEntrypoint<
        typed_entry_flow_module::typed_entry_flow::contract::entry::RawIn,
        typed_entry_flow_module::typed_entry_flow::contract::entry::RawOut,
    > = typed_entry_flow_module::typed_entry_flow::contract::entry::ENTRY;

    assert_eq!(
        typed_entry_flow_module::typed_entry_flow::contract::entry::CONTRACT_ID,
        "typed_entry_flow@1.0.0:entry->capture"
    );

    assert_eq!(
        std::mem::size_of::<typed_entry_flow_module::typed_entry_flow::contract::entry::In>(),
        std::mem::size_of::<typed_entry_flow_module::typed_entry_flow::contract::entry::RawIn>()
    );

    let wrapped: typed_entry_flow_module::typed_entry_flow::contract::entry::In =
        ExternalEntryInput.into();
    let _raw: typed_entry_flow_module::typed_entry_flow::contract::entry::RawIn = wrapped.into();
}

#[test]
fn subflow_accepts_entrypoint_const() {
    let sub = subflow!(subflow_entry_flow_module::subflow_flow::trigger);
    assert_eq!(sub.kind, dag_core::NodeKind::Subflow);
    assert_eq!(sub.effects, dag_core::Effects::Effectful);
    assert_eq!(sub.determinism, dag_core::Determinism::Nondeterministic);
    assert!(!sub.durability.checkpointable);
    assert!(!sub.durability.replayable);
    assert!(sub.durability.halts);

    match sub.in_schema {
        dag_core::SchemaSpec::Named(name) => assert!(name.contains("EntryInput")),
        dag_core::SchemaSpec::Opaque => panic!("expected named schema"),
    }

    let version = semver::Version::parse("1.2.3").expect("version parse");
    let flow_id = dag_core::FlowId::new("subflow_flow", &version);
    let expected_identifier = format!("subflow::{}::trigger", flow_id.0);
    assert_eq!(sub.identifier, expected_identifier);
}

#[test]
fn subflow_preserves_module_prefix_for_root_alias() {
    let sub = subflow!(crate::root_alias_subflow_module::root_alias_flow);

    let version = semver::Version::parse("9.9.9").expect("version parse");
    let flow_id = dag_core::FlowId::new("root_alias_flow", &version);
    let expected_identifier = format!("subflow::{}::trigger", flow_id.0);
    assert_eq!(sub.identifier, expected_identifier);
    assert_eq!(sub.effects, dag_core::Effects::Effectful);
    assert_eq!(sub.determinism, dag_core::Determinism::Nondeterministic);
    assert!(!sub.durability.checkpointable);
    assert!(!sub.durability.replayable);
    assert!(sub.durability.halts);
}

#[test]
fn connect_accepts_subflow_binding() {
    let flow = subflow_parent_flow();
    assert_eq!(flow.edges.len(), 1);
}

#[test]
fn workflow_exports_binding_contract_for_subflow_alias() {
    let _raw_in: subflow_parent_flow::bindings::sub::RawIn = EntryInput;
    let _raw_out: subflow_parent_flow::bindings::sub::RawOut = EntryOutput;

    assert_eq!(subflow_parent_flow::bindings::sub::BINDING_ALIAS, "sub");
    assert_eq!(subflow_parent_flow::bindings::sub::SOURCE_KIND, "subflow");
    assert_eq!(
        subflow_parent_flow::bindings::sub::SOURCE_CONTRACT_ID,
        subflow_entry_flow_module::subflow_flow::contract::trigger::CONTRACT_ID
    );
    assert_eq!(
        std::mem::size_of::<subflow_parent_flow::bindings::sub::In>(),
        std::mem::size_of::<subflow_parent_flow::bindings::sub::RawIn>()
    );
}

#[test]
fn flow_exports_binding_contract_for_node_alias() {
    let _raw_in: typed_entry_flow_module::typed_entry_flow::bindings::entry::RawIn = EntryInput;
    let _raw_out: typed_entry_flow_module::typed_entry_flow::bindings::entry::RawOut = EntryInput;
    assert_eq!(
        typed_entry_flow_module::typed_entry_flow::bindings::entry::BINDING_ALIAS,
        "entry"
    );
    assert_eq!(
        typed_entry_flow_module::typed_entry_flow::bindings::entry::SOURCE_KIND,
        "node"
    );
}

#[test]
fn flow_exports_binding_contract_for_external_types() {
    let _raw_in: external_type_flow_module::external_type_flow::bindings::entry::RawIn =
        std::string::String::new();
    let _raw_out: external_type_flow_module::external_type_flow::bindings::entry::RawOut =
        std::string::String::new();
    assert_eq!(
        external_type_flow_module::external_type_flow::bindings::entry::SOURCE_KIND,
        "node"
    );
}

#[test]
fn subflow_accepts_single_segment_entrypoint_const() {
    let flow = single_segment_subflow_module::single_segment_parent_flow();
    assert_eq!(flow.edges.len(), 1);
}

#[test]
fn subflow_aggregates_effects_to_most_permissive() {
    let sub = subflow!(aggregated_effects_subflow_module::effects_aggregate_flow::trigger);
    assert_eq!(sub.kind, dag_core::NodeKind::Subflow);
    assert_eq!(sub.effects, dag_core::Effects::Effectful);
    assert_eq!(sub.determinism, dag_core::Determinism::Strict);
}

#[test]
fn subflow_aggregates_determinism_to_worst_case() {
    let sub = subflow!(aggregated_determinism_subflow_module::determinism_aggregate_flow::trigger);
    assert_eq!(sub.kind, dag_core::NodeKind::Subflow);
    assert_eq!(sub.effects, dag_core::Effects::Pure);
    assert_eq!(sub.determinism, dag_core::Determinism::BestEffort);
}

#[test]
fn subflow_aggregates_durability_flags() {
    let sub = subflow!(aggregated_durability_subflow_module::durability_aggregate_flow::trigger);
    assert_eq!(sub.kind, dag_core::NodeKind::Subflow);
    assert!(!sub.durability.checkpointable);
    assert!(!sub.durability.replayable);
    assert!(sub.durability.halts);
}

#[test]
fn subflow_unions_effect_and_determinism_hints() {
    let sub = subflow!(aggregated_hints_subflow_module::hints_aggregate_flow::trigger);
    assert_eq!(sub.kind, dag_core::NodeKind::Subflow);
    assert!(sub.effect_hints.contains(&"resource::http::read"));
    assert!(sub.effect_hints.contains(&"resource::http::write"));
    assert!(sub.determinism_hints.contains(&"resource::http"));
    assert!(sub.determinism_hints.contains(&"resource::rng"));
}

#[test]
fn subflow_rollup_worst_case_semantics_unchanged_with_entrypoint_metadata() {
    let ir = aggregated_hints_subflow_module::flow();
    assert_eq!(ir.metadata.entrypoints.len(), 1);
    assert_eq!(ir.metadata.entrypoints[0].trigger_alias, "trigger");
    assert_eq!(ir.metadata.entrypoints[0].capture_alias, "rng");

    let sub = subflow!(aggregated_hints_subflow_module::hints_aggregate_flow::trigger);
    assert_eq!(sub.effects, dag_core::Effects::Effectful);
    assert_eq!(sub.determinism, dag_core::Determinism::BestEffort);
    assert!(sub.effect_hints.contains(&"resource::http::read"));
    assert!(sub.effect_hints.contains(&"resource::http::write"));
    assert!(sub.determinism_hints.contains(&"resource::http"));
    assert!(sub.determinism_hints.contains(&"resource::rng"));
}

#[test]
fn subflow_aggregates_determinism_to_nondeterministic() {
    let sub = subflow!(
        aggregated_determinism_nondet_subflow_module::nondeterministic_aggregate_flow::trigger
    );
    assert_eq!(sub.kind, dag_core::NodeKind::Subflow);
    assert_eq!(sub.determinism, dag_core::Determinism::Nondeterministic);
}
