use schemars::schema::{Metadata, RootSchema};

use crate::FlowIR;
use crate::requirements::FlowRequirements;

const FLOW_IR_SCHEMA_ID: &str = "https://lattice.dev/schemas/flow_ir.schema.json";
const FLOW_IR_SCHEMA_TITLE: &str = "Lattice Flow IR";
const FLOW_IR_SCHEMA_DESCRIPTION: &str =
    "Canonical, host-agnostic representation for workflows emitted by the Lattice Rust macro DSL.";

const FLOW_REQUIREMENTS_SCHEMA_ID: &str =
    "https://lattice.dev/schemas/flow_requirements.schema.json";
const FLOW_REQUIREMENTS_SCHEMA_TITLE: &str = "Lattice Flow Requirements";
const FLOW_REQUIREMENTS_SCHEMA_DESCRIPTION: &str = "Static requirements manifest for a flow: \
     capability hints, native-only node placement, connector contracts, durability services, \
     trigger/entrypoint surface, and host constraints, derived entirely from validated Flow IR \
     without executing anything.";

pub fn flow_ir_schema() -> RootSchema {
    let mut schema = schemars::schema_for!(FlowIR);

    schema.meta_schema = Some("https://json-schema.org/draft/2020-12/schema".to_string());

    let metadata = schema
        .schema
        .metadata
        .get_or_insert_with(|| Box::new(Metadata::default()));
    metadata.id = Some(FLOW_IR_SCHEMA_ID.to_string());
    metadata.title = Some(FLOW_IR_SCHEMA_TITLE.to_string());
    metadata.description = Some(FLOW_IR_SCHEMA_DESCRIPTION.to_string());

    schema
}

pub fn flow_requirements_schema() -> RootSchema {
    let mut schema = schemars::schema_for!(FlowRequirements);

    schema.meta_schema = Some("https://json-schema.org/draft/2020-12/schema".to_string());

    let metadata = schema
        .schema
        .metadata
        .get_or_insert_with(|| Box::new(Metadata::default()));
    metadata.id = Some(FLOW_REQUIREMENTS_SCHEMA_ID.to_string());
    metadata.title = Some(FLOW_REQUIREMENTS_SCHEMA_TITLE.to_string());
    metadata.description = Some(FLOW_REQUIREMENTS_SCHEMA_DESCRIPTION.to_string());

    schema
}

fn seal_broker_schema_constraints(schema: &mut serde_json::Value) {
    // Schemars expresses scalar/range limits from Rust attributes. These
    // collection/item constraints mirror validation that is relational or
    // otherwise not representable by field attributes.
    if let Some(slots) =
        schema.pointer_mut("/definitions/BrokerOperationBudget/properties/semantic_effect_slots")
    {
        slots["uniqueItems"] = serde_json::Value::Bool(true);
        slots["items"]["minLength"] = serde_json::json!(1);
        slots["items"]["maxLength"] = serde_json::json!(128);
        slots["items"]["pattern"] = serde_json::json!(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$");
    }
    if let Some(budgets) =
        schema.pointer_mut("/definitions/BrokerAuthority/properties/operation_budgets")
    {
        budgets["uniqueItems"] = serde_json::Value::Bool(true);
    }
    if let Some(map) = schema.pointer_mut(
        "/definitions/BrokerAuthority/properties/connection_aggregate_max_logical_calls",
    ) {
        map["propertyNames"] = serde_json::json!({
            "minLength": 1,
            "maxLength": 256,
            "pattern": r"^[ -~]{1,256}$"
        });
    }
    for pointer in [
        "/definitions/BrokerOperationBudget/properties/max_logical_calls",
        "/definitions/BrokerAuthority/properties/flow_aggregate_max_logical_calls",
        "/definitions/BrokerAuthority/properties/connection_aggregate_max_logical_calls/additionalProperties",
    ] {
        if let Some(values) = schema.pointer_mut(pointer) {
            values["minimum"] = serde_json::json!(1);
            values["maximum"] = serde_json::json!(9_007_199_254_740_991_u64);
        }
    }
}

pub fn schema_json_for_file(file_name: &str) -> Option<serde_json::Value> {
    match file_name {
        "flow_ir.schema.json" => {
            let mut value = serde_json::to_value(flow_ir_schema()).expect("schema");
            seal_broker_schema_constraints(&mut value);
            Some(value)
        }
        "flow_requirements.schema.json" => {
            Some(serde_json::to_value(flow_requirements_schema()).expect("schema"))
        }
        "flow_bundle.schema.json" => {
            // This schema is currently maintained as a canonical JSON file under `schemas/`.
            // Keeping it in the emitter coverage list prevents repo drift tests from failing
            // when additional schema files are introduced.
            let raw = include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../schemas/flow_bundle.schema.json"
            ));
            Some(serde_json::from_str(raw).expect("flow_bundle schema json"))
        }
        _ => None,
    }
}
