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
     capability hints, connector contracts, durability services, trigger/entrypoint surface, and \
     host constraints, derived entirely from validated Flow IR without executing anything.";

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

pub fn schema_json_for_file(file_name: &str) -> Option<serde_json::Value> {
    match file_name {
        "flow_ir.schema.json" => Some(serde_json::to_value(flow_ir_schema()).expect("schema")),
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
