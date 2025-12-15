use schemars::schema::{Metadata, RootSchema};

use crate::FlowIR;

const FLOW_IR_SCHEMA_ID: &str = "https://lattice.dev/schemas/flow_ir.schema.json";
const FLOW_IR_SCHEMA_TITLE: &str = "Lattice Flow IR";
const FLOW_IR_SCHEMA_DESCRIPTION: &str =
    "Canonical, host-agnostic representation for workflows emitted by the Lattice Rust macro DSL.";

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

pub fn schema_json_for_file(file_name: &str) -> Option<serde_json::Value> {
    match file_name {
        "flow_ir.schema.json" => Some(serde_json::to_value(flow_ir_schema()).expect("schema")),
        _ => None,
    }
}
