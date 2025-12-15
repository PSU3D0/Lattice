use std::env;

fn main() {
    let schema_name = env::args()
        .nth(1)
        .unwrap_or_else(|| "flow_ir.schema.json".to_string());

    let schema_json = dag_core::schema::schema_json_for_file(schema_name.as_str())
        .unwrap_or_else(|| panic!("unknown schema `{schema_name}`"));

    println!(
        "{}",
        serde_json::to_string_pretty(&schema_json).expect("serialize schema")
    );
}
