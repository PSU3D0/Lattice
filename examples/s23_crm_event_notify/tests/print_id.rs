#[test]
fn print_id() {
    println!(
        "S23_FLOW_ID={}",
        example_s23_crm_event_notify::validated_ir()
            .flow()
            .id
            .as_str()
    );
}
