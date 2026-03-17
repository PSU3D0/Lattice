use connector_google_sheets::*;

#[cfg(feature = "host-bundle")]
#[test]
fn register_all_binds_all_actions() {
    let mut registry = kernel_exec::NodeRegistry::new();
    register_all(&mut registry).expect("register nodes");
    assert!(
        registry
            .handler("connector.google.sheets.append_row")
            .is_some()
    );
    assert!(
        registry
            .handler("connector.google.sheets.find_rows")
            .is_some()
    );
    assert!(
        registry
            .handler("connector.google.sheets.upsert_row")
            .is_some()
    );
}
