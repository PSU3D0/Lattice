use connector_google_drive::*;

#[cfg(feature = "host-bundle")]
#[test]
fn register_all_binds_all_actions() {
    let mut registry = kernel_exec::NodeRegistry::new();
    register_all(&mut registry).expect("register nodes");
    assert!(
        registry
            .handler("connector.google.drive.search_files")
            .is_some()
    );
}
