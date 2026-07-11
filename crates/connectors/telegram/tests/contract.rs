use connector_telegram::*;

#[cfg(feature = "host-bundle")]
#[test]
fn register_all_binds_all_actions() {
    let mut registry = kernel_exec::NodeRegistry::new();
    register_all(&mut registry).expect("register nodes");
    assert!(
        registry
            .handler("connector.telegram.send_message")
            .is_some()
    );
}
