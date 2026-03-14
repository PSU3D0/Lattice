use connector_github_issues::*;

#[cfg(feature = "host-bundle")]
#[test]
fn register_all_binds_all_actions() {
    let mut registry = kernel_exec::NodeRegistry::new();
    register_all(&mut registry).expect("register nodes");
    assert!(registry.handler("connector.github.issues.create").is_some());
    assert!(registry.handler("connector.github.issues.get").is_some());
    assert!(registry.handler("connector.github.issues.list").is_some());
}
