use connector_http::*;

#[cfg(feature = "host-bundle")]
#[test]
fn register_all_binds_all_actions() {
    let mut registry = kernel_exec::NodeRegistry::new();
    register_all(&mut registry).expect("register nodes");
    for identifier in [
        "connector.http.get",
        "connector.http.head",
        "connector.http.post",
        "connector.http.put",
        "connector.http.patch",
        "connector.http.delete",
        "connector.http.get_any_origin",
        "connector.http.head_any_origin",
        "connector.http.post_any_origin",
        "connector.http.put_any_origin",
        "connector.http.patch_any_origin",
        "connector.http.delete_any_origin",
    ] {
        assert!(
            registry.handler(identifier).is_some(),
            "missing handler for `{identifier}`"
        );
    }
}
