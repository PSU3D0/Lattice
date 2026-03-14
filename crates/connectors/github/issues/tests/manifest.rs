use connector_github_issues::generated::manifest::{CONNECTOR_ID, CONNECTOR_YAML};

#[test]
fn generated_manifest_embeds_source_yaml() {
    assert_eq!(CONNECTOR_ID, "connector.github.issues");
    assert!(CONNECTOR_YAML.contains(CONNECTOR_ID));
}
