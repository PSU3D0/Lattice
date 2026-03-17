use connector_google_sheets::generated::manifest::{CONNECTOR_ID, CONNECTOR_YAML};
use connector_spec::ConnectorManifest;

#[test]
fn manifest_embeds_source_yaml_and_validates() {
    assert_eq!(CONNECTOR_ID, "connector.google.sheets");
    assert!(CONNECTOR_YAML.contains(CONNECTOR_ID));

    let manifest = ConnectorManifest::from_yaml_str(CONNECTOR_YAML).expect("manifest parses");
    manifest.validate().expect("manifest validates");
}
