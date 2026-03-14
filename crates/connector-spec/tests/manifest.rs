use std::fs;
use std::path::PathBuf;

use connector_spec::{ConnectorManifest, ValidationCode};

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn fixture_text(name: &str) -> String {
    fs::read_to_string(fixture_path(name)).expect("fixture text")
}

#[test]
fn github_issues_fixture_parses_and_validates_for_codegen() {
    let text = fixture_text("github_issues.connector.yaml");
    let manifest = ConnectorManifest::from_yaml_str(&text).expect("manifest parses");

    manifest.validate().expect("manifest validates");
    manifest
        .validate_for_codegen()
        .expect("manifest is phase-b codegen compatible");
}

#[test]
fn validate_reports_unknown_endpoint_profile() {
    let text = fixture_text("github_issues.connector.yaml")
        .replace("endpoint: github_default", "endpoint: github_missing");
    let manifest = ConnectorManifest::from_yaml_str(&text).expect("manifest parses");
    let errors = manifest.validate().expect_err("validation should fail");

    assert!(
        errors
            .as_slice()
            .iter()
            .any(|error| error.code == ValidationCode::UnknownEndpointProfile)
    );
}

#[test]
fn numeric_defaults_validate_for_codegen() {
    let text = r#"
connector:
  id: connector.test.numeric
  vendor: test
  family: numeric
  version: 0.1.0
  crate: connector_test_numeric
  summary: Numeric defaults
profiles:
  outbound_auth: {}
  endpoint_profiles:
    default:
      base_url: https://example.test
  provisioning_auth: {}
  inbound_verifiers: {}
types:
  Input:
    kind: object
    fields:
      limit:
        type: u32
        default: 100
  Output:
    kind: object
    fields:
      ok:
        type: bool
surfaces:
  - kind: action
    identifier: connector.test.numeric.get
    name: NumericGet
    summary: Numeric defaults action
    input: Input
    output: Output
    endpoint: default
    effects: ReadOnly
    determinism: BestEffort
    resources:
      - http_read(capabilities::http::HttpRead)
    request:
      method: GET
      path_template: /items
"#;
    let manifest = ConnectorManifest::from_yaml_str(text).expect("manifest parses");
    manifest
        .validate_for_codegen()
        .expect("numeric defaults should validate");
}

#[test]
fn validate_reports_invalid_json_escape_hatch() {
    let text = r#"
connector:
  id: connector.test.example
  vendor: test
  family: example
  version: 0.1.0
  crate: connector_test_example
  summary: Example
profiles:
  outbound_auth: {}
  endpoint_profiles:
    default:
      base_url: https://example.test
  provisioning_auth: {}
  inbound_verifiers: {}
types:
  Input:
    kind: object
    fields: {}
  Output:
    kind: object
    fields:
      payload:
        type: json
surfaces:
  - kind: action
    identifier: connector.test.example.echo
    name: ExampleEcho
    summary: Echo
    input: Input
    output: Output
    endpoint: default
    effects: ReadOnly
    determinism: BestEffort
    resources:
      - http_read(capabilities::http::HttpRead)
    request:
      method: GET
      path_template: /echo
"#;
    let manifest = ConnectorManifest::from_yaml_str(text).expect("manifest parses");
    let errors = manifest.validate().expect_err("validation should fail");

    assert!(
        errors
            .as_slice()
            .iter()
            .any(|error| error.code == ValidationCode::InvalidJsonEscapeHatch)
    );
}

#[test]
fn validate_for_codegen_rejects_reserved_surface_kinds() {
    let mut text = fixture_text("github_issues.connector.yaml");
    text.push_str(
        r#"
  - kind: webhook_trigger
    identifier: connector.github.issues.events
    name: GithubIssuesEvents
    output: GithubIssueSummary
    lifecycle: manual_external
    webhook:
      method: POST
      route_hint: github/issues
"#,
    );

    let manifest = ConnectorManifest::from_yaml_str(&text).expect("manifest parses");
    let errors = manifest
        .validate_for_codegen()
        .expect_err("codegen validation should fail");

    assert!(
        errors
            .as_slice()
            .iter()
            .any(|error| error.code == ValidationCode::UnsupportedSurfaceKind)
    );
}

#[test]
fn validate_for_codegen_rejects_unsupported_outbound_auth_kind() {
    let text = fixture_text("github_issues.connector.yaml").replace("kind: bearer", "kind: oauth2");
    let manifest = ConnectorManifest::from_yaml_str(&text).expect("manifest parses");
    let errors = manifest
        .validate_for_codegen()
        .expect_err("codegen validation should fail");

    assert!(
        errors
            .as_slice()
            .iter()
            .any(|error| error.code == ValidationCode::UnsupportedOutboundAuthKind)
    );
}
