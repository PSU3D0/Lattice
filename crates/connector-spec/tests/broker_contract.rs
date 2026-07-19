use std::fs;
use std::path::PathBuf;

use connector_spec::{ConnectorManifest, SurfaceDecl, ValidationCode, contract_hash};

fn synthetic() -> ConnectorManifest {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../connector-codegen/tests/fixtures/dev_synthetic.connector.yaml");
    let text = fs::read_to_string(path).expect("synthetic fixture");
    ConnectorManifest::from_yaml_str(&text).expect("synthetic manifest parses")
}

fn action_mut(manifest: &mut ConnectorManifest) -> &mut connector_spec::ActionSurface {
    match &mut manifest.surfaces[0] {
        SurfaceDecl::Action(action) => action,
        _ => panic!("synthetic surface must be an action"),
    }
}

fn has_code(manifest: &ConnectorManifest, code: ValidationCode) -> bool {
    manifest
        .validate()
        .expect_err("manifest must fail validation")
        .as_slice()
        .iter()
        .any(|error| error.code == code)
}

#[test]
fn synthetic_contract_validates_and_has_golden_hash() {
    let manifest = synthetic();
    manifest.validate().expect("synthetic contract validates");
    let SurfaceDecl::Action(action) = &manifest.surfaces[0] else {
        panic!("action")
    };
    let hash = contract_hash(action.contract.as_ref().expect("contract")).expect("hash");
    assert_eq!(
        hash,
        "sha256:937245180087550ae887dcf06012b020b5194ce43172a1e817c29ddd1b237a35"
    );
}

#[test]
fn duplicate_contract_ids_fail_closed() {
    let mut manifest = synthetic();
    manifest.surfaces.push(manifest.surfaces[0].clone());
    action_mut(&mut manifest).identifier = "dev.synthetic.other".to_string();
    assert!(has_code(&manifest, ValidationCode::DuplicateContractId));
}

#[test]
fn malformed_contract_id_fails_closed() {
    let mut manifest = synthetic();
    action_mut(&mut manifest)
        .contract
        .as_mut()
        .expect("contract")
        .contract_id = "dev.synthetic.echo_effect@01".to_string();
    assert!(has_code(&manifest, ValidationCode::InvalidContractId));
}

#[test]
fn unsupported_abi_and_mismatched_semantics_fail_closed() {
    let mut manifest = synthetic();
    let contract = action_mut(&mut manifest)
        .contract
        .as_mut()
        .expect("contract");
    contract.broker_abi_version = "0.2".to_string();
    contract.auth_role = "outbound_auth.missing".to_string();
    let errors = manifest.validate().expect_err("must fail");
    assert!(
        errors
            .as_slice()
            .iter()
            .any(|error| error.code == ValidationCode::InvalidBrokerAbiVersion)
    );
    assert!(
        errors
            .as_slice()
            .iter()
            .any(|error| error.code == ValidationCode::InvalidContractSemantics)
    );
}

#[test]
fn effect_slots_must_be_sorted_and_unique() {
    let mut manifest = synthetic();
    action_mut(&mut manifest)
        .contract
        .as_mut()
        .expect("contract")
        .semantic_effect_slots = vec!["z".to_string(), "a".to_string(), "a".to_string()];
    assert!(has_code(
        &manifest,
        ValidationCode::InvalidSemanticEffectSlots
    ));
}

#[test]
fn broker_placeholder_unknown_input_field_fails_closed() {
    let mut manifest = synthetic();
    let placeholder = action_mut(&mut manifest)
        .broker_request
        .as_mut()
        .expect("request")
        .placeholders
        .get_mut("effect_key")
        .expect("placeholder");
    placeholder.kind = "input".to_string();
    placeholder.input_field = Some("missing".to_string());
    assert!(has_code(
        &manifest,
        ValidationCode::InvalidInputFieldReference
    ));
}

#[test]
fn unknown_broker_placeholder_kind_fails_closed() {
    let mut manifest = synthetic();
    action_mut(&mut manifest)
        .broker_request
        .as_mut()
        .expect("request")
        .placeholders
        .get_mut("effect_key")
        .expect("placeholder")
        .kind = "random".to_string();
    assert!(has_code(&manifest, ValidationCode::UnknownPlaceholderKind));
}

#[test]
fn contract_and_request_plan_must_be_paired() {
    let mut manifest = synthetic();
    action_mut(&mut manifest).broker_request = None;
    assert!(has_code(
        &manifest,
        ValidationCode::InvalidBrokerRequestPlan
    ));
}

#[test]
fn invalid_origins_fail_closed() {
    for origin in [
        "http://synthetic.example",
        "https://synthetic.example/path",
        "https://synthetic.example?q=1",
        "https://user@synthetic.example",
        "https://*.synthetic.example",
    ] {
        let mut manifest = synthetic();
        action_mut(&mut manifest)
            .broker_request
            .as_mut()
            .expect("request")
            .origin = origin.to_string();
        assert!(
            has_code(&manifest, ValidationCode::InvalidBrokerRequestOrigin),
            "origin unexpectedly accepted: {origin}"
        );
    }
}
