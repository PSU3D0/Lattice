use std::fs;
use std::path::PathBuf;

use connector_spec::{
    ConnectorManifest, QueryValueDecl, SurfaceDecl, TrustedAdapterPin, ValidationCode,
    contract_hash, request_plan_hash,
};

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

#[test]
fn strict_broker_paths_and_headers_fail_closed() {
    for path in [
        "//evil.example/path",
        "/v1/../secret",
        "/v1/%2e%2e/secret",
        "/v1\\secret",
        "/v1/%5csecret",
        "/v1/prefix-{effect_key}",
    ] {
        let mut manifest = synthetic();
        action_mut(&mut manifest)
            .broker_request
            .as_mut()
            .unwrap()
            .path_template = path.into();
        assert!(
            has_code(&manifest, ValidationCode::InvalidBrokerRequestPlan),
            "path unexpectedly accepted: {path}"
        );
    }

    for header in [
        "Authorization",
        "Host",
        "Cookie",
        "Connection",
        "Transfer-Encoding",
    ] {
        let mut manifest = synthetic();
        let request = action_mut(&mut manifest).broker_request.as_mut().unwrap();
        request.static_headers.clear();
        request.static_headers.insert(header.into(), "x".into());
        assert!(has_code(
            &manifest,
            ValidationCode::InvalidBrokerRequestPlan
        ));
    }
}

#[test]
fn security_objects_deny_unknown_fields() {
    let source = fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../connector-codegen/tests/fixtures/dev_synthetic.connector.yaml"),
    )
    .unwrap();
    let mutated = source.replace(
        "      semantic_effect_slots: [echo_effect]",
        "      semantic_effect_slots: [echo_effect]\n      future_authority: true",
    );
    assert!(ConnectorManifest::from_yaml_str(&mutated).is_err());
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
    let hash = contract_hash(&manifest, action).expect("hash");
    assert_eq!(
        hash,
        "sha256:fd93671cfd9cedeb07dd6445ff26c007db85abae4c408f318e7721dd2fccfde1"
    );
}

#[test]
fn unicode_auth_role_is_hashed_with_full_jcs() {
    let mut manifest = synthetic();
    let profile = manifest
        .profiles
        .outbound_auth
        .remove("synthetic_auth")
        .unwrap();
    manifest
        .profiles
        .outbound_auth
        .insert("synthetic_é".into(), profile);
    let action = action_mut(&mut manifest);
    action.auth = Some("synthetic_é".into());
    action.contract.as_mut().unwrap().auth_role = "outbound_auth.synthetic_é".into();
    manifest.validate().unwrap();
    let SurfaceDecl::Action(action) = &manifest.surfaces[0] else {
        unreachable!()
    };
    assert_ne!(
        contract_hash(&manifest, action).unwrap(),
        "sha256:fd93671cfd9cedeb07dd6445ff26c007db85abae4c408f318e7721dd2fccfde1"
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
fn typed_query_mappings_are_hashed_and_invalid_shapes_fail_closed() {
    let mut manifest = synthetic();
    let request = action_mut(&mut manifest).broker_request.as_mut().unwrap();
    let before = request_plan_hash(request).unwrap();
    request.query.extend([
        (
            "mode".into(),
            QueryValueDecl {
                kind: "static".into(),
                input_field: None,
                value: Some("strict".into()),
            },
        ),
        (
            "message".into(),
            QueryValueDecl {
                kind: "input".into(),
                input_field: Some("message".into()),
                value: None,
            },
        ),
        (
            "effect".into(),
            QueryValueDecl {
                kind: "idempotency_key".into(),
                input_field: None,
                value: None,
            },
        ),
    ]);
    manifest.validate().unwrap();
    let request = action_mut(&mut manifest).broker_request.as_mut().unwrap();
    assert_ne!(request_plan_hash(request).unwrap(), before);
    request.query.get_mut("mode").unwrap().input_field = Some("message".into());
    assert!(has_code(
        &manifest,
        ValidationCode::InvalidBrokerRequestPlan
    ));
}

#[test]
fn unknown_or_unpinned_trusted_adapter_fails_closed() {
    let mut manifest = synthetic();
    action_mut(&mut manifest)
        .broker_request
        .as_mut()
        .unwrap()
        .trusted_adapter = Some(TrustedAdapterPin {
        trusted_adapter_id: "google.unknown.v1".into(),
        implementation_version: "1".into(),
        implementation_hash: format!("sha256:{}", "0".repeat(64)),
    });
    assert!(has_code(
        &manifest,
        ValidationCode::InvalidBrokerRequestPlan
    ));
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
