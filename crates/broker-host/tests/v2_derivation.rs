use broker_core::{
    BrokerError, canonical,
    commitment::CommitmentKey,
    credential::{
        ParsedV2, connection::ConnectionAuthorityViewV2, parse, receipt::BindingAttestationV2,
    },
    signing::BrokerSigner,
};
use broker_host::{
    BindingVerificationLockV2, HostBootstrapIdentity, InMemoryLiveConnectionAuthority,
    NodeLeaseLimitsV2, NodeLeaseStoreV2, VerifiedBindingV2, bootstrap_host_context,
    verify_binding_v2,
};
use serde_json::Value;

const H: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn fixture_binding() -> Value {
    let vectors: Value = serde_json::from_str(include_str!(
        "../../../impl-docs/spec/credential-plane-protocol-vectors.json"
    ))
    .unwrap();
    vectors["signed_artifact_vectors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["artifact_schema"] == "BindingAttestation")
        .unwrap()["artifact"]
        .clone()
}

fn setup() -> (
    VerifiedBindingV2,
    broker_host::TrustedHostScope,
    InMemoryLiveConnectionAuthority,
    BrokerSigner,
) {
    let signer = BrokerSigner::from_seed("v2-host-key", [31; 32]);
    let mut binding = fixture_binding();
    binding["broker_key_id"] = Value::String(signer.key_id().into());
    binding["execution_lane"] = Value::String("semantic_broker".into());
    binding["custody_location"] = Value::String("hosted_broker".into());
    binding["observed_at"] = Value::String("2026-01-01T00:00:00Z".into());
    binding["expires_at"] = Value::String("2030-01-01T00:00:00Z".into());
    let authority_value = serde_json::json!({
        "schema_version":"0.2","critical_fields":[],"extensions":{},
        "org_id":binding["org_id"],"connection_ref":binding["connection_ref"],
        "auth_profile_ref":binding["auth_profile_ref"],"auth_profile_pin":binding["auth_profile_pin"],
        "endpoint_set_hash":binding["endpoint_set_hash"],"public_config_hash":binding["public_config_hash"],
        "custodian":binding["custodian"],"transport":binding["transport"],
        "execution_lane":binding["execution_lane"],"custody_location":binding["custody_location"],
        "broker_instance_commitment":binding["broker_instance_commitment"],
        "principal_commitments":binding["principal_commitments"],
        "authorization_claims_commitment":binding["authorization_claims_commitment"],
        "authorization_claims_schema_hash":H,
        "public_claims_projection_evidence":binding["public_claims_projection_evidence"],
        "authority_epoch":7,"standing_authority_hash":binding["standing_authority_hash"],
        "contract_set_hash":binding["contract_set_hash"],"compatible_policy_profile_hashes":[],
        "registry_decision_set_hash":binding["registry_decision_set_hash"],
        "created_at":"2026-01-01T00:00:00Z"
    });
    let authority_jcs = canonical::from_serde(&authority_value, 1024 * 1024)
        .unwrap()
        .into_bytes();
    let authority: ParsedV2<ConnectionAuthorityViewV2> = parse(&authority_jcs).unwrap();
    binding["authority_view_hash"] = Value::String(authority.content_hash());
    binding["authority_epoch"] = Value::from(7);
    binding["minimum_material_generation"] = Value::from(3);
    binding["signature"] = serde_json::to_value(
        signer
            .sign_json(
                broker_core::credential::signing::BINDING_ATTESTATION_DOMAIN,
                &serde_json::to_vec(&binding).unwrap(),
            )
            .unwrap(),
    )
    .unwrap();
    let binding_jcs = canonical::from_serde(&binding, 1024 * 1024)
        .unwrap()
        .into_bytes();
    let _: ParsedV2<BindingAttestationV2> = parse(&binding_jcs).unwrap();
    let live = InMemoryLiveConnectionAuthority::new();
    live.set("x", authority_jcs, 3).unwrap();
    let auth_profile_ref: broker_core::credential::AuthProfileRefV2 = parse(
        canonical::from_serde(&binding["auth_profile_ref"], 64 * 1024)
            .unwrap()
            .as_bytes(),
    )
    .unwrap()
    .view;
    let verified = verify_binding_v2(
        &binding_jcs,
        &BindingVerificationLockV2 {
            org_id: "x".into(),
            principal: "x".into(),
            issuer: "x".into(),
            broker_key_id: signer.key_id().into(),
            deployment_id: "x".into(),
            authority_manifest_hash: H.into(),
            standing_authority_ref: "x".into(),
            standing_authority_hash: H.into(),
            contract_set_ref: "x".into(),
            contract_set_hash: H.into(),
            connection_ref: "x".into(),
            auth_profile_ref,
            execution_lane: "semantic_broker".into(),
            custody_location: "hosted_broker".into(),
        },
        &signer.verifying_key(),
        &live,
        "2027-01-01T00:00:00Z",
    )
    .unwrap();
    let scope = bootstrap_host_context(HostBootstrapIdentity {
        org_id: "x".into(),
        principal_id: "x".into(),
        bundle_id: "bundle".into(),
        flow_ir_hash: H.into(),
        binding_lock_hash: H.into(),
        flow_id: "flow".into(),
        run_id: "run".into(),
    })
    .unwrap()
    .scope_for_activation("node", "node_alias", 2)
    .unwrap();
    (verified, scope, live, signer)
}

#[test]
fn node_lease_is_non_invocable_and_child_is_exact_and_cas_budgeted() {
    let (binding, scope, live, signer) = setup();
    let store = NodeLeaseStoreV2::default();
    let lease = store
        .issue(
            &binding,
            &scope,
            NodeLeaseLimitsV2 {
                logical_binding_ref: "logical_binding_v2_fixture".into(),
                binding_revision_ref: "binding_v2_fixture_a".into(),
                operation_contract: "synthetic@1".into(),
                contract_hash: H.into(),
                logical_calls: 2,
                dispatch_attempts_per_call: 2,
                flow_logical_calls: 2,
                connection_logical_calls: 2,
                node_logical_calls: 2,
                first_activation_ordinal: 2,
                last_activation_ordinal: 2,
                semantic_effect_slots: vec!["first".into(), "second".into()],
                not_before: "2026-01-01T00:00:00Z".into(),
                expires_at: "2030-01-01T00:00:00Z".into(),
                node_lease_ref: "node_lease_test".into(),
                jti: "lease-jti".into(),
            },
            &signer,
        )
        .unwrap();
    assert_eq!(lease.view.as_value()["audience"], "broker-grant-derivation");

    let key = CommitmentKey::new("v2-input-key", [44; 32]).unwrap();
    let first = store
        .derive_child(
            "node_lease_test",
            &scope,
            "first",
            br#"{"value":1}"#,
            "2027-01-01T00:00:00Z",
            0,
            b"proof",
            b"proof",
            &live,
            &key,
        )
        .unwrap();
    assert!(!first.redelivery);
    assert_eq!(first.grant.view.as_value()["audience"], "broker-execution");
    let replay = store
        .derive_child(
            "node_lease_test",
            &scope,
            "first",
            br#"{"value":1}"#,
            "2027-01-01T00:00:00Z",
            999,
            b"proof",
            b"proof",
            &live,
            &key,
        )
        .unwrap();
    assert!(replay.redelivery);
    assert_eq!(replay.canonical_grant, first.canonical_grant);
    assert_eq!(
        store
            .derive_child(
                "node_lease_test",
                &scope,
                "first",
                br#"{"value":2}"#,
                "2027-01-01T00:00:00Z",
                1,
                b"proof",
                b"proof",
                &live,
                &key
            )
            .unwrap_err(),
        broker_host::BrokerHostError::Broker(BrokerError::Brk203),
    );
    assert_eq!(
        store
            .derive_child(
                "node_lease_test",
                &scope,
                "second",
                br#"{"value":2}"#,
                "2027-01-01T00:00:00Z",
                0,
                b"proof",
                b"proof",
                &live,
                &key
            )
            .unwrap_err(),
        broker_host::BrokerHostError::Broker(BrokerError::Brk204),
    );
    let second = store
        .derive_child(
            "node_lease_test",
            &scope,
            "second",
            br#"{"value":2}"#,
            "2027-01-01T00:00:00Z",
            1,
            b"proof",
            b"proof",
            &live,
            &key,
        )
        .unwrap();
    assert_eq!(second.cas_version, 2);
    assert_eq!(
        store
            .derive_child(
                "node_lease_test",
                &scope,
                "third",
                br#"{"value":3}"#,
                "2027-01-01T00:00:00Z",
                2,
                b"proof",
                b"proof",
                &live,
                &key
            )
            .unwrap_err(),
        broker_host::BrokerHostError::V2DerivationRejected,
    );
}

#[test]
fn a_run_aborts_instead_of_switching_binding_revisions() {
    let (binding, scope, _live, signer) = setup();
    let store = NodeLeaseStoreV2::default();
    let limits = |lease: &str, revision: &str| NodeLeaseLimitsV2 {
        logical_binding_ref: "logical_binding_v2_fixture".into(),
        binding_revision_ref: revision.into(),
        operation_contract: "synthetic@1".into(),
        contract_hash: H.into(),
        logical_calls: 3,
        dispatch_attempts_per_call: 1,
        flow_logical_calls: 3,
        connection_logical_calls: 3,
        node_logical_calls: 3,
        first_activation_ordinal: 2,
        last_activation_ordinal: 2,
        semantic_effect_slots: vec!["first".into()],
        not_before: "2026-01-01T00:00:00Z".into(),
        expires_at: "2030-01-01T00:00:00Z".into(),
        node_lease_ref: lease.into(),
        jti: format!("{lease}-jti"),
    };
    store
        .issue(
            &binding,
            &scope,
            limits("node_lease_revision_a", "binding_v2_revision_a"),
            &signer,
        )
        .unwrap();
    let error = match store.issue(
        &binding,
        &scope,
        limits("node_lease_revision_b", "binding_v2_revision_b"),
        &signer,
    ) {
        Ok(_) => panic!("run switched binding revisions"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        broker_host::BrokerHostError::Broker(BrokerError::Brk106)
    );
}

#[test]
fn grant_identity_is_namespaced_by_binding_revision() {
    let (binding, scope, live, signer) = setup();
    let key = CommitmentKey::new("v2-input-key", [44; 32]).unwrap();
    let derive = |revision: &str, lease: &str| {
        let store = NodeLeaseStoreV2::default();
        store
            .issue(
                &binding,
                &scope,
                NodeLeaseLimitsV2 {
                    logical_binding_ref: "logical_binding_v2_fixture".into(),
                    binding_revision_ref: revision.into(),
                    operation_contract: "synthetic@1".into(),
                    contract_hash: H.into(),
                    logical_calls: 1,
                    dispatch_attempts_per_call: 1,
                    flow_logical_calls: 1,
                    connection_logical_calls: 1,
                    node_logical_calls: 1,
                    first_activation_ordinal: 2,
                    last_activation_ordinal: 2,
                    semantic_effect_slots: vec!["first".into()],
                    not_before: "2026-01-01T00:00:00Z".into(),
                    expires_at: "2030-01-01T00:00:00Z".into(),
                    node_lease_ref: lease.into(),
                    jti: format!("{lease}-jti"),
                },
                &signer,
            )
            .unwrap();
        store
            .derive_child(
                lease,
                &scope,
                "first",
                br#"{"value":1}"#,
                "2027-01-01T00:00:00Z",
                0,
                b"proof",
                b"proof",
                &live,
                &key,
            )
            .unwrap()
            .grant_ref
            .as_str()
            .to_owned()
    };
    assert_ne!(
        derive("binding_v2_revision_a", "node_lease_revision_a"),
        derive("binding_v2_revision_b", "node_lease_revision_b")
    );
}

#[test]
fn concurrent_child_reservation_has_one_atomic_cas_winner() {
    use std::{sync::Arc, thread};
    let (binding, scope, live, signer) = setup();
    let store = Arc::new(NodeLeaseStoreV2::default());
    store
        .issue(
            &binding,
            &scope,
            NodeLeaseLimitsV2 {
                logical_binding_ref: "logical_binding_v2_fixture".into(),
                binding_revision_ref: "binding_v2_fixture_a".into(),
                operation_contract: "synthetic@1".into(),
                contract_hash: H.into(),
                logical_calls: 2,
                dispatch_attempts_per_call: 1,
                flow_logical_calls: 2,
                connection_logical_calls: 2,
                node_logical_calls: 2,
                first_activation_ordinal: 2,
                last_activation_ordinal: 2,
                semantic_effect_slots: vec!["first".into(), "second".into()],
                not_before: "2026-01-01T00:00:00Z".into(),
                expires_at: "2030-01-01T00:00:00Z".into(),
                node_lease_ref: "node_lease_race".into(),
                jti: "lease-race".into(),
            },
            &signer,
        )
        .unwrap();
    let live = Arc::new(live);
    let key = Arc::new(CommitmentKey::new("v2-race-key", [45; 32]).unwrap());
    let handles = ["first", "second"].map(|slot| {
        let store = Arc::clone(&store);
        let live = Arc::clone(&live);
        let key = Arc::clone(&key);
        let scope = scope.clone();
        thread::spawn(move || {
            store.derive_child(
                "node_lease_race",
                &scope,
                slot,
                br#"{"value":1}"#,
                "2027-01-01T00:00:00Z",
                0,
                b"proof",
                b"proof",
                live.as_ref(),
                key.as_ref(),
            )
        })
    });
    let results = handles.map(|handle| handle.join().unwrap());
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(
                result,
                Err(broker_host::BrokerHostError::Broker(BrokerError::Brk204))
            ))
            .count(),
        1
    );
}

#[test]
fn pop_and_live_generation_epoch_drift_fail_closed() {
    let (binding, scope, live, signer) = setup();
    let store = NodeLeaseStoreV2::default();
    store
        .issue(
            &binding,
            &scope,
            NodeLeaseLimitsV2 {
                logical_binding_ref: "logical_binding_v2_fixture".into(),
                binding_revision_ref: "binding_v2_fixture_a".into(),
                operation_contract: "synthetic@1".into(),
                contract_hash: H.into(),
                logical_calls: 1,
                dispatch_attempts_per_call: 1,
                flow_logical_calls: 1,
                connection_logical_calls: 1,
                node_logical_calls: 1,
                first_activation_ordinal: 2,
                last_activation_ordinal: 2,
                semantic_effect_slots: vec!["first".into()],
                not_before: "2026-01-01T00:00:00Z".into(),
                expires_at: "2030-01-01T00:00:00Z".into(),
                node_lease_ref: "node_lease_drift".into(),
                jti: "lease-drift".into(),
            },
            &signer,
        )
        .unwrap();
    let key = CommitmentKey::new("v2-input-key", [44; 32]).unwrap();
    assert_eq!(
        store
            .derive_child(
                "node_lease_drift",
                &scope,
                "first",
                br#"{}"#,
                "2027-01-01T00:00:00Z",
                0,
                b"wrong",
                b"proof",
                &live,
                &key
            )
            .unwrap_err(),
        broker_host::BrokerHostError::Broker(BrokerError::Brk102)
    );
    live.set("x", binding.authority_view().canonical_bytes().to_vec(), 2)
        .unwrap();
    assert_eq!(
        store
            .derive_child(
                "node_lease_drift",
                &scope,
                "first",
                br#"{}"#,
                "2027-01-01T00:00:00Z",
                0,
                b"proof",
                b"proof",
                &live,
                &key
            )
            .unwrap_err(),
        broker_host::BrokerHostError::V2AuthorityDrift
    );
}
