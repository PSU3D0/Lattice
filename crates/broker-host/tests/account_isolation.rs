use broker_core::{
    canonical,
    credential::{
        AuthProfileRefV2, ParsedV2, connection::ConnectionAuthorityViewV2, parse,
        signing::BINDING_ATTESTATION_DOMAIN,
    },
    signing::BrokerSigner,
};
use broker_host::{BindingVerificationLockV2, InMemoryLiveConnectionAuthority, verify_binding_v2};
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

fn account_commitment(byte: char) -> Value {
    serde_json::json!([{
        "kind": "account_subject",
        "commitment": {
            "alg": "hmac-sha256",
            "key_id": "account-key",
            "verification_tier": "broker_only",
            "value": format!("hmac-sha256:{}", byte.to_string().repeat(64))
        }
    }])
}

fn authority_for(binding: &Value, connection_ref: &str, account: Value) -> Vec<u8> {
    let authority = serde_json::json!({
        "schema_version":"0.2","critical_fields":[],"extensions":{},
        "org_id":binding["org_id"],"connection_ref":connection_ref,
        "auth_profile_ref":binding["auth_profile_ref"],"auth_profile_pin":binding["auth_profile_pin"],
        "endpoint_set_hash":binding["endpoint_set_hash"],"public_config_hash":binding["public_config_hash"],
        "custodian":binding["custodian"],"transport":binding["transport"],
        "execution_lane":binding["execution_lane"],"custody_location":binding["custody_location"],
        "broker_instance_commitment":binding["broker_instance_commitment"],
        "principal_commitments":account,
        "authorization_claims_commitment":binding["authorization_claims_commitment"],
        "authorization_claims_schema_hash":H,
        "public_claims_projection_evidence":binding["public_claims_projection_evidence"],
        "authority_epoch":7,"standing_authority_hash":binding["standing_authority_hash"],
        "contract_set_hash":binding["contract_set_hash"],"compatible_policy_profile_hashes":[],
        "registry_decision_set_hash":binding["registry_decision_set_hash"],
        "created_at":"2026-01-01T00:00:00Z"
    });
    canonical::from_serde(&authority, 1024 * 1024)
        .unwrap()
        .into_bytes()
}

#[test]
#[ignore = "P4 will fix: the V2 binding lock has no expected account-subject commitment"]
fn binding_refuses_a_different_account_connection_under_the_same_deployment() {
    // Mutation after P4: remove the expected-account comparison from binding verification. This
    // assertion must fail again by accepting connection B for a flow locked to account A.
    let signer = BrokerSigner::from_seed("account-isolation-key", [73; 32]);
    let mut binding = fixture_binding();
    binding["broker_key_id"] = Value::String(signer.key_id().into());
    binding["connection_ref"] = Value::String("google-connection-b".into());
    binding["execution_lane"] = Value::String("semantic_broker".into());
    binding["custody_location"] = Value::String("hosted_broker".into());
    binding["observed_at"] = Value::String("2026-01-01T00:00:00Z".into());
    binding["expires_at"] = Value::String("2030-01-01T00:00:00Z".into());

    let account_a = account_commitment('a');
    let account_b = account_commitment('b');
    let authority_a = authority_for(&binding, "google-connection-a", account_a);
    let authority_b = authority_for(&binding, "google-connection-b", account_b.clone());
    let parsed_b: ParsedV2<ConnectionAuthorityViewV2> = parse(&authority_b).unwrap();
    binding["authority_view_hash"] = Value::String(parsed_b.content_hash());
    binding["authority_epoch"] = Value::from(7);
    binding["minimum_material_generation"] = Value::from(3);
    binding["principal_commitments"] = account_b;
    binding["signature"] = serde_json::to_value(
        signer
            .sign_json(
                BINDING_ATTESTATION_DOMAIN,
                &serde_json::to_vec(&binding).unwrap(),
            )
            .unwrap(),
    )
    .unwrap();
    let binding_jcs = canonical::from_serde(&binding, 1024 * 1024)
        .unwrap()
        .into_bytes();

    let live = InMemoryLiveConnectionAuthority::new();
    live.set("google-connection-a", authority_a, 3).unwrap();
    live.set("google-connection-b", authority_b, 3).unwrap();
    let auth_profile_ref: AuthProfileRefV2 = parse(
        canonical::from_serde(&binding["auth_profile_ref"], 64 * 1024)
            .unwrap()
            .as_bytes(),
    )
    .unwrap()
    .view;
    let lock = BindingVerificationLockV2 {
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
        connection_ref: "google-connection-b".into(),
        auth_profile_ref,
        execution_lane: "semantic_broker".into(),
        custody_location: "hosted_broker".into(),
    };

    // The flow's intended account is A, but that fact cannot be represented in lock today.
    let attempted_binding_to_b = verify_binding_v2(
        &binding_jcs,
        &lock,
        &signer.verifying_key(),
        &live,
        "2027-01-01T00:00:00Z",
    );
    assert!(
        attempted_binding_to_b.is_err(),
        "binding accepted account B even though this flow is committed to account A"
    );
}
