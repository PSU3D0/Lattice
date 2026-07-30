use super::grant::ChildDerivationStore;
use super::*;
use crate::{
    BrokerError, artifacts, artifacts::VerificationTier, canonical, commitment::CommitmentKey,
    signing::BrokerSigner,
};
use serde_json::Value;

fn vectors() -> Value {
    serde_json::from_str(include_str!(
        "../../../../impl-docs/spec/credential-plane-protocol-vectors.json"
    ))
    .unwrap()
}

#[test]
fn all_141_claimed_union_fixtures_validate_at_their_exact_schema_site() {
    let vectors = vectors();
    let fixtures = vectors["union_fixtures"].as_array().unwrap();
    assert_eq!(fixtures.len(), 141);
    for fixture in fixtures {
        let pointer = fixture["schema_pointer"].as_str().unwrap();
        let reference = if pointer == "/" {
            "#".to_owned()
        } else {
            format!("#{pointer}")
        };
        model::validate_reference(&reference, &fixture["instance"]).unwrap_or_else(|error| {
            panic!("{} branch {}: {error}", pointer, fixture["branch_index"])
        });
        let canonical = canonical::from_serde(&fixture["instance"], 1024 * 1024).unwrap();
        assert_eq!(hex::encode(canonical.as_bytes()), fixture["jcs_hex"]);
        assert_eq!(canonical.sha256(), fixture["sha256"]);
    }
}

#[test]
fn all_17_signatures_and_signed_hashes_recompute() {
    let vectors = vectors();
    let key = &vectors["signing_keys"][0];
    let seed: [u8; 32] = hex::decode(key["seed_hex"].as_str().unwrap())
        .unwrap()
        .try_into()
        .unwrap();
    let signer = BrokerSigner::from_seed(key["key_id"].as_str().unwrap(), seed);
    let verifying = signer.verifying_key();
    let signed = vectors["signed_artifact_vectors"].as_array().unwrap();
    assert_eq!(signed.len(), 17);
    for vector in signed {
        let artifact = &vector["artifact"];
        let schema = vector["artifact_schema"].as_str().unwrap();
        model::validate(schema, artifact).unwrap_or_else(|error| panic!("{schema}: {error}"));
        let canonical = canonical::from_serde(artifact, 1024 * 1024).unwrap();
        assert_eq!(hex::encode(canonical.as_bytes()), vector["signed_jcs_hex"]);
        assert_eq!(canonical.sha256(), vector["signed_artifact_hash"]);
        let signature: artifacts::SignatureEnvelope =
            serde_json::from_value(artifact["signature"].clone()).unwrap();
        if schema == "RemoteEnvelope" {
            let preimage = signing::remote_preimage(artifact).unwrap();
            assert_eq!(hex::encode(&preimage), vector["preimage_hex"]);
            verifying.verify_preimage(&preimage, &signature).unwrap();
        } else {
            verifying
                .verify_json(
                    vector["domain"].as_str().unwrap(),
                    canonical.as_bytes(),
                    &signature,
                )
                .unwrap();
        }
    }
}

#[test]
fn all_14_commitments_recompute() {
    let vectors = vectors();
    let commitments = vectors["commitment_vectors"].as_array().unwrap();
    assert_eq!(commitments.len(), 14);
    for vector in commitments {
        let root: [u8; 32] = hex::decode(vector["root_key_hex"].as_str().unwrap())
            .unwrap()
            .try_into()
            .unwrap();
        let key = CommitmentKey::new("vector", root).unwrap();
        let context = hex::decode(vector["context_jcs_hex"].as_str().unwrap()).unwrap();
        let parts = vector["context"].as_array().unwrap();
        let text = |index: usize| parts[index].as_str().unwrap().to_owned();
        let attempt = || parts[6].as_u64().unwrap();
        let typed = match vector["field_name"].as_str().unwrap() {
            "authorization_claims" => commitment::CommitmentContextV2::AuthorizationClaims {
                connection_ref: text(2),
                authority_epoch: parts[3].as_u64().unwrap(),
            },
            "principal_account_subject" => {
                commitment::CommitmentContextV2::PrincipalAccountSubject {
                    connection_ref: text(2),
                    authority_epoch: parts[3].as_u64().unwrap(),
                }
            }
            "dynamic_source_value" => commitment::CommitmentContextV2::DynamicSourceValue {
                instance_id: text(2),
                source_ref: text(3),
            },
            "broker_instance" => commitment::CommitmentContextV2::BrokerInstance {
                connection_ref: text(2),
                authority_epoch: parts[3].as_u64().unwrap(),
            },
            "service_identity" => {
                commitment::CommitmentContextV2::ServiceIdentity { entry_ref: text(2) }
            }
            "connection_commitment" => commitment::CommitmentContextV2::ConnectionCommitment {
                issuer: text(2),
                run_id: text(3),
                node_id: text(4),
                effect: text(5),
                attempt: attempt(),
            },
            "grant_canonical_input" => commitment::CommitmentContextV2::GrantCanonicalInput {
                issuer: text(2),
                grant_ref: text(3),
                effect: text(4),
            },
            "canonical_input_commitment" => {
                commitment::CommitmentContextV2::CanonicalInputCommitment {
                    issuer: text(2),
                    run_id: text(3),
                    node_id: text(4),
                    effect: text(5),
                    attempt: attempt(),
                }
            }
            "response_commitment" => commitment::CommitmentContextV2::ResponseCommitment {
                issuer: text(2),
                run_id: text(3),
                node_id: text(4),
                effect: text(5),
                attempt: attempt(),
            },
            "policy_constraint" => commitment::CommitmentContextV2::PolicyConstraint {
                instance_id: text(2),
            },
            "selected_facts" => commitment::CommitmentContextV2::SelectedFacts {
                instance_id: text(2),
            },
            "effective_policy_value" => commitment::CommitmentContextV2::EffectivePolicyValue {
                instance_id: text(2),
            },
            "provider_constraint_evidence" => {
                commitment::CommitmentContextV2::ProviderConstraintEvidence {
                    issuer: text(2),
                    run_id: text(3),
                    node_id: text(4),
                    effect: text(5),
                    attempt: attempt(),
                    predicate_id: text(8),
                }
            }
            "rotation_provider_result" => commitment::CommitmentContextV2::RotationProviderResult {
                connection_ref: text(2),
                rotation_ref: text(3),
            },
            field => panic!("unexpected commitment field {field}"),
        };
        assert_eq!(typed.field_name(), vector["field_name"]);
        assert_eq!(typed.salt_context().unwrap(), context);
        let value = hex::decode(vector["value_bytes_hex"].as_str().unwrap()).unwrap();
        let (actual, opening) = key
            .commit(
                std::str::from_utf8(
                    &hex::decode(vector["org_id_bytes_hex"].as_str().unwrap()).unwrap(),
                )
                .unwrap(),
                vector["field_name"].as_str().unwrap(),
                &context,
                &value,
                VerificationTier::BrokerOnly,
            )
            .unwrap();
        assert_eq!(
            actual.value,
            format!("hmac-sha256:{}", vector["commitment_hex"].as_str().unwrap())
        );
        assert!(opening.opens(&actual, &value));
    }
}

fn root_fixture(tag: &str) -> Value {
    vectors()["union_fixtures"]
        .as_array()
        .unwrap()
        .iter()
        .find(|fixture| fixture["schema_pointer"] == "/" && fixture["branch_tag"] == tag)
        .unwrap()["instance"]
        .clone()
}

#[test]
fn closed_vocabularies_sorted_bounds_and_critical_fields_fail_closed() {
    let mut profile = root_fixture("AuthProfileDescriptor");
    profile
        .as_object_mut()
        .unwrap()
        .insert("unknown".into(), Value::Bool(true));
    assert!(serde_json::from_value::<profile::AuthProfileDescriptorV2>(profile).is_err());

    let mut scheme = vectors()["union_fixtures"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["schema_pointer"] == "/$defs/SchemeConfig" && f["branch_index"] == 0)
        .unwrap()["instance"]
        .clone();
    scheme["kind"] = Value::String("unknown_scheme".into());
    assert!(serde_json::from_value::<profile::SchemeConfigV2>(scheme).is_err());

    let mut artifact = root_fixture("RegistryDecision");
    artifact["extensions"]["must_understand"] = Value::Bool(true);
    artifact["critical_fields"] = serde_json::json!(["/extensions/must_understand"]);
    assert!(serde_json::from_value::<registry::RegistryDecisionV2>(artifact).is_err());

    let mut policy = root_fixture("CredentialResponsePolicy");
    if let Some(array) = policy
        .get_mut("sensitive_headers")
        .and_then(Value::as_array_mut)
    {
        array.extend([Value::String("z".into()), Value::String("a".into())]);
    }
    assert!(serde_json::from_value::<CredentialResponsePolicyV2>(policy).is_err());
}

#[test]
fn leases_grants_rotation_receipts_and_fences_reject_cross_stage_mutations() {
    let mut lease = root_fixture("NodeLease");
    lease["audience"] = Value::String("broker-execution".into());
    assert!(serde_json::from_value::<grant::NodeLeaseV2>(lease).is_err());

    let store = grant::InMemoryChildDerivationStore::default();
    store.insert_lease("lease", 1).unwrap();
    store
        .reserve_child("lease", "effect", "input-a", 0)
        .unwrap();
    assert_eq!(
        store.reserve_child("lease", "effect", "input-b", 1),
        Err(BrokerError::Brk203)
    );

    let mut snapshot = root_fixture("ConnectionSnapshot");
    snapshot["current_material_generation"] = serde_json::json!(99);
    assert!(serde_json::from_value::<connection::ConnectionSnapshotV2>(snapshot).is_err());

    let mut rotation = vectors()["union_fixtures"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["schema_pointer"] == "/$defs/RotationRecord" && f["branch_tag"] == "prepared")
        .unwrap()["instance"]
        .clone();
    rotation["completion_record_hash"] = Value::String(
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
    );
    assert!(serde_json::from_value::<rotation::RotationRecordV2>(rotation).is_err());

    let mut receipt = root_fixture("InvocationReceipt");
    receipt["dispatch_attempt"] = serde_json::json!(0);
    receipt
        .as_object_mut()
        .unwrap()
        .remove("pre_dispatch_stage");
    assert!(serde_json::from_value::<receipt::InvocationReceiptV2>(receipt).is_err());

    let mut fence = root_fixture("CrossVersionCredentialFence");
    fence["phase"] = Value::String("v1_authoritative".into());
    fence["v2_lease_ever_issued"] = Value::Bool(true);
    assert!(serde_json::from_value::<CrossVersionCredentialFenceV2>(fence).is_err());
}

#[test]
fn legacy_authority_and_remote_replay_are_separate_and_fail_closed() {
    let inventory: legacy::LegacyAdmissionInventoryV2 =
        serde_json::from_value(root_fixture("LegacyAdmissionInventory")).unwrap();
    let decision: legacy::LegacyInventoryDecisionV2 =
        serde_json::from_value(root_fixture("LegacyInventoryDecision")).unwrap();
    let admission = legacy::ExecutableV1Admission {
        inventory: &inventory,
        decision: &decision,
    };
    assert!(
        admission
            .admits_hash("sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
            .is_err()
    );

    let mut archive = root_fixture("HistoricalVerificationKeyArchive");
    archive["validity_evidence"]["key_id"] = Value::String("wrong".into());
    let archive: legacy::HistoricalVerificationKeyArchiveV2 =
        serde_json::from_value(archive).unwrap();
    assert!(legacy::HistoricalV1ReceiptVerifier::new(&archive).is_err());

    let replay = InMemoryReplayState::default();
    let reservation = replay
        .reserve("sender", "request", "nonce", "hash-a")
        .unwrap();
    replay
        .record_terminal(&reservation, b"encrypted".to_vec())
        .unwrap();
    assert_eq!(
        replay
            .reserve("sender", "request", "nonce", "hash-a")
            .unwrap()
            .recorded_result(),
        Some(b"encrypted".as_slice())
    );
    assert_eq!(
        replay.reserve("sender", "request", "nonce", "hash-b"),
        Err(BrokerError::Brk203)
    );
}
