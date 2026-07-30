use super::{admission, commitment, model, receipt_keys, signing};
use crate::{canonical, signing::BrokerSigner};
use serde_json::Value;
use std::collections::BTreeMap;

fn vectors() -> Value {
    serde_json::from_str(include_str!(
        "../../../../impl-docs/spec/credential-plane-protocol-vectors.json"
    ))
    .unwrap()
}

fn packet() -> Value {
    vectors()["lifecycle_separated_1"].clone()
}

fn fixed_key(packet: &Value) -> crate::signing::BrokerVerifyingKey {
    let seed: [u8; 32] = hex::decode(packet["signing_key"]["seed_hex"].as_str().unwrap())
        .unwrap()
        .try_into()
        .unwrap();
    let signer = BrokerSigner::from_seed(packet["signing_key"]["key_id"].as_str().unwrap(), seed);
    assert_eq!(
        hex::encode(signer.verifying_key().to_bytes()),
        packet["signing_key"]["public_key_hex"].as_str().unwrap()
    );
    signer.verifying_key()
}

fn positive_graph(packet: &Value) -> admission::LifecycleArtifacts {
    let mut artifacts = BTreeMap::new();
    for vector in packet["signed_artifact_vectors"].as_array().unwrap() {
        let class = vector["artifact_class"].as_str().unwrap().to_owned();
        artifacts
            .entry(class)
            .or_insert_with(Vec::new)
            .push(vector["artifact"].clone());
    }
    artifacts
}

fn hash(value: &Value) -> String {
    canonical::from_serde(value, 1024 * 1024).unwrap().sha256()
}

fn set_derived_registry_hashes(artifacts: &mut admission::LifecycleArtifacts) {
    let vector_hash = hash(&artifacts["RegistryDecisionVector"][0]);
    artifacts.get_mut("CorrectedBindingAttestation").unwrap()[0]["registry_vector_hash"] =
        Value::String(vector_hash.clone());
    artifacts.get_mut("DispatchAdmission").unwrap()[0]["registry_vector_hash"] =
        Value::String(vector_hash.clone());
    artifacts.get_mut("InvocationReceipt").unwrap()[0]["registry_vector_hash"] =
        Value::String(vector_hash);
    let binding_hash = hash(&artifacts["CorrectedBindingAttestation"][0]);
    artifacts.get_mut("DispatchAdmission").unwrap()[0]["binding_hash"] =
        Value::String(binding_hash);
    let admission_hash = hash(&artifacts["DispatchAdmission"][0]);
    artifacts.get_mut("InvocationReceipt").unwrap()[0]["dispatch_admission_hash"] =
        Value::String(admission_hash);
}

fn canonical_artifact_bytes(packet: &Value) -> Vec<(String, Vec<u8>)> {
    packet["signed_artifact_vectors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|vector| {
            (
                vector["artifact_schema"].as_str().unwrap().to_owned(),
                canonical::from_serde(&vector["artifact"], 1024 * 1024)
                    .unwrap()
                    .into_bytes(),
            )
        })
        .collect()
}

#[test]
fn corrected_and_prefixed_types_are_distinct() {
    use std::any::TypeId;
    assert_ne!(
        TypeId::of::<super::lifecycle::CorrectedBindingAttestationLs1>(),
        TypeId::of::<super::receipt::BindingAttestationV2>()
    );
    assert_ne!(
        TypeId::of::<super::lifecycle::InvocationReceiptLs1>(),
        TypeId::of::<super::receipt::InvocationReceiptV2>()
    );
}

#[test]
fn all_18_corrected_signatures_domains_and_hashes_verify_without_fallback() {
    let packet = packet();
    let key = fixed_key(&packet);
    let vectors = packet["signed_artifact_vectors"].as_array().unwrap();
    assert_eq!(vectors.len(), 18);
    for vector in vectors {
        let schema = vector["artifact_schema"].as_str().unwrap();
        let artifact = &vector["artifact"];
        assert_eq!(
            signing::lifecycle_domain_for_schema(schema),
            vector["domain"].as_str()
        );
        signing::verify_lifecycle_value(schema, artifact, &key)
            .unwrap_or_else(|error| panic!("{}: {error}", vector["vector_id"]));
        let canonical = canonical::from_serde(artifact, 1024 * 1024).unwrap();
        assert_eq!(hex::encode(canonical.as_bytes()), vector["signed_jcs_hex"]);
        assert_eq!(canonical.sha256(), vector["signed_artifact_hash"]);
        assert_eq!(artifact["key_id"], artifact["signature"]["key_id"]);
    }
    assert!(signing::lifecycle_domain_for_schema("InvocationReceipt").is_none());
    assert!(signing::domain_for_schema("LS1InvocationReceipt").is_none());
}

#[test]
fn all_9_lifecycle_commitments_recompute_exact_intermediates() {
    let packet = packet();
    let vectors = packet["commitment_vectors"].as_array().unwrap();
    assert_eq!(vectors.len(), 9);
    for vector in vectors {
        let root: [u8; 32] = (0_u8..32).collect::<Vec<_>>().try_into().unwrap();
        let owned: Vec<(String, Vec<u8>)> = vector["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|field| {
                (
                    field["name"].as_str().unwrap().to_owned(),
                    field["value_utf8"].as_str().unwrap().as_bytes().to_vec(),
                )
            })
            .collect();
        let borrowed: Vec<(&str, &[u8])> = owned
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_slice()))
            .collect();
        let private = hex::decode(vector["private_value_hex"].as_str().unwrap()).unwrap();
        let context = commitment::LifecycleCommitmentContext::try_from_fields(
            vector["context"].as_str().unwrap(),
            &borrowed,
        )
        .unwrap();
        let actual =
            commitment::commit_lifecycle_separated_with_intermediates(&root, &context, &private)
                .unwrap();
        assert_eq!(
            commitment::commit_lifecycle_separated(&root, &context, &private).unwrap(),
            vector["commitment"]
        );
        assert_eq!(
            hex::encode(&actual.framed_context),
            vector["framed_context_hex"]
        );
        assert_eq!(
            hex::encode(&actual.opening_preimage),
            vector["opening_preimage_hex"]
        );
        assert_eq!(hex::encode(actual.scoped_key), vector["scoped_key_hex"]);
        assert_eq!(
            hex::encode(&actual.commitment_preimage),
            vector["commitment_preimage_hex"]
        );
        assert_eq!(actual.value, vector["commitment"]);
    }
}

#[test]
fn lifecycle_commitment_context_and_private_bounds_fail_closed() {
    let names = [
        "tenant_id",
        "issuer",
        "provider",
        "auth_profile_ref",
        "oauth_client_id",
        "audience",
        "artifact_ref",
        "purpose",
    ];
    let values = [b"x".as_slice(); 8];
    let fields: Vec<_> = names
        .iter()
        .zip(values.iter())
        .map(|(n, v)| (*n, *v))
        .collect();
    let context_name = concat!(
        "lattice.credential-plane.0.2.lifecycle-separated-1.commitment.",
        "account-subject"
    );
    let mut valid = fields.clone();
    valid[7].1 = b"account-subject";
    let context =
        commitment::LifecycleCommitmentContext::try_from_fields(context_name, &valid).unwrap();
    assert!(commitment::commit_lifecycle_separated(&[0; 32], &context, b"").is_err());
    assert!(
        commitment::commit_lifecycle_separated(
            &[0; 32],
            &context,
            &vec![0; canonical::MAX_OPERATION_BYTES + 1],
        )
        .is_err()
    );

    let mut empty = valid.clone();
    empty[0].1 = b"";
    assert!(commitment::LifecycleCommitmentContext::try_from_fields(context_name, &empty).is_err());
    let oversized = vec![0; canonical::MAX_STRING_BYTES + 1];
    let mut oversized_fields = valid.clone();
    oversized_fields[0].1 = &oversized;
    assert!(
        commitment::LifecycleCommitmentContext::try_from_fields(context_name, &oversized_fields,)
            .is_err()
    );
    let large = vec![0; 200_000];
    let mut aggregate_fields = valid.clone();
    for field in &mut aggregate_fields[..7] {
        field.1 = &large;
    }
    assert!(
        commitment::LifecycleCommitmentContext::try_from_fields(context_name, &aggregate_fields,)
            .is_err()
    );
}

#[test]
fn positive_relationship_graph_is_complete_and_coherent() {
    let packet = packet();
    let artifacts = positive_graph(&packet);
    let errors =
        admission::lifecycle_relationship_rejections(&artifacts, &packet["relationship_context"])
            .unwrap();
    assert!(errors.is_empty(), "{errors:?}");
}

#[test]
fn registry_vector_supports_multiple_exact_pairs_and_rejects_missing_or_extra() {
    let packet = packet();
    let mut artifacts = positive_graph(&packet);
    let mut definition = artifacts["RegistryDefinition"][0].clone();
    definition["registry_definition_ref"] = Value::String("registry-secondary-1".into());
    definition["definition_hash"] = Value::String(format!("sha256:{}", "9".repeat(64)));
    let mut decision = artifacts["RegistryDecision"][0].clone();
    decision["registry_decision_ref"] = Value::String("registry-decision-secondary-1".into());
    decision["registry_definition_ref"] = definition["registry_definition_ref"].clone();
    decision["registry_definition_artifact_hash"] = Value::String(hash(&definition));
    decision["definition_hash"] = definition["definition_hash"].clone();
    decision["decision_epoch"] = Value::from(4);
    let entry = serde_json::json!({
        "entry_ref": definition["registry_definition_ref"],
        "definition_hash": definition["definition_hash"],
        "decision_ref": decision["registry_decision_ref"],
        "decision_hash": hash(&decision),
        "decision_epoch": decision["decision_epoch"],
        "status": "approved_active"
    });
    artifacts
        .get_mut("RegistryDefinition")
        .unwrap()
        .push(definition);
    artifacts
        .get_mut("RegistryDecision")
        .unwrap()
        .push(decision);
    artifacts.get_mut("RegistryDecisionVector").unwrap()[0]["entries"]
        .as_array_mut()
        .unwrap()
        .push(entry);
    set_derived_registry_hashes(&mut artifacts);
    let errors =
        admission::lifecycle_relationship_rejections(&artifacts, &packet["relationship_context"])
            .unwrap();
    assert!(errors.is_empty(), "{errors:?}");

    let mut missing = artifacts.clone();
    missing.get_mut("RegistryDecision").unwrap().pop();
    let errors =
        admission::lifecycle_relationship_rejections(&missing, &packet["relationship_context"])
            .unwrap();
    assert!(errors.contains("registry_decision_vector_unbacked_entry"));

    let mut duplicate = positive_graph(&packet);
    let duplicate_definition = duplicate["RegistryDefinition"][0].clone();
    duplicate
        .get_mut("RegistryDefinition")
        .unwrap()
        .push(duplicate_definition);
    let errors =
        admission::lifecycle_relationship_rejections(&duplicate, &packet["relationship_context"])
            .unwrap();
    assert!(errors.contains("registry_definition_duplicate"));

    let mut extra = positive_graph(&packet);
    extra
        .get_mut("RegistryDefinition")
        .unwrap()
        .push(artifacts["RegistryDefinition"][1].clone());
    let errors =
        admission::lifecycle_relationship_rejections(&extra, &packet["relationship_context"])
            .unwrap();
    assert!(errors.contains("registry_decision_vector_extra_backing_artifact"));
}

#[test]
fn raw_bytes_graph_boundary_verifies_all_artifacts_and_rejects_duplicates() {
    let packet = packet();
    let owned = canonical_artifact_bytes(&packet);
    let borrowed: Vec<_> = owned
        .iter()
        .map(|(schema, bytes)| (schema.as_str(), bytes.as_slice()))
        .collect();
    let context = canonical::from_serde(&packet["relationship_context"], 1024 * 1024)
        .unwrap()
        .into_bytes();
    let mut keys = BTreeMap::new();
    keys.insert(
        packet["signing_key"]["key_id"].as_str().unwrap().to_owned(),
        fixed_key(&packet),
    );
    let _verified = admission::verify_lifecycle_graph_bytes(&borrowed, &context, &keys).unwrap();

    let mut duplicate_owned = owned;
    let original = String::from_utf8(duplicate_owned[0].1.clone()).unwrap();
    duplicate_owned[0].1 = original
        .replacen('{', "{\"schema_version\":\"0.2\",", 1)
        .into_bytes();
    let duplicate_borrowed: Vec<_> = duplicate_owned
        .iter()
        .map(|(schema, bytes)| (schema.as_str(), bytes.as_slice()))
        .collect();
    assert!(admission::verify_lifecycle_graph_bytes(&duplicate_borrowed, &context, &keys).is_err());
}

#[test]
fn ls1_receipt_enforces_committed_64_kib_bound() {
    assert_eq!(
        <super::lifecycle::InvocationReceiptLs1 as super::SchemaType>::MAX_BYTES,
        64 * 1024
    );
    let packet = packet();
    let mut receipt = positive_graph(&packet)["InvocationReceipt"][0].clone();
    receipt["extensions"]["padding"] = Value::String("x".repeat(64 * 1024));
    assert!(model::validate("LS1InvocationReceipt", &receipt).is_err());
}

#[test]
fn receipt_terminal_consistency_and_claim_relations_fail_closed() {
    let packet = packet();
    let mut artifacts = positive_graph(&packet);
    let receipt = &mut artifacts.get_mut("InvocationReceipt").unwrap()[0];
    receipt["provider_dispatch_observation"] = Value::String("remote_operation_started".into());
    assert!(super::receipt::verify_lifecycle_terminal_consistency(receipt).is_err());

    let mut artifacts = positive_graph(&packet);
    artifacts.get_mut("ProviderGrantVersion").unwrap()[0]["provider_grant_acceptance"]["relation"] =
        Value::String("profile_defined_normalization".into());
    let errors =
        admission::lifecycle_relationship_rejections(&artifacts, &packet["relationship_context"])
            .unwrap();
    assert!(errors.contains("google_provider_grant_acceptance_not_exact"));
    assert_eq!(
        artifacts["ProviderGrantVersion"][0]["operation_claim_coverage"],
        "subset_or_equal"
    );
}

#[test]
fn all_59_negative_vectors_fail_at_the_named_boundary() {
    let packet = packet();
    let key = fixed_key(&packet);
    let positive = positive_graph(&packet);
    let negatives = packet["negative_vectors"].as_array().unwrap();
    assert_eq!(negatives.len(), 59);
    let mut seen = 0;
    for vector in negatives {
        let id = vector["negative_id"].as_str().unwrap();
        let schema = vector["artifact_schema"].as_str().unwrap();
        let artifact = &vector["artifact"];
        match vector["rejection_stage"].as_str().unwrap() {
            "schema" | "internal_phase" => {
                assert!(model::validate(schema, artifact).is_err(), "{id}");
            }
            "signature" | "semantic_identity" => {
                assert!(
                    signing::verify_lifecycle_value(schema, artifact, &key).is_err(),
                    "{id}"
                );
            }
            "domain_confusion" => {
                signing::verify_lifecycle_value_in_domain(
                    artifact,
                    vector["domain"].as_str().unwrap(),
                    &key,
                )
                .unwrap_or_else(|error| panic!("{id} old domain: {error}"));
                assert!(
                    signing::verify_lifecycle_value(schema, artifact, &key).is_err(),
                    "{id}"
                );
            }
            "relationship" => {
                signing::verify_lifecycle_value(schema, artifact, &key)
                    .unwrap_or_else(|error| panic!("{id} signature: {error}"));
                let class = vector["artifact_class"].as_str().unwrap();
                let mut artifacts = positive.clone();
                artifacts.insert(class.to_owned(), vec![artifact.clone()]);
                let errors = admission::lifecycle_relationship_rejections(
                    &artifacts,
                    &packet["relationship_context"],
                )
                .unwrap();
                let expected = vector["expected_rejection"].as_str().unwrap();
                assert!(
                    errors.contains(expected),
                    "{id}: expected {expected}, got {errors:?}"
                );
            }
            stage => panic!("{id}: unknown stage {stage}"),
        }
        seen += 1;
    }
    assert_eq!(seen, 59);
}

#[test]
fn all_7_historical_classifications_use_nanosecond_time_and_never_authorize() {
    let packet = packet();
    let key = fixed_key(&packet);
    let artifacts = positive_graph(&packet);
    let keyset = &artifacts["ReceiptVerificationKeyset"][0];
    let compromise = &artifacts["ReceiptKeyCompromiseRecord"][0];
    let keyset = canonical::from_serde(keyset, 1024 * 1024)
        .unwrap()
        .into_bytes();
    let compromise = canonical::from_serde(compromise, 1024 * 1024)
        .unwrap()
        .into_bytes();
    let vectors = packet["historical_receipt_classification_vectors"]
        .as_array()
        .unwrap();
    assert_eq!(vectors.len(), 7);
    for vector in vectors {
        let receipt = canonical::from_serde(&vector["artifact"], 64 * 1024)
            .unwrap()
            .into_bytes();
        let actual =
            receipt_keys::classify_historical_receipt_bytes(&receipt, &keyset, &compromise, &key)
                .unwrap_or_else(|error| panic!("{}: {error}", vector["vector_id"]));
        assert_eq!(
            actual.protocol_name(),
            vector["expected_classification"].as_str().unwrap(),
            "{}",
            vector["vector_id"]
        );
        assert!(!actual.dispatch_authority());
        assert_eq!(vector["dispatch_authority"], false);
    }
    assert!(receipt_keys::parse_protocol_timestamp("2026-02-29T00:00:00Z").is_err());
    assert!(receipt_keys::parse_protocol_timestamp("2024-02-29T00:00:00.000000001Z").is_ok());
}
