use broker_core::{
    BrokerError,
    credential::{parse, registry::RegistryDefinitionV2},
};
use broker_workers::{composition::verified_google_registry, registry::StaticRegistry};

#[test]
fn google_registry_requires_complete_signed_approved_active_records() {
    let registry = verified_google_registry("2026-07-21T00:00:00Z").unwrap();
    assert_eq!(registry.len(), 10);

    let mut bundle = provider_google::signed_registry::deterministic_signed_registry().unwrap();
    bundle.seeds[0].1 = bundle.seeds[1].1.clone();
    assert_eq!(
        StaticRegistry::load(
            &bundle.seeds,
            &bundle.publisher_roots,
            &bundle.decision_roots,
            "2026-07-21T00:00:00Z"
        )
        .err()
        .unwrap(),
        BrokerError::Brk106
    );

    let mut bundle = provider_google::signed_registry::deterministic_signed_registry().unwrap();
    let definition = parse::<RegistryDefinitionV2>(&bundle.seeds[0].0).unwrap();
    let entry = definition.view.as_value()["entry_ref"].as_str().unwrap();
    bundle.seeds[0].1 = provider_google::signed_registry::deterministic_decision(
        entry,
        &definition.content_hash(),
        "approved",
        "revoked",
    )
    .unwrap();
    assert_eq!(
        StaticRegistry::load(
            &bundle.seeds,
            &bundle.publisher_roots,
            &bundle.decision_roots,
            "2026-07-21T00:00:00Z"
        )
        .err()
        .unwrap(),
        BrokerError::Brk106
    );
}
