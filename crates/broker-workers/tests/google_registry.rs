use broker_core::{
    BrokerError,
    credential::{parse, registry::RegistryDefinitionV2},
};
use broker_workers::{composition::verified_google_registry, registry::StaticRegistry};

#[test]
fn google_registry_requires_complete_signed_approved_active_records() {
    let registry = verified_google_registry("2026-07-21T00:00:00Z").unwrap();
    assert_eq!(registry.len(), 12);

    let mut bundle = provider_google::signed_registry::signed_registry_bundle().unwrap();
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

    let mut bundle = provider_google::signed_registry::signed_registry_bundle().unwrap();
    let definition = parse::<RegistryDefinitionV2>(&bundle.seeds[0].0).unwrap();
    assert!(definition.view.as_value()["entry_ref"].as_str().is_some());
    let decision = String::from_utf8(bundle.seeds[0].1.clone()).unwrap();
    bundle.seeds[0].1 = decision
        .replacen(
            "\"approval_status\":\"approved\"",
            "\"approval_status\":\"denied\"",
            1,
        )
        .into_bytes();
    assert_eq!(
        StaticRegistry::load(
            &bundle.seeds,
            &bundle.publisher_roots,
            &bundle.decision_roots,
            "2026-07-21T00:00:00Z"
        )
        .err()
        .unwrap(),
        BrokerError::Brk004
    );
}
