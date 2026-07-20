use connector_spec::{
    BrokerDispatchDescriptor, ConnectorManifest, SurfaceDecl, contract_hash,
    validate_broker_dispatch_descriptor,
};

#[test]
fn send_message_descriptor_matches_the_manifest_contract() {
    let manifest = ConnectorManifest::from_yaml_str(include_str!("../connector.yaml")).unwrap();
    let action = manifest
        .surfaces
        .iter()
        .find_map(|surface| match surface {
            SurfaceDecl::Action(action)
                if action.identifier == "connector.google.gmail.send_message" =>
            {
                Some(action)
            }
            _ => None,
        })
        .unwrap();
    let descriptor: BrokerDispatchDescriptor =
        serde_json::from_slice(include_bytes!("../broker/operations/send_message.json")).unwrap();
    assert!(validate_broker_dispatch_descriptor(&descriptor));
    assert_eq!(
        descriptor.contract_hash,
        contract_hash(&manifest, action).unwrap()
    );
    let metadata = connector_google_gmail::ops::GoogleGmailSendMessage::BROKER_CONTRACT.unwrap();
    assert_eq!(metadata.contract_id, descriptor.contract.contract_id);
    assert_eq!(metadata.contract_hash, descriptor.contract_hash);
    assert_eq!(
        descriptor.request_plan.origin,
        "https://gmail.googleapis.com"
    );
    assert_eq!(
        descriptor.request_plan.path_template,
        "/gmail/v1/users/me/messages/send"
    );
    assert_eq!(descriptor.request_plan.method.as_str(), "POST");
    assert_eq!(
        descriptor.request_plan.body.get("raw").map(String::as_str),
        Some("text_body")
    );
    assert_eq!(
        descriptor
            .request_plan
            .trusted_adapter
            .as_ref()
            .map(|pin| pin.trusted_adapter_id.as_str()),
        Some("google.gmail.rfc822_message.v1")
    );
}
