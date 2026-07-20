use connector_spec::{
    BrokerDispatchDescriptor, ConnectorManifest, SurfaceDecl, contract_hash,
    validate_broker_dispatch_descriptor,
};

#[test]
fn append_row_descriptor_matches_the_manifest_contract() {
    let manifest = ConnectorManifest::from_yaml_str(include_str!("../connector.yaml")).unwrap();
    let action = manifest
        .surfaces
        .iter()
        .find_map(|surface| match surface {
            SurfaceDecl::Action(action)
                if action.identifier == "connector.google.sheets.append_row" =>
            {
                Some(action)
            }
            _ => None,
        })
        .unwrap();
    let descriptor: BrokerDispatchDescriptor =
        serde_json::from_slice(include_bytes!("../broker/operations/append_row.json")).unwrap();
    assert!(validate_broker_dispatch_descriptor(&descriptor));
    assert_eq!(
        descriptor.contract_hash,
        contract_hash(&manifest, action).unwrap()
    );
    let metadata = connector_google_sheets::ops::GoogleSheetsAppendRow::BROKER_CONTRACT.unwrap();
    assert_eq!(metadata.contract_id, descriptor.contract.contract_id);
    assert_eq!(metadata.contract_hash, descriptor.contract_hash);
    assert_eq!(
        descriptor.request_plan.origin,
        "https://sheets.googleapis.com"
    );
    assert_eq!(descriptor.request_plan.method.as_str(), "POST");
    assert_eq!(
        descriptor
            .request_plan
            .body
            .get("values")
            .map(String::as_str),
        Some("row")
    );
    assert_eq!(
        descriptor.request_plan.path_template,
        "/v4/spreadsheets/{spreadsheet_id}/values/{range}:append"
    );
    assert_eq!(
        descriptor
            .request_plan
            .query
            .get("valueInputOption")
            .and_then(|value| value.input_field.as_deref()),
        Some("value_input_option")
    );
    assert_eq!(
        descriptor
            .request_plan
            .trusted_adapter
            .as_ref()
            .map(|pin| pin.trusted_adapter_id.as_str()),
        Some("google.sheets.append_row.v1")
    );
}
