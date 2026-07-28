use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use connector_spec::{BrokerDispatchDescriptor, ConnectorManifest, SurfaceDecl, contract_hash};

struct ContractCopies {
    operation_id: &'static str,
    manifest_yaml: &'static str,
    descriptor_json: &'static [u8],
    generated_contract_id: &'static str,
    generated_contract_hash: &'static str,
}

fn checked_in_descriptor_contract_ids() -> BTreeSet<String> {
    let google_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../connectors/google");
    let mut ids = BTreeSet::new();
    for connector in fs::read_dir(google_root).unwrap() {
        let operations = connector.unwrap().path().join("broker/operations");
        if !operations.is_dir() {
            continue;
        }
        for descriptor in fs::read_dir(operations).unwrap() {
            let path = descriptor.unwrap().path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            let descriptor: BrokerDispatchDescriptor =
                serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
            assert!(ids.insert(descriptor.contract.contract_id));
        }
    }
    ids
}

fn contract_copies() -> [ContractCopies; 3] {
    [
        ContractCopies {
            operation_id: "connector.google.gmail.send_message",
            manifest_yaml: include_str!("../../connectors/google/gmail/connector.yaml"),
            descriptor_json: include_bytes!(
                "../../connectors/google/gmail/broker/operations/send_message.json"
            ),
            generated_contract_id:
                connector_google_gmail::ops::GoogleGmailSendMessage::BROKER_CONTRACT
                    .unwrap()
                    .contract_id,
            generated_contract_hash:
                connector_google_gmail::ops::GoogleGmailSendMessage::BROKER_CONTRACT
                    .unwrap()
                    .contract_hash,
        },
        ContractCopies {
            operation_id: "connector.google.sheets.append_row",
            manifest_yaml: include_str!("../../connectors/google/sheets/connector.yaml"),
            descriptor_json: include_bytes!(
                "../../connectors/google/sheets/broker/operations/append_row.json"
            ),
            generated_contract_id:
                connector_google_sheets::ops::GoogleSheetsAppendRow::BROKER_CONTRACT
                    .unwrap()
                    .contract_id,
            generated_contract_hash:
                connector_google_sheets::ops::GoogleSheetsAppendRow::BROKER_CONTRACT
                    .unwrap()
                    .contract_hash,
        },
        ContractCopies {
            operation_id: "connector.google.sheets.create_spreadsheet",
            manifest_yaml: include_str!("../../connectors/google/sheets/connector.yaml"),
            descriptor_json: include_bytes!(
                "../../connectors/google/sheets/broker/operations/create_spreadsheet.json"
            ),
            generated_contract_id:
                connector_google_sheets::ops::GoogleSheetsCreateSpreadsheet::BROKER_CONTRACT
                    .unwrap()
                    .contract_id,
            generated_contract_hash:
                connector_google_sheets::ops::GoogleSheetsCreateSpreadsheet::BROKER_CONTRACT
                    .unwrap()
                    .contract_hash,
        },
    ]
}

#[test]
fn google_minimum_scopes_match_manifest_generated_descriptor_and_profile_copies() {
    // Mutation: edit minimum_scopes in one Google connector.yaml only. The YAML-to-descriptor
    // scope/hash checks below must fail before that drift can reach production.
    let composition = provider_google::google_v1();
    let mut descriptor_contract_ids = BTreeSet::new();

    for copies in contract_copies() {
        let manifest = ConnectorManifest::from_yaml_str(copies.manifest_yaml).unwrap();
        let action = manifest
            .surfaces
            .iter()
            .find_map(|surface| match surface {
                SurfaceDecl::Action(action) if action.identifier == copies.operation_id => {
                    Some(action)
                }
                _ => None,
            })
            .unwrap();
        let yaml_contract = action.contract.as_ref().unwrap();
        let descriptor: BrokerDispatchDescriptor =
            serde_json::from_slice(copies.descriptor_json).unwrap();
        let profile_scopes = composition
            .profile
            .contract_claims
            .get(&descriptor.contract.contract_id)
            .unwrap();
        let descriptor_scopes = descriptor
            .contract
            .minimum_scopes
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();

        assert!(descriptor_contract_ids.insert(descriptor.contract.contract_id.clone()));
        assert_eq!(
            yaml_contract.minimum_scopes,
            descriptor.contract.minimum_scopes
        );
        assert_eq!(profile_scopes, &descriptor_scopes);
        assert_eq!(
            copies.generated_contract_id,
            descriptor.contract.contract_id
        );
        assert_eq!(copies.generated_contract_hash, descriptor.contract_hash);
        assert_eq!(
            contract_hash(&manifest, action).unwrap(),
            descriptor.contract_hash
        );
    }

    assert_eq!(
        descriptor_contract_ids,
        checked_in_descriptor_contract_ids()
    );
    assert_eq!(
        composition
            .profile
            .contract_claims
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>(),
        descriptor_contract_ids
    );
}

#[test]
fn descriptor_contract_hash_changes_when_minimum_scopes_change() {
    // Mutation: remove minimum_scopes from the contract-hash preimage. This assertion must fail;
    // a passing hash after the scope mutation would leave stale bindings looking current.
    let manifest = ConnectorManifest::from_yaml_str(include_str!(
        "../../connectors/google/gmail/connector.yaml"
    ))
    .unwrap();
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
    let original_hash = contract_hash(&manifest, action).unwrap();
    let mut scope_mutation = action.clone();
    scope_mutation
        .contract
        .as_mut()
        .unwrap()
        .minimum_scopes
        .push("https://www.googleapis.com/auth/drive.file".to_string());
    let mutated_hash = contract_hash(&manifest, &scope_mutation).unwrap();

    assert_ne!(
        original_hash, mutated_hash,
        "SECURITY DEFECT: minimum_scopes is not covered by the operation contract hash"
    );
}
