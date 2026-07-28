use std::fs;
use std::path::Path;

use connector_spec::{ConnectorManifest, SurfaceDecl, generated_module_name};

#[test]
fn broker_contract_member_presence_matches_contract_declaration_for_every_google_operation() {
    // Mutation: emit BROKER_CONTRACT for Drive search_files while leaving another
    // non-contracted operation without it. The per-operation shape check must fail.
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let codegen_source = fs::read_to_string(crate_root.join("src/lib.rs")).unwrap();
    assert!(
        codegen_source.contains("if let Some(contract) = &action.contract"),
        "current codegen shape must remain conditional until P2 changes it deliberately"
    );

    let connectors_root = crate_root.join("../connectors/google");
    for family in ["drive", "gmail", "sheets"] {
        let root = connectors_root.join(family);
        let yaml = fs::read_to_string(root.join("connector.yaml")).unwrap();
        let manifest = ConnectorManifest::from_yaml_str(&yaml).unwrap();

        for surface in &manifest.surfaces {
            let SurfaceDecl::Action(action) = surface else {
                continue;
            };
            let module = generated_module_name(&action.identifier);
            let checked_in = fs::read_to_string(root.join(format!("src/ops/{module}.rs"))).unwrap();
            let expects_member = action.contract.is_some();

            assert_eq!(
                checked_in.contains("pub const BROKER_CONTRACT"),
                expects_member,
                "checked-in BROKER_CONTRACT shape drifted for {}",
                action.identifier
            );
        }
    }
}
