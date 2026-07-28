use std::fs;
use std::path::Path;

use connector_codegen::generate_files;
use connector_spec::{ConnectorManifest, SurfaceDecl, generated_module_name};

#[test]
fn broker_contract_member_is_unconditional_and_matches_contract_declaration() {
    // Mutation: omit BROKER_CONTRACT from any generated operation or emit Some
    // for a non-contracted operation. The per-operation shape check must fail.
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));

    let connector_crates = [
        ("../connectors/google/drive", "src/ops"),
        ("../connectors/google/gmail", "src/ops"),
        ("../connectors/google/sheets", "src/ops"),
        ("../connectors/github/issues", "src/generated/ops"),
    ];
    for (relative_root, ops_dir) in connector_crates {
        let root = crate_root.join(relative_root);
        let yaml = fs::read_to_string(root.join("connector.yaml")).unwrap();
        let manifest = ConnectorManifest::from_yaml_str(&yaml).unwrap();

        for surface in &manifest.surfaces {
            let SurfaceDecl::Action(action) = surface else {
                continue;
            };
            let module = generated_module_name(&action.identifier);
            let checked_in =
                fs::read_to_string(root.join(format!("{ops_dir}/{module}.rs"))).unwrap();
            assert!(
                checked_in.contains("pub const BROKER_CONTRACT"),
                "checked-in BROKER_CONTRACT is absent for {}",
                action.identifier
            );
            assert!(
                checked_in.contains("broker_contract: Self::BROKER_CONTRACT"),
                "checked-in META is not tied to BROKER_CONTRACT for {}",
                action.identifier
            );
            assert_eq!(
                checked_in.contains("= Some("),
                action.contract.is_some(),
                "checked-in BROKER_CONTRACT option drifted for {}",
                action.identifier
            );
        }
    }
}

#[test]
fn github_checked_in_operations_match_fresh_codegen() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = crate_root.join("../connectors/github/issues");
    let yaml = fs::read_to_string(root.join("connector.yaml")).unwrap();
    let manifest = ConnectorManifest::from_yaml_str(&yaml).unwrap();
    let generated = generate_files(&manifest, &yaml).unwrap();

    for surface in &manifest.surfaces {
        let SurfaceDecl::Action(action) = surface else {
            continue;
        };
        let module = generated_module_name(&action.identifier);
        let relative = format!("src/generated/ops/{module}.rs");
        let expected = &generated
            .iter()
            .find(|file| file.relative_path == relative)
            .unwrap()
            .contents;
        let checked_in = fs::read_to_string(root.join(&relative)).unwrap();
        assert_eq!(
            tokens_without_formatting(&checked_in),
            tokens_without_formatting(expected),
            "checked-in generated operation drifted for {}",
            action.identifier
        );
    }
}

fn tokens_without_formatting(source: &str) -> String {
    let generated_body = source
        .find("const ")
        .map_or(source, |index| &source[index..]);
    let mut normalized: String = generated_body.split_whitespace().collect();
    loop {
        let without_trailing_commas = normalized
            .replace(",}", "}")
            .replace(",]", "]")
            .replace(",)", ")");
        if without_trailing_commas == normalized {
            return normalized;
        }
        normalized = without_trailing_commas;
    }
}
