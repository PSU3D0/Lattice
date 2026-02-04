use std::fs;

#[test]
fn harness_writes_exporter_crate() {
    let dir = tempfile::tempdir().expect("temp dir");
    let crate_dir = dir.path().join("exporter");
    let package_dir = dir.path().join("pkg");
    fs::create_dir_all(&crate_dir).expect("create crate dir");
    fs::create_dir_all(&package_dir).expect("create package dir");
    fs::write(
        package_dir.join("Cargo.toml"),
        "[package]\nname = \"pkg\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .expect("write package Cargo.toml");

    let config = exporters::harness::HarnessConfig {
        default_flow: Some("registry_flow".to_string()),
        flows: Some(vec!["registry_flow".to_string()]),
    };

    let manifest_path =
        exporters::harness::write_exporter_crate(&crate_dir, &package_dir, "pkg", &config)
            .expect("write exporter crate");

    assert_eq!(manifest_path, crate_dir.join("Cargo.toml"));
    assert!(crate_dir.join("src/main.rs").exists());

    let main_rs = fs::read_to_string(crate_dir.join("src/main.rs")).expect("main.rs");
    let cargo_toml = fs::read_to_string(crate_dir.join("Cargo.toml")).expect("Cargo.toml");
    assert!(
        main_rs.contains("exporters::bundle::emit_bundle"),
        "main.rs should call emit_bundle"
    );
    assert!(
        cargo_toml.contains("flow-registry"),
        "Cargo.toml should enable flow-registry"
    );
    assert!(
        main_rs.contains("BundleConfig"),
        "main.rs should configure BundleConfig"
    );
}
