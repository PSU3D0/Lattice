#[cfg(feature = "flow-registry")]
mod flow_registry_tests {
    use std::path::PathBuf;

    use dag_core::{
        flow_registry::{EntrypointSpec, FlowRegistration},
        Determinism, Effects, FlowBuilder, NodeSpec, Profile, SchemaSpec,
    };
    use semver::Version;

    fn sample_flow() -> dag_core::FlowIR {
        let mut builder = FlowBuilder::new("registry_flow", Version::new(1, 0, 0), Profile::Web);
        let node = NodeSpec::inline(
            "node://example",
            "Example",
            SchemaSpec::Named("input"),
            SchemaSpec::Named("output"),
            Effects::Effectful,
            Determinism::Nondeterministic,
            None,
        );
        builder.add_node("step", &node).expect("add node");
        builder.build()
    }

    const ENTRYPOINTS: &[EntrypointSpec] = &[EntrypointSpec {
        trigger: "ingress",
        capture: "out",
        route_aliases: &["/echo"],
        method: Some("GET"),
        deadline_ms: Some(1000),
    }];

    dag_core::flow_registry::submit!(FlowRegistration {
        name: "registry_flow",
        version: "1.0.0",
        profile: Profile::Web,
        entrypoints: ENTRYPOINTS,
        flow_ir: sample_flow,
    });

    #[test]
    fn exporter_builds_manifest_from_registry() {
        let export =
            exporters::bundle::build_manifest_from_registry(&exporters::bundle::BundleConfig {
                default_flow: Some("registry_flow".to_string()),
                flows: None,
            })
            .expect("manifest export");

        assert_eq!(export.manifest.flows.len(), 1);
        let flow = &export.manifest.flows[0];
        assert_eq!(flow.entrypoints.len(), 1);
        assert_eq!(flow.entrypoints[0].trigger, "ingress");
        assert_eq!(flow.entrypoints[0].route_aliases, vec!["/echo".to_string()]);
        assert!(flow.nodes.contains_key("step"));

        let ir_path = PathBuf::from("flows/registry_flow/flow_ir.json");
        assert!(export.ir_files.contains_key(&ir_path));
    }

    #[test]
    fn exporter_emits_manifest_and_ir_files() {
        let export =
            exporters::bundle::build_manifest_from_registry(&exporters::bundle::BundleConfig {
                default_flow: Some("registry_flow".to_string()),
                flows: None,
            })
            .expect("manifest export");

        let out_dir = tempfile::tempdir().expect("temp dir");
        exporters::bundle::emit_bundle(out_dir.path(), &export).expect("emit bundle");

        assert!(out_dir.path().join("manifest.json").exists());
        assert!(out_dir
            .path()
            .join("flows/registry_flow/flow_ir.json")
            .exists());
    }
}
