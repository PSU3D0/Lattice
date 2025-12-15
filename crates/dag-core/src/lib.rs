//! Core types, diagnostics, and Flow IR structures for Lattice.

mod builder;
pub mod determinism;
mod diagnostics;
mod effects;
pub mod effects_registry;
mod ir;
pub mod schema;

pub use builder::{EdgeHandle, FlowBuilder, FlowBuilderError, NodeHandle};
pub use diagnostics::{DIAGNOSTIC_CODES, Diagnostic, DiagnosticCode, Severity, diagnostic_codes};
pub use effects::{Determinism, Effects, NodeError, NodeResult};
pub use ir::*;
pub use serde_json;

/// Convenient prelude re-exporting the most commonly used items.
pub mod prelude {
    pub use crate::builder::{FlowBuilder, FlowBuilderError};
    pub use crate::effects::{Determinism, Effects, NodeError, NodeResult};
    pub use crate::ir::{
        BufferPolicy, Delivery, FlowIR, FlowId, NodeId, NodeKind, NodeSpec, Profile, SchemaRef,
        SchemaSpec,
    };
    pub use semver::Version;
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use semver::Version;
    use serde_json::json;

    use super::*;

    #[test]
    fn diagnostics_registry_matches_doc() {
        let mut doc_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        doc_path.pop(); // crates/
        doc_path.push("../impl-docs/error-codes.md");
        let registry_doc = fs::read_to_string(doc_path).expect("read error-code registry doc");

        for code in diagnostic_codes() {
            assert!(
                registry_doc.contains(code.code),
                "diagnostic code `{}` missing from registry document",
                code.code
            );
        }
    }

    fn repo_root() -> PathBuf {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.pop(); // dag-core
        path.pop(); // crates
        path
    }

    fn walk_files(
        dir: &std::path::Path,
        filter: &dyn Fn(&std::path::Path) -> bool,
    ) -> Vec<PathBuf> {
        let mut out = Vec::new();
        for entry in fs::read_dir(dir).expect("read dir") {
            let entry = entry.expect("read entry");
            let path = entry.path();
            if path.is_dir() {
                out.extend(walk_files(&path, filter));
                continue;
            }
            if filter(&path) {
                out.push(path);
            }
        }
        out.sort();
        out
    }

    #[test]
    fn schemas_match_rust_emission() {
        let schema_dir = repo_root().join("schemas");
        let schema_files = walk_files(&schema_dir, &|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".schema.json"))
        });

        assert!(
            !schema_files.is_empty(),
            "no schema files found under schemas/"
        );

        for path in schema_files {
            let file_name = path
                .file_name()
                .and_then(|name| name.to_str())
                .expect("schema file name");

            let expected = crate::schema::schema_json_for_file(file_name)
                .unwrap_or_else(|| panic!("schema `{file_name}` is not covered by emitter"));

            let actual_text = fs::read_to_string(&path).expect("read schema");
            let actual: serde_json::Value =
                serde_json::from_str(&actual_text).expect("parse schema");

            assert_eq!(
                actual,
                expected,
                "schema drift detected for `{}`; re-run `cargo run -p dag-core --bin emit_schemas -- {}`",
                path.display(),
                file_name
            );
        }
    }

    #[test]
    fn schema_examples_deserialize_to_flow_ir() {
        let examples_dir = repo_root().join("schemas/examples");
        let example_files = walk_files(&examples_dir, &|path| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext == "json")
        });

        assert!(
            !example_files.is_empty(),
            "no example JSON files found under schemas/examples/"
        );

        for path in example_files {
            let content = fs::read_to_string(&path).expect("read example");
            let parsed: Result<FlowIR, _> = serde_json::from_str(&content);
            parsed.unwrap_or_else(|err| {
                panic!("example `{}` failed to deserialize: {err}", path.display())
            });
        }
    }

    #[test]
    fn flow_builder_constructs_serializable_ir() {
        let version = Version::parse("1.0.0").expect("parse version");
        let mut builder = FlowBuilder::new("echo_norm", version, Profile::Web);
        builder.summary(Some("Simple echo workflow"));

        let trigger_spec = NodeSpec {
            identifier: "examples::s1_echo::http_trigger",
            name: "HttpTrigger",
            kind: NodeKind::Trigger,
            summary: Some("Entry point for HTTP requests"),
            in_schema: SchemaSpec::Opaque,
            out_schema: SchemaSpec::Named("EchoRequest"),
            effects: Effects::ReadOnly,
            determinism: Determinism::Strict,
            determinism_hints: &[],
            effect_hints: &[],
        };

        let normalize_spec = NodeSpec::inline(
            "examples::s1_echo::normalize",
            "Normalize",
            SchemaSpec::Named("EchoRequest"),
            SchemaSpec::Named("EchoResponse"),
            Effects::Pure,
            Determinism::Strict,
            Some("Trim and lowercase payload"),
        );

        let respond_spec = NodeSpec::inline(
            "examples::s1_echo::respond",
            "Respond",
            SchemaSpec::Named("EchoResponse"),
            SchemaSpec::Named("HttpResponse"),
            Effects::Effectful,
            Determinism::BestEffort,
            Some("Write HTTP response"),
        );

        let trigger = builder
            .add_node("trigger", &trigger_spec)
            .expect("add trigger");
        let normalize = builder
            .add_node("normalize", &normalize_spec)
            .expect("add normalize");
        let respond = builder
            .add_node("respond", &respond_spec)
            .expect("add respond");
        builder.connect(&trigger, &normalize);
        builder.connect(&normalize, &respond);

        let flow = builder.build();

        assert_eq!(flow.nodes.len(), 3);
        assert_eq!(flow.edges.len(), 2);

        let json = serde_json::to_value(&flow).expect("serialize flow");
        assert_eq!(
            json["profile"],
            json!(Profile::Web),
            "profile should serialize correctly"
        );
    }
}
