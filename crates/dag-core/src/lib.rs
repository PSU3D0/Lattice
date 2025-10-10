//! Core types, diagnostics, and Flow IR structures for LatticeFlow.

mod builder;
mod diagnostics;
mod effects;
mod ir;

pub use builder::{EdgeHandle, FlowBuilder, FlowBuilderError, NodeHandle};
pub use diagnostics::{DIAGNOSTIC_CODES, Diagnostic, DiagnosticCode, Severity, diagnostic_codes};
pub use effects::{Determinism, Effects, NodeError, NodeResult};
pub use ir::*;

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
