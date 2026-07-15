#![allow(dead_code)]

use dag_core::{
    ConnectorOpMetadata, ConnectorResolutionContract, ConnectorResolutionModeDecl,
    ConnectorRoleKindDecl, ConnectorRoleRequirement, Determinism, Effects,
    ImplementationDependency, NodeResult,
};
use dag_macros::{def_node, node};

struct DemoAppendRow;

impl DemoAppendRow {
    pub const META: ConnectorOpMetadata = ConnectorOpMetadata {
        operation_id: "connector.demo.append_row",
        connector_id: "connector.demo",
        summary: "Append a row to the demo connector",
        min_effects: Effects::Effectful,
        max_determinism: Determinism::BestEffort,
        determinism_hints: &[capabilities::http::HINT_HTTP],
        effect_hints: &[capabilities::http::HINT_HTTP_WRITE],
        roles: &[
            ConnectorRoleRequirement {
                kind: ConnectorRoleKindDecl::EndpointProfile,
                name: "demo_default",
                expected_handle_kind: "endpoint.profile",
                required: true,
            },
            ConnectorRoleRequirement {
                kind: ConnectorRoleKindDecl::OutboundAuth,
                name: "demo_auth",
                expected_handle_kind: "http.bearer",
                required: false,
            },
        ],
        resolution: ConnectorResolutionContract {
            supported_modes: &[ConnectorResolutionModeDecl::BoundConnection],
            default_mode: ConnectorResolutionModeDecl::BoundConnection,
        },
    };
}

#[def_node(
    name = "MaybeAppendRow",
    summary = "Conditionally append a row using a declared connector operation",
    connector_ops(DemoAppendRow)
)]
async fn maybe_append_row(_: ()) -> NodeResult<()> {
    Ok(())
}

#[def_node(
    name = "Composite",
    summary = "Invoke fixed implementations without spoofing their lookup identifiers",
    effects = "Pure",
    determinism = "Strict",
    implementation_dependencies(
        ImplementationDependency::StdDocumentExtractPdfText,
        ImplementationDependency::StdDocumentExtractPdfText
    )
)]
async fn composite(_: ()) -> NodeResult<()> {
    Ok(())
}

#[test]
fn def_node_preserves_implementation_dependencies_without_changing_identity() {
    let spec = node!(composite);
    assert!(spec.identifier.ends_with("::composite"));
    assert_ne!(spec.identifier, "std.document.extract_pdf_text");
    assert_eq!(
        spec.implementation_dependencies,
        &[
            ImplementationDependency::StdDocumentExtractPdfText,
            ImplementationDependency::StdDocumentExtractPdfText
        ]
    );
}

#[test]
fn def_node_connector_ops_auto_hoist_effects_and_hints() {
    let spec = node!(maybe_append_row);
    assert_eq!(spec.effects, Effects::Effectful);
    assert_eq!(spec.determinism, Determinism::BestEffort);
    assert!(
        spec.effect_hints
            .contains(&capabilities::http::HINT_HTTP_WRITE)
    );
    assert!(
        spec.determinism_hints
            .contains(&capabilities::http::HINT_HTTP)
    );
    assert_eq!(spec.connector_ops.len(), 1);
    assert_eq!(
        spec.connector_ops[0].operation_id,
        "connector.demo.append_row"
    );
    assert_eq!(spec.connector_ops[0].connector_id, "connector.demo");
    assert_eq!(
        spec.connector_ops[0].resolution.default_mode,
        ConnectorResolutionModeDecl::BoundConnection
    );
    assert_eq!(
        spec.connector_ops[0].resolution.supported_modes,
        &[ConnectorResolutionModeDecl::BoundConnection]
    );

    let refs = spec.connector_op_refs();
    assert_eq!(refs.len(), 1);
    assert_eq!(
        refs[0].default_resolution_mode,
        ConnectorResolutionModeDecl::BoundConnection
    );
    assert_eq!(
        refs[0].selected_resolution_mode,
        ConnectorResolutionModeDecl::BoundConnection
    );
    assert_eq!(
        refs[0].supported_resolution_modes,
        vec![ConnectorResolutionModeDecl::BoundConnection]
    );

    // Role optionality threads from the static declaration into the IR ref.
    assert_eq!(refs[0].roles.len(), 2);
    assert!(
        refs[0]
            .roles
            .iter()
            .any(|role| role.name == "demo_default" && role.required)
    );
    assert!(
        refs[0]
            .roles
            .iter()
            .any(|role| role.name == "demo_auth" && !role.required)
    );
}
