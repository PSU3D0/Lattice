#![allow(dead_code)]

use dag_core::{
    ConnectorOpMetadata, ConnectorResolutionContract, ConnectorResolutionModeDecl,
    ConnectorRoleKindDecl, ConnectorRoleRequirement, Determinism, Effects, NodeResult,
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
            },
            ConnectorRoleRequirement {
                kind: ConnectorRoleKindDecl::OutboundAuth,
                name: "demo_auth",
                expected_handle_kind: "http.bearer",
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
}
