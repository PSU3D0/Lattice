//! Manifest honesty tests (connector verification harness, step 1).
//!
//! The embedded `connector.yaml` is the connector's public claim surface; the
//! generated `ops::*::META` constants are what the kernel actually trusts at
//! plan/preflight time. These tests prove the two never drift: the manifest
//! parses and validates under `connector-spec`, and every action surface
//! matches its generated op metadata (identifier, effects floor, declared
//! resources -> effect hints, auth/endpoint roles).

use connector_github_issues::generated::manifest::{CONNECTOR_ID, CONNECTOR_YAML};
use connector_github_issues::ops::{GithubIssuesCreate, GithubIssuesGet, GithubIssuesList};
use connector_spec::{ConnectorManifest, ResourceRequirement, SurfaceDecl};
use dag_core::{ConnectorOpMetadata, ConnectorRoleKindDecl};

fn ops_metadata() -> [&'static ConnectorOpMetadata; 3] {
    [
        &GithubIssuesGet::META,
        &GithubIssuesList::META,
        &GithubIssuesCreate::META,
    ]
}

fn parsed_manifest() -> ConnectorManifest {
    let manifest = ConnectorManifest::from_yaml_str(CONNECTOR_YAML).expect("manifest parses");
    manifest.validate().expect("manifest validates");
    manifest
}

#[test]
fn generated_manifest_embeds_source_yaml_and_validates() {
    assert_eq!(CONNECTOR_ID, "connector.github.issues");
    assert!(CONNECTOR_YAML.contains(CONNECTOR_ID));

    let manifest = parsed_manifest();
    assert_eq!(manifest.connector.id, CONNECTOR_ID);
    assert_eq!(manifest.connector.crate_name, "connector_github_issues");
}

#[test]
fn every_action_surface_matches_generated_op_metadata() {
    let manifest = parsed_manifest();
    let ops = ops_metadata();

    let actions: Vec<_> = manifest
        .surfaces
        .iter()
        .filter_map(|surface| match surface {
            SurfaceDecl::Action(action) => Some(action),
            _ => None,
        })
        .collect();
    assert_eq!(
        actions.len(),
        ops.len(),
        "every manifest action must have generated op metadata (and vice versa)"
    );

    for action in actions {
        let meta = ops
            .iter()
            .find(|meta| meta.operation_id == action.identifier)
            .unwrap_or_else(|| panic!("no generated op metadata for `{}`", action.identifier));

        // Effects floor: the manifest claim and the kernel-visible claim agree.
        assert_eq!(
            meta.min_effects,
            action.effects.as_dag_core(),
            "effects mismatch for `{}`",
            action.identifier
        );

        // Declared resources map 1:1 onto the op's effect hints.
        let expected_hints: Vec<&str> = action
            .resources
            .iter()
            .map(|resource| match resource {
                ResourceRequirement::HttpRead => capabilities::http::HINT_HTTP_READ,
                ResourceRequirement::HttpWrite => capabilities::http::HINT_HTTP_WRITE,
            })
            .collect();
        assert_eq!(
            meta.effect_hints,
            &expected_hints[..],
            "effect hint mismatch for `{}`",
            action.identifier
        );

        // Endpoint role is always required.
        assert!(
            meta.roles.iter().any(|role| {
                role.kind == ConnectorRoleKindDecl::EndpointProfile && role.name == action.endpoint
            }),
            "missing endpoint role `{}` for `{}`",
            action.endpoint,
            action.identifier
        );

        // Auth role is required exactly when the surface declares auth.
        let auth_roles: Vec<_> = meta
            .roles
            .iter()
            .filter(|role| role.kind == ConnectorRoleKindDecl::OutboundAuth)
            .collect();
        match &action.auth {
            Some(auth) => {
                assert!(
                    auth_roles.iter().any(|role| role.name == auth),
                    "missing outbound auth role `{auth}` for `{}`",
                    action.identifier
                );
            }
            None => assert!(
                auth_roles.is_empty(),
                "op `{}` claims auth roles its manifest surface does not declare",
                action.identifier
            ),
        }
    }
}
