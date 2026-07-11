//! Manifest honesty tests (connector verification harness, step 1): the
//! embedded `connector.yaml` parses/validates and every action surface agrees
//! with the op metadata the kernel trusts at plan/lock/preflight time.

use connector_http::generated::manifest::{CONNECTOR_ID, CONNECTOR_YAML};
use connector_http::ops::{
    HttpDelete, HttpDeleteAnyOrigin, HttpGet, HttpGetAnyOrigin, HttpHead, HttpHeadAnyOrigin,
    HttpPatch, HttpPatchAnyOrigin, HttpPost, HttpPostAnyOrigin, HttpPut, HttpPutAnyOrigin,
};
use connector_spec::{ConnectorManifest, ResourceRequirement, SurfaceDecl};
use dag_core::{ConnectorOpMetadata, ConnectorRoleKindDecl};

fn ops_metadata() -> [&'static ConnectorOpMetadata; 12] {
    [
        &HttpGet::META,
        &HttpHead::META,
        &HttpPost::META,
        &HttpPut::META,
        &HttpPatch::META,
        &HttpDelete::META,
        &HttpGetAnyOrigin::META,
        &HttpHeadAnyOrigin::META,
        &HttpPostAnyOrigin::META,
        &HttpPutAnyOrigin::META,
        &HttpPatchAnyOrigin::META,
        &HttpDeleteAnyOrigin::META,
    ]
}

fn parsed_manifest() -> ConnectorManifest {
    let manifest = ConnectorManifest::from_yaml_str(CONNECTOR_YAML).expect("manifest parses");
    manifest.validate().expect("manifest validates");
    manifest
}

#[test]
fn generated_manifest_embeds_source_yaml_and_validates() {
    assert_eq!(CONNECTOR_ID, "connector.http");
    assert!(CONNECTOR_YAML.contains(CONNECTOR_ID));

    let manifest = parsed_manifest();
    assert_eq!(manifest.connector.id, CONNECTOR_ID);
    assert_eq!(manifest.connector.crate_name, "connector_http");
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
        "every manifest action must have op metadata (and vice versa)"
    );

    for action in actions {
        let meta = ops
            .iter()
            .find(|meta| meta.operation_id == action.identifier)
            .unwrap_or_else(|| panic!("no op metadata for `{}`", action.identifier));

        assert_eq!(
            meta.min_effects,
            action.effects.as_dag_core(),
            "effects mismatch for `{}`",
            action.identifier
        );

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

        assert!(
            meta.roles.iter().any(|role| {
                role.kind == ConnectorRoleKindDecl::EndpointProfile && role.name == action.endpoint
            }),
            "missing endpoint role `{}` for `{}`",
            action.endpoint,
            action.identifier
        );

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
                "op `{}` claims auth roles its manifest surface does not declare (Tier-2 binds no auth)",
                action.identifier
            ),
        }
    }
}

#[test]
fn method_effect_metadata_is_honest_by_construction() {
    // GET/HEAD → ReadOnly + http_read; the rest → Effectful + http_write.
    for meta in [&HttpGet::META, &HttpHead::META, &HttpGetAnyOrigin::META] {
        assert_eq!(meta.min_effects, dag_core::Effects::ReadOnly);
        assert_eq!(meta.effect_hints, &[capabilities::http::HINT_HTTP_READ]);
    }
    for meta in [
        &HttpPost::META,
        &HttpPut::META,
        &HttpPatch::META,
        &HttpDelete::META,
        &HttpPostAnyOrigin::META,
    ] {
        assert_eq!(meta.min_effects, dag_core::Effects::Effectful);
        assert_eq!(meta.effect_hints, &[capabilities::http::HINT_HTTP_WRITE]);
    }

    // Tier-2 ops declare no outbound-auth role (auth × dynamic host forbidden).
    assert!(
        !HttpGetAnyOrigin::META
            .roles
            .iter()
            .any(|role| role.kind == ConnectorRoleKindDecl::OutboundAuth)
    );
    // Tier-0 auth role is OPTIONAL (spec §7, required: false).
    let auth = HttpGet::META
        .roles
        .iter()
        .find(|role| role.kind == ConnectorRoleKindDecl::OutboundAuth)
        .expect("tier-0 declares an optional auth role");
    assert!(!auth.required, "connector.http auth role must be optional");
}
