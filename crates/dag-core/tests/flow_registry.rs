#![cfg(feature = "flow-registry")]

use dag_core::flow_registry;
use dag_core::{FlowIR, FlowId, FlowMetadata, FlowPolicies, Profile};
use semver::Version;

fn sample_flow_ir() -> FlowIR {
    let version = Version::parse("0.1.0").expect("parse version");
    FlowIR {
        id: FlowId::new("registry_test", &version),
        name: "registry_test".to_string(),
        version,
        profile: Profile::Dev,
        summary: None,
        nodes: Vec::new(),
        edges: Vec::new(),
        control_surfaces: Vec::new(),
        checkpoints: Vec::new(),
        policies: FlowPolicies::default(),
        metadata: FlowMetadata::default(),
        artifacts: Vec::new(),
    }
}

flow_registry::submit! {
    flow_registry::FlowRegistration {
        name: "registry_test",
        version: "0.1.0",
        profile: Profile::Dev,
        entrypoints: &[flow_registry::EntrypointSpec {
            trigger: "http",
            capture: "default",
            route_aliases: &["/registry-test"],
            method: Some("POST"),
            deadline_ms: Some(1_000),
        }],
        flow_ir: sample_flow_ir,
    }
}

#[test]
fn registry_iterates_submitted_flows() {
    let registrations: Vec<_> = flow_registry::iter().collect();

    assert!(
        registrations.iter().any(|registration| {
            registration.name == "registry_test" && registration.version == "0.1.0"
        }),
        "expected registry_test to be present"
    );
}

#[test]
fn registry_entrypoint_carries_method() {
    let registration = flow_registry::FlowRegistration {
        name: "method_check",
        version: "0.1.0",
        profile: Profile::Dev,
        entrypoints: &[flow_registry::EntrypointSpec {
            trigger: "http",
            capture: "default",
            route_aliases: &["/method-check"],
            method: Some("PUT"),
            deadline_ms: None,
        }],
        flow_ir: sample_flow_ir,
    };

    assert_eq!(
        registration.entrypoints[0].method,
        Some("PUT"),
        "expected entrypoint method to be preserved"
    );
}

#[test]
fn registry_validation_rejects_metadata_drift() {
    let registration = flow_registry::FlowRegistration {
        name: "mismatched",
        version: "9.9.9",
        profile: Profile::Web,
        entrypoints: &[flow_registry::EntrypointSpec {
            trigger: "http",
            capture: "default",
            route_aliases: &["/mismatch"],
            method: None,
            deadline_ms: None,
        }],
        flow_ir: sample_flow_ir,
    };

    let error = registration
        .validate()
        .expect_err("expected metadata drift to be rejected");

    assert!(
        matches!(
            error,
            flow_registry::FlowRegistryError::MetadataMismatch { .. }
        ),
        "expected metadata mismatch error"
    );
}
