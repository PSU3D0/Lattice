use std::sync::Arc;

use async_trait::async_trait;
use capabilities::ResourceBag;
use capabilities::connector::{
    ConnectorBindingScope, ConnectorRuntime, ConnectorRuntimeError, EndpointProfileDescriptor,
    OutboundAuthProfileDescriptor, ResolvedConnectorConnection, ResolvedEndpointProfile,
};
use capabilities::context;
use connector_formualizer_sheetport::actions::sheetport_evaluate;
use connector_formualizer_sheetport::errors::SheetPortConnectorError;
use connector_formualizer_sheetport::ops::SheetPortEvaluate;
use connector_formualizer_sheetport::runtime::{
    EffectiveSheetPortModel, merge_eval_policy, resolve_current_connection, resolve_effective_model,
};
use connector_formualizer_sheetport::types::{
    ManifestSourceRef, SheetPortConnectionConfig, SheetPortEvalDefaults, SheetPortEvalOverride,
    SheetPortEvaluateInput, SheetPortInputPayload, SheetPortModelSelector, WorkbookSourceRef,
};
use serde_json::json;

#[derive(Clone)]
struct MockRuntime {
    connection: Option<ResolvedConnectorConnection>,
}

#[async_trait]
impl ConnectorRuntime for MockRuntime {
    async fn apply_outbound_auth(
        &self,
        _scope: &ConnectorBindingScope,
        _profile: &OutboundAuthProfileDescriptor,
        _request: &mut capabilities::http::HttpRequest,
    ) -> Result<(), ConnectorRuntimeError> {
        unreachable!("auth is not used in these tests")
    }

    async fn resolve_endpoint_profile(
        &self,
        _scope: &ConnectorBindingScope,
        _profile: &EndpointProfileDescriptor,
    ) -> Result<ResolvedEndpointProfile, ConnectorRuntimeError> {
        unreachable!("endpoint resolution is not used in these tests")
    }

    async fn resolve_connection(
        &self,
        _scope: &ConnectorBindingScope,
    ) -> Result<Option<ResolvedConnectorConnection>, ConnectorRuntimeError> {
        Ok(self.connection.clone())
    }
}

fn connector_scope() -> ConnectorBindingScope {
    ConnectorBindingScope::new(
        "flow.sheetport.tests",
        "sheetport_node",
        "tests::sheetport_node",
        connector_formualizer_sheetport::CONNECTOR_FAMILY,
    )
}

fn sample_connection_config_value() -> serde_json::Value {
    json!({
        "workbook_source": {
            "kind": "blob",
            "key": "models/quote.xlsx"
        },
        "manifest_source": {
            "kind": "inline_yaml",
            "value": "spec: fio\nspec_version: \"0.3.0\"\nmanifest:\n  id: quote-model\n  name: Quote Model\nports: []\n"
        },
        "eval_defaults": {
            "freeze_volatile": true,
            "rng_seed": 7
        },
        "artifact_policy": {
            "allow_workspace_export": true
        }
    })
}

fn late_bound_input() -> SheetPortEvaluateInput {
    SheetPortEvaluateInput {
        model: Some(SheetPortModelSelector::LateBoundSources {
            workbook_source: WorkbookSourceRef::Blob {
                key: "models/runtime.xlsx".to_string(),
            },
            manifest_source: ManifestSourceRef::InlineYaml {
                value: "spec: fio\nspec_version: \"0.3.0\"\nmanifest:\n  id: runtime-model\n  name: Runtime Model\nports: []\n"
                    .to_string(),
            },
            eval_defaults: Some(SheetPortEvalDefaults {
                freeze_volatile: Some(false),
                rng_seed: Some(11),
            }),
        }),
        inputs: SheetPortInputPayload::default(),
        eval: None,
        emit_debug_artifacts: false,
    }
}

#[tokio::test]
async fn resolve_current_connection_deserializes_bound_sheetport_config() {
    let bag = ResourceBag::new()
        .with_connector_runtime(Arc::new(MockRuntime {
            connection: Some(ResolvedConnectorConnection {
                connection_name: Some("pricing_model_v7".to_string()),
                connector_id: connector_formualizer_sheetport::CONNECTOR_FAMILY.to_string(),
                config: sample_connection_config_value(),
            }),
        }))
        .with_connector_scope(connector_scope());

    let resolved = context::with_resources(Arc::new(bag), async {
        resolve_current_connection(connector_formualizer_sheetport::SHEETPORT_EVALUATE_IDENTIFIER)
            .await
    })
    .await
    .expect("bound connection resolves");

    assert_eq!(
        resolved.connection_name.as_deref(),
        Some("pricing_model_v7")
    );
    assert_eq!(
        resolved.config,
        SheetPortConnectionConfig {
            workbook_source: WorkbookSourceRef::Blob {
                key: "models/quote.xlsx".to_string(),
            },
            manifest_source: ManifestSourceRef::InlineYaml {
                value: "spec: fio\nspec_version: \"0.3.0\"\nmanifest:\n  id: quote-model\n  name: Quote Model\nports: []\n"
                    .to_string(),
            },
            eval_defaults: Some(SheetPortEvalDefaults {
                freeze_volatile: Some(true),
                rng_seed: Some(7),
            }),
            artifact_policy: Some(
                connector_formualizer_sheetport::types::SheetPortArtifactPolicy {
                    allow_workspace_export: true,
                },
            ),
        }
    );
}

#[tokio::test]
async fn resolve_current_connection_rejects_invalid_bound_config() {
    let bag = ResourceBag::new()
        .with_connector_runtime(Arc::new(MockRuntime {
            connection: Some(ResolvedConnectorConnection {
                connection_name: Some("broken_model".to_string()),
                connector_id: connector_formualizer_sheetport::CONNECTOR_FAMILY.to_string(),
                config: json!({
                    "manifest_source": {
                        "kind": "inline_yaml",
                        "value": "spec: fio\n..."
                    }
                }),
            }),
        }))
        .with_connector_scope(connector_scope());

    let err = context::with_resources(Arc::new(bag), async {
        resolve_current_connection(connector_formualizer_sheetport::SHEETPORT_EVALUATE_IDENTIFIER)
            .await
            .expect_err("invalid config should fail")
    })
    .await;

    match err {
        SheetPortConnectorError::InvalidConnectionConfig { reason } => {
            assert!(
                reason.contains("workbook_source") || reason.contains("missing field"),
                "{reason}"
            );
        }
        other => panic!("expected InvalidConnectionConfig, got {other:?}"),
    }
}

#[tokio::test]
async fn late_bound_mode_bypasses_connector_runtime() {
    let resolved = resolve_effective_model(
        connector_formualizer_sheetport::SHEETPORT_EVALUATE_IDENTIFIER,
        late_bound_input().model.as_ref(),
    )
    .await
    .expect("late-bound model resolves without connector runtime");

    match resolved {
        EffectiveSheetPortModel::LateBound {
            workbook_source,
            manifest_source,
            ..
        } => {
            assert!(matches!(workbook_source, WorkbookSourceRef::Blob { .. }));
            assert!(matches!(
                manifest_source,
                ManifestSourceRef::InlineYaml { .. }
            ));
        }
        other => panic!("expected LateBound model, got {other:?}"),
    }
}

#[tokio::test]
async fn bound_mode_internal_op_and_canonical_node_share_the_same_resolution_path() {
    let bag = ResourceBag::new()
        .with_connector_runtime(Arc::new(MockRuntime {
            connection: Some(ResolvedConnectorConnection {
                connection_name: Some("pricing_model_v7".to_string()),
                connector_id: connector_formualizer_sheetport::CONNECTOR_FAMILY.to_string(),
                config: sample_connection_config_value(),
            }),
        }))
        .with_connector_scope(connector_scope());

    let input = SheetPortEvaluateInput {
        model: None,
        inputs: SheetPortInputPayload::default(),
        eval: Some(SheetPortEvalOverride {
            freeze_volatile: Some(false),
            rng_seed: Some(99),
        }),
        emit_debug_artifacts: false,
    };

    context::with_resources(Arc::new(bag), async {
        let internal_err = SheetPortEvaluate::invoke(&input)
            .await
            .expect_err("missing blob capability should fail consistently");
        match internal_err {
            SheetPortConnectorError::MissingBlobCapability => {}
            other => panic!("expected MissingBlobCapability, got {other:?}"),
        }

        let node_err = sheetport_evaluate(input)
            .await
            .expect_err("canonical node should delegate to same op");
        let message = node_err.to_string();
        assert!(
            message.contains("connector.formualizer.sheetport.evaluate failed"),
            "{message}"
        );
        assert!(message.contains("missing blob capability"), "{message}");
    })
    .await;
}

#[test]
fn merge_eval_policy_prefers_invocation_override_over_defaults() {
    let merged = merge_eval_policy(
        Some(&SheetPortEvalDefaults {
            freeze_volatile: Some(true),
            rng_seed: Some(7),
        }),
        Some(&SheetPortEvalOverride {
            freeze_volatile: Some(false),
            rng_seed: Some(99),
        }),
    );

    assert_eq!(merged.freeze_volatile, Some(false));
    assert_eq!(merged.rng_seed, Some(99));
}
