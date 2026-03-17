mod diagnostics;
mod model;
mod validate;

use std::fs;
use std::path::Path;

pub use diagnostics::{ValidationCode, ValidationError, ValidationErrors};
pub use model::{
    ActionImplementation, ActionSurface, ConnectorManifest, ConnectorMetadata, ConnectorProfiles,
    DefaultValue, DeterminismLevel, EffectLevel, EndpointProfile, FieldDecl, FieldKind,
    OutboundAuthProfile, PaginationDecl, PaginationKind, RequestMapping, RequestMethod,
    ReservedProfile, ReservedTriggerConfig, ResourceRequirement, ResponseDecl, ResponseKind,
    StaticHeaderDecl, SurfaceDecl, TypeDecl, WebhookTriggerSurface,
};
pub use validate::{
    generated_module_name, paginated_collection_field, validate_manifest,
    validate_manifest_for_codegen,
};

#[derive(Debug, thiserror::Error)]
pub enum ManifestLoadError {
    #[error("failed to read manifest: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse manifest yaml: {0}")]
    Yaml(#[from] serde_yaml::Error),
}

impl ConnectorManifest {
    pub fn from_yaml_str(text: &str) -> Result<Self, ManifestLoadError> {
        Ok(serde_yaml::from_str(text)?)
    }

    pub fn from_yaml_file(path: impl AsRef<Path>) -> Result<Self, ManifestLoadError> {
        let text = fs::read_to_string(path)?;
        Self::from_yaml_str(&text)
    }

    pub fn validate(&self) -> Result<(), ValidationErrors> {
        validate_manifest(self)
    }

    pub fn validate_for_codegen(&self) -> Result<(), ValidationErrors> {
        validate_manifest_for_codegen(self)
    }
}
