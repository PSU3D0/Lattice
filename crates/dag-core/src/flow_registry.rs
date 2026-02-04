//! Flow registry entries collected via `inventory`.
//!
//! Note: `inventory` relies on linker-based collection and is unsupported on
//! `wasm32` targets.

use crate::{FlowIR, Profile};

#[cfg(target_arch = "wasm32")]
compile_error!("flow-registry relies on inventory and is not supported on wasm32 targets");

#[derive(Debug, Clone, Copy)]
pub struct EntrypointSpec {
    pub trigger: &'static str,
    pub capture: &'static str,
    pub route_aliases: &'static [&'static str],
    pub method: Option<&'static str>,
    pub deadline_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub struct FlowRegistration {
    pub name: &'static str,
    pub version: &'static str,
    pub profile: Profile,
    pub entrypoints: &'static [EntrypointSpec],
    pub flow_ir: fn() -> FlowIR,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowRegistryError {
    MetadataMismatch {
        expected_name: &'static str,
        expected_version: &'static str,
        expected_profile: Profile,
        actual_name: String,
        actual_version: String,
        actual_profile: Profile,
    },
}

impl FlowRegistration {
    pub fn validate(&self) -> Result<(), FlowRegistryError> {
        let flow = (self.flow_ir)();
        let actual_version = flow.version.to_string();

        if self.name != flow.name || self.version != actual_version || self.profile != flow.profile
        {
            return Err(FlowRegistryError::MetadataMismatch {
                expected_name: self.name,
                expected_version: self.version,
                expected_profile: self.profile,
                actual_name: flow.name,
                actual_version,
                actual_profile: flow.profile,
            });
        }

        Ok(())
    }
}

inventory::collect!(FlowRegistration);

pub fn iter() -> impl Iterator<Item = &'static FlowRegistration> {
    inventory::iter::<FlowRegistration>.into_iter()
}

pub use inventory::submit;
