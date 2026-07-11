use kernel_exec::{NodeRegistry, RegistryError};

pub fn register_all(registry: &mut NodeRegistry) -> Result<(), RegistryError> {
    crate::actions::llm_complete_register(registry)?;
    Ok(())
}
