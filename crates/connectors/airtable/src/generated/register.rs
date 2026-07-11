use kernel_exec::{NodeRegistry, RegistryError};

pub fn register_all(registry: &mut NodeRegistry) -> Result<(), RegistryError> {
    crate::actions::airtable_create_record_register(registry)?;
    Ok(())
}
