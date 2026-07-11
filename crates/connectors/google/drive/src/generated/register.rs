use kernel_exec::{NodeRegistry, RegistryError};

pub fn register_all(registry: &mut NodeRegistry) -> Result<(), RegistryError> {
    crate::actions::google_drive_search_files_register(registry)?;
    Ok(())
}
