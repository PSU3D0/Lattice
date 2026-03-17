use kernel_exec::{NodeRegistry, RegistryError};

pub fn register_all(registry: &mut NodeRegistry) -> Result<(), RegistryError> {
    crate::actions::google_sheets_append_row_register(registry)?;
    crate::actions::google_sheets_find_rows_register(registry)?;
    crate::actions::google_sheets_upsert_row_register(registry)?;
    Ok(())
}
