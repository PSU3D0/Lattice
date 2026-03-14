use kernel_exec::{NodeRegistry, RegistryError};

pub fn register_all(registry: &mut NodeRegistry) -> Result<(), RegistryError> {
    crate::generated::actions::github_issues_create_register(registry)?;
    crate::generated::actions::github_issues_get_register(registry)?;
    crate::generated::actions::github_issues_list_register(registry)?;
    Ok(())
}
