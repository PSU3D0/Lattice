use kernel_exec::{NodeRegistry, RegistryError};

pub fn register_all(registry: &mut NodeRegistry) -> Result<(), RegistryError> {
    crate::actions::google_gmail_send_message_register(registry)?;
    Ok(())
}
