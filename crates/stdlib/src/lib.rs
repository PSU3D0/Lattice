#[cfg(feature = "callback")]
pub mod callback;

#[cfg(feature = "timer")]
pub mod timer;

#[cfg(feature = "document")]
pub mod document;

#[cfg(feature = "workspace")]
pub mod workspace;

use kernel_exec::{NodeRegistry, RegistryError};

pub fn register_all(registry: &mut NodeRegistry) -> Result<(), RegistryError> {
    let _ = registry;

    #[cfg(all(feature = "timer", feature = "host-bundle"))]
    timer::timer_wait_register(registry)?;

    #[cfg(all(feature = "callback", feature = "host-bundle"))]
    callback::callback_wait_register(registry)?;

    #[cfg(all(feature = "workspace", feature = "host-bundle"))]
    workspace::register_all(registry)?;

    #[cfg(all(feature = "document", feature = "host-bundle"))]
    document::extract_pdf_text_register(registry)?;

    Ok(())
}
