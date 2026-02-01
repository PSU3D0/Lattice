#[cfg(feature = "callback")]
pub mod callback;

#[cfg(feature = "timer")]
pub mod timer;

use kernel_exec::{NodeRegistry, RegistryError};

pub fn register_all(registry: &mut NodeRegistry) -> Result<(), RegistryError> {
    #[cfg(feature = "timer")]
    timer::timer_wait_register(registry)?;

    #[cfg(feature = "callback")]
    callback::callback_wait_register(registry)?;

    Ok(())
}
