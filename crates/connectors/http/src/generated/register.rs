use kernel_exec::{NodeRegistry, RegistryError};

pub fn register_all(registry: &mut NodeRegistry) -> Result<(), RegistryError> {
    // Tier 0.
    crate::actions::http_get_register(registry)?;
    crate::actions::http_head_register(registry)?;
    crate::actions::http_post_register(registry)?;
    crate::actions::http_put_register(registry)?;
    crate::actions::http_patch_register(registry)?;
    crate::actions::http_delete_register(registry)?;
    // Tier 2 (any-origin).
    crate::actions::http_get_any_origin_register(registry)?;
    crate::actions::http_head_any_origin_register(registry)?;
    crate::actions::http_post_any_origin_register(registry)?;
    crate::actions::http_put_any_origin_register(registry)?;
    crate::actions::http_patch_any_origin_register(registry)?;
    crate::actions::http_delete_any_origin_register(registry)?;
    // Byte-plane ops (spec §16.5, H5c-ops native): get_binary + multipart.
    crate::actions::http_get_binary_register(registry)?;
    crate::actions::http_post_multipart_register(registry)?;
    crate::actions::http_put_multipart_register(registry)?;
    Ok(())
}
