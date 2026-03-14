use capabilities::http::HttpHeaders;

#[derive(Debug, Clone, Copy)]
pub struct EndpointProfileDescriptor {
    pub name: &'static str,
    pub env_base_url_var: &'static str,
    pub base_url: &'static str,
    pub default_headers: &'static [(&'static str, &'static str)],
}

pub fn resolve_base_url(profile: &EndpointProfileDescriptor) -> String {
    #[cfg(not(target_arch = "wasm32"))]
    {
        if let Ok(override_url) = std::env::var(profile.env_base_url_var) {
            return override_url;
        }
    }

    profile.base_url.to_string()
}

pub fn apply_default_headers(headers: &mut HttpHeaders, profile: &EndpointProfileDescriptor) {
    for (name, value) in profile.default_headers {
        if headers.get(name).is_none() {
            headers.insert(*name, *value);
        }
    }
}
