use capabilities::http::HttpHeaders;

pub use capabilities::connector::{EndpointProfileDescriptor, ResolvedEndpointProfile};

pub fn apply_default_headers(headers: &mut HttpHeaders, profile: &ResolvedEndpointProfile) {
    for (name, value) in &profile.default_headers {
        if headers.get(name).is_none() {
            headers.insert(name.clone(), value.clone());
        }
    }
}
