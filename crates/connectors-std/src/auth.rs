use capabilities::http::HttpHeaders;

use crate::errors::ConnectorRuntimeError;
use crate::http::append_query_pair;

#[derive(Debug, Clone, Copy)]
pub struct OutboundAuthProfileDescriptor {
    pub name: &'static str,
    pub env_var: &'static str,
    pub kind: OutboundAuthKind,
}

#[derive(Debug, Clone, Copy)]
pub enum OutboundAuthKind {
    Bearer {
        handle_kind: &'static str,
    },
    ApiKeyHeader {
        header_name: &'static str,
        prefix: Option<&'static str>,
        handle_kind: &'static str,
    },
    ApiKeyQuery {
        query_name: &'static str,
        handle_kind: &'static str,
    },
    Unsupported {
        kind_name: &'static str,
        handle_kind: &'static str,
    },
}

impl OutboundAuthKind {
    pub const fn kind_name(self) -> &'static str {
        match self {
            OutboundAuthKind::Bearer { .. } => "bearer",
            OutboundAuthKind::ApiKeyHeader { .. } => "api_key_header",
            OutboundAuthKind::ApiKeyQuery { .. } => "api_key_query",
            OutboundAuthKind::Unsupported { kind_name, .. } => kind_name,
        }
    }

    pub const fn handle_kind(self) -> &'static str {
        match self {
            OutboundAuthKind::Bearer { handle_kind }
            | OutboundAuthKind::ApiKeyHeader { handle_kind, .. }
            | OutboundAuthKind::ApiKeyQuery { handle_kind, .. }
            | OutboundAuthKind::Unsupported { handle_kind, .. } => handle_kind,
        }
    }
}

pub fn apply_outbound_auth(
    url: &mut String,
    headers: &mut HttpHeaders,
    profile: &OutboundAuthProfileDescriptor,
) -> Result<(), ConnectorRuntimeError> {
    let secret = resolve_secret(profile)?;
    match profile.kind {
        OutboundAuthKind::Bearer { .. } => {
            headers.insert("Authorization", format!("Bearer {secret}"));
        }
        OutboundAuthKind::ApiKeyHeader {
            header_name,
            prefix,
            ..
        } => {
            let value = prefix
                .map(|prefix| format!("{prefix} {secret}"))
                .unwrap_or(secret);
            headers.insert(header_name, value);
        }
        OutboundAuthKind::ApiKeyQuery { query_name, .. } => {
            append_query_pair(url, query_name, &secret);
        }
        OutboundAuthKind::Unsupported { kind_name, .. } => {
            return Err(ConnectorRuntimeError::UnsupportedAuthKind {
                profile: profile.name,
                kind: kind_name,
            });
        }
    }
    Ok(())
}

fn resolve_secret(
    profile: &OutboundAuthProfileDescriptor,
) -> Result<String, ConnectorRuntimeError> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::env::var(profile.env_var).map_err(|_| ConnectorRuntimeError::MissingAuthOverride {
            profile: profile.name,
            env_var: profile.env_var,
        })
    }

    #[cfg(target_arch = "wasm32")]
    {
        let _ = profile;
        Err(ConnectorRuntimeError::MissingAuthOverride {
            profile: profile.name,
            env_var: profile.env_var,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn api_key_header_applies_prefix() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let profile = OutboundAuthProfileDescriptor {
            name: "demo",
            env_var: "LATTICE_CONNECTOR_AUTH_DEMO",
            kind: OutboundAuthKind::ApiKeyHeader {
                header_name: "X-Api-Key",
                prefix: Some("Token"),
                handle_kind: "raw.secret",
            },
        };
        let mut headers = HttpHeaders::default();
        let mut url = "https://example.test".to_string();
        unsafe {
            std::env::set_var(profile.env_var, "abc123");
        }

        apply_outbound_auth(&mut url, &mut headers, &profile).expect("auth applied");

        assert_eq!(headers.get("X-Api-Key"), Some(&"Token abc123".to_string()));
        unsafe {
            std::env::remove_var(profile.env_var);
        }
    }
}
