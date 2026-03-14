use capabilities::connector::ConnectorRuntimeError;
use capabilities::http::HttpRequest;

use crate::http::append_query_pair;

pub use capabilities::connector::{OutboundAuthKind, OutboundAuthProfileDescriptor};

pub(crate) fn apply_static_outbound_auth(
    request: &mut HttpRequest,
    profile: &OutboundAuthProfileDescriptor,
    secret: String,
) -> Result<(), ConnectorRuntimeError> {
    match profile.kind {
        OutboundAuthKind::Bearer { .. } => {
            request
                .headers
                .insert("Authorization", format!("Bearer {secret}"));
        }
        OutboundAuthKind::ApiKeyHeader {
            header_name,
            prefix,
            ..
        } => {
            let value = prefix
                .map(|prefix| format!("{prefix} {secret}"))
                .unwrap_or(secret);
            request.headers.insert(header_name, value);
        }
        OutboundAuthKind::ApiKeyQuery { query_name, .. } => {
            append_query_pair(&mut request.url, query_name, &secret);
        }
        OutboundAuthKind::Unsupported { kind_name, .. } => {
            return Err(ConnectorRuntimeError::UnsupportedAuthKind {
                role_name: profile.name,
                kind: kind_name,
            });
        }
    }

    Ok(())
}
