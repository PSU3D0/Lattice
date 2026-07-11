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
        OutboundAuthKind::Basic { .. } => {
            // The `http.basic` secret handle stores `user:pass`; the wire form
            // is `Authorization: Basic base64(user:pass)` (spec §7, Q2). A
            // caller that already base64-encoded is out of contract.
            use base64::Engine as _;
            let encoded = base64::engine::general_purpose::STANDARD.encode(secret.as_bytes());
            request
                .headers
                .insert("Authorization", format!("Basic {encoded}"));
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
