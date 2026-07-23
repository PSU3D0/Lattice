use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const MAX_MANAGEMENT_BODY: usize = 16 * 1024;
pub const MAX_INVOKE_BODY: usize = 128 * 1024;
pub const MAX_PROVIDER_RESPONSE: usize = 64 * 1024;
pub const SESSION_TTL_SECONDS: i64 = 300;
pub const OAUTH_STATE_TTL_SECONDS: i64 = 600;
pub const REFRESH_LEASE_SECONDS: i64 = 15;
pub const INVOCATION_LEASE_SECONDS: i64 = 30;
pub const POP_CLOCK_SKEW_SECONDS: i64 = 60;
pub const SESSION_EXCHANGE_AUDIENCE: &str = "lattice-broker-session";
pub const BROKER_REQUEST_AUDIENCE: &str = "lattice-broker";

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionExchangeRequest {
    pub deployment_key: String,
    /// Base64url (unpadded) Ed25519 public key (exactly 32 decoded bytes).
    pub client_public_key: String,
    pub client_nonce: String,
    pub timestamp: i64,
    pub audience: String,
    /// Base64url (unpadded) Ed25519 signature over the canonical exchange transcript.
    pub signature: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct SessionExchangeResponse {
    pub session_ref: String,
    pub expires_at: i64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionIntentRequest {
    pub connector_ref: String,
    pub auth_profile_ref: String,
    pub execution_lane: String,
    pub custody: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ConnectionIntentResponse {
    pub intent_ref: String,
    pub next_action: NextAction,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NextAction {
    OpenUrl { url: String },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallBindingRequest {
    pub connection_ref: String,
    pub deployment_id: String,
    pub bundle_id: String,
    pub flow_ir_hash: String,
    pub binding_lock_hash: String,
    pub flow_id: String,
    /// Exact canonical Flow IR JSON bytes carried as a JSON string.
    pub flow_ir_json: String,
    /// Exact canonical FlowAuthorityManifest bytes carried as a JSON string.
    pub authority_manifest_json: String,
    pub contracts: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvokeRequest {
    pub session_ref: String,
    pub grant_ref: String,
    pub bundle_id: String,
    pub flow_ir_hash: String,
    pub binding_lock_hash: String,
    pub flow_id: String,
    pub node_id: String,
    pub node_alias: String,
    pub run_id: String,
    pub activation_ordinal: u64,
    pub logical_effect_id: String,
    pub input: Value,
}

#[derive(Clone, Debug, Serialize)]
pub struct InvokeResponse {
    pub receipt_ref: String,
    pub receipt: Value,
    pub redelivery: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct PublicError {
    pub error: PublicErrorBody,
}

#[derive(Clone, Debug, Serialize)]
pub struct PublicErrorBody {
    pub code: &'static str,
    pub message: &'static str,
}

impl PublicError {
    pub fn broker(error: broker_core::BrokerError) -> Self {
        Self {
            error: PublicErrorBody {
                code: error.code(),
                message: "broker request rejected",
            },
        }
    }

    pub fn invalid() -> Self {
        Self::broker(broker_core::BrokerError::Brk001)
    }

    pub fn unavailable() -> Self {
        Self::broker(broker_core::BrokerError::Brk401)
    }
}
