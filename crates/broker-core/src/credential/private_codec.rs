use super::model::{ModelTag, PrivateModel};
use crate::BrokerError;
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Mutex};

macro_rules! private_type {
    ($name:ident, $tag:ident, $schema:literal) => {
        enum $tag {}
        impl ModelTag for $tag {
            const SCHEMA: &'static str = $schema;
        }
        pub struct $name(PrivateModel<$tag>);
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                PrivateModel::deserialize(deserializer).map(Self)
            }
        }
        impl $name {
            #[allow(dead_code)]
            pub(crate) fn expose<R>(&self, f: impl FnOnce(&Value) -> R) -> R {
                self.0.expose(f)
            }
        }
    };
}

private_type!(SecretEnvelopeV2, SecretEnvelopeTag, "SecretEnvelope");
private_type!(CredentialLeaseV2, CredentialLeaseTag, "CredentialLease");
private_type!(
    RemoteAuthorizeDispatchPrivateV2,
    RemoteRequestTag,
    "RemoteRequestPrivate"
);
private_type!(
    RemoteDispatchResultPrivateV2,
    RemoteResultTag,
    "RemoteResultPrivate"
);

/// Authenticated requests are constructible only by a trusted auth driver.
pub struct PrivateAuthenticatedRequest {
    bytes: Vec<u8>,
}

impl PrivateAuthenticatedRequest {
    pub(crate) fn new(bytes: Vec<u8>) -> Result<Self, BrokerError> {
        if bytes.is_empty() || bytes.len() > 1024 * 1024 {
            return Err(BrokerError::Brk305);
        }
        Ok(Self { bytes })
    }

    /// Exposes authenticated bytes only to the selected privileged transport.
    /// The closure keeps the request out of serializable planner and plugin
    /// types while making the transport SPI implementable outside this crate.
    pub fn with_transport_bytes<R>(&self, use_bytes: impl FnOnce(&[u8]) -> R) -> R {
        use_bytes(&self.bytes)
    }
}
impl Drop for PrivateAuthenticatedRequest {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.bytes.zeroize();
    }
}

pub trait ReplayState: Send + Sync {
    fn reserve(
        &self,
        sender: &str,
        request_ref: &str,
        nonce_jti: &str,
        request_hash: &str,
    ) -> Result<ReplayReservation, BrokerError>;
    fn record_terminal(
        &self,
        reservation: &ReplayReservation,
        encrypted_result: Vec<u8>,
    ) -> Result<(), BrokerError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayReservation {
    key: (String, String, String),
    request_hash: String,
    recorded_result: Option<Vec<u8>>,
}
impl ReplayReservation {
    pub fn recorded_result(&self) -> Option<&[u8]> {
        self.recorded_result.as_deref()
    }
}

#[derive(Default)]
pub struct InMemoryReplayState(Mutex<BTreeMap<(String, String, String), ReplayEntry>>);
struct ReplayEntry {
    request_hash: String,
    encrypted_result: Option<Vec<u8>>,
}

impl ReplayState for InMemoryReplayState {
    fn reserve(
        &self,
        sender: &str,
        request_ref: &str,
        nonce_jti: &str,
        request_hash: &str,
    ) -> Result<ReplayReservation, BrokerError> {
        let key = (
            sender.to_owned(),
            request_ref.to_owned(),
            nonce_jti.to_owned(),
        );
        let mut entries = self.0.lock().map_err(|_| BrokerError::Brk401)?;
        if let Some(entry) = entries.get(&key) {
            return if entry.request_hash == request_hash {
                Ok(ReplayReservation {
                    key,
                    request_hash: request_hash.to_owned(),
                    recorded_result: entry.encrypted_result.clone(),
                })
            } else {
                Err(BrokerError::Brk203)
            };
        }
        entries.insert(
            key.clone(),
            ReplayEntry {
                request_hash: request_hash.to_owned(),
                encrypted_result: None,
            },
        );
        Ok(ReplayReservation {
            key,
            request_hash: request_hash.to_owned(),
            recorded_result: None,
        })
    }
    fn record_terminal(
        &self,
        reservation: &ReplayReservation,
        encrypted_result: Vec<u8>,
    ) -> Result<(), BrokerError> {
        let mut entries = self.0.lock().map_err(|_| BrokerError::Brk401)?;
        let entry = entries
            .get_mut(&reservation.key)
            .ok_or(BrokerError::Brk204)?;
        if entry.request_hash != reservation.request_hash {
            return Err(BrokerError::Brk203);
        }
        if entry
            .encrypted_result
            .as_ref()
            .is_some_and(|old| old != &encrypted_result)
        {
            return Err(BrokerError::Brk203);
        }
        entry.encrypted_result = Some(encrypted_result);
        Ok(())
    }
}
