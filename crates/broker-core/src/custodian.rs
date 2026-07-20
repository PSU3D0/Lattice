use crate::{
    BrokerError,
    artifacts::{CommitmentAlg, CommitmentEnvelope},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Mutex,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectionMetadata {
    pub connection_ref: String,
    pub provider: String,
    pub account_commitment: CommitmentEnvelope,
    pub roles: BTreeMap<String, String>,
    pub scopes: BTreeSet<String>,
    pub revocation_epoch: u64,
}

pub struct AccessMaterial<'a> {
    secret: &'a [u8],
}
impl AccessMaterial<'_> {
    pub fn expose_to_dispatcher(&self) -> &[u8] {
        self.secret
    }
}
impl fmt::Debug for AccessMaterial<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AccessMaterial([REDACTED])")
    }
}

pub trait CredentialCustodian: Send + Sync {
    fn connection_metadata(&self) -> Result<ConnectionMetadata, BrokerError>;
    fn validate_scopes(&self, required: &BTreeSet<String>) -> Result<(), BrokerError>;
    fn with_access_material<T>(
        &self,
        use_material: impl FnOnce(AccessMaterial<'_>) -> Result<T, BrokerError>,
    ) -> Result<T, BrokerError>;
    fn refresh(&self) -> Result<(), BrokerError>;
    fn revoke(&self) -> Result<u64, BrokerError>;
}

/// Broker-owned immutable connection-ref lookup. The invoke request never
/// supplies a separate connection assertion or epoch.
pub trait CustodianLookup: CredentialCustodian {
    fn lookup(&self, connection_ref: &str) -> Result<&Self, BrokerError> {
        if self.connection_metadata()?.connection_ref == connection_ref {
            Ok(self)
        } else {
            Err(BrokerError::Brk109)
        }
    }
}
impl<T: CredentialCustodian> CustodianLookup for T {}

struct State {
    secret: Vec<u8>,
    metadata: ConnectionMetadata,
    revoked: bool,
    refreshes: u64,
}
impl Drop for State {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.secret.zeroize();
    }
}
pub struct SyntheticCustodian {
    state: Mutex<State>,
}
impl SyntheticCustodian {
    pub fn new(
        connection_ref: impl Into<String>,
        provider: impl Into<String>,
        _account_subject: impl Into<String>,
        scopes: impl IntoIterator<Item = String>,
        secret: Vec<u8>,
    ) -> Self {
        Self {
            state: Mutex::new(State {
                secret,
                metadata: ConnectionMetadata {
                    connection_ref: connection_ref.into(),
                    provider: provider.into(),
                    // Synthetic fixtures use the protocol's deterministic
                    // placeholder commitment. Production custodians return
                    // their persisted immutable account commitment here.
                    account_commitment: CommitmentEnvelope {
                        alg: CommitmentAlg::HmacSha256,
                        key_id: "account-key".into(),
                        verification_tier: None,
                        value: format!("hmac-sha256:{}", "0".repeat(64)),
                        extensions: Default::default(),
                    },
                    roles: BTreeMap::from([
                        ("role".into(), "synthetic.secret".into()),
                        ("outbound_auth.synthetic".into(), "synthetic.secret".into()),
                    ]),
                    scopes: scopes.into_iter().collect(),
                    revocation_epoch: 0,
                },
                revoked: false,
                refreshes: 0,
            }),
        }
    }
    pub fn replace_scopes(
        &self,
        scopes: impl IntoIterator<Item = String>,
    ) -> Result<(), BrokerError> {
        self.state
            .lock()
            .map_err(|_| BrokerError::Brk401)?
            .metadata
            .scopes = scopes.into_iter().collect();
        Ok(())
    }
    pub fn set_epoch(&self, epoch: u64) -> Result<(), BrokerError> {
        self.state
            .lock()
            .map_err(|_| BrokerError::Brk401)?
            .metadata
            .revocation_epoch = epoch;
        Ok(())
    }
    pub fn bump_epoch(&self) -> Result<u64, BrokerError> {
        let mut state = self.state.lock().map_err(|_| BrokerError::Brk401)?;
        state.metadata.revocation_epoch = state
            .metadata
            .revocation_epoch
            .checked_add(1)
            .ok_or(BrokerError::Brk401)?;
        Ok(state.metadata.revocation_epoch)
    }
    pub fn set_account_commitment(
        &self,
        commitment: CommitmentEnvelope,
    ) -> Result<(), BrokerError> {
        self.state
            .lock()
            .map_err(|_| BrokerError::Brk401)?
            .metadata
            .account_commitment = commitment;
        Ok(())
    }
    pub fn refresh_count(&self) -> u64 {
        self.state.lock().map(|s| s.refreshes).unwrap_or(0)
    }
}
impl fmt::Debug for SyntheticCustodian {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SyntheticCustodian { material: [REDACTED] }")
    }
}
impl CredentialCustodian for SyntheticCustodian {
    fn connection_metadata(&self) -> Result<ConnectionMetadata, BrokerError> {
        let state = self.state.lock().map_err(|_| BrokerError::Brk401)?;
        if state.revoked {
            return Err(BrokerError::Brk106);
        }
        Ok(state.metadata.clone())
    }
    fn validate_scopes(&self, required: &BTreeSet<String>) -> Result<(), BrokerError> {
        let state = self.state.lock().map_err(|_| BrokerError::Brk401)?;
        if state.revoked || !required.is_subset(&state.metadata.scopes) {
            return Err(BrokerError::Brk109);
        }
        Ok(())
    }
    fn with_access_material<T>(
        &self,
        use_material: impl FnOnce(AccessMaterial<'_>) -> Result<T, BrokerError>,
    ) -> Result<T, BrokerError> {
        let state = self.state.lock().map_err(|_| BrokerError::Brk401)?;
        if state.revoked {
            return Err(BrokerError::Brk106);
        }
        use_material(AccessMaterial {
            secret: &state.secret,
        })
    }
    fn refresh(&self) -> Result<(), BrokerError> {
        let mut state = self.state.lock().map_err(|_| BrokerError::Brk401)?;
        if state.revoked {
            return Err(BrokerError::Brk106);
        }
        state.refreshes += 1;
        Ok(())
    }
    fn revoke(&self) -> Result<u64, BrokerError> {
        let mut state = self.state.lock().map_err(|_| BrokerError::Brk401)?;
        state.revoked = true;
        state.metadata.revocation_epoch = state
            .metadata
            .revocation_epoch
            .checked_add(1)
            .ok_or(BrokerError::Brk401)?;
        state.secret.fill(0);
        Ok(state.metadata.revocation_epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn material_is_borrowed_and_redacted() {
        let c = SyntheticCustodian::new("c", "p", "a", [], b"synthetic-secret".to_vec());
        assert_eq!(
            c.with_access_material(|m| Ok(m.expose_to_dispatcher().len()))
                .unwrap(),
            16
        );
        assert!(!format!("{c:?}").contains("synthetic-secret"));
        c.revoke().unwrap();
        assert_eq!(
            c.with_access_material(|_| Ok(())).unwrap_err(),
            BrokerError::Brk106
        );
    }
}
