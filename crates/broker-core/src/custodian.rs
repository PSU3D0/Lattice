use crate::BrokerError;
use std::{collections::BTreeSet, fmt, sync::Mutex};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectionMetadata {
    pub connection_ref: String,
    pub provider: String,
    pub account_subject: String,
    pub scopes: BTreeSet<String>,
    pub revocation_epoch: u64,
}

pub struct AccessMaterial<'a> {
    secret: &'a [u8],
}
impl AccessMaterial<'_> {
    pub(crate) fn expose_to_broker(&self) -> &[u8] {
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

struct State {
    secret: Vec<u8>,
    metadata: ConnectionMetadata,
    revoked: bool,
    refreshes: u64,
}
pub struct SyntheticCustodian {
    state: Mutex<State>,
}
impl SyntheticCustodian {
    pub fn new(
        connection_ref: impl Into<String>,
        provider: impl Into<String>,
        account_subject: impl Into<String>,
        scopes: impl IntoIterator<Item = String>,
        secret: Vec<u8>,
    ) -> Self {
        Self {
            state: Mutex::new(State {
                secret,
                metadata: ConnectionMetadata {
                    connection_ref: connection_ref.into(),
                    provider: provider.into(),
                    account_subject: account_subject.into(),
                    scopes: scopes.into_iter().collect(),
                    revocation_epoch: 0,
                },
                revoked: false,
                refreshes: 0,
            }),
        }
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
            c.with_access_material(|m| Ok(m.expose_to_broker().len()))
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
