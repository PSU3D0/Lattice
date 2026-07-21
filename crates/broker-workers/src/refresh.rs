use crate::protocol::REFRESH_LEASE_SECONDS;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, fmt};
use zeroize::Zeroize;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RefreshError {
    #[error("connection is revoked")]
    Revoked,
    #[error("refresh state conflict")]
    Conflict,
    #[error("refresh state is unavailable")]
    Unavailable,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SecretBytes(Vec<u8>);

impl SecretBytes {
    pub fn new(value: impl Into<Vec<u8>>) -> Self {
        Self(value.into())
    }

    pub fn expose_to_internal_binding(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for SecretBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretBytes([REDACTED])")
    }
}

impl Drop for SecretBytes {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ConnectionTokenState {
    pub org_id: String,
    pub connection_ref: String,
    pub account_commitment: String,
    pub granted_scopes: BTreeSet<String>,
    pub effective_scopes: BTreeSet<String>,
    refresh_token: Option<SecretBytes>,
    access_token: Option<SecretBytes>,
    access_expires_at: Option<i64>,
    pub revocation_epoch: u64,
    pub revoked: bool,
    lease: Option<RefreshLease>,
}

impl ConnectionTokenState {
    pub fn matches_registration(&self, registration: &ConnectionRegistration) -> bool {
        use subtle::ConstantTimeEq;
        let refresh_matches = self
            .refresh_token
            .as_ref()
            .is_some_and(|stored| bool::from(stored.0.ct_eq(&registration.refresh_token.0)));
        let access_matches = match (&self.access_token, &registration.access_token) {
            (Some(stored), Some(candidate)) => bool::from(stored.0.ct_eq(&candidate.0)),
            (None, None) => true,
            _ => false,
        };
        !self.revoked
            && self.org_id == registration.org_id
            && self.connection_ref == registration.connection_ref
            && self.account_commitment == registration.account_commitment
            && self.granted_scopes == registration.granted_scopes
            && self.revocation_epoch == registration.revocation_epoch
            && self.access_expires_at == registration.access_expires_at
            && refresh_matches
            && access_matches
    }
}

impl fmt::Debug for ConnectionTokenState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionTokenState")
            .field("org_id", &self.org_id)
            .field("connection_ref", &self.connection_ref)
            .field("account_commitment", &self.account_commitment)
            .field("granted_scopes", &self.granted_scopes)
            .field("effective_scopes", &self.effective_scopes)
            .field("refresh_token", &"[REDACTED]")
            .field("access_token", &"[REDACTED]")
            .field("access_expires_at", &self.access_expires_at)
            .field("revocation_epoch", &self.revocation_epoch)
            .field("revoked", &self.revoked)
            .field("lease", &self.lease)
            .finish()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RefreshLease {
    lease_id: String,
    expected_epoch: u64,
    expires_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectionRegistration {
    pub org_id: String,
    pub connection_ref: String,
    pub account_commitment: String,
    pub granted_scopes: BTreeSet<String>,
    pub refresh_token: SecretBytes,
    pub access_token: Option<SecretBytes>,
    pub access_expires_at: Option<i64>,
    pub revocation_epoch: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AcquireResult {
    Ready {
        access_token: SecretBytes,
        revocation_epoch: u64,
    },
    Refresh {
        lease_id: String,
        expected_epoch: u64,
        refresh_token: SecretBytes,
    },
    Waiting {
        retry_after_ms: u64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RefreshResult {
    Success {
        access_token: SecretBytes,
        expires_in: u64,
        scopes: BTreeSet<String>,
    },
    InvalidGrant,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CompleteResult {
    Ready { revocation_epoch: u64 },
    Blocked { revocation_epoch: u64 },
    Retryable,
}

impl ConnectionTokenState {
    pub fn register(registration: ConnectionRegistration) -> Result<Self, RefreshError> {
        if registration.org_id.is_empty()
            || registration.connection_ref.is_empty()
            || registration.account_commitment.is_empty()
            || registration.granted_scopes.is_empty()
            || registration
                .refresh_token
                .expose_to_internal_binding()
                .is_empty()
            || registration.access_token.is_some() != registration.access_expires_at.is_some()
        {
            return Err(RefreshError::Unavailable);
        }
        Ok(Self {
            org_id: registration.org_id,
            connection_ref: registration.connection_ref,
            account_commitment: registration.account_commitment,
            effective_scopes: registration.granted_scopes.clone(),
            granted_scopes: registration.granted_scopes,
            refresh_token: Some(registration.refresh_token),
            access_token: registration.access_token,
            access_expires_at: registration.access_expires_at,
            revocation_epoch: registration.revocation_epoch,
            revoked: false,
            lease: None,
        })
    }

    /// Lease phase only. External token I/O must happen after this returns and
    /// before `complete_refresh`; no storage transaction spans that interval.
    pub fn acquire(
        &mut self,
        now: i64,
        lease_id: impl Into<String>,
    ) -> Result<AcquireResult, RefreshError> {
        if self.revoked {
            return Err(RefreshError::Revoked);
        }
        if self
            .access_expires_at
            .is_some_and(|expires_at| expires_at > now)
        {
            return Ok(AcquireResult::Ready {
                access_token: self.access_token.clone().ok_or(RefreshError::Unavailable)?,
                revocation_epoch: self.revocation_epoch,
            });
        }
        if self
            .lease
            .as_ref()
            .is_some_and(|lease| lease.expires_at > now)
        {
            return Ok(AcquireResult::Waiting { retry_after_ms: 10 });
        }
        let lease_id = lease_id.into();
        if lease_id.len() < 16 || lease_id.len() > 128 || !lease_id.is_ascii() {
            return Err(RefreshError::Unavailable);
        }
        let refresh_token = self.refresh_token.clone().ok_or(RefreshError::Revoked)?;
        let lease = RefreshLease {
            lease_id: lease_id.clone(),
            expected_epoch: self.revocation_epoch,
            expires_at: now
                .checked_add(REFRESH_LEASE_SECONDS)
                .ok_or(RefreshError::Unavailable)?,
        };
        self.lease = Some(lease.clone());
        Ok(AcquireResult::Refresh {
            lease_id,
            expected_epoch: lease.expected_epoch,
            refresh_token,
        })
    }

    pub fn complete_refresh(
        &mut self,
        lease_id: &str,
        expected_epoch: u64,
        now: i64,
        result: RefreshResult,
    ) -> Result<CompleteResult, RefreshError> {
        if self.revoked {
            return Err(RefreshError::Revoked);
        }
        let lease = self.lease.as_ref().ok_or(RefreshError::Conflict)?;
        if lease.lease_id != lease_id
            || lease.expected_epoch != expected_epoch
            || self.revocation_epoch != expected_epoch
            || lease.expires_at <= now
        {
            return Err(RefreshError::Conflict);
        }
        match result {
            RefreshResult::Success {
                access_token,
                expires_in,
                scopes,
            } => {
                if self.granted_scopes != scopes {
                    self.block_and_bump(scopes)?;
                    return Ok(CompleteResult::Blocked {
                        revocation_epoch: self.revocation_epoch,
                    });
                }
                let expires_in =
                    i64::try_from(expires_in).map_err(|_| RefreshError::Unavailable)?;
                self.access_token = Some(access_token);
                self.access_expires_at = Some(
                    now.checked_add(expires_in)
                        .ok_or(RefreshError::Unavailable)?,
                );
                self.effective_scopes = scopes;
                self.lease = None;
                Ok(CompleteResult::Ready {
                    revocation_epoch: self.revocation_epoch,
                })
            }
            RefreshResult::InvalidGrant => {
                self.block_and_bump(self.effective_scopes.clone())?;
                Ok(CompleteResult::Blocked {
                    revocation_epoch: self.revocation_epoch,
                })
            }
            RefreshResult::Unavailable => {
                self.lease = None;
                Ok(CompleteResult::Retryable)
            }
        }
    }

    pub fn revoke(&mut self) -> Result<u64, RefreshError> {
        if !self.revoked {
            self.block_and_bump(self.effective_scopes.clone())?;
        }
        Ok(self.revocation_epoch)
    }

    fn block_and_bump(&mut self, effective_scopes: BTreeSet<String>) -> Result<(), RefreshError> {
        self.revocation_epoch = self
            .revocation_epoch
            .checked_add(1)
            .ok_or(RefreshError::Unavailable)?;
        self.revoked = true;
        self.effective_scopes = effective_scopes;
        self.refresh_token = None;
        self.access_token = None;
        self.access_expires_at = None;
        self.lease = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    fn scopes() -> BTreeSet<String> {
        BTreeSet::from(["gmail.send".into(), "sheets.write".into()])
    }

    fn state() -> ConnectionTokenState {
        ConnectionTokenState::register(ConnectionRegistration {
            org_id: "org-1".into(),
            connection_ref: "connection-1".into(),
            account_commitment: "account-1".into(),
            granted_scopes: scopes(),
            refresh_token: SecretBytes::new(b"refresh-never-log".to_vec()),
            access_token: Some(SecretBytes::new(b"expired-never-log".to_vec())),
            access_expires_at: Some(0),
            revocation_epoch: 4,
        })
        .unwrap()
    }

    #[test]
    fn concurrent_expiry_issues_exactly_one_refresh_lease() {
        let state = Arc::new(Mutex::new(state()));
        let mut threads = Vec::new();
        for index in 0..16 {
            let state = state.clone();
            threads.push(std::thread::spawn(move || {
                state
                    .lock()
                    .unwrap()
                    .acquire(100, format!("lease-{index:032}"))
                    .unwrap()
            }));
        }
        let results = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, AcquireResult::Refresh { .. }))
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, AcquireResult::Waiting { .. }))
                .count(),
            15
        );
    }

    #[test]
    fn peers_observe_rotation_and_scope_shrink_blocks_with_epoch_bump() {
        let mut state = state();
        let acquired = state
            .acquire(100, "lease-00000000000000000000000001")
            .unwrap();
        let (lease_id, epoch) = match acquired {
            AcquireResult::Refresh {
                lease_id,
                expected_epoch,
                ..
            } => (lease_id, expected_epoch),
            _ => unreachable!(),
        };
        state
            .complete_refresh(
                &lease_id,
                epoch,
                101,
                RefreshResult::Success {
                    access_token: SecretBytes::new(b"rotated-never-log".to_vec()),
                    expires_in: 3600,
                    scopes: scopes(),
                },
            )
            .unwrap();
        assert!(matches!(
            state.acquire(102, "unused-0000000000000000").unwrap(),
            AcquireResult::Ready {
                revocation_epoch: 4,
                ..
            }
        ));

        state.access_expires_at = Some(0);
        let acquired = state
            .acquire(200, "lease-00000000000000000000000002")
            .unwrap();
        let (lease_id, epoch) = match acquired {
            AcquireResult::Refresh {
                lease_id,
                expected_epoch,
                ..
            } => (lease_id, expected_epoch),
            _ => unreachable!(),
        };
        let result = state
            .complete_refresh(
                &lease_id,
                epoch,
                201,
                RefreshResult::Success {
                    access_token: SecretBytes::new(b"must-be-wiped".to_vec()),
                    expires_in: 3600,
                    scopes: BTreeSet::from(["gmail.send".into()]),
                },
            )
            .unwrap();
        assert_eq!(
            result,
            CompleteResult::Blocked {
                revocation_epoch: 5
            }
        );
        assert_eq!(
            state.acquire(202, "unused-0000000000000000").unwrap_err(),
            RefreshError::Revoked
        );
        let public = format!("{state:?}");
        for secret in [
            "refresh-never-log",
            "expired-never-log",
            "rotated-never-log",
        ] {
            assert!(!public.contains(secret));
        }
    }

    #[test]
    fn stale_slow_refresh_completion_cannot_overwrite_newer_lease() {
        let mut state = state();
        let old = state
            .acquire(100, "lease-old-00000000000000000000001")
            .unwrap();
        let (old_id, epoch) = match old {
            AcquireResult::Refresh {
                lease_id,
                expected_epoch,
                ..
            } => (lease_id, expected_epoch),
            _ => unreachable!(),
        };
        let newer = state
            .acquire(
                100 + crate::protocol::REFRESH_LEASE_SECONDS,
                "lease-new-00000000000000000000001",
            )
            .unwrap();
        let (new_id, new_epoch) = match newer {
            AcquireResult::Refresh {
                lease_id,
                expected_epoch,
                ..
            } => (lease_id, expected_epoch),
            _ => unreachable!(),
        };
        assert_eq!(
            state
                .complete_refresh(
                    &old_id,
                    epoch,
                    100 + crate::protocol::REFRESH_LEASE_SECONDS,
                    RefreshResult::Success {
                        access_token: SecretBytes::new(b"stale-token".to_vec()),
                        expires_in: 3600,
                        scopes: scopes(),
                    },
                )
                .unwrap_err(),
            RefreshError::Conflict
        );
        assert_eq!(
            state
                .complete_refresh(
                    &new_id,
                    new_epoch,
                    100 + crate::protocol::REFRESH_LEASE_SECONDS + 1,
                    RefreshResult::Success {
                        access_token: SecretBytes::new(b"accepted-token".to_vec()),
                        expires_in: 3600,
                        scopes: scopes(),
                    },
                )
                .unwrap(),
            CompleteResult::Ready {
                revocation_epoch: epoch
            }
        );
    }

    #[test]
    fn provider_added_scope_is_rejected_and_wiped() {
        let mut state = state();
        let acquired = state
            .acquire(100, "lease-added-000000000000000000000001")
            .unwrap();
        let lease = match acquired {
            AcquireResult::Refresh { expected_epoch, .. } => expected_epoch,
            _ => unreachable!(),
        };
        let mut expanded = scopes();
        expanded.insert("openid".into());
        let result = state
            .complete_refresh(
                "lease-added-000000000000000000000001",
                lease,
                101,
                RefreshResult::Success {
                    access_token: SecretBytes::new(b"must-be-wiped".to_vec()),
                    expires_in: 3600,
                    scopes: expanded,
                },
            )
            .unwrap();
        assert!(matches!(result, CompleteResult::Blocked { .. }));
        assert!(state.revoked);
        assert!(state.refresh_token.is_none());
        assert!(state.access_token.is_none());
    }

    #[test]
    fn invalid_grant_and_revoke_bump_epoch_and_wipe() {
        let mut state = state();
        let acquired = state
            .acquire(100, "lease-00000000000000000000000001")
            .unwrap();
        let (lease_id, epoch) = match acquired {
            AcquireResult::Refresh {
                lease_id,
                expected_epoch,
                ..
            } => (lease_id, expected_epoch),
            _ => unreachable!(),
        };
        assert_eq!(
            state
                .complete_refresh(&lease_id, epoch, 101, RefreshResult::InvalidGrant)
                .unwrap(),
            CompleteResult::Blocked {
                revocation_epoch: 5
            }
        );
        assert_eq!(state.revoke().unwrap(), 5);
    }
}
