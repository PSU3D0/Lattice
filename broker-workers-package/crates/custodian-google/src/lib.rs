#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use broker_core::{
    BrokerError,
    artifacts::CommitmentEnvelope,
    custodian::{AccessMaterial, ConnectionMetadata, CredentialCustodian},
    grant::Clock,
};
use chacha20poly1305::{
    ChaCha20Poly1305, KeyInit,
    aead::{Aead, Payload},
};
use zeroize::{Zeroize, Zeroizing};

const PROVIDER: &str = "google";
const REFRESH_AAD: &[u8] = b"lattice.google.refresh-token.v1";
const ACCESS_AAD: &[u8] = b"lattice.google.access-token.v1";

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GoogleCustodianError {
    #[error("google connection is unavailable")]
    ConnectionUnavailable,
    #[error("google connection is revoked")]
    Revoked,
    #[error("google token grant is invalid")]
    InvalidGrant,
    #[error("google token scopes no longer cover the connection grant")]
    ScopeShrunk,
    #[error("google token endpoint is unavailable")]
    TokenEndpointUnavailable,
    #[error("google credential sealing failed")]
    SealingFailed,
    #[error("google custodian state is unavailable")]
    StateUnavailable,
}

impl GoogleCustodianError {
    fn broker(self) -> BrokerError {
        match self {
            Self::Revoked | Self::InvalidGrant => BrokerError::Brk106,
            Self::ScopeShrunk | Self::ConnectionUnavailable => BrokerError::Brk109,
            Self::TokenEndpointUnavailable | Self::SealingFailed | Self::StateUnavailable => {
                BrokerError::Brk401
            }
        }
    }
}

/// Deployment root key. It is never persisted with connection records.
pub struct RootKey([u8; 32]);

impl RootKey {
    pub fn new(key: [u8; 32]) -> Self {
        Self(key)
    }
}

impl fmt::Debug for RootKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RootKey([REDACTED])")
    }
}

impl Drop for RootKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

#[derive(Clone)]
pub struct SecretBytes(Vec<u8>);

impl SecretBytes {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    fn expose(&self) -> &[u8] {
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

#[derive(Clone)]
pub struct SealedSecret {
    nonce: [u8; 12],
    ciphertext: Vec<u8>,
}

impl fmt::Debug for SealedSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SealedSecret([REDACTED])")
    }
}

impl Drop for SealedSecret {
    fn drop(&mut self) {
        self.nonce.zeroize();
        self.ciphertext.zeroize();
    }
}

#[derive(Clone, Debug)]
pub struct StoredGoogleConnection {
    pub connection_ref: String,
    pub org_id: String,
    pub provider: String,
    pub account_commitment: CommitmentEnvelope,
    pub roles: BTreeMap<String, String>,
    pub granted_scopes: BTreeSet<String>,
    pub effective_scopes: BTreeSet<String>,
    pub sealed_refresh_token: Option<SealedSecret>,
    pub sealed_access_token: Option<SealedSecret>,
    pub access_token_expires_at: Option<i64>,
    pub revocation_epoch: u64,
    pub revoked: bool,
}

pub trait ConnectionStore: Send + Sync {
    fn load(
        &self,
        connection_ref: &str,
    ) -> Result<Option<StoredGoogleConnection>, GoogleCustodianError>;
    fn save(&self, connection: StoredGoogleConnection) -> Result<(), GoogleCustodianError>;
}

#[derive(Default)]
pub struct InMemoryConnectionStore {
    connections: Mutex<BTreeMap<String, StoredGoogleConnection>>,
}

impl InMemoryConnectionStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl fmt::Debug for InMemoryConnectionStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("InMemoryConnectionStore { credentials: [REDACTED] }")
    }
}

impl ConnectionStore for InMemoryConnectionStore {
    fn load(
        &self,
        connection_ref: &str,
    ) -> Result<Option<StoredGoogleConnection>, GoogleCustodianError> {
        self.connections
            .lock()
            .map_err(|_| GoogleCustodianError::StateUnavailable)
            .map(|connections| connections.get(connection_ref).cloned())
    }

    fn save(&self, connection: StoredGoogleConnection) -> Result<(), GoogleCustodianError> {
        self.connections
            .lock()
            .map_err(|_| GoogleCustodianError::StateUnavailable)?
            .insert(connection.connection_ref.clone(), connection);
        Ok(())
    }
}

pub struct ConnectionRegistration {
    pub connection_ref: String,
    pub org_id: String,
    pub account_commitment: CommitmentEnvelope,
    pub roles: BTreeMap<String, String>,
    pub granted_scopes: BTreeSet<String>,
    pub refresh_token: SecretBytes,
    pub cached_access_token: Option<CachedAccessToken>,
    pub revocation_epoch: u64,
}

impl fmt::Debug for ConnectionRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionRegistration")
            .field("connection_ref", &self.connection_ref)
            .field("org_id", &self.org_id)
            .field("account_commitment", &self.account_commitment)
            .field("roles", &self.roles)
            .field("granted_scopes", &self.granted_scopes)
            .field("refresh_token", &"[REDACTED]")
            .field("cached_access_token", &"[REDACTED]")
            .field("revocation_epoch", &self.revocation_epoch)
            .finish()
    }
}

pub struct CachedAccessToken {
    pub token: SecretBytes,
    pub expires_at: i64,
    pub scopes: BTreeSet<String>,
}

impl fmt::Debug for CachedAccessToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedAccessToken")
            .field("token", &"[REDACTED]")
            .field("expires_at", &self.expires_at)
            .field("scopes", &self.scopes)
            .finish()
    }
}

pub struct RefreshTokenRequest<'a> {
    pub grant_type: &'static str,
    refresh_token: &'a [u8],
}

impl RefreshTokenRequest<'_> {
    pub fn refresh_token(&self) -> &[u8] {
        self.refresh_token
    }
}

impl fmt::Debug for RefreshTokenRequest<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RefreshTokenRequest")
            .field("grant_type", &self.grant_type)
            .field("refresh_token", &"[REDACTED]")
            .finish()
    }
}

pub struct TokenResponse {
    pub access_token: SecretBytes,
    pub expires_in: u64,
    pub scopes: BTreeSet<String>,
}

impl fmt::Debug for TokenResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenResponse")
            .field("access_token", &"[REDACTED]")
            .field("expires_in", &self.expires_in)
            .field("scopes", &self.scopes)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TokenEndpointError {
    #[error("token grant is invalid")]
    InvalidGrant,
    #[error("token endpoint is unavailable")]
    Unavailable,
}

pub trait TokenEndpoint: Send + Sync {
    fn refresh(
        &self,
        request: RefreshTokenRequest<'_>,
    ) -> Result<TokenResponse, TokenEndpointError>;
}

pub enum MockTokenResult {
    Response(TokenResponse),
    InvalidGrant,
    Unavailable,
}

impl fmt::Debug for MockTokenResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Response(_) => f.write_str("Response([REDACTED])"),
            Self::InvalidGrant => f.write_str("InvalidGrant"),
            Self::Unavailable => f.write_str("Unavailable"),
        }
    }
}

pub struct MockTokenEndpoint {
    results: Mutex<VecDeque<MockTokenResult>>,
    calls: AtomicUsize,
}

impl MockTokenEndpoint {
    pub fn new(results: impl IntoIterator<Item = MockTokenResult>) -> Self {
        Self {
            results: Mutex::new(results.into_iter().collect()),
            calls: AtomicUsize::new(0),
        }
    }

    pub fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl fmt::Debug for MockTokenEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MockTokenEndpoint { scripts: [REDACTED] }")
    }
}

impl TokenEndpoint for MockTokenEndpoint {
    fn refresh(
        &self,
        request: RefreshTokenRequest<'_>,
    ) -> Result<TokenResponse, TokenEndpointError> {
        let _ = request.refresh_token();
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self
            .results
            .lock()
            .map_err(|_| TokenEndpointError::Unavailable)?
            .pop_front()
            .unwrap_or(MockTokenResult::Unavailable)
        {
            MockTokenResult::Response(response) => Ok(response),
            MockTokenResult::InvalidGrant => Err(TokenEndpointError::InvalidGrant),
            MockTokenResult::Unavailable => Err(TokenEndpointError::Unavailable),
        }
    }
}

pub struct GoogleCustodian<S, E, C> {
    store: Arc<S>,
    endpoint: Arc<E>,
    clock: C,
    root_key: RootKey,
    connection_ref: String,
    refresh_lock: Mutex<()>,
}

impl<S, E, C> fmt::Debug for GoogleCustodian<S, E, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GoogleCustodian")
            .field("connection_ref", &self.connection_ref)
            .field("material", &"[REDACTED]")
            .finish()
    }
}

impl<S: ConnectionStore, E: TokenEndpoint, C: Clock> GoogleCustodian<S, E, C> {
    pub fn register(
        store: Arc<S>,
        endpoint: Arc<E>,
        clock: C,
        root_key: RootKey,
        registration: ConnectionRegistration,
    ) -> Result<Self, GoogleCustodianError> {
        if registration.connection_ref.is_empty()
            || registration.org_id.is_empty()
            || registration.granted_scopes.is_empty()
            || registration.roles.is_empty()
        {
            return Err(GoogleCustodianError::StateUnavailable);
        }
        let connection_ref = registration.connection_ref.clone();
        let sealed_refresh_token = seal(
            &root_key,
            &connection_ref,
            REFRESH_AAD,
            registration.refresh_token.expose(),
        )?;
        let (sealed_access_token, access_token_expires_at, effective_scopes) =
            match registration.cached_access_token {
                Some(cached) => {
                    if !registration.granted_scopes.is_subset(&cached.scopes) {
                        return Err(GoogleCustodianError::ScopeShrunk);
                    }
                    (
                        Some(seal(
                            &root_key,
                            &connection_ref,
                            ACCESS_AAD,
                            cached.token.expose(),
                        )?),
                        Some(cached.expires_at),
                        cached.scopes,
                    )
                }
                None => (None, None, registration.granted_scopes.clone()),
            };
        store.save(StoredGoogleConnection {
            connection_ref: connection_ref.clone(),
            org_id: registration.org_id,
            provider: PROVIDER.into(),
            account_commitment: registration.account_commitment,
            roles: registration.roles,
            granted_scopes: registration.granted_scopes,
            effective_scopes,
            sealed_refresh_token: Some(sealed_refresh_token),
            sealed_access_token,
            access_token_expires_at,
            revocation_epoch: registration.revocation_epoch,
            revoked: false,
        })?;
        Ok(Self {
            store,
            endpoint,
            clock,
            root_key,
            connection_ref,
            refresh_lock: Mutex::new(()),
        })
    }

    pub fn refresh_access_token(&self) -> Result<(), GoogleCustodianError> {
        let _guard = self
            .refresh_lock
            .lock()
            .map_err(|_| GoogleCustodianError::StateUnavailable)?;
        self.refresh_locked()
    }

    pub fn current_epoch(&self) -> Result<u64, GoogleCustodianError> {
        Ok(self.load()?.revocation_epoch)
    }

    pub fn org_id(&self) -> Result<String, GoogleCustodianError> {
        Ok(self.load()?.org_id)
    }

    fn load(&self) -> Result<StoredGoogleConnection, GoogleCustodianError> {
        self.store
            .load(&self.connection_ref)?
            .ok_or(GoogleCustodianError::ConnectionUnavailable)
    }

    fn bump_and_wipe(
        &self,
        connection: &mut StoredGoogleConnection,
        effective_scopes: Option<BTreeSet<String>>,
    ) -> Result<(), GoogleCustodianError> {
        connection.revocation_epoch = connection
            .revocation_epoch
            .checked_add(1)
            .ok_or(GoogleCustodianError::StateUnavailable)?;
        connection.revoked = true;
        if let Some(scopes) = effective_scopes {
            connection.effective_scopes = scopes;
        }
        connection.sealed_refresh_token = None;
        connection.sealed_access_token = None;
        connection.access_token_expires_at = None;
        self.store.save(connection.clone())
    }

    fn refresh_locked(&self) -> Result<(), GoogleCustodianError> {
        let mut connection = self.load()?;
        if connection.revoked {
            return Err(GoogleCustodianError::Revoked);
        }
        let sealed = connection
            .sealed_refresh_token
            .as_ref()
            .ok_or(GoogleCustodianError::Revoked)?;
        let refresh = open(
            &self.root_key,
            &connection.connection_ref,
            REFRESH_AAD,
            sealed,
        )?;
        let response = match self.endpoint.refresh(RefreshTokenRequest {
            grant_type: "refresh_token",
            refresh_token: &refresh,
        }) {
            Ok(response) => response,
            Err(TokenEndpointError::InvalidGrant) => {
                self.bump_and_wipe(&mut connection, None)?;
                return Err(GoogleCustodianError::InvalidGrant);
            }
            Err(TokenEndpointError::Unavailable) => {
                return Err(GoogleCustodianError::TokenEndpointUnavailable);
            }
        };
        if !connection.granted_scopes.is_subset(&response.scopes) {
            self.bump_and_wipe(&mut connection, Some(response.scopes))?;
            return Err(GoogleCustodianError::ScopeShrunk);
        }
        let expires_in = i64::try_from(response.expires_in)
            .map_err(|_| GoogleCustodianError::StateUnavailable)?;
        let expires_at = self
            .clock
            .monotonic_seconds()
            .checked_add(expires_in)
            .ok_or(GoogleCustodianError::StateUnavailable)?;
        connection.sealed_access_token = Some(seal(
            &self.root_key,
            &connection.connection_ref,
            ACCESS_AAD,
            response.access_token.expose(),
        )?);
        connection.access_token_expires_at = Some(expires_at);
        connection.effective_scopes = response.scopes;
        self.store.save(connection)
    }

    fn ensure_access(&self) -> Result<StoredGoogleConnection, GoogleCustodianError> {
        let connection = self.load()?;
        if connection.revoked {
            return Err(GoogleCustodianError::Revoked);
        }
        let fresh = connection.sealed_access_token.is_some()
            && connection
                .access_token_expires_at
                .is_some_and(|expiry| expiry > self.clock.monotonic_seconds());
        if fresh {
            return Ok(connection);
        }
        self.refresh_access_token()?;
        self.load()
    }
}

impl<S: ConnectionStore, E: TokenEndpoint, C: Clock> CredentialCustodian
    for GoogleCustodian<S, E, C>
{
    fn connection_metadata(&self) -> Result<ConnectionMetadata, BrokerError> {
        let current = self.load().map_err(GoogleCustodianError::broker)?;
        let connection = if current.revoked {
            current
        } else {
            // Refresh before admission reconciliation, never after the broker
            // has crossed its durable provider-dispatch boundary.
            self.ensure_access().map_err(GoogleCustodianError::broker)?
        };
        Ok(ConnectionMetadata {
            connection_ref: connection.connection_ref,
            provider: connection.provider,
            account_commitment: connection.account_commitment,
            roles: connection.roles,
            scopes: connection.effective_scopes,
            revocation_epoch: connection.revocation_epoch,
        })
    }

    fn validate_scopes(&self, required: &BTreeSet<String>) -> Result<(), BrokerError> {
        let connection = self.load().map_err(GoogleCustodianError::broker)?;
        if connection.revoked || !required.is_subset(&connection.effective_scopes) {
            return Err(BrokerError::Brk109);
        }
        Ok(())
    }

    fn with_access_material<T>(
        &self,
        use_material: impl FnOnce(AccessMaterial<'_>) -> Result<T, BrokerError>,
    ) -> Result<T, BrokerError> {
        let connection = self.ensure_access().map_err(GoogleCustodianError::broker)?;
        let sealed = connection
            .sealed_access_token
            .as_ref()
            .ok_or(BrokerError::Brk106)?;
        let plaintext = open(
            &self.root_key,
            &connection.connection_ref,
            ACCESS_AAD,
            sealed,
        )
        .map_err(GoogleCustodianError::broker)?;

        use_material(AccessMaterial::borrow_for_custodian(&plaintext))
    }

    fn refresh(&self) -> Result<(), BrokerError> {
        self.refresh_access_token()
            .map_err(GoogleCustodianError::broker)
    }

    fn revoke(&self) -> Result<u64, BrokerError> {
        let mut connection = self.load().map_err(GoogleCustodianError::broker)?;
        self.bump_and_wipe(&mut connection, None)
            .map_err(GoogleCustodianError::broker)?;
        Ok(connection.revocation_epoch)
    }
}

fn aad(connection_ref: &str, purpose: &[u8]) -> Zeroizing<Vec<u8>> {
    let mut value = Zeroizing::new(Vec::with_capacity(purpose.len() + 1 + connection_ref.len()));
    value.extend_from_slice(purpose);
    value.push(0);
    value.extend_from_slice(connection_ref.as_bytes());
    value
}

fn seal(
    root_key: &RootKey,
    connection_ref: &str,
    purpose: &[u8],
    plaintext: &[u8],
) -> Result<SealedSecret, GoogleCustodianError> {
    let mut nonce = [0_u8; 12];
    getrandom::fill(&mut nonce).map_err(|_| GoogleCustodianError::SealingFailed)?;
    let cipher = ChaCha20Poly1305::new((&root_key.0).into());
    let ciphertext = cipher
        .encrypt(
            (&nonce).into(),
            Payload {
                msg: plaintext,
                aad: &aad(connection_ref, purpose),
            },
        )
        .map_err(|_| GoogleCustodianError::SealingFailed)?;
    Ok(SealedSecret { nonce, ciphertext })
}

fn open(
    root_key: &RootKey,
    connection_ref: &str,
    purpose: &[u8],
    sealed: &SealedSecret,
) -> Result<Zeroizing<Vec<u8>>, GoogleCustodianError> {
    let cipher = ChaCha20Poly1305::new((&root_key.0).into());
    cipher
        .decrypt(
            (&sealed.nonce).into(),
            Payload {
                msg: &sealed.ciphertext,
                aad: &aad(connection_ref, purpose),
            },
        )
        .map(Zeroizing::new)
        .map_err(|_| GoogleCustodianError::SealingFailed)
}
