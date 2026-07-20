use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use broker_core::{
    BrokerError,
    artifacts::{CommitmentAlg, CommitmentEnvelope},
    custodian::CredentialCustodian,
    grant::FixedClock,
};
use custodian_google::{
    CachedAccessToken, ConnectionRegistration, ConnectionStore, GoogleCustodian,
    GoogleCustodianError, InMemoryConnectionStore, MockTokenEndpoint, MockTokenResult, RootKey,
    SecretBytes, TokenResponse,
};

const REFRESH: &str = "refresh-token-never-log";
const STALE: &str = "stale-access-never-log";
const FRESH: &str = "fresh-access-never-log";
const SHEETS: &str = "https://www.googleapis.com/auth/spreadsheets";
const GMAIL: &str = "https://www.googleapis.com/auth/gmail.send";

fn account() -> CommitmentEnvelope {
    CommitmentEnvelope {
        alg: CommitmentAlg::HmacSha256,
        key_id: "account-key".into(),
        verification_tier: None,
        value: format!("hmac-sha256:{}", "0".repeat(64)),
        extensions: Default::default(),
    }
}

fn registration(cached_expiry: Option<i64>) -> ConnectionRegistration {
    let scopes = BTreeSet::from([GMAIL.into(), SHEETS.into()]);
    ConnectionRegistration {
        connection_ref: "google-primary".into(),
        org_id: "org".into(),
        account_commitment: account(),
        roles: BTreeMap::from([(
            "outbound_auth.google_workspace_auth".into(),
            "oauth2.access_token".into(),
        )]),
        granted_scopes: scopes.clone(),
        refresh_token: SecretBytes::new(REFRESH.as_bytes().to_vec()),
        cached_access_token: cached_expiry.map(|expires_at| CachedAccessToken {
            token: SecretBytes::new(STALE.as_bytes().to_vec()),
            expires_at,
            scopes,
        }),
        revocation_epoch: 4,
    }
}

fn response(scopes: BTreeSet<String>) -> MockTokenResult {
    MockTokenResult::Response(TokenResponse {
        access_token: SecretBytes::new(FRESH.as_bytes().to_vec()),
        expires_in: 3600,
        scopes,
    })
}

fn custodian(
    endpoint: Arc<MockTokenEndpoint>,
    cached_expiry: Option<i64>,
) -> (
    GoogleCustodian<InMemoryConnectionStore, MockTokenEndpoint, FixedClock>,
    Arc<InMemoryConnectionStore>,
) {
    let store = Arc::new(InMemoryConnectionStore::new());
    let custodian = GoogleCustodian::register(
        store.clone(),
        endpoint,
        FixedClock("2026-07-19T12:00:00Z".into()),
        RootKey::new([7; 32]),
        registration(cached_expiry),
    )
    .unwrap();
    (custodian, store)
}

#[test]
fn cached_material_stays_sealed_and_is_handed_out_only_in_scope() {
    let endpoint = Arc::new(MockTokenEndpoint::new([]));
    let (custodian, store) = custodian(endpoint.clone(), Some(i64::MAX));
    let record = store.load("google-primary").unwrap().unwrap();
    assert_ne!(
        format!("{:?}", record.sealed_refresh_token.as_ref().unwrap()),
        REFRESH
    );
    assert_ne!(
        format!("{:?}", record.sealed_access_token.as_ref().unwrap()),
        STALE
    );
    let observed = custodian
        .with_access_material(|material| {
            Ok(String::from_utf8(material.expose_to_dispatcher().to_vec()).unwrap())
        })
        .unwrap();
    assert_eq!(observed, STALE);
    assert_eq!(endpoint.call_count(), 0);

    let public = format!("{custodian:?} {store:?} {record:?}");
    for secret in [REFRESH, STALE, FRESH] {
        assert!(!public.contains(secret));
    }
}

#[test]
fn expired_access_refreshes_once_before_material_handoff() {
    let scopes = BTreeSet::from([GMAIL.into(), SHEETS.into()]);
    let endpoint = Arc::new(MockTokenEndpoint::new([response(scopes)]));
    let (custodian, _) = custodian(endpoint.clone(), Some(0));
    let observed = custodian
        .with_access_material(|material| {
            Ok(String::from_utf8(material.expose_to_dispatcher().to_vec()).unwrap())
        })
        .unwrap();
    assert_eq!(observed, FRESH);
    assert_eq!(endpoint.call_count(), 1);
    assert_eq!(custodian.connection_metadata().unwrap().revocation_epoch, 4);
}

#[test]
fn shrunk_scope_is_typed_bumps_epoch_and_fails_closed() {
    let endpoint = Arc::new(MockTokenEndpoint::new([response(BTreeSet::from([
        GMAIL.into()
    ]))]));
    let (custodian, store) = custodian(endpoint, Some(0));
    assert_eq!(
        custodian.refresh_access_token().unwrap_err(),
        GoogleCustodianError::ScopeShrunk
    );
    assert_eq!(custodian.current_epoch().unwrap(), 5);
    let metadata = custodian.connection_metadata().unwrap();
    assert_eq!(metadata.revocation_epoch, 5);
    assert_eq!(metadata.scopes, BTreeSet::from([GMAIL.into()]));
    assert_eq!(
        custodian.with_access_material(|_| Ok(())).unwrap_err(),
        BrokerError::Brk106
    );
    let record = store.load("google-primary").unwrap().unwrap();
    assert!(record.revoked);
    assert!(record.sealed_refresh_token.is_none());
    assert!(record.sealed_access_token.is_none());
}

#[test]
fn invalid_grant_is_typed_bumps_epoch_and_never_exposes_material() {
    let endpoint = Arc::new(MockTokenEndpoint::new([MockTokenResult::InvalidGrant]));
    let (custodian, _) = custodian(endpoint.clone(), Some(0));
    let error = custodian.refresh_access_token().unwrap_err();
    assert_eq!(error, GoogleCustodianError::InvalidGrant);
    assert_eq!(endpoint.call_count(), 1);
    assert_eq!(custodian.current_epoch().unwrap(), 5);
    assert_eq!(
        custodian.with_access_material(|_| Ok(())).unwrap_err(),
        BrokerError::Brk106
    );

    let public = format!("{error:?} {error}");
    for secret in [REFRESH, STALE, FRESH] {
        assert!(!public.contains(secret));
    }
}

#[test]
fn revoke_bumps_epoch_wipes_material_and_rejects_access() {
    let endpoint = Arc::new(MockTokenEndpoint::new([]));
    let (custodian, store) = custodian(endpoint, Some(i64::MAX));
    assert_eq!(custodian.revoke().unwrap(), 5);
    assert_eq!(custodian.connection_metadata().unwrap().revocation_epoch, 5);
    assert_eq!(
        custodian.with_access_material(|_| Ok(())).unwrap_err(),
        BrokerError::Brk106
    );
    let record = store.load("google-primary").unwrap().unwrap();
    assert!(record.sealed_refresh_token.is_none());
    assert!(record.sealed_access_token.is_none());
}
