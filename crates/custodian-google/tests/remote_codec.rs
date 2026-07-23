use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use broker_core::{BrokerError, signing::BrokerSigner};
use custodian_google::remote::*;

fn h(n: u8) -> String {
    format!("sha256:{}", format!("{n:02x}").repeat(32))
}
fn pin(name: &str, n: u8) -> ServicePin {
    ServicePin {
        entry_ref: name.into(),
        version: "1".into(),
        definition_hash: h(n),
        approval_epoch: 1,
        revocation_epoch: 0,
    }
}
fn identities() -> (RemoteServiceIdentity, RemoteServiceIdentity) {
    let caller = BrokerSigner::from_seed("caller-key", [1; 32]);
    let remote = BrokerSigner::from_seed("remote-key", [2; 32]);
    let caller_pin = pin("transport.caller", 1);
    let remote_pin = pin("custodian.remote", 2);
    let caller_hpke_private = [7; 32];
    let remote_hpke_private = [8; 32];
    let caller_hpke_public =
        x25519_dalek::PublicKey::from(&x25519_dalek::StaticSecret::from(caller_hpke_private))
            .to_bytes();
    let remote_hpke_public =
        x25519_dalek::PublicKey::from(&x25519_dalek::StaticSecret::from(remote_hpke_private))
            .to_bytes();
    let caller_verifying = caller.verifying_key();
    let remote_verifying = remote.verifying_key();
    let a = RemoteServiceIdentity::new(
        caller_pin.clone(),
        remote_pin.clone(),
        "caller-hpke-1",
        "remote-hpke-1",
        caller,
        remote_verifying,
        caller_hpke_private,
        remote_hpke_public,
    );
    let b = RemoteServiceIdentity::new(
        remote_pin,
        caller_pin,
        "remote-hpke-1",
        "caller-hpke-1",
        remote,
        caller_verifying,
        remote_hpke_private,
        caller_hpke_public,
    );
    (a, b)
}
fn metadata() -> RemoteRequestMetadata {
    RemoteRequestMetadata {
        request_ref: "remote-request-1".into(),
        org_id: "org".into(),
        connection_ref: "connection".into(),
        effect_grant_hash: h(3),
        logical_effect_id: h(4),
        dispatch_attempt: 1,
        authority_view_hash: h(5),
        minimum_material_generation: 2,
        nonce_jti: "nonce-one".into(),
        issued_at: "2026-01-01T00:00:00Z".into(),
        expires_at: "2026-01-01T00:01:00Z".into(),
    }
}
fn private() -> RemotePrivateRequest {
    RemotePrivateRequest {
        unauthenticated_plan_jcs: br#"{"body":{"value":"never-public"},"method":"POST"}"#.to_vec(),
        contract_hash: h(6),
        auth_profile_pin: pin("profile", 7),
        broker_context_jcs: br#"{"run":"one"}"#.to_vec(),
        endpoint_set_hash: h(8),
        response_firewall_policy_hash: h(9),
    }
}

#[test]
fn codec_authenticates_encrypts_replays_and_records_terminal_result() {
    let (caller, remote_identity) = identities();
    let request = RemoteCustodianMock::seal_request(
        &caller,
        remote_identity.pin(),
        &metadata(),
        private(),
        [9; 32],
    )
    .unwrap();
    assert!(!String::from_utf8_lossy(&request).contains("never-public"));
    let state = Arc::new(RemoteDurableState::default());
    let remote = RemoteCustodianMock::with_state(remote_identity, state.clone());
    let response = remote
        .authorize_and_dispatch(&request, "2026-01-01T00:00:30Z", CrashPoint::None, |_| {
            Ok(serde_json::json!({"ok":true}))
        })
        .unwrap();
    drop(remote);
    let (_, restarted_identity) = identities();
    let restarted = RemoteCustodianMock::with_state(restarted_identity, state);
    let replay = restarted
        .authorize_and_dispatch(&request, "2026-01-01T00:00:30Z", CrashPoint::None, |_| {
            panic!("replay dispatched")
        })
        .unwrap();
    assert_eq!(response, replay);
    let opened =
        RemoteCustodianMock::open_response(&caller, &response, &metadata(), "2026-01-01T00:00:30Z")
            .unwrap();
    assert_eq!(opened["outcome"], "confirmed");
    assert_eq!(opened["leased_material_generation"], 2);
    let mut wrong = metadata();
    wrong.dispatch_attempt = 2;
    assert_eq!(
        RemoteCustodianMock::open_response(&caller, &response, &wrong, "2026-01-01T00:00:30Z")
            .unwrap_err(),
        BrokerError::Brk109
    );
}

#[test]
fn prepared_outbox_resumes_once_after_service_restart() {
    let (caller, remote_identity) = identities();
    let request = RemoteCustodianMock::seal_request(
        &caller,
        remote_identity.pin(),
        &metadata(),
        private(),
        [9; 32],
    )
    .unwrap();
    let state = Arc::new(RemoteDurableState::default());
    let remote = RemoteCustodianMock::with_state(remote_identity, state.clone());
    assert_eq!(
        remote
            .authorize_and_dispatch(
                &request,
                "2026-01-01T00:00:30Z",
                CrashPoint::AfterPrepared,
                |_| panic!("prepared request crossed provider"),
            )
            .unwrap_err(),
        BrokerError::Brk401
    );
    drop(remote);
    let calls = AtomicUsize::new(0);
    let (_, restarted_identity) = identities();
    let restarted = RemoteCustodianMock::with_state(restarted_identity, state);
    restarted
        .authorize_and_dispatch(&request, "2026-01-01T00:00:30Z", CrashPoint::None, |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(serde_json::json!({"ok":true}))
        })
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn tamper_and_crossing_crash_fail_closed_without_retry() {
    let (caller, remote_identity) = identities();
    let request = RemoteCustodianMock::seal_request(
        &caller,
        remote_identity.pin(),
        &metadata(),
        private(),
        [9; 32],
    )
    .unwrap();
    let state = Arc::new(RemoteDurableState::default());
    let remote = RemoteCustodianMock::with_state(remote_identity, state.clone());
    let mut tampered = request.clone();
    let index = tampered.iter().position(|b| *b == b'A').unwrap_or(10);
    tampered[index] ^= 1;
    assert!(
        remote
            .authorize_and_dispatch(&tampered, "2026-01-01T00:00:30Z", CrashPoint::None, |_| Ok(
                serde_json::json!({})
            ))
            .is_err()
    );
    let response = remote
        .authorize_and_dispatch(
            &request,
            "2026-01-01T00:00:30Z",
            CrashPoint::AfterProviderCrossing,
            |_| panic!("crash path does not return provider result"),
        )
        .unwrap();
    assert_eq!(
        RemoteCustodianMock::open_response(&caller, &response, &metadata(), "2026-01-01T00:00:30Z")
            .unwrap()["outcome"],
        "ambiguous"
    );
    let replay = remote
        .authorize_and_dispatch(&request, "2026-01-01T00:00:30Z", CrashPoint::None, |_| {
            panic!("uncertain dispatch retried")
        })
        .unwrap();
    assert_eq!(response, replay);
}
