use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use broker_auth::*;
use broker_core::{
    BrokerError,
    credential::{
        CredentialLeaseV2,
        spi::{BoundedRawResponse, authorize_with_driver},
    },
};
use std::collections::{BTreeMap, BTreeSet};

fn h(n: u8) -> String {
    format!("sha256:{}", format!("{n:02x}").repeat(32))
}
fn pin(name: &str, n: u8) -> RegistryPin {
    RegistryPin {
        entry_ref: name.into(),
        version: "1".into(),
        definition_hash: h(n),
        approval_epoch: 1,
        revocation_epoch: 0,
    }
}
fn profile(name: &str, activation: ActivationKind, scheme: AuthScheme) -> AuthProfile {
    let mut endpoints = BTreeMap::from([
        ("api".into(), "https://synthetic.invalid/api".into()),
        ("auth".into(), "https://synthetic.invalid/authorize".into()),
        ("token".into(), "https://synthetic.invalid/token".into()),
        ("revoke".into(), "https://synthetic.invalid/revoke".into()),
        (
            "principal".into(),
            "https://synthetic.invalid/whoami".into(),
        ),
        (
            "exchange".into(),
            "https://synthetic.invalid/exchange".into(),
        ),
    ]);
    if matches!(
        scheme,
        AuthScheme::HeaderKey { .. }
            | AuthScheme::QueryKey { .. }
            | AuthScheme::Bearer { .. }
            | AuthScheme::Basic { .. }
            | AuthScheme::SignedRequest { .. }
    ) {
        endpoints = BTreeMap::from([("api".into(), "https://synthetic.invalid/api".into())]);
    }
    AuthProfile {
        profile_ref: name.into(),
        version: "1".into(),
        definition_hash: h(1),
        connector_ref: format!("connector.{name}"),
        activation,
        scheme_ref: format!("credential.{name}@1"),
        scheme,
        endpoints,
        callback_uri: "https://broker.invalid/v0.2/credential-callback".into(),
        connection_claims: BTreeSet::new(),
        contract_claims: BTreeMap::from([(
            "contract.one".into(),
            BTreeSet::from(["claim.read".into()]),
        )]),
        lifecycle: BTreeSet::from(["activate".into(), "rotate".into(), "destroy".into()]),
        material_schema_hash: h(2),
        assertion_schema_hash: Some(h(3)),
        claims_schema_hash: h(4),
        public_claims: PublicClaimsPolicy::None,
        auth_driver: pin("driver", 5),
        claim_normalizer: pin("normalizer", 6),
        custodian: pin("custodian", 7),
        transport: pin("transport", 8),
        response_firewall: pin("firewall", 9),
    }
}
fn request(p: &AuthProfile, jti: &str) -> ActivationRequest {
    ActivationRequest {
        org_id: "org".into(),
        operator_id: "operator".into(),
        connector_ref: p.connector_ref.clone(),
        profile_ref: p.profile_ref.clone(),
        profile_version: p.version.clone(),
        standing_authority_ref: "standing".into(),
        standing_authority_hash: h(10),
        contract_set_ref: "contracts".into(),
        contract_set_hash: h(11),
        contract_ids: BTreeSet::from(["contract.one".into()]),
        deployment_id: "deployment".into(),
        execution_lane: "semantic_broker".into(),
        custody_location: "hosted".into(),
        request_jti: jti.into(),
    }
}
struct Token;
impl TokenService for Token {
    fn exchange_authorization_code(
        &self,
        r: TokenExchangeRequest<'_>,
    ) -> Result<ActivationMaterial, BrokerError> {
        if r.endpoint != "https://synthetic.invalid/token"
            || r.redirect_uri != "https://broker.invalid/v0.2/credential-callback"
        {
            return Err(BrokerError::Brk302);
        }
        Ok(ActivationMaterial {
            material: PrivateMaterial::new(br#"{"access_token":"oauth-private"}"#.to_vec())?,
            normalized_claims: r.expected_claims.clone(),
            principal_subject: PrivateMaterial::new(b"subject-1".to_vec())?,
        })
    }
    fn exchange_workload_assertion(
        &self,
        r: WorkloadExchangeRequest<'_>,
    ) -> Result<ActivationMaterial, BrokerError> {
        if r.endpoint != "https://synthetic.invalid/exchange" {
            return Err(BrokerError::Brk302);
        }
        Ok(ActivationMaterial {
            material: PrivateMaterial::new(br#"{"access_token":"workload-private"}"#.to_vec())?,
            normalized_claims: r.expected_claims.clone(),
            principal_subject: PrivateMaterial::new(b"workload-subject".to_vec())?,
        })
    }
}
fn oauth() -> AuthProfile {
    profile(
        "synthetic.oauth",
        ActivationKind::OAuthAuthorizationCodePkce,
        AuthScheme::OAuthPkce {
            authorization_endpoint_key: "auth".into(),
            token_endpoint_key: "token".into(),
            revocation_endpoint_key: "revoke".into(),
            principal_endpoint_key: "principal".into(),
            client_auth_binding: "client".into(),
        },
    )
}

#[test]
fn oauth_refresh_account_scope_and_callback_replay_conformance() {
    let p = oauth();
    let mut e = ActivationEngine::new(
        ApprovedRegistry::load_static([p.clone()]).unwrap(),
        [1; 32],
        "recipient",
    );
    let mut n = DeterministicNonceSource::default();
    let action = e.create(request(&p, "jti-1"), 100, &mut n).unwrap();
    let (handle, url) = match action {
        NextAction::OpenUrl {
            correlation_handle,
            url,
            ..
        } => (correlation_handle, url),
        _ => panic!(),
    };
    assert!(url.starts_with("https://synthetic.invalid/authorize?"));
    assert!(!url.contains("oauth-private"));
    let state = url.split("state=").nth(1).unwrap().to_string();
    let done = e
        .universal_callback(
            OAuthCallback {
                correlation_handle: handle.clone(),
                state: state.clone(),
                code: Some("code".into()),
                error: None,
            },
            101,
            &Token,
        )
        .unwrap();
    assert!(matches!(done, NextAction::Complete { .. }));
    let replay = e
        .universal_callback(
            OAuthCallback {
                correlation_handle: handle,
                state,
                code: Some("second-code".into()),
                error: None,
            },
            102,
            &Token,
        )
        .unwrap();
    assert_eq!(done, replay);
    let snap = e
        .snapshot(match &done {
            NextAction::Complete { activation_ref, .. } => activation_ref,
            _ => unreachable!(),
        })
        .unwrap();
    let public = format!("{snap:?}");
    assert!(
        !public.contains("claim.read")
            && !public.contains("subject-1")
            && !public.contains("oauth-private")
    );
    let mut injected = request(&p, "jti-2");
    injected.profile_ref = "unknown".into();
    assert_eq!(
        e.create(injected, 100, &mut n).unwrap_err(),
        BrokerError::Brk004
    );
}

fn lease(material: serde_json::Value) -> CredentialLeaseV2 {
    serde_json::from_value(serde_json::json!({"private_codec_version":"0.2","critical_fields":[],"extensions":{},"lease_ref":"lease-1","effect_grant_hash":h(20),"dispatch_attempt":1,"org_id":"org","connection_ref":"connection","scheme_ref":"scheme","auth_profile_pin":{"entry_ref":"profile","version":"1","definition_hash":h(21),"approval_epoch":1,"revocation_epoch":0},"material_kind":"credential","authority_view_hash":h(22),"authority_epoch":1,"leased_material_generation":1,"issued_at":"2026-01-01T00:00:00Z","expires_at":"2026-01-01T00:01:00Z","use_limit":1,"private_material_b64u":URL_SAFE_NO_PAD.encode(serde_json::to_vec(&material).unwrap())})).unwrap()
}
fn authorized(p: AuthProfile, material: serde_json::Value) -> serde_json::Value {
    let plan = FinalizedUnsignedRequest::validate(
        &p,
        "api",
        "POST",
        "/resource",
        vec![],
        vec![Header {
            name: "content-type".into(),
            value: "application/json".into(),
        }],
        br#"{"x":1}"#.to_vec(),
    )
    .unwrap()
    .into_plan()
    .unwrap();
    let driver = ProfileAuthDriver::new(p, || 1_700_000_000).unwrap();
    let request =
        authorize_with_driver(&driver, &plan, &lease(material), br#"{"run":"one"}"#).unwrap();
    request.with_transport_bytes(|bytes| serde_json::from_slice(bytes).unwrap())
}

#[test]
fn static_header_query_basic_bearer_rotation_and_smuggling_conformance() {
    let cases = [
        (
            AuthScheme::HeaderKey {
                name: "x-key".into(),
                prefix: "Key".into(),
            },
            serde_json::json!({"secret":"s-header"}),
            "headers",
            "s-header",
        ),
        (
            AuthScheme::QueryKey {
                name: "api_key".into(),
            },
            serde_json::json!({"secret":"s-query"}),
            "query",
            "s-query",
        ),
        (
            AuthScheme::Bearer {
                header: "Authorization".into(),
                prefix: "Bearer".into(),
            },
            serde_json::json!({"access_token":"s-bearer"}),
            "headers",
            "s-bearer",
        ),
        (
            AuthScheme::Basic {
                header: "Authorization".into(),
            },
            serde_json::json!({"username":"user","password":"s-basic"}),
            "headers",
            "Basic",
        ),
    ];
    for (scheme, material, placement, needle) in cases {
        let p = profile("synthetic.static", ActivationKind::SecretSubmission, scheme);
        let value = authorized(p.clone(), material);
        assert!(value[placement].to_string().contains(needle));
        let wrong = if placement == "query" {
            "headers"
        } else {
            "query"
        };
        assert!(!value[wrong].to_string().contains(needle));
        let smuggled = FinalizedUnsignedRequest::validate(
            &p,
            "api",
            "GET",
            "/",
            vec![],
            vec![Header {
                name: "Authorization".into(),
                value: "caller".into(),
            }],
            vec![],
        );
        assert_eq!(smuggled.unwrap_err(), BrokerError::Brk305);
    }
    let p = profile(
        "synthetic.static",
        ActivationKind::SecretSubmission,
        AuthScheme::HeaderKey {
            name: "x-key".into(),
            prefix: "".into(),
        },
    );
    let principal = PrivateMaterial::new(b"subject".to_vec()).unwrap();
    let material = PrivateMaterial::new(br#"{"secret":"generation-one"}"#.to_vec()).unwrap();
    let claims = p.derive_claims(&p.connector_ref, ["contract.one"]).unwrap();
    let mut vault = CredentialVault::prepare(
        [3; 32],
        [4; 32],
        "org",
        "connection",
        &p,
        pin("profile", 1),
        "local",
        claims.clone(),
        &principal,
        material,
        100,
        None,
    )
    .unwrap();
    vault.activate_v2_after_test_seal().unwrap();
    let first = vault.lease(&h(30), 1, 101, 10).unwrap();
    let plan = FinalizedUnsignedRequest::validate(&p, "api", "GET", "/", vec![], vec![], vec![])
        .unwrap()
        .into_plan()
        .unwrap();
    let driver = ProfileAuthDriver::new(p.clone(), || 100).unwrap();
    assert!(authorize_with_driver(&driver, &plan, &first, b"{}").is_ok());
    let cas = vault.snapshot().cas_version;
    vault.begin_rotation("definite-failure", cas).unwrap();
    vault.record_provider_request(b"failed-request").unwrap();
    assert_eq!(
        vault
            .observe_rotation(ProviderRotationResult::DefinitelyFailed, 102)
            .unwrap_err(),
        BrokerError::Brk401
    );
    assert_eq!(vault.snapshot().status, ConnectionStatus::Active);
    let cas = vault.snapshot().cas_version;
    vault.begin_rotation("rotation", cas).unwrap();
    vault.record_provider_request(b"request").unwrap();
    vault
        .observe_rotation(
            ProviderRotationResult::Observed {
                material: PrivateMaterial::new(br#"{"secret":"generation-two"}"#.to_vec()).unwrap(),
                claims,
                expires_at: None,
            },
            102,
        )
        .unwrap();
    let cas = vault.snapshot().cas_version;
    vault.switch_rotation(cas).unwrap();
    assert_eq!(vault.snapshot().current_material_generation, 2);
    assert_eq!(
        vault.begin_rotation("bad", cas).unwrap_err(),
        BrokerError::Brk204
    );
    let cas = vault.snapshot().cas_version;
    vault.begin_rotation("uncertain", cas).unwrap();
    vault.record_provider_request(b"uncertain-request").unwrap();
    assert_eq!(
        vault
            .observe_rotation(ProviderRotationResult::CrossingUncertain, 103)
            .unwrap_err(),
        BrokerError::Brk401
    );
    assert_eq!(vault.snapshot().status, ConnectionStatus::Blocked);
}

#[test]
fn sigv4_final_request_signing_and_rotation_uncertainty_conformance() {
    let p = profile(
        "synthetic.sigv4",
        ActivationKind::SecretSubmission,
        AuthScheme::SignedRequest {
            region: "us-test-1".into(),
            service: "objects".into(),
            timestamp_header: "x-date".into(),
            signed_headers: BTreeSet::from(["content-type".into(), "x-date".into()]),
        },
    );
    let value = authorized(
        p.clone(),
        serde_json::json!({"access_key_id":"AKID","secret_access_key":"private-signing-key"}),
    );
    let auth = value["headers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|h| h["name"] == "Authorization")
        .unwrap()["value"]
        .as_str()
        .unwrap();
    assert!(auth.contains("Region=us-test-1,Service=objects") && auth.contains("Signature="));
    assert!(!auth.contains("private-signing-key"));
    let substituted = p.clone();
    assert_eq!(
        FinalizedUnsignedRequest::validate(
            &substituted,
            "api",
            "POST",
            "/",
            vec![
                QueryPair {
                    name: "x".into(),
                    value: "1".into()
                },
                QueryPair {
                    name: "x".into(),
                    value: "2".into()
                }
            ],
            vec![],
            vec![]
        )
        .unwrap_err(),
        BrokerError::Brk305
    );
}

#[test]
fn workload_exchange_replay_firewall_and_remote_shape_conformance() {
    let p = profile(
        "synthetic.workload",
        ActivationKind::WorkloadBinding,
        AuthScheme::WorkloadTokenExchange {
            exchange_endpoint_key: "exchange".into(),
            trusted_issuers: BTreeSet::from(["https://issuer.invalid".into()]),
            audience: "lattice".into(),
            maximum_assertion_age_seconds: 60,
        },
    );
    let mut e = ActivationEngine::new(
        ApprovedRegistry::load_static([p.clone()]).unwrap(),
        [9; 32],
        "recipient",
    );
    let mut n = DeterministicNonceSource::default();
    let action = e.create(request(&p, "workload-jti"), 100, &mut n).unwrap();
    let (activation, channel, nonce) = match action {
        NextAction::PresentWorkloadAssertion {
            activation_ref,
            channel_ref,
            nonce,
            ..
        } => (activation_ref, channel_ref, nonce),
        _ => panic!(),
    };
    let (aad, key) = e.submission_context(&activation).unwrap();
    let encrypted =
        encrypt_submission(&key, &channel, "assertion-jti", &aad, [7; 12], b"assertion").unwrap();
    let replay = encrypted.clone();
    let assertion = e
        .submit_private_material(&activation, encrypted, 101)
        .unwrap();
    assert_eq!(
        e.submit_private_material(&activation, replay, 101)
            .unwrap_err(),
        BrokerError::Brk109
    );
    let done = e
        .complete_workload(
            &activation,
            assertion,
            WorkloadPresentation {
                now: 101,
                issued_at: 100,
                issuer: "https://issuer.invalid",
                audience: "lattice",
                nonce: &nonce,
            },
            &Token,
        )
        .unwrap();
    assert!(matches!(done, NextAction::Complete { .. }));
    assert_eq!(
        e.complete_workload(
            &activation,
            PrivateMaterial::new(b"replay".to_vec()).unwrap(),
            WorkloadPresentation {
                now: 102,
                issued_at: 100,
                issuer: "https://evil.invalid",
                audience: "lattice",
                nonce: &nonce,
            },
            &Token
        )
        .unwrap_err(),
        BrokerError::Brk203
    );
    let firewall = StrictResponseFirewall;
    let forbidden = FirewallPolicy::Forbidden {
        sensitive_pointers: BTreeSet::new(),
    };
    assert_eq!(
        firewall
            .apply(
                BoundedRawResponse::from_privileged_transport(
                    br#"{"access_token":"smuggled"}"#.to_vec()
                )
                .unwrap(),
                &forbidden
            )
            .err()
            .unwrap(),
        BrokerError::Brk305
    );
    let mut result = firewall
        .apply(
            BoundedRawResponse::from_privileged_transport(
                br#"{"access_token":"private","ok":true}"#.to_vec(),
            )
            .unwrap(),
            &FirewallPolicy::PrivilegedExtract {
                credential_pointers: BTreeMap::from([("/access_token".into(), 64)]),
                echo_pointers: BTreeSet::new(),
            },
        )
        .unwrap();
    assert_eq!(result.scrubbed.value(), &serde_json::json!({"ok":true}));
    assert!(result.take_private_update().is_some());
}
