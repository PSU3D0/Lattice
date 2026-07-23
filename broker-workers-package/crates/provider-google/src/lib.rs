#![forbid(unsafe_code)]

pub mod signed_registry;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use broker_auth::{
    ActivationKind, ApprovedRegistry, AuthProfile, AuthScheme, NormalizedClaims, PrivateMaterial,
    ProfileAuthDriver, PublicClaimsPolicy, RegistryPin,
};
use broker_core::{
    BrokerError,
    credential::{
        parse,
        registry::{RegistryDecisionV2, RegistryDefinitionV2},
        signing::verify_signed,
    },
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

pub const CONNECTOR_REF: &str = "connector.google.workspace@1";
pub const PROFILE_REF: &str = "auth.google.workspace.oauth2";
pub const PROFILE_VERSION: &str = "1";
pub const LEGACY_PROFILE_REF: &str = "auth.google.workspace.oauth2@1";
pub const EXECUTION_LANE: &str = "semantic_broker";
pub const CUSTODY_LOCATION: &str = "hosted_broker";
pub const AUTHORIZATION_ENDPOINT: &str = "https://accounts.google.com/o/oauth2/v2/auth";
pub const TOKEN_ENDPOINT: &str = "https://oauth2.googleapis.com/token";
pub const REVOCATION_ENDPOINT: &str = "https://oauth2.googleapis.com/revoke";
pub const PRINCIPAL_ENDPOINT: &str = "https://oauth2.googleapis.com/tokeninfo";
pub const UNIVERSAL_CALLBACK: &str = "https://broker.invalid/v0.2/credential-callback";
pub const TOKEN_SERVICE_BINDING: &str = "GOOGLE_TOKEN_SERVICE";
pub const PROVIDER_SERVICE_BINDING: &str = "GOOGLE_PROVIDER_SERVICE";
pub const AUTHORIZATION_ENDPOINT_BINDING: &str = "GOOGLE_AUTHORIZE_ENDPOINT";
pub const OAUTH_CLIENT_ID_BINDING: &str = "GOOGLE_OAUTH_CLIENT_ID";
pub const GMAIL_SCOPE: &str = "https://www.googleapis.com/auth/gmail.send";
pub const SHEETS_SCOPE: &str = "https://www.googleapis.com/auth/spreadsheets";
pub const GMAIL_CONTRACT_ID: &str = "connector.google.gmail.send_message@1";
pub const GMAIL_CONTRACT_HASH: &str =
    "sha256:8fbdd2dbb63877b92004b7b5e6a7dc665a0ec5788850e4a466c0b6200639de2a";
pub const SHEETS_CONTRACT_ID: &str = "connector.google.sheets.append_row@1";
pub const SHEETS_CONTRACT_HASH: &str =
    "sha256:d02ed39536d396d66895f97672551a9eb443e701900112613865170bf157e999";

const PROFILE_HASH: &str =
    "sha256:a97b7aed775144652866f288bf4fdd52d329ae479180069721d41a94282f1890";
const DRIVER_HASH: &str = "sha256:b97b7aed775144652866f288bf4fdd52d329ae479180069721d41a94282f1890";
const NORMALIZER_HASH: &str =
    "sha256:c97b7aed775144652866f288bf4fdd52d329ae479180069721d41a94282f1890";
const CUSTODIAN_HASH: &str =
    "sha256:d97b7aed775144652866f288bf4fdd52d329ae479180069721d41a94282f1890";
const TRANSPORT_HASH: &str =
    "sha256:e97b7aed775144652866f288bf4fdd52d329ae479180069721d41a94282f1890";
const FIREWALL_HASH: &str =
    "sha256:f97b7aed775144652866f288bf4fdd52d329ae479180069721d41a94282f1890";
const SCHEMA_HASH: &str = "sha256:197b7aed775144652866f288bf4fdd52d329ae479180069721d41a94282f1890";

fn pin(entry_ref: &str, hash: &str) -> RegistryPin {
    RegistryPin {
        entry_ref: entry_ref.into(),
        version: "1".into(),
        definition_hash: hash.into(),
        approval_epoch: 1,
        revocation_epoch: 0,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdapterRegistration {
    pub contract_id: &'static str,
    pub contract_hash: &'static str,
    pub required_claim: &'static str,
    pub semantic_effect_slot: &'static str,
    pub origin: &'static str,
    pub implementation_hash: &'static str,
    pub descriptor: &'static [u8],
    pub authority_facts_jcs: &'static [u8],
    pub planner: RegistryPin,
    pub projector: RegistryPin,
    pub response_firewall: RegistryPin,
}

#[derive(Clone, Debug)]
pub struct GoogleComposition {
    pub profile: AuthProfile,
    pub adapters: Vec<AdapterRegistration>,
    pub token_service_binding: &'static str,
    pub provider_service_binding: &'static str,
}

pub fn google_v1() -> GoogleComposition {
    let profile = AuthProfile {
        profile_ref: PROFILE_REF.into(),
        version: PROFILE_VERSION.into(),
        definition_hash: PROFILE_HASH.into(),
        connector_ref: CONNECTOR_REF.into(),
        activation: ActivationKind::OAuthAuthorizationCodePkce,
        scheme_ref: "credential.oauth2.authorization_code_pkce@1".into(),
        scheme: AuthScheme::OAuthPkce {
            authorization_endpoint_key: "authorization".into(),
            token_endpoint_key: "token".into(),
            revocation_endpoint_key: "revocation".into(),
            principal_endpoint_key: "principal_discovery".into(),
            client_auth_binding: "google.oauth.client-secret".into(),
        },
        endpoints: BTreeMap::from([
            ("authorization".into(), AUTHORIZATION_ENDPOINT.into()),
            ("token".into(), TOKEN_ENDPOINT.into()),
            ("revocation".into(), REVOCATION_ENDPOINT.into()),
            ("principal_discovery".into(), PRINCIPAL_ENDPOINT.into()),
        ]),
        callback_uri: UNIVERSAL_CALLBACK.into(),
        contract_claims: BTreeMap::from([
            (
                GMAIL_CONTRACT_ID.into(),
                BTreeSet::from([GMAIL_SCOPE.into()]),
            ),
            (
                SHEETS_CONTRACT_ID.into(),
                BTreeSet::from([SHEETS_SCOPE.into()]),
            ),
        ]),
        lifecycle: BTreeSet::from([
            "activate".into(),
            "refresh".into(),
            "rotate".into(),
            "revoke".into(),
            "destroy".into(),
            "discover_principal".into(),
        ]),
        material_schema_hash: SCHEMA_HASH.into(),
        assertion_schema_hash: None,
        claims_schema_hash: SCHEMA_HASH.into(),
        public_claims: PublicClaimsPolicy::None,
        auth_driver: pin("auth-driver.google.oauth2", DRIVER_HASH),
        claim_normalizer: pin("claim-normalizer.google.oauth-scopes", NORMALIZER_HASH),
        custodian: pin("custodian.google.oauth2", CUSTODIAN_HASH),
        transport: pin("transport.google.https", TRANSPORT_HASH),
        response_firewall: pin("firewall.google.oauth-token", FIREWALL_HASH),
    };
    let adapters = [
        AdapterRegistration {
            contract_id: GMAIL_CONTRACT_ID,
            contract_hash: GMAIL_CONTRACT_HASH,
            required_claim: GMAIL_SCOPE,
            semantic_effect_slot: "send_message",
            origin: "https://gmail.googleapis.com",
            implementation_hash:
                "sha256:5a5de77f756b49aac0fb5339bf764f9e53a9437cdc41619c2c978aa5dbb4e3fc",
            descriptor: include_bytes!(
                "../../connectors/google/gmail/broker/operations/send_message.json"
            ),
            authority_facts_jcs: br#"{"allowed":true}"#,
            planner: pin(&format!("planner.{GMAIL_CONTRACT_ID}"), DRIVER_HASH),
            projector: pin(&format!("projector.{GMAIL_CONTRACT_ID}"), NORMALIZER_HASH),
            response_firewall: profile.response_firewall.clone(),
        },
        AdapterRegistration {
            contract_id: SHEETS_CONTRACT_ID,
            contract_hash: SHEETS_CONTRACT_HASH,
            required_claim: SHEETS_SCOPE,
            semantic_effect_slot: "append_row",
            origin: "https://sheets.googleapis.com",
            implementation_hash:
                "sha256:54db6603967e1ce4e46ef45ece7bfa947c129564e40a290989fa2ae901a23966",
            descriptor: include_bytes!(
                "../../connectors/google/sheets/broker/operations/append_row.json"
            ),
            authority_facts_jcs: br#"{"google":{"sheets":{"headers":["column"]}}}"#,
            planner: pin(&format!("planner.{SHEETS_CONTRACT_ID}"), DRIVER_HASH),
            projector: pin(&format!("projector.{SHEETS_CONTRACT_ID}"), NORMALIZER_HASH),
            response_firewall: profile.response_firewall.clone(),
        },
    ]
    .into_iter()
    .collect();
    GoogleComposition {
        profile,
        adapters,
        token_service_binding: TOKEN_SERVICE_BINDING,
        provider_service_binding: PROVIDER_SERVICE_BINDING,
    }
}

pub fn verified_google_v1(now: &str) -> Result<GoogleComposition, BrokerError> {
    let bundle = signed_registry::signed_registry_bundle()?;
    let mut composition = google_v1();
    let mut pins = BTreeMap::new();
    for (definition_bytes, decision_bytes) in &bundle.seeds {
        let definition = parse::<RegistryDefinitionV2>(definition_bytes)?;
        let decision = parse::<RegistryDecisionV2>(decision_bytes)?;
        let publisher_key = definition
            .view
            .as_value()
            .get("publisher_key_id")
            .and_then(serde_json::Value::as_str)
            .and_then(|key| bundle.publisher_roots.get(key))
            .ok_or(BrokerError::Brk004)?;
        let decision_key = decision
            .view
            .as_value()
            .get("authority_key_id")
            .and_then(serde_json::Value::as_str)
            .and_then(|key| bundle.decision_roots.get(key))
            .ok_or(BrokerError::Brk004)?;
        verify_signed(&definition, publisher_key)?;
        verify_signed(&decision, decision_key)?;
        let definition_value = definition.view.as_value();
        let decision_value = decision.view.as_value();
        if decision_value
            .get("definition_hash")
            .and_then(serde_json::Value::as_str)
            != Some(definition.content_hash().as_str())
            || decision_value
                .get("approval_status")
                .and_then(serde_json::Value::as_str)
                != Some("approved")
            || decision_value
                .get("revocation_status")
                .and_then(serde_json::Value::as_str)
                != Some("active")
            || decision_value
                .get("not_before")
                .and_then(serde_json::Value::as_str)
                .is_none_or(|value| value > now)
            || decision_value
                .get("expires_at")
                .and_then(serde_json::Value::as_str)
                .is_none_or(|value| value <= now)
            || definition_value.get("class") != definition_value.pointer("/class_payload/kind")
        {
            return Err(BrokerError::Brk106);
        }
        let entry_ref = definition_value
            .get("entry_ref")
            .and_then(serde_json::Value::as_str)
            .ok_or(BrokerError::Brk004)?;
        pins.insert(
            entry_ref.to_string(),
            RegistryPin {
                entry_ref: entry_ref.into(),
                version: "1".into(),
                definition_hash: definition.content_hash(),
                approval_epoch: 1,
                revocation_epoch: 0,
            },
        );
    }
    composition.profile.definition_hash = pins
        .get(bundle.profile_entry_ref)
        .ok_or(BrokerError::Brk004)?
        .definition_hash
        .clone();
    composition.profile.auth_driver = pins
        .get("auth-driver.google.oauth2")
        .cloned()
        .ok_or(BrokerError::Brk004)?;
    composition.profile.claim_normalizer = pins
        .get("claim-normalizer.google.oauth-scopes")
        .cloned()
        .ok_or(BrokerError::Brk004)?;
    composition.profile.custodian = pins
        .get("custodian.google.oauth2")
        .cloned()
        .ok_or(BrokerError::Brk004)?;
    composition.profile.transport = pins
        .get("transport.google.https")
        .cloned()
        .ok_or(BrokerError::Brk004)?;
    composition.profile.response_firewall = pins
        .get("firewall.google.oauth-token")
        .cloned()
        .ok_or(BrokerError::Brk004)?;
    for adapter in &mut composition.adapters {
        let planner_entry = match adapter.contract_id {
            GMAIL_CONTRACT_ID => connector_google_platform::broker::GMAIL_RFC822_ADAPTER_ID,
            SHEETS_CONTRACT_ID => connector_google_platform::broker::SHEETS_APPEND_ROW_ADAPTER_ID,
            _ => return Err(BrokerError::Brk004),
        };
        adapter.planner = pins
            .get(planner_entry)
            .cloned()
            .ok_or(BrokerError::Brk004)?;
        adapter.projector = pins
            .get(&format!("projector.{}", adapter.contract_id))
            .cloned()
            .ok_or(BrokerError::Brk004)?;
        adapter.response_firewall = composition.profile.response_firewall.clone();
    }
    Ok(composition)
}

struct GooglePlanner(&'static str);
impl broker_host::CredentialBlindPlannerImplementation for GooglePlanner {
    fn plan(
        &self,
        input: &serde_json::Value,
        authority: &serde_json::Value,
    ) -> Result<serde_json::Map<String, serde_json::Value>, BrokerError> {
        match self.0 {
            GMAIL_CONTRACT_ID => {
                connector_google_platform::broker::adapt_gmail_rfc822_message(input)
                    .map_err(|_| BrokerError::Brk301)
            }
            SHEETS_CONTRACT_ID => {
                connector_google_platform::broker::adapt_sheets_append_row(input, authority)
                    .map_err(|_| BrokerError::Brk301)
            }
            _ => Err(BrokerError::Brk108),
        }
    }
}

struct GoogleProjector(&'static str);
impl broker_host::ResponseProjectorImplementation for GoogleProjector {
    fn project(
        &self,
        response: &serde_json::Value,
    ) -> Result<serde_json::Map<String, serde_json::Value>, BrokerError> {
        let object = response.as_object().ok_or(BrokerError::Brk305)?;
        let mut projected = serde_json::Map::new();
        match self.0 {
            GMAIL_CONTRACT_ID => {
                for (field, wire) in [("id", "id"), ("thread_id", "threadId")] {
                    projected.insert(
                        field.into(),
                        object.get(wire).cloned().ok_or(BrokerError::Brk305)?,
                    );
                }
            }
            SHEETS_CONTRACT_ID => {
                let source = object
                    .get("updates")
                    .and_then(serde_json::Value::as_object)
                    .unwrap_or(object);
                for (field, wire) in [
                    ("updated_cells", "updatedCells"),
                    ("updated_columns", "updatedColumns"),
                    ("updated_range", "updatedRange"),
                    ("updated_rows", "updatedRows"),
                ] {
                    projected.insert(
                        field.into(),
                        source.get(wire).cloned().ok_or(BrokerError::Brk305)?,
                    );
                }
            }
            _ => return Err(BrokerError::Brk108),
        }
        Ok(projected)
    }
}

struct GoogleCredentialFirewall;
impl broker_host::ResponseFirewallImplementation for GoogleCredentialFirewall {
    fn scrub(&self, bounded_response: &[u8]) -> Result<serde_json::Value, BrokerError> {
        if bounded_response.len() > 65_536 {
            return Err(BrokerError::Brk305);
        }
        let value: serde_json::Value =
            serde_json::from_slice(bounded_response).map_err(|_| BrokerError::Brk305)?;
        fn contains_credential(value: &serde_json::Value) -> bool {
            match value {
                serde_json::Value::Array(values) => values.iter().any(contains_credential),
                serde_json::Value::Object(object) => object.iter().any(|(key, value)| {
                    matches!(
                        key.to_ascii_lowercase().as_str(),
                        "access_token"
                            | "refresh_token"
                            | "id_token"
                            | "client_secret"
                            | "authorization"
                            | "proxy-authorization"
                    ) || contains_credential(value)
                }),
                _ => false,
            }
        }
        if contains_credential(&value) {
            return Err(BrokerError::Brk302);
        }
        Ok(value)
    }
}

/// Application-root composition of signed registry evidence and concrete
/// implementations. `broker-host` remains provider-neutral.
pub fn host_registry(now: &str) -> Result<broker_host::TrustedAdapterRegistry, BrokerError> {
    let bundle = signed_registry::signed_registry_bundle()?;
    let composition = verified_google_v1(now)?;
    let publisher = bundle
        .publisher_roots
        .values()
        .next()
        .ok_or(BrokerError::Brk004)?;
    let decision_key = bundle
        .decision_roots
        .values()
        .next()
        .ok_or(BrokerError::Brk004)?;
    let find = |entry_ref: &str| -> Result<(&[u8], &[u8]), BrokerError> {
        bundle
            .seeds
            .iter()
            .find_map(|(definition, decision)| {
                let parsed = parse::<RegistryDefinitionV2>(definition).ok()?;
                (parsed
                    .view
                    .as_value()
                    .get("entry_ref")
                    .and_then(serde_json::Value::as_str)
                    == Some(entry_ref))
                .then_some((definition.as_slice(), decision.as_slice()))
            })
            .ok_or(BrokerError::Brk004)
    };
    let mut registry = broker_host::TrustedAdapterRegistry::empty();
    for adapter in &composition.adapters {
        let (definition, decision) = find(&adapter.planner.entry_ref)?;
        registry
            .install_planner(
                definition,
                decision,
                publisher,
                decision_key,
                now,
                adapter.contract_hash,
                Arc::new(GooglePlanner(adapter.contract_id)),
            )
            .map_err(|_| BrokerError::Brk106)?;
        let (definition, decision) = find(&adapter.projector.entry_ref)?;
        registry
            .install_projector(
                definition,
                decision,
                publisher,
                decision_key,
                now,
                adapter.contract_hash,
                Arc::new(GoogleProjector(adapter.contract_id)),
            )
            .map_err(|_| BrokerError::Brk106)?;
    }
    let (definition, decision) = find("firewall.google.oauth-token")?;
    registry
        .install_firewall(
            definition,
            decision,
            publisher,
            decision_key,
            now,
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            Arc::new(GoogleCredentialFirewall),
        )
        .map_err(|_| BrokerError::Brk106)?;
    Ok(registry)
}

pub fn verified_provider_plane(now: &str) -> Result<GoogleComposition, BrokerError> {
    verified_google_v1(now)
}

pub fn contract_registration(
    contract_id: &str,
    now: &str,
) -> Result<AdapterRegistration, BrokerError> {
    verified_provider_plane(now)?
        .adapters
        .into_iter()
        .find(|registration| registration.contract_id == contract_id)
        .ok_or(BrokerError::Brk108)
}

pub fn contract_registrations(now: &str) -> Result<Vec<AdapterRegistration>, BrokerError> {
    Ok(verified_provider_plane(now)?.adapters)
}

#[deprecated(note = "use contract_registration")]
pub fn adapter(contract_id: &str, now: &str) -> Result<AdapterRegistration, BrokerError> {
    contract_registration(contract_id, now)
}

pub fn approved_registry(now: &str) -> Result<ApprovedRegistry, BrokerError> {
    ApprovedRegistry::load_static([verified_google_v1(now)?.profile])
}
pub fn auth_driver(
    clock: impl Fn() -> i64 + Send + Sync + 'static,
) -> Result<ProfileAuthDriver, BrokerError> {
    ProfileAuthDriver::new(google_v1().profile, clock)
}
pub fn normalized_scopes(value: &str) -> Result<NormalizedClaims, BrokerError> {
    let values = value
        .split_ascii_whitespace()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    if values.len() != 2 || values.iter().any(|v| v != GMAIL_SCOPE && v != SHEETS_SCOPE) {
        return Err(BrokerError::Brk109);
    }
    Ok(NormalizedClaims { values })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DiscoveryResponse {
    user_id: String,
    scope: String,
}
pub fn discover_account(
    response: &[u8],
    expected: &NormalizedClaims,
) -> Result<PrivateMaterial, BrokerError> {
    if response.len() > 64 * 1024 {
        return Err(BrokerError::Brk305);
    }
    let discovered: DiscoveryResponse =
        serde_json::from_slice(response).map_err(|_| BrokerError::Brk109)?;
    if discovered.user_id.is_empty()
        || normalized_scopes(&discovered.scope)?.compare(expected)
            != broker_auth::ClaimRelation::Equal
    {
        return Err(BrokerError::Brk109);
    }
    PrivateMaterial::new(discovered.user_id.into_bytes())
}

pub fn gmail_message(
    to: &str,
    cc: Option<&str>,
    bcc: Option<&str>,
    subject: &str,
    body: &str,
) -> String {
    connector_google_platform::gmail::build_plain_text_email(to, cc, bcc, subject, body)
}
pub fn account_subject_commitment(subject: &[u8]) -> String {
    format!("sha256:{}", hex(&Sha256::digest(subject)))
}
fn hex(bytes: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut o = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        o.push(H[(b >> 4) as usize] as char);
        o.push(H[(b & 15) as usize] as char)
    }
    o
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn definitions_are_static_complete_and_device_is_unsupported() {
        let c = google_v1();
        c.profile.validate().unwrap();
        assert_eq!(c.adapters.len(), 2);
        assert!(!c.profile.lifecycle.contains("device_authorization"));
        assert_eq!(c.profile.endpoint("token").unwrap(), TOKEN_ENDPOINT);
    }
    #[test]
    fn generic_host_registry_composes_exact_signed_implementations() {
        let registry = host_registry("2026-07-21T00:00:00Z").unwrap();
        let gmail = contract_registration(GMAIL_CONTRACT_ID, "2026-07-21T00:00:00Z").unwrap();
        let descriptor: connector_spec::BrokerDispatchDescriptor =
            serde_json::from_slice(gmail.descriptor).unwrap();
        let planned = broker_host::descriptor_plan_template(
            &descriptor,
            br#"{"bcc":null,"cc":null,"subject":"subject","text_body":"body","to":"to@example.com"}"#,
            gmail.authority_facts_jcs,
            &registry,
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "2026-07-21T00:00:00Z",
        ).unwrap();
        assert!(String::from_utf8(planned).unwrap().contains("raw"));
        let projected = registry
            .project(
                gmail.contract_hash,
                &serde_json::json!({"id":"message-1","threadId":"thread-1"}),
            )
            .unwrap();
        assert_eq!(projected["thread_id"], "thread-1");
    }

    #[test]
    fn generic_host_registry_applies_signed_revocation_without_fallback() {
        let now = "2026-07-21T00:00:00Z";
        let mut registry = host_registry(now).unwrap();
        let composition = verified_google_v1(now).unwrap();
        let planner = &composition.adapters[0].planner;
        let revoked = signed_registry::deterministic_decision(
            &planner.entry_ref,
            &planner.definition_hash,
            "approved",
            "revoked",
        )
        .unwrap();
        let roots = signed_registry::signed_registry_bundle().unwrap();
        registry
            .apply_decision(&revoked, roots.decision_roots.values().next().unwrap(), now)
            .unwrap();
        let descriptor: connector_spec::BrokerDispatchDescriptor =
            serde_json::from_slice(composition.adapters[0].descriptor).unwrap();
        assert!(matches!(
            broker_host::descriptor_plan_template(
                &descriptor,
                br#"{"subject":"s","text_body":"b","to":"to@example.com"}"#,
                composition.adapters[0].authority_facts_jcs,
                &registry,
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                now,
            ),
            Err(broker_host::BrokerHostError::RegistryRejected)
        ));
    }

    #[test]
    fn scope_normalization_and_discovery_are_profile_pinned_and_private() {
        let expected = NormalizedClaims {
            values: BTreeSet::from([GMAIL_SCOPE.into(), SHEETS_SCOPE.into()]),
        };
        assert_eq!(
            normalized_scopes(&format!("{SHEETS_SCOPE} {GMAIL_SCOPE} {GMAIL_SCOPE}")).unwrap(),
            expected
        );
        assert_eq!(
            normalized_scopes("openid").unwrap_err(),
            BrokerError::Brk109
        );
        let subject = discover_account(
            format!(r#"{{"user_id":"account-1","scope":"{GMAIL_SCOPE} {SHEETS_SCOPE}"}}"#)
                .as_bytes(),
            &expected,
        )
        .unwrap();
        assert!(!format!("{subject:?}").contains("account-1"));
    }
}
