use std::collections::{BTreeMap, BTreeSet};

use broker_core::BrokerError;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryPin {
    pub entry_ref: String,
    pub version: String,
    pub definition_hash: String,
    pub approval_epoch: u64,
    pub revocation_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActivationKind {
    OAuthAuthorizationCodePkce,
    SecretSubmission,
    ExternalCustodianBinding,
    WorkloadBinding,
    None,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthScheme {
    OAuthPkce {
        authorization_endpoint_key: String,
        token_endpoint_key: String,
        revocation_endpoint_key: String,
        principal_endpoint_key: String,
        client_auth_binding: String,
    },
    HeaderKey {
        name: String,
        prefix: String,
    },
    QueryKey {
        name: String,
    },
    Bearer {
        header: String,
        prefix: String,
    },
    Basic {
        header: String,
    },
    SignedRequest {
        region: String,
        service: String,
        timestamp_header: String,
        signed_headers: BTreeSet<String>,
    },
    WorkloadTokenExchange {
        exchange_endpoint_key: String,
        trusted_issuers: BTreeSet<String>,
        audience: String,
        maximum_assertion_age_seconds: u64,
    },
    ExternalCustodian {
        allowed_custodians: Vec<RegistryPin>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublicClaimsPolicy {
    None,
    Allowlisted(BTreeSet<String>),
}

#[derive(Clone, Debug)]
pub struct AuthProfile {
    pub profile_ref: String,
    pub version: String,
    pub definition_hash: String,
    pub connector_ref: String,
    pub activation: ActivationKind,
    pub scheme_ref: String,
    pub scheme: AuthScheme,
    pub endpoints: BTreeMap<String, String>,
    pub callback_uri: String,
    pub contract_claims: BTreeMap<String, BTreeSet<String>>,
    pub lifecycle: BTreeSet<String>,
    pub material_schema_hash: String,
    pub assertion_schema_hash: Option<String>,
    pub claims_schema_hash: String,
    pub public_claims: PublicClaimsPolicy,
    pub auth_driver: RegistryPin,
    pub claim_normalizer: RegistryPin,
    pub custodian: RegistryPin,
    pub transport: RegistryPin,
    pub response_firewall: RegistryPin,
}

impl AuthProfile {
    pub fn key(&self) -> (&str, &str) {
        (&self.profile_ref, &self.version)
    }

    pub fn validate(&self) -> Result<(), BrokerError> {
        if self.profile_ref.is_empty()
            || self.version.is_empty()
            || self.connector_ref.is_empty()
            || self.definition_hash.is_empty()
            || self.contract_claims.is_empty()
        {
            return Err(BrokerError::Brk004);
        }
        if self.lifecycle.contains("device_authorization") {
            return Err(BrokerError::Brk004);
        }
        if !self.callback_uri.starts_with("https://")
            || !self.callback_uri.ends_with("/v0.2/credential-callback")
        {
            return Err(BrokerError::Brk302);
        }
        for endpoint in self.endpoints.values() {
            validate_https_endpoint(endpoint)?;
        }
        match (&self.activation, &self.scheme) {
            (
                ActivationKind::OAuthAuthorizationCodePkce,
                AuthScheme::OAuthPkce {
                    authorization_endpoint_key,
                    token_endpoint_key,
                    revocation_endpoint_key,
                    principal_endpoint_key,
                    ..
                },
            ) => {
                for key in [
                    authorization_endpoint_key,
                    token_endpoint_key,
                    revocation_endpoint_key,
                    principal_endpoint_key,
                ] {
                    if !self.endpoints.contains_key(key) {
                        return Err(BrokerError::Brk302);
                    }
                }
            }
            (
                ActivationKind::SecretSubmission,
                AuthScheme::HeaderKey { name, prefix }
                | AuthScheme::Bearer {
                    header: name,
                    prefix,
                },
            ) if valid_header_name(name) && !prefix.bytes().any(|byte| byte.is_ascii_control()) => {
            }
            (ActivationKind::SecretSubmission, AuthScheme::QueryKey { name })
                if valid_placement_name(name) => {}
            (ActivationKind::SecretSubmission, AuthScheme::Basic { header })
                if header.eq_ignore_ascii_case("authorization") => {}
            (
                ActivationKind::SecretSubmission,
                AuthScheme::SignedRequest {
                    region,
                    service,
                    timestamp_header,
                    signed_headers,
                },
            ) if valid_placement_name(region)
                && valid_placement_name(service)
                && valid_header_name(timestamp_header)
                && !signed_headers.is_empty()
                && signed_headers.iter().all(|name| valid_header_name(name)) => {}
            (
                ActivationKind::WorkloadBinding,
                AuthScheme::WorkloadTokenExchange {
                    exchange_endpoint_key,
                    ..
                },
            ) if self.endpoints.contains_key(exchange_endpoint_key) => {}
            (
                ActivationKind::ExternalCustodianBinding,
                AuthScheme::ExternalCustodian { allowed_custodians },
            ) if !allowed_custodians.is_empty() => {}
            (ActivationKind::None, _) => {}
            _ => return Err(BrokerError::Brk004),
        }
        Ok(())
    }

    pub fn endpoint(&self, key: &str) -> Result<&str, BrokerError> {
        self.endpoints
            .get(key)
            .map(String::as_str)
            .ok_or(BrokerError::Brk302)
    }

    pub fn derive_claims<'a>(
        &self,
        connector_ref: &str,
        contract_ids: impl IntoIterator<Item = &'a str>,
    ) -> Result<NormalizedClaims, BrokerError> {
        if connector_ref != self.connector_ref {
            return Err(BrokerError::Brk109);
        }
        let mut claims = BTreeSet::new();
        let mut saw_contract = false;
        for contract in contract_ids {
            let required = self
                .contract_claims
                .get(contract)
                .ok_or(BrokerError::Brk109)?;
            claims.extend(required.iter().cloned());
            saw_contract = true;
        }
        if !saw_contract || claims.is_empty() {
            return Err(BrokerError::Brk109);
        }
        Ok(NormalizedClaims { values: claims })
    }
}

fn valid_placement_name(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn valid_header_name(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn validate_https_endpoint(endpoint: &str) -> Result<(), BrokerError> {
    if !endpoint.starts_with("https://")
        || endpoint.contains('#')
        || endpoint.bytes().any(|byte| byte.is_ascii_control())
        || endpoint
            .split("//")
            .nth(1)
            .is_none_or(|rest| rest.is_empty())
    {
        return Err(BrokerError::Brk302);
    }
    Ok(())
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedClaims {
    pub values: BTreeSet<String>,
}

impl std::fmt::Debug for NormalizedClaims {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NormalizedClaims([PRIVATE])")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimRelation {
    Equal,
    ProperSubset,
    ProperSuperset,
    Incomparable,
}

impl NormalizedClaims {
    pub fn compare(&self, other: &Self) -> ClaimRelation {
        if self == other {
            ClaimRelation::Equal
        } else if self.values.is_subset(&other.values) {
            ClaimRelation::ProperSubset
        } else if other.values.is_subset(&self.values) {
            ClaimRelation::ProperSuperset
        } else {
            ClaimRelation::Incomparable
        }
    }

    pub fn safe_projection(&self, policy: &PublicClaimsPolicy) -> Option<BTreeMap<String, bool>> {
        match policy {
            PublicClaimsPolicy::None => None,
            PublicClaimsPolicy::Allowlisted(allowlist) => Some(
                allowlist
                    .iter()
                    .map(|name| (name.clone(), self.values.contains(name)))
                    .collect(),
            ),
        }
    }
}

#[derive(Default)]
pub struct ApprovedRegistry {
    profiles: BTreeMap<(String, String), AuthProfile>,
}

impl ApprovedRegistry {
    pub fn load_static(
        profiles: impl IntoIterator<Item = AuthProfile>,
    ) -> Result<Self, BrokerError> {
        let mut registry = Self::default();
        for profile in profiles {
            profile.validate()?;
            let key = (profile.profile_ref.clone(), profile.version.clone());
            if registry.profiles.insert(key, profile).is_some() {
                return Err(BrokerError::Brk203);
            }
        }
        Ok(registry)
    }

    pub fn profile(&self, profile_ref: &str, version: &str) -> Result<&AuthProfile, BrokerError> {
        self.profiles
            .get(&(profile_ref.to_owned(), version.to_owned()))
            .ok_or(BrokerError::Brk004)
    }

    pub fn profile_for_connector(
        &self,
        connector_ref: &str,
        profile_ref: &str,
        version: &str,
    ) -> Result<&AuthProfile, BrokerError> {
        self.profile(profile_ref, version).and_then(|profile| {
            if profile.connector_ref == connector_ref {
                Ok(profile)
            } else {
                Err(BrokerError::Brk109)
            }
        })
    }

    pub fn len(&self) -> usize {
        self.profiles.len()
    }
    pub fn is_empty(&self) -> bool {
        self.profiles.is_empty()
    }
}
