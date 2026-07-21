use crate::{BrokerError, canonical, custodian::AccessMaterial, effect_id};
use sha2::{Digest, Sha256};
use std::{collections::VecDeque, sync::Mutex};

#[derive(Clone, Eq, PartialEq)]
pub struct FinalRequestPlan {
    canonical: Vec<u8>,
    approved_origin: String,
}
impl FinalRequestPlan {
    pub(crate) fn from_template(
        template: &[u8],
        logical_effect_id: &str,
        approved_origin: &str,
    ) -> Result<Self, BrokerError> {
        validate_https_origin(approved_origin)?;
        let canonical = canonical::canonicalize_bounded(template, canonical::MAX_OPERATION_BYTES)?;
        let mut value: serde_json::Value =
            serde_json::from_slice(canonical.as_bytes()).map_err(|_| BrokerError::Brk301)?;
        substitute(&mut value, &idempotency_key(logical_effect_id)?);
        let canonical = canonical::from_serde(&value, canonical::MAX_OPERATION_BYTES)?;
        Ok(Self {
            canonical: canonical.into_bytes(),
            approved_origin: approved_origin.to_owned(),
        })
    }
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical
    }
    /// Destination is broker-supplied and never read from planner JSON.
    pub fn destination_origin(&self) -> &str {
        &self.approved_origin
    }
    pub fn hash(&self) -> String {
        format!("sha256:{}", hex::encode(Sha256::digest(&self.canonical)))
    }
}
fn substitute(value: &mut serde_json::Value, key: &str) {
    match value {
        serde_json::Value::Object(map)
            if map.len() == 1
                && map.get("$broker")
                    == Some(&serde_json::Value::String("idempotency_key".into())) =>
        {
            *value = serde_json::Value::String(key.into())
        }
        serde_json::Value::Object(map) => {
            for value in map.values_mut() {
                substitute(value, key);
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                substitute(value, key);
            }
        }
        _ => {}
    }
}
pub fn validate_https_origin(origin: &str) -> Result<(), BrokerError> {
    let rest = origin.strip_prefix("https://").ok_or(BrokerError::Brk302)?;
    if rest.is_empty()
        || rest.contains(['/', '?', '#', '@'])
        || rest
            .bytes()
            .any(|b| b.is_ascii_whitespace() || !b.is_ascii())
        || rest.starts_with('.')
        || rest.ends_with('.')
    {
        return Err(BrokerError::Brk302);
    }
    Ok(())
}

pub fn idempotency_key(effect_id: &str) -> Result<String, BrokerError> {
    Ok(hex::encode(effect_id::digest_bytes(effect_id)?))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatchResponse {
    pub bounded_projection: Vec<u8>,
    pub provider_request_id: Option<String>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DispatchResult {
    Confirmed(DispatchResponse),
    Failed,
    Ambiguous,
}
pub trait ProviderDispatcher: Send + Sync {
    /// The only boundary that receives credential material. Implementations
    /// must derive the destination from `destination_origin`, never from a URL
    /// embedded in canonical planner-controlled JSON.
    fn dispatch(
        &self,
        plan: &FinalRequestPlan,
        access: AccessMaterial<'_>,
    ) -> Result<DispatchResult, BrokerError>;
}

#[derive(Clone, Debug)]
pub enum ScriptedDispatch {
    Confirmed {
        projection: Vec<u8>,
        provider_request_id: Option<String>,
    },
    Failed,
    TimeoutAmbiguous,
    Unavailable,
}
pub struct MockDispatcher {
    scripts: Mutex<VecDeque<ScriptedDispatch>>,
    recorded: Mutex<Vec<Vec<u8>>>,
}
impl MockDispatcher {
    pub fn new(scripts: impl IntoIterator<Item = ScriptedDispatch>) -> Self {
        Self {
            scripts: Mutex::new(scripts.into_iter().collect()),
            recorded: Mutex::new(Vec::new()),
        }
    }
    pub fn recorded_plans(&self) -> Vec<Vec<u8>> {
        self.recorded.lock().map(|v| v.clone()).unwrap_or_default()
    }
}
impl ProviderDispatcher for MockDispatcher {
    fn dispatch(
        &self,
        plan: &FinalRequestPlan,
        _access: AccessMaterial<'_>,
    ) -> Result<DispatchResult, BrokerError> {
        validate_https_origin(plan.destination_origin())?;
        if plan.canonical.len() > canonical::MAX_OPERATION_BYTES {
            return Err(BrokerError::Brk301);
        }
        self.recorded
            .lock()
            .map_err(|_| BrokerError::Brk401)?
            .push(plan.canonical.clone());
        match self
            .scripts
            .lock()
            .map_err(|_| BrokerError::Brk401)?
            .pop_front()
            .ok_or(BrokerError::Brk401)?
        {
            ScriptedDispatch::Confirmed {
                projection,
                provider_request_id,
            } => {
                if projection.len() > canonical::MAX_OPERATION_BYTES
                    || provider_request_id
                        .as_ref()
                        .is_some_and(|v| v.len() > 128 || !v.is_ascii())
                {
                    return Err(BrokerError::Brk305);
                }
                Ok(DispatchResult::Confirmed(DispatchResponse {
                    bounded_projection: projection,
                    provider_request_id,
                }))
            }
            ScriptedDispatch::Failed => Ok(DispatchResult::Failed),
            ScriptedDispatch::TimeoutAmbiguous => Ok(DispatchResult::Ambiguous),
            ScriptedDispatch::Unavailable => Err(BrokerError::Brk303),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fills_broker_slot_and_records_final() {
        let effect = crate::effect_id::derive("r", "n", 1, "s").unwrap();
        let plan = FinalRequestPlan::from_template(
            br#"{"key":{"$broker":"idempotency_key"}}"#,
            &effect,
            "https://provider.example",
        )
        .unwrap();
        assert!(
            std::str::from_utf8(plan.canonical_bytes())
                .unwrap()
                .contains(&idempotency_key(&effect).unwrap())
        );
    }
}
