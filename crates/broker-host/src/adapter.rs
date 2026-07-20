use connector_google_platform::broker::{
    GMAIL_RFC822_ADAPTER_HASH, GMAIL_RFC822_ADAPTER_ID, GMAIL_RFC822_ADAPTER_VERSION,
    SHEETS_APPEND_ROW_ADAPTER_HASH, SHEETS_APPEND_ROW_ADAPTER_ID,
    SHEETS_APPEND_ROW_ADAPTER_VERSION, adapt_gmail_rfc822_message, adapt_sheets_append_row,
};
use connector_spec::TrustedAdapterPin;

use crate::BrokerHostError;

/// Closed set of first-party adapters installed by authenticated host
/// bootstrap. There is deliberately no public registration or callback API.
#[derive(Clone, Debug)]
pub struct TrustedAdapterRegistry {
    google_v1: bool,
}

impl TrustedAdapterRegistry {
    pub fn google_v1() -> Self {
        Self { google_v1: true }
    }

    pub fn empty() -> Self {
        Self { google_v1: false }
    }

    pub(crate) fn adapt(
        &self,
        pin: &TrustedAdapterPin,
        input: &serde_json::Value,
        authority_facts: &serde_json::Value,
    ) -> Result<serde_json::Map<String, serde_json::Value>, BrokerHostError> {
        if !self.google_v1 {
            return Err(BrokerHostError::DescriptorMismatch);
        }
        match (
            pin.trusted_adapter_id.as_str(),
            pin.implementation_version.as_str(),
            pin.implementation_hash.as_str(),
        ) {
            (
                SHEETS_APPEND_ROW_ADAPTER_ID,
                SHEETS_APPEND_ROW_ADAPTER_VERSION,
                SHEETS_APPEND_ROW_ADAPTER_HASH,
            ) => adapt_sheets_append_row(input, authority_facts)
                .map_err(|_| BrokerHostError::DescriptorMismatch),
            (GMAIL_RFC822_ADAPTER_ID, GMAIL_RFC822_ADAPTER_VERSION, GMAIL_RFC822_ADAPTER_HASH) => {
                adapt_gmail_rfc822_message(input).map_err(|_| BrokerHostError::DescriptorMismatch)
            }
            _ => Err(BrokerHostError::DescriptorMismatch),
        }
    }
}
