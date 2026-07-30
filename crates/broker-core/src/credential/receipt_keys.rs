use crate::{BrokerError, artifacts::SignatureEnvelope, canonical, signing::BrokerVerifyingKey};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::Value;

const HISTORICAL_RECEIPT_DOMAIN: &str = "lattice.invocation-receipt.v0.2";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoricalReceiptClassification {
    Valid,
    HistoricallyAmbiguous,
    Invalid,
}

impl HistoricalReceiptClassification {
    pub const fn dispatch_authority(self) -> bool {
        false
    }

    pub const fn protocol_name(self) -> &'static str {
        match self {
            Self::Valid => "valid",
            Self::HistoricallyAmbiguous => "historically_ambiguous",
            Self::Invalid => "invalid",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct UtcInstant {
    pub seconds: i64,
    pub nanoseconds: u32,
}

pub fn parse_protocol_timestamp(value: &str) -> Result<UtcInstant, BrokerError> {
    let bytes = value.as_bytes();
    if !(20..=30).contains(&bytes.len())
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || bytes.get(10) != Some(&b'T')
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
        || bytes.last() != Some(&b'Z')
    {
        return Err(BrokerError::Brk004);
    }
    let parse = |range: std::ops::Range<usize>| -> Result<i64, BrokerError> {
        std::str::from_utf8(&bytes[range])
            .ok()
            .and_then(|part| part.parse().ok())
            .ok_or(BrokerError::Brk004)
    };
    let year = parse(0..4)?;
    let month = parse(5..7)?;
    let day = parse(8..10)?;
    let hour = parse(11..13)?;
    let minute = parse(14..16)?;
    let second = parse(17..19)?;
    if !(1..=12).contains(&month)
        || day < 1
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return Err(BrokerError::Brk004);
    }
    let nanoseconds = if bytes.len() == 20 {
        0
    } else {
        if bytes.get(19) != Some(&b'.') {
            return Err(BrokerError::Brk004);
        }
        let fraction = &bytes[20..bytes.len() - 1];
        if fraction.is_empty() || fraction.len() > 9 || !fraction.iter().all(u8::is_ascii_digit) {
            return Err(BrokerError::Brk004);
        }
        let mut nanos = std::str::from_utf8(fraction)
            .ok()
            .and_then(|part| part.parse::<u32>().ok())
            .ok_or(BrokerError::Brk004)?;
        for _ in fraction.len()..9 {
            nanos *= 10;
        }
        nanos
    };
    let days = days_from_civil(year, month, day);
    Ok(UtcInstant {
        seconds: days
            .checked_mul(86_400)
            .and_then(|value| value.checked_add(hour * 3_600 + minute * 60 + second))
            .ok_or(BrokerError::Brk004)?,
        nanoseconds,
    })
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let adjusted_year = year - i64::from(month <= 2);
    let era = adjusted_year.div_euclid(400);
    let year_of_era = adjusted_year - era * 400;
    let shifted_month = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

pub fn classify_historical_receipt_bytes(
    receipt: &[u8],
    keyset: &[u8],
    compromise: &[u8],
    key: &BrokerVerifyingKey,
) -> Result<HistoricalReceiptClassification, BrokerError> {
    let parse = |bytes: &[u8], maximum| -> Result<Value, BrokerError> {
        let canonical = canonical::canonicalize_bounded(bytes, maximum)?;
        serde_json::from_slice(canonical.as_bytes()).map_err(|_| BrokerError::Brk109)
    };
    classify_historical_receipt(
        &parse(receipt, 64 * 1024)?,
        &parse(keyset, canonical::MAX_OPERATION_BYTES)?,
        &parse(compromise, canonical::MAX_OPERATION_BYTES)?,
        key,
    )
}

fn classify_historical_receipt(
    receipt: &Value,
    keyset: &Value,
    compromise: &Value,
    key: &BrokerVerifyingKey,
) -> Result<HistoricalReceiptClassification, BrokerError> {
    super::model::validate("LS1HistoricalInvocationReceiptVerificationOnly", receipt)?;
    super::signing::verify_lifecycle_value("LS1ReceiptVerificationKeyset", keyset, key)?;
    super::signing::verify_lifecycle_value("LS1ReceiptKeyCompromiseRecord", compromise, key)?;
    let issuer = receipt
        .get("issuer")
        .and_then(Value::as_str)
        .ok_or(BrokerError::Brk109)?;
    let key_id = receipt
        .get("broker_key_id")
        .and_then(Value::as_str)
        .ok_or(BrokerError::Brk109)?;
    if receipt.pointer("/signature/key_id").and_then(Value::as_str) != Some(key_id)
        || keyset.get("receipt_issuer").and_then(Value::as_str) != Some(issuer)
    {
        return Ok(HistoricalReceiptClassification::Invalid);
    }
    let Some(receipt_key) = keyset
        .get("keys")
        .and_then(Value::as_array)
        .and_then(|keys| {
            keys.iter()
                .find(|item| item.get("key_id").and_then(Value::as_str) == Some(key_id))
        })
    else {
        return Ok(HistoricalReceiptClassification::Invalid);
    };
    let Some(public) = receipt_key
        .get("public_key_base64url")
        .and_then(Value::as_str)
        .and_then(|encoded| URL_SAFE_NO_PAD.decode(encoded).ok())
        .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
    else {
        return Ok(HistoricalReceiptClassification::Invalid);
    };
    let receipt_verifying_key = BrokerVerifyingKey::from_bytes(key_id, public)?;
    let issued = parse_protocol_timestamp(
        receipt
            .get("issued_at")
            .and_then(Value::as_str)
            .ok_or(BrokerError::Brk109)?,
    )?;
    let valid_from = parse_protocol_timestamp(
        receipt_key
            .get("valid_from")
            .and_then(Value::as_str)
            .ok_or(BrokerError::Brk109)?,
    )?;
    let valid_until = parse_protocol_timestamp(
        receipt_key
            .get("valid_until")
            .and_then(Value::as_str)
            .ok_or(BrokerError::Brk109)?,
    )?;
    if issued < valid_from || issued > valid_until {
        return Ok(HistoricalReceiptClassification::Invalid);
    }
    let signature: SignatureEnvelope = serde_json::from_value(
        receipt
            .get("signature")
            .cloned()
            .ok_or(BrokerError::Brk109)?,
    )
    .map_err(|_| BrokerError::Brk109)?;
    let canonical = canonical::from_serde(receipt, 1024 * 1024)?;
    if receipt_verifying_key
        .verify_json(HISTORICAL_RECEIPT_DOMAIN, canonical.as_bytes(), &signature)
        .is_err()
    {
        return Ok(HistoricalReceiptClassification::Invalid);
    }
    if compromise.get("receipt_issuer").and_then(Value::as_str) != Some(issuer)
        || compromise.get("affected_key_id").and_then(Value::as_str) != Some(key_id)
        || compromise.get("keyset_ref") != keyset.get("keyset_ref")
    {
        return Ok(HistoricalReceiptClassification::HistoricallyAmbiguous);
    }
    let revoked = parse_protocol_timestamp(
        compromise
            .get("revoked_at")
            .and_then(Value::as_str)
            .ok_or(BrokerError::Brk109)?,
    )?;
    let affected_start = parse_protocol_timestamp(
        compromise
            .get("affected_interval_start")
            .and_then(Value::as_str)
            .ok_or(BrokerError::Brk109)?,
    )?;
    let affected_end = parse_protocol_timestamp(
        compromise
            .get("affected_interval_end")
            .and_then(Value::as_str)
            .ok_or(BrokerError::Brk109)?,
    )?;
    if issued >= revoked {
        Ok(HistoricalReceiptClassification::Invalid)
    } else if issued >= affected_start && issued <= affected_end {
        Ok(HistoricalReceiptClassification::HistoricallyAmbiguous)
    } else {
        Ok(HistoricalReceiptClassification::Valid)
    }
}
