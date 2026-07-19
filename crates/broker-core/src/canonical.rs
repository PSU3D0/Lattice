use crate::BrokerError;
use sha2::{Digest, Sha256};
use std::{cmp::Ordering, collections::HashSet};

pub const MAX_DEPTH: usize = 64;
pub const MAX_OBJECT_MEMBERS: usize = 4_096;
pub const MAX_ARRAY_ELEMENTS: usize = 65_536;
pub const MAX_NAME_BYTES: usize = 256;
pub const MAX_STRING_BYTES: usize = 256 * 1024;
pub const MAX_OPERATION_BYTES: usize = 256 * 1024;
pub const MAX_EXACT_INTEGER: i64 = 9_007_199_254_740_991;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<Value>),
    Object(Vec<(String, Value)>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalJson(Vec<u8>);
impl CanonicalJson {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
    pub fn sha256(&self) -> String {
        format!("sha256:{}", hex::encode(Sha256::digest(&self.0)))
    }
}

pub fn canonicalize(input: &[u8]) -> Result<CanonicalJson, BrokerError> {
    canonicalize_bounded(input, usize::MAX)
}

pub fn canonicalize_bounded(
    input: &[u8],
    max_canonical_bytes: usize,
) -> Result<CanonicalJson, BrokerError> {
    if input.starts_with(&[0xef, 0xbb, 0xbf]) {
        return Err(BrokerError::Brk001);
    }
    let source = std::str::from_utf8(input).map_err(|_| BrokerError::Brk001)?;
    let mut parser = Parser { source, pos: 0 };
    let value = parser.value(0)?;
    parser.ws();
    if parser.pos != source.len() {
        return Err(BrokerError::Brk001);
    }
    let mut out = Vec::new();
    encode(&value, &mut out);
    if out.len() > max_canonical_bytes {
        return Err(BrokerError::Brk001);
    }
    Ok(CanonicalJson(out))
}

pub fn from_serde<T: serde::Serialize>(
    value: &T,
    max: usize,
) -> Result<CanonicalJson, BrokerError> {
    let bytes = serde_json::to_vec(value).map_err(|_| BrokerError::Brk001)?;
    canonicalize_bounded(&bytes, max)
}

fn encode(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(v) => out.extend_from_slice(if *v { b"true" } else { b"false" }),
        Value::Number(v) => out.extend_from_slice(ryu_js::Buffer::new().format(*v).as_bytes()),
        Value::String(v) => out.extend_from_slice(
            serde_json::to_string(v)
                .expect("string serialization")
                .as_bytes(),
        ),
        Value::Array(values) => {
            out.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    out.push(b',');
                }
                encode(value, out);
            }
            out.push(b']');
        }
        Value::Object(entries) => {
            let mut entries: Vec<_> = entries.iter().collect();
            entries.sort_by(|a, b| utf16_cmp(&a.0, &b.0));
            out.push(b'{');
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index != 0 {
                    out.push(b',');
                }
                out.extend_from_slice(
                    serde_json::to_string(key)
                        .expect("key serialization")
                        .as_bytes(),
                );
                out.push(b':');
                encode(value, out);
            }
            out.push(b'}');
        }
    }
}

fn utf16_cmp(left: &str, right: &str) -> Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

struct Parser<'a> {
    source: &'a str,
    pos: usize,
}
impl Parser<'_> {
    fn ws(&mut self) {
        while matches!(self.byte(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.pos += 1;
        }
    }
    fn byte(&self) -> Option<u8> {
        self.source.as_bytes().get(self.pos).copied()
    }
    fn value(&mut self, depth: usize) -> Result<Value, BrokerError> {
        self.ws();
        match self.byte() {
            Some(b'{') => self.object(depth + 1),
            Some(b'[') => self.array(depth + 1),
            Some(b'"') => Ok(Value::String(self.string()?)),
            Some(b't') => {
                self.literal("true")?;
                Ok(Value::Bool(true))
            }
            Some(b'f') => {
                self.literal("false")?;
                Ok(Value::Bool(false))
            }
            Some(b'n') => {
                self.literal("null")?;
                Ok(Value::Null)
            }
            Some(b'-' | b'0'..=b'9') => self.number(),
            _ => Err(BrokerError::Brk001),
        }
    }
    fn literal(&mut self, literal: &str) -> Result<(), BrokerError> {
        if self.source[self.pos..].starts_with(literal) {
            self.pos += literal.len();
            Ok(())
        } else {
            Err(BrokerError::Brk001)
        }
    }
    fn object(&mut self, depth: usize) -> Result<Value, BrokerError> {
        if depth > MAX_DEPTH {
            return Err(BrokerError::Brk001);
        }
        self.pos += 1;
        self.ws();
        let mut values = Vec::new();
        let mut keys = HashSet::new();
        if self.byte() == Some(b'}') {
            self.pos += 1;
            return Ok(Value::Object(values));
        }
        loop {
            self.ws();
            if self.byte() != Some(b'"') {
                return Err(BrokerError::Brk001);
            }
            let key = self.string()?;
            if key.as_bytes().len() > MAX_NAME_BYTES || !keys.insert(key.clone()) {
                return Err(BrokerError::Brk001);
            }
            self.ws();
            if self.byte() != Some(b':') {
                return Err(BrokerError::Brk001);
            }
            self.pos += 1;
            values.push((key, self.value(depth)?));
            if values.len() > MAX_OBJECT_MEMBERS {
                return Err(BrokerError::Brk001);
            }
            self.ws();
            match self.byte() {
                Some(b',') => self.pos += 1,
                Some(b'}') => {
                    self.pos += 1;
                    break;
                }
                _ => return Err(BrokerError::Brk001),
            }
        }
        Ok(Value::Object(values))
    }
    fn array(&mut self, depth: usize) -> Result<Value, BrokerError> {
        if depth > MAX_DEPTH {
            return Err(BrokerError::Brk001);
        }
        self.pos += 1;
        self.ws();
        let mut values = Vec::new();
        if self.byte() == Some(b']') {
            self.pos += 1;
            return Ok(Value::Array(values));
        }
        loop {
            values.push(self.value(depth)?);
            if values.len() > MAX_ARRAY_ELEMENTS {
                return Err(BrokerError::Brk001);
            }
            self.ws();
            match self.byte() {
                Some(b',') => self.pos += 1,
                Some(b']') => {
                    self.pos += 1;
                    break;
                }
                _ => return Err(BrokerError::Brk001),
            }
        }
        Ok(Value::Array(values))
    }
    fn string(&mut self) -> Result<String, BrokerError> {
        let start = self.pos;
        self.pos += 1;
        let bytes = self.source.as_bytes();
        let mut escaped = false;
        while self.pos < bytes.len() {
            let byte = bytes[self.pos];
            if !escaped && byte == b'"' {
                self.pos += 1;
                let decoded: String = serde_json::from_str(&self.source[start..self.pos])
                    .map_err(|_| BrokerError::Brk001)?;
                if decoded.len() > MAX_STRING_BYTES {
                    return Err(BrokerError::Brk001);
                }
                return Ok(decoded);
            }
            if !escaped && byte < 0x20 {
                return Err(BrokerError::Brk001);
            }
            if !escaped && byte == b'\\' {
                escaped = true;
            } else {
                escaped = false;
            }
            self.pos += 1;
        }
        Err(BrokerError::Brk001)
    }
    fn number(&mut self) -> Result<Value, BrokerError> {
        let start = self.pos;
        if self.byte() == Some(b'-') {
            self.pos += 1;
        }
        match self.byte() {
            Some(b'0') => {
                self.pos += 1;
                if matches!(self.byte(), Some(b'0'..=b'9')) {
                    return Err(BrokerError::Brk001);
                }
            }
            Some(b'1'..=b'9') => {
                while matches!(self.byte(), Some(b'0'..=b'9')) {
                    self.pos += 1;
                }
            }
            _ => return Err(BrokerError::Brk001),
        }
        let integer_token = self.byte() != Some(b'.') && !matches!(self.byte(), Some(b'e' | b'E'));
        if self.byte() == Some(b'.') {
            self.pos += 1;
            let before = self.pos;
            while matches!(self.byte(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
            if self.pos == before {
                return Err(BrokerError::Brk001);
            }
        }
        if matches!(self.byte(), Some(b'e' | b'E')) {
            self.pos += 1;
            if matches!(self.byte(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            let before = self.pos;
            while matches!(self.byte(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
            if self.pos == before {
                return Err(BrokerError::Brk001);
            }
        }
        let raw = &self.source[start..self.pos];
        let value: f64 = raw.parse().map_err(|_| BrokerError::Brk001)?;
        if !value.is_finite() || (value == 0.0 && raw.bytes().any(|b| matches!(b, b'1'..=b'9'))) {
            return Err(BrokerError::Brk001);
        }
        if integer_token {
            let exact: i64 = raw.parse().map_err(|_| BrokerError::Brk001)?;
            if exact.unsigned_abs() > MAX_EXACT_INTEGER as u64 {
                return Err(BrokerError::Brk001);
            }
        }
        Ok(Value::Number(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest;

    #[test]
    fn all_normative_vectors() {
        let cases = [
            (
                r#"{}"#,
                "7b7d",
                "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
            ),
            (
                r#"[]"#,
                "5b5d",
                "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945",
            ),
            (
                r#"{"b":1,"a":2}"#,
                "7b2261223a322c2262223a317d",
                "d3626ac30a87e6f7a6428233b3c68299976865fa5508e4267c5415c76af7a772",
            ),
            (
                r#"{"z":{"b":false,"a":null},"a":[3,2,1]}"#,
                "7b2261223a5b332c322c315d2c227a223a7b2261223a6e756c6c2c2262223a66616c73657d7d",
                "96f8d6beb512491404b2479bff1cf19741b07dedb4ad68e7a9ab9e1b0a028ca2",
            ),
            (
                "{\"s\":\"line\\nquote\\\"\"}",
                "7b2273223a226c696e655c6e71756f74655c22227d",
                "6f122115d79068199d627109d4b9bd60f52696b5dccbc933aa0cf9cd470ffb4d",
            ),
            (
                r#"{"u":"\u00e9"}"#,
                "7b2275223a22c3a9227d",
                "606ffff9f63ae3058a32788b12169fffef7f4f86e8e34e22cf3056949620ab37",
            ),
            (
                r#"{"u":"e\u0301"}"#,
                "7b2275223a2265cc81227d",
                "6a5fd66a30d6c934c359406ff8dffdca28f728ed12ae329bccebf933570da4be",
            ),
            (
                r#"{"n":-0}"#,
                "7b226e223a307d",
                "f3013f933b9fb80ab6d995e7ad9da36f683837ba1d81e950c943d40111eac2f0",
            ),
            (
                r#"{"n":1.5}"#,
                "7b226e223a312e357d",
                "cb14d55cfe562fd6592d919f5dfacfa8708687b746a1d110c6dd5529c410e772",
            ),
            (
                r#"{"n":9007199254740991}"#,
                "7b226e223a393030373139393235343734303939317d",
                "e1da48c6a6089f06ecb4e0a2259e658e3786b2420f52baccdf929ec6460d7b41",
            ),
            (
                r#"{"n":-9007199254740991}"#,
                "7b226e223a2d393030373139393235343734303939317d",
                "d49d713821fc149f81ef6ca8054beeba696f5da052f0ab3e2d773808c5a9d625",
            ),
            (
                r#"{"n":1.7976931348623157e308}"#,
                "7b226e223a312e37393736393331333438363233313537652b3330387d",
                "9599e0f9672bfa5d654b432fa5f9b7ee04f35c607b3a3ff01ce97b070f8f2648",
            ),
            (
                "{\"\\u20ac\":1,\"1\":3,\"\\r\":2}",
                "7b225c72223a322c2231223a332c22e282ac223a317d",
                "c09bf1b4a80778801254479094b6c4055a41ffbcc98fd721d4a89c27c9d465fa",
            ),
            (
                r#"{"b":{},"a":[]}"#,
                "7b2261223a5b5d2c2262223a7b7d7d",
                "9959f7ea5ff37e0cf81634a894845a335eb6e26fbad0877944e9bc009b4f0644",
            ),
        ];
        for (input, expected_hex, expected_hash) in cases {
            let actual = canonicalize(input.as_bytes()).unwrap();
            assert_eq!(hex::encode(actual.as_bytes()), expected_hex);
            assert_eq!(
                hex::encode(Sha256::digest(actual.as_bytes())),
                expected_hash
            );
        }
    }

    #[test]
    fn mandatory_rejections() {
        for input in [
            r#"{"a":1,"\u0061":2}"#,
            r#""\ud800""#,
            "9007199254740992",
            "1e400",
            "1e-400",
            "NaN",
            "Infinity",
        ] {
            assert_eq!(
                canonicalize(input.as_bytes()),
                Err(BrokerError::Brk001),
                "{input}"
            );
        }
        assert!(canonicalize(b"\xef\xbb\xbf{}").is_err());
        assert!(canonicalize(b"{} trailing").is_err());
        assert!(canonicalize(&[0xff]).is_err());
    }
}
