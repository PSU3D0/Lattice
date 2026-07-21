//! Shared Gmail plumbing: paths, scopes, and the RFC 2822 + base64url message
//! composition the Gmail REST API expects for outbound sends.
//!
//! The Gmail `users.messages.send` endpoint takes a JSON body whose `raw`
//! field carries a full RFC 2822 message, encoded with the URL-safe base64
//! alphabet and no `=` padding. Both helpers live here (pure, unit-tested) so
//! connector crates only deal in semantic fields.

pub const GOOGLE_GMAIL_BASE_URL: &str = "https://www.googleapis.com";
pub const GOOGLE_GMAIL_SEND_SCOPE: &str = "https://www.googleapis.com/auth/gmail.send";

pub const fn gmail_send_message_path() -> &'static str {
    "/gmail/v1/users/me/messages/send"
}

/// Compose a minimal plain-text RFC 2822 message. Headers use CRLF line
/// endings; the body is passed through verbatim as UTF-8 text.
///
/// Note: header values are used as-is (no RFC 2047 encoded-word encoding).
/// Callers should keep `to`/`cc`/`bcc`/`subject` ASCII-safe; Gmail tolerates
/// UTF-8 header bytes in `raw` payloads but strict MTAs may not.
pub fn build_plain_text_email(
    to: &str,
    cc: Option<&str>,
    bcc: Option<&str>,
    subject: &str,
    text_body: &str,
) -> String {
    let mut message = String::new();
    message.push_str(&format!("To: {to}\r\n"));
    if let Some(cc) = cc {
        message.push_str(&format!("Cc: {cc}\r\n"));
    }
    if let Some(bcc) = bcc {
        message.push_str(&format!("Bcc: {bcc}\r\n"));
    }
    message.push_str(&format!("Subject: {subject}\r\n"));
    message.push_str("MIME-Version: 1.0\r\n");
    message.push_str("Content-Type: text/plain; charset=\"UTF-8\"\r\n");
    message.push_str("\r\n");
    message.push_str(text_body);
    message
}

const BASE64URL_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// URL-safe base64 without padding — the variant Gmail's `raw` field expects.
pub fn base64url_no_pad(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;

        out.push(BASE64URL_ALPHABET[(triple >> 18) as usize & 0x3f] as char);
        out.push(BASE64URL_ALPHABET[(triple >> 12) as usize & 0x3f] as char);
        if chunk.len() > 1 {
            out.push(BASE64URL_ALPHABET[(triple >> 6) as usize & 0x3f] as char);
        }
        if chunk.len() > 2 {
            out.push(BASE64URL_ALPHABET[triple as usize & 0x3f] as char);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64url_no_pad_matches_known_vectors() {
        assert_eq!(base64url_no_pad(b""), "");
        assert_eq!(base64url_no_pad(b"f"), "Zg");
        assert_eq!(base64url_no_pad(b"fo"), "Zm8");
        assert_eq!(base64url_no_pad(b"foo"), "Zm9v");
        assert_eq!(base64url_no_pad(b"foob"), "Zm9vYg");
        assert_eq!(base64url_no_pad(b"fooba"), "Zm9vYmE");
        assert_eq!(base64url_no_pad(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn base64url_no_pad_uses_url_safe_alphabet() {
        // 0xfb 0xff encodes to characters that would be `+` / `/` in the
        // standard alphabet; here they must be `-` / `_` and unpadded.
        let encoded = base64url_no_pad(&[0xfb, 0xff]);
        assert_eq!(encoded, "-_8");
        assert!(!encoded.contains('+'));
        assert!(!encoded.contains('/'));
        assert!(!encoded.contains('='));
    }

    #[test]
    fn plain_text_email_has_crlf_headers_and_blank_line() {
        let message = build_plain_text_email(
            "ops@example.test",
            Some("audit@example.test"),
            None,
            "Weekly report",
            "line one\nline two",
        );
        assert_eq!(
            message,
            "To: ops@example.test\r\nCc: audit@example.test\r\nSubject: Weekly report\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=\"UTF-8\"\r\n\r\nline one\nline two"
        );
    }

    #[test]
    fn plain_text_email_omits_missing_recipient_headers() {
        let message = build_plain_text_email("a@example.test", None, None, "Hi", "body");
        assert!(!message.contains("Cc:"));
        assert!(!message.contains("Bcc:"));
    }
}
