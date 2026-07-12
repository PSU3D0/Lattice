//! `multipart/form-data` body assembly for the connector.http byte-egress ops
//! (`post_multipart`/`put_multipart`, spec §16.5).
//!
//! The bytes are already resolved (inline, or dereferenced from an `Artifact`
//! handle through the `workspace_read()` view) by the time they reach here —
//! this module is pure byte assembly with no capability access.

use capabilities::artifact::sha256_hex;

/// One resolved part: the field name, an optional filename, the part's
/// `Content-Type`, and the raw bytes.
pub struct ResolvedPart {
    pub field: String,
    pub filename: Option<String>,
    pub content_type: String,
    pub bytes: Vec<u8>,
}

/// An assembled multipart body plus the `Content-Type` header value (which
/// carries the boundary) to set on the request.
pub struct BuiltMultipart {
    pub content_type: String,
    pub body: Vec<u8>,
}

/// Assemble `parts` into a `multipart/form-data` body. The boundary is derived
/// deterministically from the parts' bytes (a hash prefix) so it is stable
/// across replays and cannot collide with the content it delimits.
pub fn build_multipart(parts: &[ResolvedPart]) -> BuiltMultipart {
    let boundary = derive_boundary(parts);
    let mut body = Vec::new();
    for part in parts {
        push_str(&mut body, "--");
        push_str(&mut body, &boundary);
        push_str(&mut body, "\r\n");

        push_str(&mut body, "Content-Disposition: form-data; name=\"");
        push_str(&mut body, &escape_quoted(&part.field));
        push_str(&mut body, "\"");
        if let Some(filename) = &part.filename {
            push_str(&mut body, "; filename=\"");
            push_str(&mut body, &escape_quoted(filename));
            push_str(&mut body, "\"");
        }
        push_str(&mut body, "\r\n");

        push_str(&mut body, "Content-Type: ");
        push_str(&mut body, &part.content_type);
        push_str(&mut body, "\r\n\r\n");

        body.extend_from_slice(&part.bytes);
        push_str(&mut body, "\r\n");
    }
    push_str(&mut body, "--");
    push_str(&mut body, &boundary);
    push_str(&mut body, "--\r\n");

    BuiltMultipart {
        content_type: format!("multipart/form-data; boundary={boundary}"),
        body,
    }
}

fn derive_boundary(parts: &[ResolvedPart]) -> String {
    let mut acc = Vec::new();
    for part in parts {
        acc.extend_from_slice(part.field.as_bytes());
        acc.push(0);
        acc.extend_from_slice(&part.bytes);
        acc.push(0);
    }
    let digest = sha256_hex(&acc);
    format!("----LatticeBoundary{}", &digest[..16])
}

fn push_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(s.as_bytes());
}

/// Quote-escape a `Content-Disposition` parameter value. CR/LF are stripped
/// (header-injection defense, §10); embedded quotes/backslashes are escaped.
fn escape_quoted(value: &str) -> String {
    value
        .chars()
        .filter(|ch| !matches!(ch, '\r' | '\n' | '\0'))
        .flat_map(|ch| match ch {
            '"' | '\\' => vec!['\\', ch],
            other => vec![other],
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assembles_well_formed_body_with_stable_boundary() {
        let parts = vec![
            ResolvedPart {
                field: "file".to_string(),
                filename: Some("report.csv".to_string()),
                content_type: "text/csv".to_string(),
                bytes: b"a,b\n1,2\n".to_vec(),
            },
            ResolvedPart {
                field: "kind".to_string(),
                filename: None,
                content_type: "text/plain".to_string(),
                bytes: b"daily".to_vec(),
            },
        ];
        let built = build_multipart(&parts);
        let text = String::from_utf8(built.body.clone()).expect("utf8 body");

        assert!(
            built
                .content_type
                .starts_with("multipart/form-data; boundary=----LatticeBoundary")
        );
        let boundary = built
            .content_type
            .trim_start_matches("multipart/form-data; boundary=");
        assert!(text.contains(&format!("--{boundary}\r\n")));
        assert!(text.contains(
            "Content-Disposition: form-data; name=\"file\"; filename=\"report.csv\"\r\n"
        ));
        assert!(text.contains("Content-Type: text/csv\r\n\r\na,b\n1,2\n\r\n"));
        assert!(text.contains("Content-Disposition: form-data; name=\"kind\"\r\n"));
        assert!(text.ends_with(&format!("--{boundary}--\r\n")));

        // Deterministic: same parts -> same boundary.
        assert_eq!(build_multipart(&parts).content_type, built.content_type);
    }
}
