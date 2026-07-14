use pdf_extract::{Document, PlainTextOutput};
use std::cell::UnsafeCell;
use std::io::{self, Write};

const MAX_INPUT_BYTES: usize = 8 * 1024 * 1024;
const MAX_TEXT_BYTES: usize = 512 * 1024;
const MAX_PAGES: usize = 200;
const UNSUPPORTED_DOCUMENT: i32 = 1;

struct GuestState(UnsafeCell<State>);

struct State {
    input: Vec<u8>,
    output: Vec<u8>,
}

unsafe impl Sync for GuestState {}

static STATE: GuestState = GuestState(UnsafeCell::new(State {
    input: Vec::new(),
    output: Vec::new(),
}));

unsafe fn state() -> &'static mut State {
    &mut *STATE.0.get()
}

#[no_mangle]
pub unsafe extern "C" fn lf_alloc(input_len: i32) -> i32 {
    let Ok(input_len) = usize::try_from(input_len) else {
        return -1;
    };
    if input_len > MAX_INPUT_BYTES {
        return -1;
    }

    let state = state();
    state.input.clear();
    if state.input.try_reserve_exact(input_len).is_err() {
        return -1;
    }
    state.input.resize(input_len, 0);
    state.input.as_mut_ptr() as i32
}

#[no_mangle]
pub unsafe extern "C" fn lf_transform(input_ptr: i32, input_len: i32) -> i32 {
    let state = state();
    state.output.clear();
    let Ok(input_len) = usize::try_from(input_len) else {
        return UNSUPPORTED_DOCUMENT;
    };
    if input_len > MAX_INPUT_BYTES
        || input_len != state.input.len()
        || input_ptr != state.input.as_ptr() as i32
    {
        return UNSUPPORTED_DOCUMENT;
    }

    // lopdf attempts to parse/decrypt before callers can inspect is_encrypted().
    // Reject the original bytes first, including escaped spellings of the PDF
    // name. Scanning streams and comments too is intentionally conservative.
    if contains_encrypt_name(&state.input) {
        return UNSUPPORTED_DOCUMENT;
    }
    let Ok(document) = Document::load_mem(&state.input) else {
        return UNSUPPORTED_DOCUMENT;
    };
    if document.is_encrypted() {
        return UNSUPPORTED_DOCUMENT;
    }
    let pages = document.get_pages().len();
    if pages == 0 || pages > MAX_PAGES {
        return UNSUPPORTED_DOCUMENT;
    }

    let mut sink = BoundedCanonicalSink::new(MAX_TEXT_BYTES);
    {
        let writer: &mut dyn Write = &mut sink;
        let mut text_output = PlainTextOutput::new(writer);
        if pdf_extract::output_doc(&document, &mut text_output).is_err() {
            return UNSUPPORTED_DOCUMENT;
        }
    }
    if sink.finish().is_err() {
        return UNSUPPORTED_DOCUMENT;
    }
    if !contains_meaningful_text(&sink.bytes) {
        return UNSUPPORTED_DOCUMENT;
    }

    if state
        .output
        .try_reserve_exact(4 + sink.bytes.len())
        .is_err()
    {
        return UNSUPPORTED_DOCUMENT;
    }
    state
        .output
        .extend_from_slice(&(pages as u32).to_le_bytes());
    state.output.extend_from_slice(&sink.bytes);
    0
}

fn contains_encrypt_name(input: &[u8]) -> bool {
    const EXPECTED: &[u8] = b"Encrypt";

    let mut index = 0;
    while index < input.len() {
        if input[index] != b'/' {
            index += 1;
            continue;
        }
        index += 1;

        let mut decoded_len = 0;
        let mut matches = true;
        while index < input.len() && !is_pdf_delimiter(input[index]) {
            let (decoded, consumed) = if input[index] == b'#' && index + 2 < input.len() {
                match (hex_value(input[index + 1]), hex_value(input[index + 2])) {
                    (Some(high), Some(low)) => ((high << 4) | low, 3),
                    _ => (input[index], 1),
                }
            } else {
                (input[index], 1)
            };
            if EXPECTED.get(decoded_len) != Some(&decoded) {
                matches = false;
            }
            decoded_len += 1;
            index += consumed;
        }
        if matches && decoded_len == EXPECTED.len() {
            return true;
        }
    }
    false
}

fn is_pdf_delimiter(byte: u8) -> bool {
    matches!(
        byte,
        0 | b'\t'
            | b'\n'
            | b'\x0c'
            | b'\r'
            | b' '
            | b'('
            | b')'
            | b'<'
            | b'>'
            | b'['
            | b']'
            | b'{'
            | b'}'
            | b'/'
            | b'%'
    )
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn contains_meaningful_text(bytes: &[u8]) -> bool {
    std::str::from_utf8(bytes)
        .map(|text| {
            text.chars()
                .any(|character| !character.is_whitespace() && character != '\u{fffd}')
        })
        .unwrap_or(false)
}

#[no_mangle]
pub unsafe extern "C" fn lf_output_ptr() -> i32 {
    state().output.as_ptr() as i32
}

#[no_mangle]
pub unsafe extern "C" fn lf_output_len() -> i32 {
    state().output.len() as i32
}

struct BoundedCanonicalSink {
    bytes: Vec<u8>,
    limit: usize,
    pending_cr: bool,
}

impl BoundedCanonicalSink {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
            pending_cr: false,
        }
    }

    fn push_char(&mut self, character: char) -> io::Result<()> {
        let character = match character {
            '\t' | '\n' => character,
            '\u{0}'..='\u{8}' | '\u{b}'..='\u{1f}' | '\u{7f}' => '\u{fffd}',
            other => other,
        };
        let mut encoded = [0; 4];
        self.push_bytes(character.encode_utf8(&mut encoded).as_bytes())
    }

    fn push_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        let new_len = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|new_len| *new_len <= self.limit)
            .ok_or_else(|| io::Error::other("output limit exceeded"))?;
        self.bytes.reserve(new_len - self.bytes.len());
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        if self.pending_cr {
            self.pending_cr = false;
            self.push_char('\n')?;
        }
        Ok(())
    }
}

impl Write for BoundedCanonicalSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let text = std::str::from_utf8(bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-UTF-8 parser output"))?;
        for character in text.chars() {
            if self.pending_cr {
                self.pending_cr = false;
                self.push_char('\n')?;
                if character == '\n' {
                    continue;
                }
            }
            if character == '\r' {
                self.pending_cr = true;
            } else {
                self.push_char(character)?;
            }
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn canonicalize(limit: usize, chunks: &[&[u8]]) -> io::Result<Vec<u8>> {
        let mut sink = BoundedCanonicalSink::new(limit);
        for chunk in chunks {
            sink.write_all(chunk)?;
        }
        sink.finish()?;
        Ok(sink.bytes)
    }

    #[test]
    fn encrypt_scanner_decodes_pdf_name_escapes_and_requires_exact_name() {
        for input in [
            &b"trailer << /Encrypt 1 0 R >>"[..],
            &b"trailer << /#45ncrypt 1 0 R >>"[..],
            &b"trailer << /Encr#79pt 1 0 R >>"[..],
            &b"trailer << /Encrypt/Next >>"[..],
        ] {
            assert!(contains_encrypt_name(input), "input: {input:?}");
        }
        for input in [
            &b"/encrypt"[..],
            &b"/Encrypted"[..],
            &b"/Encrypt#"[..],
            &b"/Encr#7Zpt"[..],
            &b"Encrypt"[..],
        ] {
            assert!(!contains_encrypt_name(input), "input: {input:?}");
        }
    }

    #[test]
    fn bounded_sink_accepts_exact_cap() {
        assert_eq!(canonicalize(4, &[b"test"]).unwrap(), b"test");
    }

    #[test]
    fn bounded_sink_rejects_cap_plus_one() {
        let mut sink = BoundedCanonicalSink::new(4);
        sink.write_all(b"test").unwrap();
        assert_eq!(
            sink.write_all(b"!").unwrap_err().kind(),
            io::ErrorKind::Other
        );

        let mut deferred = BoundedCanonicalSink::new(4);
        deferred.write_all(b"test\r").unwrap();
        assert_eq!(deferred.finish().unwrap_err().kind(), io::ErrorKind::Other);
    }

    #[test]
    fn bounded_sink_normalizes_crlf_cr_controls_and_preserves_multibyte_text() {
        let actual = canonicalize(
            64,
            &[
                b"h\xc3\xa9\r",
                b"\nline\rnext\t",
                b"\x01\x7f",
                "🙂".as_bytes(),
            ],
        )
        .unwrap();
        assert_eq!(
            std::str::from_utf8(&actual).unwrap(),
            "hé\nline\nnext\t\u{fffd}\u{fffd}🙂"
        );
        assert!(!actual.contains(&b'\r'));
    }

    #[test]
    fn control_only_output_does_not_count_as_text() {
        let output = canonicalize(16, &[b"\x01\x7f\r\n\t"]).unwrap();
        assert_eq!(
            std::str::from_utf8(&output).unwrap(),
            "\u{fffd}\u{fffd}\n\t"
        );
        assert!(!contains_meaningful_text(&output));
        assert!(contains_meaningful_text(" \né ".as_bytes()));
    }
}
