# PDF harness fixture provenance

- `qpdf-encrypted.pdf` was generated independently of lopdf/pdf-extract with qpdf 11.9.0 from a minimal PDF, using `qpdf --object-streams=disable --encrypt fixture-user fixture-owner 256 -- source.pdf qpdf-encrypted.pdf`. `qpdf --password=fixture-user --check qpdf-encrypted.pdf` reports encryption revision 6 and a valid file. SHA-256: `3a17f9dddbd2a237beea83794b01b0a2bfe16628467c26c300212727850732ee`.
- `flate-output-expansion.pdf` is a minimal PDF whose content stream is compressed with Python 3 zlib level 9 and declares `/Filter /FlateDecode`. Its 1.2 KiB file expands to more than the 512 KiB guest output ceiling. SHA-256: `ec7012d592fd5ff43186b3fb6d10c9edc038c609eec299f41028470361ecdcef`.
- `type0-missing-descendants.pdf` is a minimal PDF with a referenced Type0 font that deliberately omits the required `DescendantFonts` entry. pdf-extract 0.12.0 reaches its known `expect("Descendant fonts required")` panic. SHA-256: `24241aa9f57178df4e8f1cd18a57a6c998646aefb0469d0658b0caaa460806bd`.

The two malformed/adversarial fixtures were assembled directly from the PDF syntax documented in ISO 32000 and do not use the guest dependency graph.
