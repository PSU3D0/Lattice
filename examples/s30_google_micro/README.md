# S30 Google micro

The five-node `/google-micro` POST flow creates a disposable Google spreadsheet with a `note` header, appends one note row using the returned spreadsheet ID, sends one Gmail message containing the spreadsheet URL, and returns the provider IDs.

Print the exact canonical Flow IR bytes to stdout and their SHA-256 to stderr:

```bash
cargo run -q -p s30_google_micro --bin print-flow-ir > flow-ir.json
```
