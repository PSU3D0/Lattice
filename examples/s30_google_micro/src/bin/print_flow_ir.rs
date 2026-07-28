use std::io::Write;

fn main() {
    let canonical = s30_google_micro::canonical_flow_ir();
    std::io::stdout()
        .write_all(canonical.as_bytes())
        .expect("write canonical Flow IR");
    eprintln!("{}", canonical.sha256());
}
