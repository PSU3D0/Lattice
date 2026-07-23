use std::{env, fs};

fn main() {
    if let Err(error) = run() {
        eprintln!("operator artifact verification failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let mut bundle = None;
    let mut trust = None;
    let mut now = None;
    while let Some(name) = args.next() {
        let value = args.next().ok_or("missing option value")?;
        match name.as_str() {
            "--bundle" => bundle = Some(value),
            "--trust-root" => trust = Some(value),
            "--now" => now = Some(value),
            _ => return Err("unknown option".into()),
        }
    }
    let exact = fs::read(bundle.ok_or("missing --bundle")?)?;
    let trust: serde_json::Value =
        serde_json::from_slice(&fs::read(trust.ok_or("missing --trust-root")?)?)?;
    let key_id = trust
        .get("key_id")
        .and_then(|v| v.as_str())
        .ok_or("invalid trust root")?;
    let public_key = trust
        .get("public_key_b64u")
        .and_then(|v| v.as_str())
        .ok_or("invalid trust root")?;
    let hash = broker_workers::operator_bundle::verify_operator_bundle(
        &exact,
        key_id,
        public_key,
        &now.ok_or("missing --now")?,
    )
    .map_err(|_| "C1 bundle verification rejected")?;
    println!("{{\"bundle_hash\":\"{hash}\",\"key_id\":\"{key_id}\"}}");
    Ok(())
}
