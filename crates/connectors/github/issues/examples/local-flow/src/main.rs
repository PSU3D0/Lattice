use anyhow::Result;
use example_connector_github_issues_local_flow::{
    ENDPOINT_ENV, example_input_from_env, maybe_start_mock_server, run_flow,
};

#[tokio::main]
async fn main() -> Result<()> {
    let input = example_input_from_env();
    let _mock = maybe_start_mock_server();
    let output = run_flow(input.clone()).await?;

    println!(
        "Connector flow returned {} issue(s) for {}/{}:",
        output.items.len(),
        input.owner,
        input.repo
    );
    println!("{}", serde_json::to_string_pretty(&output)?);
    println!();
    println!("Tip: set {ENDPOINT_ENV} to hit a real GitHub-compatible endpoint instead.");
    println!("If your endpoint requires auth, set LATTICE_CONNECTOR_AUTH_GITHUB_PAT as well.");

    Ok(())
}
