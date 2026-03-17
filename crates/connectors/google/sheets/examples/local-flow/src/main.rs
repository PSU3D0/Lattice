#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _env_lock = example_connector_google_sheets_local_flow::env_lock();
    let _mock = example_connector_google_sheets_local_flow::maybe_start_mock_server();
    let input = example_connector_google_sheets_local_flow::example_input_from_env();
    let output = example_connector_google_sheets_local_flow::run_flow(input).await?;
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}
