# connector_slack_core local flow

A portable, connector-owned flow example that drives
`connector.slack.core.post_message` against a mock Slack endpoint. Mirrors the
`connector_google_gmail` local-flow layout.

Run the offline test:

```sh
mise exec -- cargo test -p example-connector-slack-core-local-flow
```
