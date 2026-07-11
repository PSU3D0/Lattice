# connector_slack_core

Semantic Slack connector (F1 first slice).

First-slice surface (ops-not-APIs — only what the cloned template invokes):

| op | request | effects | resources |
|---|---|---|---|
| `connector.slack.core.post_message` | `POST /chat.postMessage` | Effectful | `http_write` |

`post_message` composes a JSON `{channel, text, blocks?}` body and
post-processes Slack's `ok`/`error` envelope (Slack returns HTTP 200 even on
logical failures), which is why it is a `handwritten_semantic` op.

Auth: a single bearer role `slack_auth`
(`LATTICE_CONNECTOR_AUTH_SLACK_AUTH`), handle kind `http.bearer`. Endpoint
profile `slack_default` targets `https://slack.com/api`
(`LATTICE_CONNECTOR_ENDPOINT_SLACK_DEFAULT_BASE_URL` overrides for tests).

Facts (endpoints, auth header, `ok`/`error` envelope) were seeded from the N2
brief `ops/connector-briefs/slack.md`; no third-party source text is embedded.
The connector is written independently against the official Slack Web API docs.
