# Discord connector — local flow example

Minimal `flow!` exercising `connector.discord.send_message` end-to-end through
the real executor (scoped enforcement on).

Run against a bindings lock (mock or real endpoints):

```sh
flows run local --example connector_discord_local_flow \
  --bindings-lock <lock.json> \
  --payload '{"title":"New Lead","description":"hello"}'
```

Hand-author a lock with an `endpoint.profile.static` handle for
`endpoint_profile.discord_webhook_default` and an `auth.static_bearer` handle
for `outbound_auth.discord_webhook_auth`. The bearer secret is the webhook
`<id>/<token>` pair — it is spliced into the `/api/webhooks/<id>/<token>`
request path (Discord executes a webhook by posting to a URL whose path embeds
the id and token; the whole pair is the secret).

Offline tests: `cargo test -p example-connector-discord-local-flow`.
