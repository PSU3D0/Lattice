# Telegram connector — local flow example

Minimal `flow!` exercising `connector.telegram.send_message` end-to-end through
the real executor (scoped enforcement on).

Run against a bindings lock (mock or real endpoints):

```sh
flows run local --example connector_telegram_local_flow \
  --bindings-lock <lock.json> \
  --payload '{"chat_id":"1001","text":"hello"}'
```

Hand-author a lock with an `endpoint.profile.static` handle for
`endpoint_profile.telegram_bot_default` and an `auth.static_bearer` handle for
`outbound_auth.telegram_bot_auth` (the bot token — it is spliced into the
`/bot<token>/sendMessage` request path).

Offline tests: `cargo test -p example-connector-telegram-local-flow`.
