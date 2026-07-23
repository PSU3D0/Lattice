# Private Google egress Workers

This package owns the two non-public Google network edges used by the generic broker.

- `token-worker.mjs` accepts only authenticated `/exchange` and `/refresh` service-binding calls. It pins Google's token and OpenID userinfo endpoints, keeps the OAuth client ID/secret in Worker secrets, enforces PKCE, exact scopes and bounded streaming, and encrypts idempotent token results in a Durable Object.
- `provider-worker.mjs` accepts only authenticated B4a Gmail-send and Sheets-append plans. It constructs the two pinned Google origins itself and cannot proxy a caller-selected origin, method, path, query, or headers.

Both Workers require `GOOGLE_EGRESS_SERVICE_AUTH`. Token egress additionally requires `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET`, `GOOGLE_OAUTH_REDIRECT_URI`, and a 32-byte lowercase-hex `GOOGLE_TOKEN_RESULT_KEY`. No route is public (`workers_dev` and previews are disabled).

`npm test` uses Miniflare service bindings and a local mock upstream; it performs no live fetch. `npm run package` emits the deterministic combined source hash, and `npm run dry-run` emits both Wrangler bundles without remote calls. `npm run deploy -- ...` and `npm run cleanup -- ...` are dry-run by default; apply mode requires exact account, ownership prefix, callback, approval, secret-name qualification, and Cloudflare credential inputs.
