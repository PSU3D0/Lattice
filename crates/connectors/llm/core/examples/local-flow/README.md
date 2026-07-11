# LLM connector — local flow example

Minimal `flow!` exercising `connector.llm.complete` end-to-end through the
real executor (scoped enforcement on). The provider endpoint + credential are
selected the same way as any HTTP connector: an `endpoint.profile.static`
handle for `endpoint_profile.llm_default` and an `auth.static_bearer` handle
for `outbound_auth.llm_api_key` in the bindings lock (or the equivalent
`LATTICE_CONNECTOR_ENDPOINT_LLM_DEFAULT_BASE_URL` /
`LATTICE_CONNECTOR_AUTH_LLM_API_KEY` env vars under `EnvConnectorRuntime`).

Run against a bindings lock (mock or real endpoints):

```sh
flows run local --example connector_llm_local_flow \
  --bindings-lock <lock.json> \
  --payload '{"model":"gpt-5.4-mini","prompt":"Say hello."}'
```

Point the lock's `llm_default` base URL at any OpenAI-compatible endpoint
(mock server, gateway, or the real API). The flow pins the `openai_compat`
dialect; set `provider: "anthropic"` on `LlmCompleteInput` for the Anthropic
Messages dialect.

Offline tests (httpmock provider, no real LLM API):
`cargo test -p example-connector-llm-local-flow`.
