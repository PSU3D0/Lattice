# connector_llm — `connector.llm.complete`

Typed LLM completion op wrapping the existing `llm-*` crates, exposed through
the same connector-op surface the clone flows consume (packet N4, `llm:complete`).

## Shape decision: connector family wrapping the provider crates

`connector.llm` is a **connector family crate** (F1 layout: manifest / ops /
actions / runtime / five-file test harness / local-flow example), not a bare
node helper — but unlike the HTTP connectors it does **not** hand-compose wire
requests. The runtime is a thin adapter:

1. **Endpoint + credential come from the lock**, exactly like gmail/drive:
   role `endpoint_profile.llm_default` (base URL) and role
   `outbound_auth.llm_api_key` (`http.bearer` handle,
   `LATTICE_CONNECTOR_AUTH_LLM_API_KEY` under `EnvConnectorRuntime`). A mock
   provider is just a lock/env base URL pointing at an httpmock server — the
   whole harness runs without any real LLM API.
2. **The wire is composed by the existing provider crates**
   (`llm-provider-openai`, `llm-provider-anthropic`) driven through
   `llm-agent`'s typed client, so request/response semantics stay in one
   place and future capabilities (tools, streaming, extraction) inherit.
3. **Dispatch rides `llm-lattice`'s `LatticeHttpClient`**, which resolves the
   node's *scoped* HTTP capabilities. Capability enforcement is therefore
   identical to every other connector: an undeclared `http_write` records a
   CAP110 denial and fails closed before any bytes leave the process
   (load-bearing honesty test in `tests/honesty.rs`).

Rejected alternatives:
- *Hand-rolled wire descriptors* (github/issues style) would duplicate the
  provider crates the packet explicitly says to wrap.
- *One connector family per provider* (`connector.openai`, `connector.anthropic`)
  would force flows to hardwire a vendor; the corpus demand is "call a model",
  with the vendor a deployment choice — i.e. lock material.

### Provider selection

The lock selects **where** (base URL) and **as whom** (secret). The wire
*dialect* is the input field `provider` (`openai_compat` default |
`anthropic`), because the connector-role vocabulary today carries only
base URL + secret — there is no per-connection config slot to hang a dialect
on. The dialect must agree with the lock's base URL; `openai_compat` covers
the long tail (OpenAI, gateways, Ollama, mocks), `anthropic` speaks the
Messages API (`x-api-key` + `anthropic-version`, composed by the provider
crate from the same bearer secret). If/when connections grow typed config,
the dialect should migrate into the lock — flagged for the coordinator.

## Effect / determinism metadata (and why)

- `min_effects: Effectful`, `effect_hints: [http_write]` — a completion is a
  remote POST (the transport vocabulary routes non-GET through the write
  capability) and consumes billed quota: replays are observable and not free,
  so the delivery/dedupe gate must see the op. `ReadOnly + http_read` would
  misroute the POST and is rejected by the honesty tests in both directions.
- `max_determinism: Nondeterministic`, `determinism_hints: [http]` — sampled
  output differs across identical inputs; never `Strict`, and weaker than the
  CRUD connectors' `BestEffort`. Matches the s11 precedent
  (`connector.openai.complete`).

## Surface (v1, ops-not-APIs)

`LlmCompleteInput`: `provider`, `model`, `prompt`, `system?`, `temperature?`,
`max_tokens?` (recommended for anthropic), `output_schema?` (JSON Schema
passthrough to native structured output). `LlmCompleteOutput`: `text`,
`structured?` (parsed JSON iff `output_schema` was set), `model`, `usage`
(input/output/total tokens).

## What templates #2/#3/#5 will need (gaps, deliberately not built)

- **Chat-style messages / multi-turn history**: v1 is single-turn
  (`system` + `prompt`). The underlying `CompletionRequestBuilder.messages(...)`
  supports history; add a `messages: [...]` input variant when a template
  demands it.
- **Tool use / agent loops**: `llm-agent` supports tools; out of scope for a
  completion op — would be a separate op (or op family) with its own effects.
- **Streaming**: `llm-lattice` deliberately rejects streaming requests; flows
  are batch-shaped today.
- **Provider dialect in the lock** (see above) and **per-connection default
  model**: today the flow supplies `model`; a lock-level default needs a
  connection-config slot.
- **Binary/multimodal inputs**: needs the workspace-capability handoff
  decision (playbook §3).

## Tests

`tests/{manifest,contract,runtime,honesty,live_smoke}.rs` mirror the
github/issues harness: manifest↔META cross-check (incl. determinism honesty:
never Strict), registration, per-dialect runtime contracts against httpmock
(auth headers, mapped params, error/auth-misconfig paths, structured output),
exact-grants success + empty-grants CAP110 denial + duplicate-injection
dedupe, and a gate-only live smoke (the op is Effectful/billed; deliberate
live runs go through the local-flow example with a real lock).
