# connector_http local flow

A portable, connector-owned flow example that drives `connector.http.get`
(JSON mode) into a pure shape node and then `connector.http.post` against a
mock HTTP endpoint. Mirrors the other connectors' local-flow layout.

Run the offline test:

```sh
mise exec -- cargo test -p example-connector-http-local-flow
```
