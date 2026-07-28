# S30 Worker deployment inputs

This application-owned Worker package renders the five-node S30 flow and then adds the private `LATTICE_BROKER_PRIVATE` service binding plus the broker binding variables.

```bash
npm run package
```

Before deployment, replace every `REPLACE_WITH_*` value in `deploy/wrangler.toml`. Set `LATTICE_BROKER_DEPLOYMENT_KEY`, `LATTICE_BROKER_POP_SEED_B64U`, and `LATTICE_BROKER_SERVICE_AUTH` as Worker secrets. The package contains no Google bearer credential and is not deployed by this example.
