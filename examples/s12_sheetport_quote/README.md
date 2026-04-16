# s12_sheetport_quote

Representative SheetPort example proving the connector-resolution pattern in a
real flow package.

It contains two flows:

- `s12_sheetport_quote_flow`
  - canonical graph-visible usage
  - deployment-bound / bound-connection mode
  - uses `connector.formualizer.sheetport.evaluate` as a discrete topology node
  - proves resolution-aware preflight can require blob before execution

- `s12_sheetport_quote_internal_flow`
  - custom-node internal usage
  - explicit late-bound typed source refs
  - still uses the same declared connector op at the node boundary
  - proves dynamic late-bound blob selection is deferred to runtime rather than
    over-constrained at preflight

The example package tests also prove:
- real SheetPort calculation through both paths
- parity between the canonical and internal flows
- workspace export of the evaluated workbook snapshot via the canonical wrapper

## Assets

- Manifest: `assets/quote_model.fio.yaml`
- Workbook: `assets/quote_model.xlsx`

Regenerate the workbook deterministically with:

```bash
uv run examples/s12_sheetport_quote/scripts/provision_quote_workbook.py
```

The script uses `openpyxl` and then repacks the XLSX ZIP entries with fixed
metadata to keep the generated asset stable.
