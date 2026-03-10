# Public Examples

`examples/` is intentionally curated for the narrow GA release and its closest secondary surfaces.

## Public configs

- `configs/canonical/belgium_full_stack.yaml`
  Belgium canonical path: portfolio + `schedule_revision` + `base_workflow: da_plus_afrr`
- `configs/reserve/belgium_da_plus_afrr_base.yaml`
  Smaller Belgium single-asset reserve baseline
- `configs/custom/belgium_full_stack_custom_python.yaml`
  Trusted local `custom_python` integration example
- `configs/basic/netherlands_da_only_base.yaml`
  Secondary-surface Netherlands energy-only example

## Batch example

- `batches/canonical_belgium_full_stack.yaml`

## Data

The CSV files under `examples/data/` are frozen sample inputs for demos, docs, CI, and Docker/Compose smoke paths. They are not live feeds and not a substitute for licensed historical data. See the full provenance and redistribution notes in [`docs/data_provenance.md`](../docs/data_provenance.md).

## First run

```bash
euroflex validate-config examples/configs/canonical/belgium_full_stack.yaml
euroflex validate-data examples/configs/canonical/belgium_full_stack.yaml
euroflex backtest examples/configs/canonical/belgium_full_stack.yaml --market belgium --workflow schedule_revision
euroflex reconcile artifacts/examples/<run_id> examples/configs/canonical/belgium_full_stack.yaml
euroflex export-schedule artifacts/examples/<run_id> --profile operator
euroflex export-bids artifacts/examples/<run_id> --profile bid_planning
euroflex batch examples/batches/canonical_belgium_full_stack.yaml
```

Non-promoted configs and sweeps used for tests live under `tests/fixtures/`.
