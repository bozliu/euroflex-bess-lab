# Reconciliation And Audit

`euroflex reconcile` compares three views of the same run:

- baseline expected PnL
- revised expected PnL
- realized PnL

## CLI

```bash
euroflex reconcile artifacts/examples/<run_id> examples/configs/canonical/belgium_full_stack.yaml
```

The second argument can be:

- a config-like file with actual input paths
- a directory containing realized input files

## Stable outputs

Revision-aware runs write:

- `reconciliation_summary.json`
- `reconciliation_breakdown.parquet`

These are also produced by the standalone `reconcile` command.

## Attribution buckets

The audit layer breaks realized-vs-expected differences into benchmark-grade buckets such as:

- forecast error
- locked commitment opportunity cost
- reserve headroom opportunity cost
- degradation cost drift
- availability deviation
- imbalance or settlement deviation

## Scope note

This is an audit aid for public-data benchmarking. It is not a substitute for:

- operator settlement statements
- production reconciliation tooling
- live market operations support
