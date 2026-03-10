# Operator Runbook

This runbook assumes the narrow GA path:

- Belgium
- portfolio / shared POI
- `workflow: schedule_revision`
- `revision.base_workflow: da_plus_afrr`
- forecast path: `persistence` or `csv`

## Daily flow

1. Validate the config:
   `euroflex validate-config examples/configs/canonical/belgium_full_stack.yaml`
2. Validate the data:
   `euroflex validate-data examples/configs/canonical/belgium_full_stack.yaml`
3. Run the backtest / schedule build:
   `euroflex backtest examples/configs/canonical/belgium_full_stack.yaml --market belgium --workflow schedule_revision`
4. Review the generated artifacts:
   `summary.json`, `site_dispatch.parquet`, `asset_dispatch.parquet`, `decision_log.parquet`
5. Reconcile against realized inputs:
   `euroflex reconcile artifacts/examples/<run_id> examples/configs/canonical/belgium_full_stack.yaml`
6. Export the operator schedule:
   `euroflex export-schedule artifacts/examples/<run_id> --profile operator`
7. Export the bid-planning package:
   `euroflex export-bids artifacts/examples/<run_id> --profile bid_planning`
8. If a downstream execution handoff needs a stricter payload shape, export a submission candidate:
   `euroflex export-schedule artifacts/examples/<run_id> --profile submission_candidate`

## Warnings that should not be ignored

- any validation failure around timezone, cadence, or missing aFRR inputs
- any explicit runtime rejection from unsupported workflow/market combinations
- large reconciliation deltas in forecast error or activation settlement deviation
- a reserve feasibility warning for the configured site and asset set

## Approval expectation

The framework supports metadata-first run states. A typical human-in-the-loop flow is:

- `draft`
- `reviewed`
- `approved`
- `exported`
- `reconciled`

These states are for operator control and auditability. They do not turn the tool into a live submission engine.
