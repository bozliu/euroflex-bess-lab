# Artifact Contract

Stable run artifacts:

- `summary.json`
- `decision_log.parquet`
- `forecast_snapshots.parquet`
- `settlement_breakdown.parquet`
- `site_dispatch.parquet`
- `asset_dispatch.parquet`
- `asset_pnl_attribution.parquet`
- `baseline_schedule.parquet`
- `revision_schedule.parquet`
- `schedule_lineage.parquet`
- `reconciliation_summary.json`
- `reconciliation_breakdown.parquet`

## Stable `summary.json` fields

Core identity:

- `schema_version`
- `run_id`
- `site_id`
- `run_scope`
- `asset_count`
- `market_id`
- `market_timezone`
- `workflow`
- `base_workflow`
- `benchmark_name`
- `benchmark_family`
- `provider_name`
- `settlement_basis`
- `gate_closure_definition`
- `data_provenance`

Site and portfolio fields:

- `poi_import_limit_mw`
- `poi_export_limit_mw`
- `max_site_charge_mw`
- `max_site_discharge_mw`
- `asset_contribution_ranking`

PnL and utilization fields:

- `da_revenue_eur`
- `imbalance_revenue_eur`
- `reserve_capacity_revenue_eur`
- `reserve_penalty_eur`
- `degradation_cost_eur`
- `total_pnl_eur`
- `expected_total_pnl_eur`
- `baseline_expected_total_pnl_eur`
- `revised_expected_total_pnl_eur`
- `realized_total_pnl_eur`
- `throughput_mwh`
- `idle_share`

Reserve-aware fields:

- `reserve_product_id`
- `reserve_settlement_mode`
- `reserve_activation_mode`
- `reserve_sustain_duration_minutes`
- `simplified_product_logic`
- `reserved_capacity_mw_avg`
- `reserved_capacity_mw_max`
- `reserve_share_of_total_revenue`

## Stable parquet fields

All stable parquet artifacts include:

- `market_id`
- `workflow_family`
- `run_scope`

`site_dispatch.parquet` includes at least:

- `site_id`
- `charge_mw`
- `discharge_mw`
- `net_export_mw`
- `soc_mwh`
- `fcr_reserved_mw`
- `reserve_headroom_up_mw`
- `reserve_headroom_down_mw`
- `schedule_version`
- `lock_state`
- `reason_code`

`asset_dispatch.parquet` includes at least:

- `site_id`
- `asset_id`
- `asset_name`
- `charge_mw`
- `discharge_mw`
- `net_export_mw`
- `soc_mwh`
- `fcr_reserved_mw`
- `availability_factor`
- `schedule_version`
- `lock_state`
- `reason_code`

`asset_pnl_attribution.parquet` includes at least:

- `asset_id`
- `total_pnl_eur`
- `da_revenue_eur`
- `imbalance_revenue_eur`
- `reserve_capacity_revenue_eur`
- `degradation_cost_eur`

`decision_log.parquet` and `forecast_snapshots.parquet` include revision-aware metadata such as `schedule_version`, checkpoint timestamps, and locked-horizon details when `workflow == schedule_revision`.

`schedule_lineage.parquet` records the version chain:

- `baseline`
- `revision_01`, `revision_02`, ...
- `final_realized`

`reconciliation_breakdown.parquet` records the realized-vs-expected attribution buckets for revision runs.

## Export contract

`export-schedule` writes:

- `site_schedule.{csv,parquet,json}`
- `asset_allocation.{csv,parquet,json}`
- `baseline_schedule.{csv,parquet,json}` when available
- `latest_revised_schedule.{csv,parquet,json}` when available
- `manifest.json`

`export-revision` writes:

- `baseline_schedule.{csv,parquet,json}`
- `latest_revised_schedule.{csv,parquet,json}`
- `schedule_lineage.{csv,parquet,json}` when available
- `asset_revision_allocation.{csv,parquet,json}`
- `manifest.json`

`export-bids` writes:

- `site_bids.{csv,parquet,json}`
- `asset_reserve_allocation.{csv,parquet,json}`
- `manifest.json`

Each manifest includes:

- `schema_version`
- `source_run_dir`
- `metadata`
- `files`
- per-file SHA-256 checksums
