# Config Schema

All public configs use `schema_version: 4`.

Top-level fields:

- `schema_version`
- `run_name`
- `market`
- `workflow`
- `forecast_provider`
- `timing`
- `site`
- `assets`
- `degradation`
- `data`
- optional `fcr`
- optional `revision`
- `artifacts`

## `site`

Required fields:

- `id`
- `poi_import_limit_mw`
- `poi_export_limit_mw`

## `assets`

`assets` must be a non-empty list.

Each entry must contain:

- `id`
- `kind: battery`
- `battery`

The nested `battery` object reuses the single-asset battery fields:

- `power_mw`
- `energy_mwh`
- `initial_soc_mwh`
- `terminal_soc_mwh`
- `soc_min_mwh`
- `soc_max_mwh`
- `charge_efficiency`
- `discharge_efficiency`
- `connection_limit_mw`
- `minimum_headroom_mwh`

## `workflow`

Supported values:

- `da_only`
- `da_plus_fcr`
- `da_plus_afrr`
- `schedule_revision`

`da_plus_imbalance` remains available in code as a legacy/internal single-asset workflow, but it is not part of the `v1.0.0` public GA promise.

When `workflow == schedule_revision`, the actual market logic is controlled by `revision.base_workflow`.

## `forecast_provider`

Supported values:

- `persistence`
- `csv`
- `custom_python`

Operational GA forecast paths are `persistence` and `csv`.

`perfect_foresight` remains available as an oracle benchmark path, outside the operational GA promise.

CSV forecasts may require:

- `day_ahead_path`
- `imbalance_path`
- `fcr_capacity_path`
- `afrr_capacity_up_path`
- `afrr_capacity_down_path`
- `afrr_activation_price_up_path`
- `afrr_activation_price_down_path`
- `afrr_activation_ratio_up_path`
- `afrr_activation_ratio_down_path`
- optional `scenario_id`

For `schedule_revision`, CSV inputs still have to respect as-of visibility at each revision checkpoint.

## `timing`

- `timezone`
- `resolution_minutes`
- `rebalance_cadence_minutes`
- `execution_lock_intervals`
- `day_ahead_gate_closure_local`
- `delivery_start_date`
- `delivery_end_date`

The current release line only supports a 15-minute market grid.

## `fcr`

Required for `da_plus_fcr` and `schedule_revision` with `base_workflow == da_plus_fcr`:

- `product_id: fcr_symmetric`
- `sustain_duration_minutes`
- `settlement_mode: capacity_only`
- `activation_mode: none`
- `non_delivery_penalty_eur_per_mw`
- `simplified_product_logic: true`

## `afrr`

Required for `da_plus_afrr` and `schedule_revision` with `base_workflow == da_plus_afrr`:

- `product_id: afrr_asymmetric`
- `settlement_mode: capacity_plus_activation_expected_value`
- `activation_mode: expected_value`
- `sustain_duration_minutes`
- `non_delivery_penalty_eur_per_mw`
- `simplified_product_logic: true`

## `revision`

Required for `schedule_revision`:

- `base_workflow: da_only | da_plus_fcr | da_plus_afrr | da_plus_imbalance`
- `revision_market_mode: public_checkpoint_reoptimization`
- `revision_checkpoints_local`
- `lock_policy: committed_intervals_only`
- `allow_day_ahead_revision: false`
- `allow_fcr_revision: false`
- `allow_energy_revision: true`
- `max_revision_horizon_intervals`
- optional `realized_inputs`

Important semantics:

- `schedule_revision` is a wrapper, not a new market product
- `base_workflow` controls settlement, reserve constraints, and forecast requirements
- `da_plus_imbalance` revision remains legacy/internal single-asset only and is not part of the public GA promise
- `da_plus_fcr` revision keeps FCR commitments locked in the current release line
- `da_plus_afrr` revision keeps Belgium aFRR commitments locked in the current release line
