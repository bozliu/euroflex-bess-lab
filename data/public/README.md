# Public data (`data/public`)

Normalized, redistributable public market data used by the euroflex-bess-lab backtests. Every file is a
zstd-compressed parquet table with a `<name>.meta.json` sidecar (source URLs, licence, attribution, retrieval
time, coverage, rows, columns, sha256, transformation notes, validation results). `MANIFEST.json` aggregates all
sidecars. Total committed size: 111.7 MB (budget 200 MB, at most 50 MB per file).

Conventions: timestamps are tz-aware UTC and mark the *start* of the interval; intervals are half-open
`[start, end)`; Elia SI > 0 means the system is long; numeric columns are float32 with units in the column name.

## Files

| File | Dataset id(s) | Content | Licence | Coverage (UTC, [start, end)) | Rows | Size (MB) |
| --- | --- | --- | --- | --- | ---: | ---: |
| `belgium/afrr_capacity_awards.parquet` | ods125, ods207 | Belgian aFRR capacity auction awards aggregated per delivery block and direction. | CC BY 4.0 | 2022-05-03 22:00 -> 2026-10-25 23:00 | 19,632 | 0.21 |
| `belgium/energy_bid_curves_qh_2025.parquet` | ods156, ods157 | Merit-order curves of available aFRR/mFRR energy bids per quarter-hour on a fixed MW grid. | CC BY 4.0 | 2025-01-01 00:00 -> 2026-01-01 00:00 | 5,212,593 | 2.90 |
| `belgium/energy_bid_curves_qh_2026.parquet` | ods156, ods157 | Merit-order curves of available aFRR/mFRR energy bids per quarter-hour on a fixed MW grid. | CC BY 4.0 | 2026-01-01 00:00 -> 2026-10-24 22:00 | 4,617,648 | 3.43 |
| `belgium/mfrr_capacity_awards.parquet` | ods055, ods206 | Belgian mFRR capacity auction awards aggregated per delivery block and direction. | CC BY 4.0 | 2020-12-31 23:00 -> 2026-10-25 23:00 | 12,573 | 0.12 |
| `belgium/minute_2023_legacy.parquet` | ods046 | Pre-MARI per-minute imbalance prices (2023-01 to 2024-05-21), legacy layout. | CC BY 4.0 | 2023-01-01 00:00 -> 2024-01-01 00:00 | 525,600 | 8.32 |
| `belgium/minute_2024.parquet` | ods133, ods126, ods165 | Per-minute imbalance price (cumulative QH-to-date), SI/ACE/volumes and price components. | CC BY 4.0 | 2024-05-21 22:00 -> 2025-01-01 00:00 | 322,680 | 10.34 |
| `belgium/minute_2024_legacy.parquet` | ods046 | Pre-MARI per-minute imbalance prices (2023-01 to 2024-05-21), legacy layout. | CC BY 4.0 | 2024-01-01 00:00 -> 2024-05-21 22:00 | 204,360 | 3.53 |
| `belgium/minute_2025.parquet` | ods133, ods126, ods165 | Per-minute imbalance price (cumulative QH-to-date), SI/ACE/volumes and price components. | CC BY 4.0 | 2025-01-01 00:00 -> 2026-01-01 00:00 | 525,600 | 17.24 |
| `belgium/minute_2026.parquet` | ods133, ods126, ods165 | Per-minute imbalance price (cumulative QH-to-date), SI/ACE/volumes and price components. | CC BY 4.0 | 2026-01-01 00:00 -> 2026-10-24 22:00 | 427,560 | 14.52 |
| `belgium/new_formula_minute_2026.parquet` | ods211 | Elia simulation of the new 2026 imbalance-price formula, per minute (Jan-Jul 2026). | CC BY 4.0 | 2026-01-01 00:15 -> 2026-08-01 00:00 | 305,265 | 10.65 |
| `belgium/new_formula_qh_2026.parquet` | ods212 | Elia simulation of the new 2026 imbalance-price formula, per quarter-hour (Jan-Jul 2026). | CC BY 4.0 | 2026-01-01 00:00 -> 2026-08-01 00:00 | 20,352 | 0.95 |
| `belgium/qh_2019_legacy.parquet` | ods047 | Pre-MARI quarter-hour imbalance prices (before 2024-05-22), legacy layout. | CC BY 4.0 | 2019-01-01 00:00 -> 2020-01-01 00:00 | 35,040 | 0.50 |
| `belgium/qh_2020_legacy.parquet` | ods047 | Pre-MARI quarter-hour imbalance prices (before 2024-05-22), legacy layout. | CC BY 4.0 | 2020-01-01 00:00 -> 2021-01-01 00:00 | 35,136 | 0.56 |
| `belgium/qh_2021_legacy.parquet` | ods047 | Pre-MARI quarter-hour imbalance prices (before 2024-05-22), legacy layout. | CC BY 4.0 | 2021-01-01 00:00 -> 2022-01-01 00:00 | 35,040 | 0.75 |
| `belgium/qh_2022_legacy.parquet` | ods047 | Pre-MARI quarter-hour imbalance prices (before 2024-05-22), legacy layout. | CC BY 4.0 | 2022-01-01 00:00 -> 2023-01-01 00:00 | 35,040 | 0.81 |
| `belgium/qh_2023_legacy.parquet` | ods047 | Pre-MARI quarter-hour imbalance prices (before 2024-05-22), legacy layout. | CC BY 4.0 | 2023-01-01 00:00 -> 2024-01-01 00:00 | 35,040 | 0.80 |
| `belgium/qh_2024.parquet` | ods134, ods166, ods127 | Per-quarter-hour final imbalance prices, price components and volumes. | CC BY 4.0 | 2024-05-21 22:00 -> 2025-01-01 00:00 | 21,512 | 0.99 |
| `belgium/qh_2024_legacy.parquet` | ods047 | Pre-MARI quarter-hour imbalance prices (before 2024-05-22), legacy layout. | CC BY 4.0 | 2024-01-01 00:00 -> 2024-05-21 22:00 | 13,624 | 0.44 |
| `belgium/qh_2025.parquet` | ods134, ods166, ods127 | Per-quarter-hour final imbalance prices, price components and volumes. | CC BY 4.0 | 2025-01-01 00:00 -> 2026-01-01 00:00 | 35,040 | 1.46 |
| `belgium/qh_2026.parquet` | ods134, ods166, ods127 | Per-quarter-hour final imbalance prices, price components and volumes. | CC BY 4.0 | 2026-01-01 00:00 -> 2026-10-24 22:00 | 28,504 | 1.20 |
| `belgium/res_load_qh_2023.parquet` | ods001, ods031, ods032 | Belgian load, wind and PV measured + forecast per quarter-hour. | CC BY 4.0 | 2023-01-01 00:00 -> 2024-01-01 00:00 | 35,040 | 1.36 |
| `belgium/res_load_qh_2024.parquet` | ods001, ods031, ods032 | Belgian load, wind and PV measured + forecast per quarter-hour. | CC BY 4.0 | 2024-01-01 00:00 -> 2025-01-01 00:00 | 35,136 | 1.32 |
| `belgium/res_load_qh_2025.parquet` | ods001, ods031, ods032 | Belgian load, wind and PV measured + forecast per quarter-hour. | CC BY 4.0 | 2025-01-01 00:00 -> 2026-01-01 00:00 | 35,040 | 1.34 |
| `belgium/res_load_qh_2026.parquet` | ods001, ods031, ods032 | Belgian load, wind and PV measured + forecast per quarter-hour. | CC BY 4.0 | 2026-01-01 00:00 -> 2026-10-25 23:00 | 28,604 | 1.26 |
| `belgium/si_forecast_current_qh_2024.parquet` | ods136 | Elia SI forecast for the current quarter-hour, issued every minute. | CC BY 4.0 | 2024-05-21 22:00 -> 2025-01-01 00:00 | 318,715 | 3.74 |
| `belgium/si_forecast_current_qh_2025.parquet` | ods136 | Elia SI forecast for the current quarter-hour, issued every minute. | CC BY 4.0 | 2025-01-01 00:00 -> 2026-01-01 00:00 | 512,694 | 5.14 |
| `belgium/si_forecast_current_qh_2026.parquet` | ods136 | Elia SI forecast for the current quarter-hour, issued every minute. | CC BY 4.0 | 2026-01-01 00:00 -> 2026-10-25 23:00 | 424,491 | 3.11 |
| `belgium/si_forecast_next_qh_2024.parquet` | ods147 | Elia SI forecast for the next quarter-hour, issued every minute. | CC BY 4.0 | 2024-05-21 22:00 -> 2025-01-01 00:00 | 318,713 | 4.39 |
| `belgium/si_forecast_next_qh_2025.parquet` | ods147 | Elia SI forecast for the next quarter-hour, issued every minute. | CC BY 4.0 | 2025-01-01 00:00 -> 2026-01-01 00:00 | 512,699 | 6.26 |
| `belgium/si_forecast_next_qh_2026.parquet` | ods147 | Elia SI forecast for the next quarter-hour, issued every minute. | CC BY 4.0 | 2026-01-01 00:00 -> 2026-10-25 23:00 | 424,510 | 3.74 |
| `energy_charts/da_prices_BE.parquet` | energy_charts_price_BE | Belgian day-ahead auction prices (hourly until 2025-09-30, 15-minute afterwards). | CC BY 4.0 | 2022-12-31 23:00 -> 2026-10-25 23:00 | 61,539 | 0.18 |
| `energy_charts/da_prices_DE-LU.parquet` | energy_charts_price_DE-LU | German-Luxembourg day-ahead auction prices (hourly until 2025-09-30, 15-minute afterwards). | CC BY 4.0 | 2022-12-31 23:00 -> 2026-10-25 23:00 | 61,539 | 0.18 |
| `data/raw/germany/afrr_capacity.parquet` (local build only, not redistributed) | regelleistung_aFRR_capacity_results, regelleistung_aFRR_capacity_demands | German aFRR capacity auction results (4-hour blocks, up/down). | proprietary (copyright German TSOs; reproduction needs prior written consent) | 2023-12-31 23:00 -> 2026-10-25 23:00 | 12,348 | - |
| `data/raw/germany/fcr_capacity.parquet` (local build only, not redistributed) | regelleistung_FCR_capacity_results | German FCR capacity auction results (4-hour blocks). | proprietary (copyright German TSOs; reproduction needs prior written consent) | 2023-12-31 23:00 -> 2026-10-25 23:00 | 6,174 | - |

## Use

```python
from euroflex_bess_lab.public_data import load_belgium_minute, load_belgium_qh, load_day_ahead_prices

minute = load_belgium_minute("2025-06-10", "2025-06-13")
qh = load_belgium_qh("2025-06-10", "2025-06-13", columns=["imbalance_price_eur_mwh", "si_mw"])
da = load_day_ahead_prices("BE", "2025-06-10", "2025-06-13")
```

Set `EUROFLEX_DATA_DIR` to point the loaders at another tree (the unit tests use `tests/fixtures/public`).

## Refresh / reproduce

```bash
python scripts/data/fetch_all_public.py            # rebuild everything (raw downloads cached in data/raw/)
python scripts/data/fetch_all_public.py --offline --as-of 2026-10-26   # rebuild this snapshot from the cache
euroflex data fetch belgium-minute --start 2026-01-01 --end 2027-01-01   # one table group
euroflex data verify                               # sha256 of every file against MANIFEST.json
euroflex data list
```

Every table ends at the snapshot boundary recorded as `snapshot_end_utc` in its sidecar (a Brussels midnight;
`--as-of` selects it). Raw upstream exports are cached (gitignored) under `data/raw/`; closed periods are never
re-downloaded unless `--refresh` is passed, and a refresh never replaces a cached chunk with a smaller one.
Rebuilding from the same raw cache is byte-identical (same sha256); rebuilding from fresh downloads can differ
where Elia later validates or revises data. Elia removes the new-formula simulation (ODS211/ODS212) on
2026-11-17 and ODS156/ODS157 keep a rolling window only, so `belgium/new_formula_*_2026.parquet` and the oldest
bid curves cannot be rebuilt from upstream later: keep the committed files (and the raw cache).

See `LICENSES.md` for attribution and `docs/public_datasets.md` for field definitions and caveats.
