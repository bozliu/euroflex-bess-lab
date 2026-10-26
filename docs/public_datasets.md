# Public datasets

euroflex-bess-lab ships a reproducible public-data layer (`euroflex_bess_lab.public_data`): downloaders with
retries and a raw cache, normalizers that map upstream fields to a snake_case contract, writers (zstd parquet +
`.meta.json` sidecar + `MANIFEST.json`) and loaders. The committed tree lives in `data/public/`
(111.7 MB); the raw downloads are cached in the gitignored `data/raw/`.

## Datasets

| File | Dataset id(s) | Content | Licence | Coverage (UTC, [start, end)) | Rows | Size (MB) |
| --- | --- | --- | --- | --- | ---: | ---: |
| `data/public/belgium/afrr_capacity_awards.parquet` | ods125, ods207 | Belgian aFRR capacity auction awards aggregated per delivery block and direction. | CC BY 4.0 | 2022-05-03 22:00 -> 2026-10-25 23:00 | 19,632 | 0.21 |
| `data/public/belgium/energy_bid_curves_qh_2025.parquet` | ods156, ods157 | Merit-order curves of available aFRR/mFRR energy bids per quarter-hour on a fixed MW grid. | CC BY 4.0 | 2025-01-01 00:00 -> 2026-01-01 00:00 | 5,212,593 | 2.90 |
| `data/public/belgium/energy_bid_curves_qh_2026.parquet` | ods156, ods157 | Merit-order curves of available aFRR/mFRR energy bids per quarter-hour on a fixed MW grid. | CC BY 4.0 | 2026-01-01 00:00 -> 2026-10-24 22:00 | 4,617,648 | 3.43 |
| `data/public/belgium/mfrr_capacity_awards.parquet` | ods055, ods206 | Belgian mFRR capacity auction awards aggregated per delivery block and direction. | CC BY 4.0 | 2020-12-31 23:00 -> 2026-10-25 23:00 | 12,573 | 0.12 |
| `data/public/belgium/minute_2023_legacy.parquet` | ods046 | Pre-MARI per-minute imbalance prices (2023-01 to 2024-05-21), legacy layout. | CC BY 4.0 | 2023-01-01 00:00 -> 2024-01-01 00:00 | 525,600 | 8.32 |
| `data/public/belgium/minute_2024.parquet` | ods133, ods126, ods165 | Per-minute imbalance price (cumulative QH-to-date), SI/ACE/volumes and price components. | CC BY 4.0 | 2024-05-21 22:00 -> 2025-01-01 00:00 | 322,680 | 10.34 |
| `data/public/belgium/minute_2024_legacy.parquet` | ods046 | Pre-MARI per-minute imbalance prices (2023-01 to 2024-05-21), legacy layout. | CC BY 4.0 | 2024-01-01 00:00 -> 2024-05-21 22:00 | 204,360 | 3.53 |
| `data/public/belgium/minute_2025.parquet` | ods133, ods126, ods165 | Per-minute imbalance price (cumulative QH-to-date), SI/ACE/volumes and price components. | CC BY 4.0 | 2025-01-01 00:00 -> 2026-01-01 00:00 | 525,600 | 17.24 |
| `data/public/belgium/minute_2026.parquet` | ods133, ods126, ods165 | Per-minute imbalance price (cumulative QH-to-date), SI/ACE/volumes and price components. | CC BY 4.0 | 2026-01-01 00:00 -> 2026-10-24 22:00 | 427,560 | 14.52 |
| `data/public/belgium/new_formula_minute_2026.parquet` | ods211 | Elia simulation of the new 2026 imbalance-price formula, per minute (Jan-Jul 2026). | CC BY 4.0 | 2026-01-01 00:15 -> 2026-08-01 00:00 | 305,265 | 10.65 |
| `data/public/belgium/new_formula_qh_2026.parquet` | ods212 | Elia simulation of the new 2026 imbalance-price formula, per quarter-hour (Jan-Jul 2026). | CC BY 4.0 | 2026-01-01 00:00 -> 2026-08-01 00:00 | 20,352 | 0.95 |
| `data/public/belgium/qh_2019_legacy.parquet` | ods047 | Pre-MARI quarter-hour imbalance prices (before 2024-05-22), legacy layout. | CC BY 4.0 | 2019-01-01 00:00 -> 2020-01-01 00:00 | 35,040 | 0.50 |
| `data/public/belgium/qh_2020_legacy.parquet` | ods047 | Pre-MARI quarter-hour imbalance prices (before 2024-05-22), legacy layout. | CC BY 4.0 | 2020-01-01 00:00 -> 2021-01-01 00:00 | 35,136 | 0.56 |
| `data/public/belgium/qh_2021_legacy.parquet` | ods047 | Pre-MARI quarter-hour imbalance prices (before 2024-05-22), legacy layout. | CC BY 4.0 | 2021-01-01 00:00 -> 2022-01-01 00:00 | 35,040 | 0.75 |
| `data/public/belgium/qh_2022_legacy.parquet` | ods047 | Pre-MARI quarter-hour imbalance prices (before 2024-05-22), legacy layout. | CC BY 4.0 | 2022-01-01 00:00 -> 2023-01-01 00:00 | 35,040 | 0.81 |
| `data/public/belgium/qh_2023_legacy.parquet` | ods047 | Pre-MARI quarter-hour imbalance prices (before 2024-05-22), legacy layout. | CC BY 4.0 | 2023-01-01 00:00 -> 2024-01-01 00:00 | 35,040 | 0.80 |
| `data/public/belgium/qh_2024.parquet` | ods134, ods166, ods127 | Per-quarter-hour final imbalance prices, price components and volumes. | CC BY 4.0 | 2024-05-21 22:00 -> 2025-01-01 00:00 | 21,512 | 0.99 |
| `data/public/belgium/qh_2024_legacy.parquet` | ods047 | Pre-MARI quarter-hour imbalance prices (before 2024-05-22), legacy layout. | CC BY 4.0 | 2024-01-01 00:00 -> 2024-05-21 22:00 | 13,624 | 0.44 |
| `data/public/belgium/qh_2025.parquet` | ods134, ods166, ods127 | Per-quarter-hour final imbalance prices, price components and volumes. | CC BY 4.0 | 2025-01-01 00:00 -> 2026-01-01 00:00 | 35,040 | 1.46 |
| `data/public/belgium/qh_2026.parquet` | ods134, ods166, ods127 | Per-quarter-hour final imbalance prices, price components and volumes. | CC BY 4.0 | 2026-01-01 00:00 -> 2026-10-24 22:00 | 28,504 | 1.20 |
| `data/public/belgium/res_load_qh_2023.parquet` | ods001, ods031, ods032 | Belgian load, wind and PV measured + forecast per quarter-hour. | CC BY 4.0 | 2023-01-01 00:00 -> 2024-01-01 00:00 | 35,040 | 1.36 |
| `data/public/belgium/res_load_qh_2024.parquet` | ods001, ods031, ods032 | Belgian load, wind and PV measured + forecast per quarter-hour. | CC BY 4.0 | 2024-01-01 00:00 -> 2025-01-01 00:00 | 35,136 | 1.32 |
| `data/public/belgium/res_load_qh_2025.parquet` | ods001, ods031, ods032 | Belgian load, wind and PV measured + forecast per quarter-hour. | CC BY 4.0 | 2025-01-01 00:00 -> 2026-01-01 00:00 | 35,040 | 1.34 |
| `data/public/belgium/res_load_qh_2026.parquet` | ods001, ods031, ods032 | Belgian load, wind and PV measured + forecast per quarter-hour. | CC BY 4.0 | 2026-01-01 00:00 -> 2026-10-25 23:00 | 28,604 | 1.26 |
| `data/public/belgium/si_forecast_current_qh_2024.parquet` | ods136 | Elia SI forecast for the current quarter-hour, issued every minute. | CC BY 4.0 | 2024-05-21 22:00 -> 2025-01-01 00:00 | 318,715 | 3.74 |
| `data/public/belgium/si_forecast_current_qh_2025.parquet` | ods136 | Elia SI forecast for the current quarter-hour, issued every minute. | CC BY 4.0 | 2025-01-01 00:00 -> 2026-01-01 00:00 | 512,694 | 5.14 |
| `data/public/belgium/si_forecast_current_qh_2026.parquet` | ods136 | Elia SI forecast for the current quarter-hour, issued every minute. | CC BY 4.0 | 2026-01-01 00:00 -> 2026-10-25 23:00 | 424,491 | 3.11 |
| `data/public/belgium/si_forecast_next_qh_2024.parquet` | ods147 | Elia SI forecast for the next quarter-hour, issued every minute. | CC BY 4.0 | 2024-05-21 22:00 -> 2025-01-01 00:00 | 318,713 | 4.39 |
| `data/public/belgium/si_forecast_next_qh_2025.parquet` | ods147 | Elia SI forecast for the next quarter-hour, issued every minute. | CC BY 4.0 | 2025-01-01 00:00 -> 2026-01-01 00:00 | 512,699 | 6.26 |
| `data/public/belgium/si_forecast_next_qh_2026.parquet` | ods147 | Elia SI forecast for the next quarter-hour, issued every minute. | CC BY 4.0 | 2026-01-01 00:00 -> 2026-10-25 23:00 | 424,510 | 3.74 |
| `data/public/energy_charts/da_prices_BE.parquet` | energy_charts_price_BE | Belgian day-ahead auction prices (hourly until 2025-09-30, 15-minute afterwards). | CC BY 4.0 | 2022-12-31 23:00 -> 2026-10-25 23:00 | 61,539 | 0.18 |
| `data/public/energy_charts/da_prices_DE-LU.parquet` | energy_charts_price_DE-LU | German-Luxembourg day-ahead auction prices (hourly until 2025-09-30, 15-minute afterwards). | CC BY 4.0 | 2022-12-31 23:00 -> 2026-10-25 23:00 | 61,539 | 0.18 |
| `data/raw/germany/afrr_capacity.parquet` (local build only, not redistributed) | regelleistung_aFRR_capacity_results, regelleistung_aFRR_capacity_demands | German aFRR capacity auction results (4-hour blocks, up/down). | proprietary (copyright German TSOs; reproduction needs prior written consent) | 2023-12-31 23:00 -> 2026-10-25 23:00 | 12,348 | - |
| `data/raw/germany/fcr_capacity.parquet` (local build only, not redistributed) | regelleistung_FCR_capacity_results | German FCR capacity auction results (4-hour blocks). | proprietary (copyright German TSOs; reproduction needs prior written consent) | 2023-12-31 23:00 -> 2026-10-25 23:00 | 6,174 | - |

## Attribution

Contains data from Elia Open Data (https://opendata.elia.be), (c) Elia Transmission Belgium SA, licensed under the Creative Commons Attribution 4.0 International licence (CC BY 4.0). The data were modified: fields were renamed, merged, type-cast, re-gridded and/or aggregated as documented in each file's `.meta.json` sidecar.

Day-ahead prices: Bundesnetzagentur | SMARD.de, retrieved via the Energy-Charts API of Fraunhofer ISE (https://api.energy-charts.info), licensed under CC BY 4.0; de-duplicated and annotated with the product resolution.

German reserve auction results from regelleistung.net (50Hertz, Amprion, TenneT TSO, TransnetBW) are NOT redistributed: the site's terms (https://www.regelleistung.net/de-de/Impressum, 'Nutzung der Internet-Seiten') require the TSOs' prior written consent for reproducing its content. Only the fetch code and a manifest stub (`redistributed: false`) are committed; build the files locally with `euroflex data fetch germany-reserve`.

## Loader API

All loaders take half-open `[start, end)` bounds (strings, `datetime` or `pd.Timestamp`; naive = UTC), return
frames sorted by time with `datetime64[ns, UTC]` timestamps, and raise `FileNotFoundError` naming the
`euroflex data fetch ...` command that creates a missing file.

| Function | Table(s) |
| --- | --- |
| `data_root()` | root of the tree (`$EUROFLEX_DATA_DIR` or the nearest `data/public`) |
| `load_belgium_minute(start, end, columns=None)` | `belgium/minute_<YYYY>.parquet` |
| `load_belgium_qh(start, end, columns=None)` | `belgium/qh_<YYYY>.parquet` |
| `load_belgium_new_formula_minute(start, end, columns=None)` | `belgium/new_formula_minute_2026.parquet` |
| `load_belgium_new_formula_qh(start, end, columns=None)` | `belgium/new_formula_qh_2026.parquet` |
| `load_belgium_si_forecast(kind, start, end, columns=None, on="prediction_time_utc")` | `belgium/si_forecast_{current,next}_qh_<YYYY>.parquet` |
| `load_day_ahead_prices(zone, start, end)` | `energy_charts/da_prices_{BE,DE-LU}.parquet` |
| `load_capacity_awards(product, start, end)` | `belgium/{afrr,mfrr}_capacity_awards.parquet` |
| `load_res_load(start, end, columns=None)` | `belgium/res_load_qh_<YYYY>.parquet` |
| `load_energy_bid_curves(start, end, direction=None)` | `belgium/energy_bid_curves_qh_<YYYY>.parquet` |
| `load_germany_reserve(product, start, end)` | `germany/{fcr,afrr}_capacity.parquet` (local build only) |
| `load_belgium_legacy(kind, start, end, columns=None)` | `belgium/{qh,minute}_<YYYY>_legacy.parquet` |
| `read_manifest()` | `MANIFEST.json` |

CLI (`euroflex data ...`): `fetch <target>` (`belgium-minute`, `belgium-qh`, `belgium-new-formula`,
`belgium-si-forecast`, `belgium-capacity-awards`, `belgium-res-load`, `belgium-bid-curves`, `belgium-legacy`,
`day-ahead`, `germany-reserve`, `all`, or `raw --dataset odsXXX`; options `--start/--end/--zone/--product/--kind/
--refresh/--offline`), `list`, `verify`, `manifest`. `python scripts/data/fetch_all_public.py` rebuilds the tree.

## Field semantics and caveats

- **Minute table** (`minute_<YYYY>`): ODS133 columns (`si_cum_mw`, `ace_cum_mw`, `alpha_eur_mwh`,
  `alpha_prime_eur_mwh`, `mip_eur_mwh`, `mdp_eur_mwh`, `price_cum_eur_mwh`, `quality_status`) are cumulative
  quarter-hour-to-date estimates; the row labelled hh:mm includes events up to hh:mm:59, so a strategy may use
  it from hh:mm+1 plus its publication lag. ODS126 columns (`si_mw`, `ace_mw`, volumes) are instantaneous.
  ODS165 adds aFRR/mFRR price components, floor/cap and reserve-sharing prices. The grid is complete; minutes
  missing upstream are null (counts per source in the sidecar).
- **Minute 14 vs. final quarter-hour.** The last minute of a quarter-hour is a close but not exact estimate of
  the final ODS134 values (ODS134 is computed from quarter-hour metering and later validated):

| Minute file | QH compared | price equal (<= 0.01 EUR/MWh) | price within 1 EUR/MWh | median abs price diff (EUR/MWh) | median abs SI diff (MW) | SI sign agreement |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `belgium/minute_2024.parquet` | 21,407 | 62.3% | 79.2% | 0.000 | 10.9 | 94.9% |
| `belgium/minute_2025.parquet` | 35,020 | 44.0% | 77.2% | 0.105 | 8.4 | 96.8% |
| `belgium/minute_2026.parquet` | 28,387 | 36.0% | 67.7% | 0.277 | 12.3 | 95.4% |

  Use the QH table for settlement and the minute table for intra-QH information.
- **New formula** (`new_formula_*_2026`, ODS211/ODS212): Elia's best-effort, non-validated simulation of the
  imbalance price under the formula planned for 24/11/2026, computed on January-July 2026 history. Minute rows
  are cumulative QH-to-date like ODS133. Elia deletes the upstream data on 17/11/2026.
- **SI forecasts** (ODS136 current QH, ODS147 next QH): one forecast per minute; `prediction_time_utc` is the
  issue time (filter on it to avoid look-ahead). `prediction_quality` is normalized to a fraction (Elia switched
  from a fraction to a percentage on 2025-08-22 with a new forecasting system; it reports 0 for October 2025
  until 2025-11-14). Probabilities are rounded to 1e-4 and the point forecast to 0.01 MW for storage.
- **Capacity awards** (aFRR ODS125+ODS207, mFRR ODS055+ODS206): one row per 4-hour delivery block and direction.
  Daily (`0 - 24`) aFRR bids deliver in every block and are counted in each (`awarded_mw_multi_block`).
  `vwap_eur_per_mw_h` is the awarded-volume-weighted bid price (pay-as-bid), `marginal_eur_per_mw_h` the highest
  awarded price, `n_bids` the number of awarded bids. mFRR capacity is procured upward only; ODS055 lists awarded
  bids only, so offer statistics are null before 2026-05-20.
- **RES / load** (`res_load_qh_<YYYY>`): `*_da_11h_*` forecasts are published at 11:00 the day before (usable for
  day-ahead bidding), `*_da_forecast_*` at 18:00 (after the auction), `*_most_recent_*` is the last revision
  before delivery (look-ahead risk). Wind columns sum all ODS031 regions/connection types; PV uses the ODS032
  `Belgium` rows. Elia moves PV history from ODS032 to ODS209 from 26/10/2026; ODS032 was captured before that.
- **Energy bid curves** (`energy_bid_curves_qh_<YYYY>`, from ODS156/ODS157, raw rows not redistributed): for
  each quarter-hour and direction the available aFRR + mFRR bids are sorted by price (cheapest first for up,
  highest first for down; bid volumes = increments of Elia's cumulative volume, so mutually exclusive or
  unavailable bids add nothing) and `price_eur_mwh` at `cum_mw = V` is the price of the bid covering the V-th MW.
  Grid 0..2000 MW, step 25 MW; null above the available volume; upward curves start on 2025-03-01 15:45 UTC
  (start of the ODS156 rolling window). Reading each curve at the volume Elia actually activated reproduces the
  published marginal prices only roughly (quarter-hours with |SI| > 100 MW):

| Curve file | Comparison | QH | Pearson | Spearman | median abs diff (EUR/MWh) |
| --- | --- | ---: | ---: | ---: | ---: |
| `belgium/energy_bid_curves_qh_2025.parquet` | up vs MIP | 5,386 | 0.42 | 0.51 | 54.8 |
| `belgium/energy_bid_curves_qh_2025.parquet` | down vs MDP | 7,921 | 0.56 | 0.72 | 39.9 |
| `belgium/energy_bid_curves_qh_2026.parquet` | up vs MIP | 6,818 | 0.52 | 0.67 | 52.5 |
| `belgium/energy_bid_curves_qh_2026.parquet` | down vs MDP | 6,752 | 0.66 | 0.78 | 41.7 |

  It is a stylised local merit order for price-impact studies (real aFRR activation is continuous and priced via
  PICASSO, mFRR via MARI), not a settlement price. Elia's own rank order (aFRR before mFRR) was tested and
  tracks MIP/MDP worse.
- **Day-ahead prices**: hourly products until delivery day 2025-09-30, 15-minute products from 2025-10-01;
  `resolution_minutes` is stored per row.
- **Germany** (regelleistung.net): FCR `price_eur_per_mw_h` = settlement price per 4-hour block divided by the
  block length; aFRR uses the German average (and marginal) capacity price and the German block demand. The
  FCR results also list BELGIUM (demand, settlement price, deficit/surplus) as an FCR-cooperation member; the
  local build keeps them in `data/raw/germany/fcr_capacity_by_country.parquet`.
