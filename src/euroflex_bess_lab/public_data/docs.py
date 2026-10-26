"""Render ``data/public/README.md``, ``data/public/LICENSES.md`` and ``docs/public_datasets.md`` from the manifest.

Generated so the documentation always matches the committed files (sizes, rows, coverage, licences).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from .elia import ELIA_ATTRIBUTION, ELIA_LICENCE_URL
from .energy_charts import ENERGY_CHARTS_ATTRIBUTION
from .regelleistung import REGELLEISTUNG_TERMS_URL

ELIA_STATEMENT = (
    "Contains data from Elia Open Data (https://opendata.elia.be), (c) Elia Transmission Belgium SA, licensed "
    "under the Creative Commons Attribution 4.0 International licence (CC BY 4.0). The data were modified: "
    "fields were renamed, merged, type-cast, re-gridded and/or aggregated as documented in each file's "
    "`.meta.json` sidecar."
)
ENERGY_CHARTS_STATEMENT = (
    "Day-ahead prices: Bundesnetzagentur | SMARD.de, retrieved via the Energy-Charts API of Fraunhofer ISE "
    "(https://api.energy-charts.info), licensed under CC BY 4.0; de-duplicated and annotated with the product "
    "resolution."
)
REGELLEISTUNG_STATEMENT = (
    "German reserve auction results from regelleistung.net (50Hertz, Amprion, TenneT TSO, TransnetBW) are NOT "
    "redistributed: the site's terms (" + REGELLEISTUNG_TERMS_URL + ", 'Nutzung der Internet-Seiten') require "
    "the TSOs' prior written consent for reproducing its content. Only the fetch code and a manifest stub "
    "(`redistributed: false`) are committed; build the files locally with `euroflex data fetch germany-reserve`."
)


def _mb(value: object) -> str:
    return f"{int(str(value or 0)) / 1e6:.2f}"


def _cov(entry: Mapping[str, Any]) -> str:
    start = str(entry.get("coverage_start_utc") or "?")[:16].replace("T", " ")
    end = str(entry.get("coverage_end_utc") or "?")[:16].replace("T", " ")
    return f"{start} -> {end}"


def _files(manifest: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    files: Sequence[Mapping[str, Any]] = manifest.get("files", [])
    return list(files)


def render_dataset_table(manifest: Mapping[str, Any], *, link_prefix: str = "") -> str:
    lines = [
        "| File | Dataset id(s) | Content | Licence | Coverage (UTC, [start, end)) | Rows | Size (MB) |",
        "| --- | --- | --- | --- | --- | ---: | ---: |",
    ]
    for entry in _files(manifest):
        path = str(entry["path"])
        shown = f"`{link_prefix}{path}`"
        if not entry.get("redistributed", True):
            shown = f"`{entry.get('local_path', path)}` (local build only, not redistributed)"
        ids = ", ".join(str(d) for d in entry.get("dataset_ids", []))
        size = _mb(entry.get("size_bytes")) if entry.get("redistributed", True) else "-"
        lines.append(
            f"| {shown} | {ids} | {entry.get('title', '')} | {entry.get('licence', '')} | {_cov(entry)} | "
            f"{int(entry.get('rows', 0)):,} | {size} |"
        )
    return "\n".join(lines)


def _consistency_rows(manifest: Mapping[str, Any]) -> list[str]:
    rows = []
    for entry in _files(manifest):
        validation = entry.get("validation") or {}
        check = validation.get("last_minute_vs_qh") if isinstance(validation, Mapping) else None
        if not isinstance(check, Mapping):
            continue
        price = check.get("price_cum_eur_mwh_vs_imbalance_price_eur_mwh") or {}
        si = check.get("si_cum_mw_vs_si_mw") or {}
        if not price:
            continue
        rows.append(
            f"| `{entry['path']}` | {int(price.get('compared', 0)):,} | {100 * float(price.get('match_rate', 0)):.1f}% | "
            f"{100 * float(price.get('match_rate_within_1', 0)):.1f}% | {float(price.get('median_abs_diff', 0)):.3f} | "
            f"{float(si.get('median_abs_diff', 0)):.1f} | {100 * float(si.get('sign_match_rate', 0)):.1f}% |"
        )
    return rows


def render_consistency_table(manifest: Mapping[str, Any]) -> str:
    rows = _consistency_rows(manifest)
    if not rows:
        return "_No minute tables in the manifest._"
    header = [
        "| Minute file | QH compared | price equal (<= 0.01 EUR/MWh) | price within 1 EUR/MWh | median abs price diff "
        "(EUR/MWh) | median abs SI diff (MW) | SI sign agreement |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    return "\n".join(header + rows)


def render_curve_table(manifest: Mapping[str, Any]) -> str:
    rows = []
    for entry in _files(manifest):
        validation = entry.get("validation") or {}
        check = validation.get("curve_vs_published_prices") if isinstance(validation, Mapping) else None
        if not isinstance(check, Mapping):
            continue
        for direction, target in (("up", "MIP"), ("down", "MDP")):
            item = check.get(direction) or {}
            if not item or "pearson" not in item:
                continue
            rows.append(
                f"| `{entry['path']}` | {direction} vs {target} | {int(item['compared']):,} | {float(item['pearson']):.2f} | "
                f"{float(item['spearman']):.2f} | {float(item['median_abs_diff_eur_mwh']):.1f} |"
            )
    if not rows:
        return "_No curve validation in the manifest._"
    header = [
        "| Curve file | Comparison | QH | Pearson | Spearman | median abs diff (EUR/MWh) |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    return "\n".join(header + rows)


def _snapshot(manifest: Mapping[str, Any]) -> str:
    ends = sorted({str(e.get("snapshot_end_utc")) for e in _files(manifest) if e.get("snapshot_end_utc")})
    if not ends:
        return "YYYY-MM-DD"
    # snapshot_end_utc is a Brussels midnight; --as-of takes the Brussels date of that midnight.
    return f"{pd.Timestamp(ends[-1]).tz_convert('Europe/Brussels'):%Y-%m-%d}"


def render_readme(manifest: Mapping[str, Any]) -> str:
    total = int(manifest.get("total_size_bytes", 0)) / 1e6
    snapshot = _snapshot(manifest)
    return f"""# Public data (`data/public`)

Normalized, redistributable public market data used by the euroflex-bess-lab backtests. Every file is a
zstd-compressed parquet table with a `<name>.meta.json` sidecar (source URLs, licence, attribution, retrieval
time, coverage, rows, columns, sha256, transformation notes, validation results). `MANIFEST.json` aggregates all
sidecars. Total committed size: {total:.1f} MB (budget 200 MB, at most 50 MB per file).

Conventions: timestamps are tz-aware UTC and mark the *start* of the interval; intervals are half-open
`[start, end)`; Elia SI > 0 means the system is long; numeric columns are float32 with units in the column name.

## Files

{render_dataset_table(manifest)}

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
python scripts/data/fetch_all_public.py --offline --as-of {snapshot}   # rebuild this snapshot from the cache
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
"""


def render_licenses(manifest: Mapping[str, Any]) -> str:
    elia_files = [str(e["path"]) for e in _files(manifest) if "Elia" in str(e.get("attribution", ""))]
    ec_files = [str(e["path"]) for e in _files(manifest) if str(e.get("path", "")).startswith("energy_charts/")]
    stub_files = [str(e["path"]) for e in _files(manifest) if not e.get("redistributed", True)]

    def bullet(items: Sequence[str]) -> str:
        return "\n".join(f"- `{item}`" for item in items) or "- (none)"

    return f"""# Licences and attribution for `data/public`

## Elia Open Data — CC BY 4.0

{ELIA_STATEMENT}

Attribution: **{ELIA_ATTRIBUTION}**. Licence: https://creativecommons.org/licenses/by/4.0/ (Elia's licence page:
{ELIA_LICENCE_URL}). Files:

{bullet(elia_files)}

## Energy-Charts day-ahead prices — CC BY 4.0

{ENERGY_CHARTS_STATEMENT} Attribution: **{ENERGY_CHARTS_ATTRIBUTION}**. Files:

{bullet(ec_files)}

## regelleistung.net — not redistributed

{REGELLEISTUNG_STATEMENT} Manifest stubs:

{bullet(stub_files)}

## Code

The code that downloads and transforms these data is part of euroflex-bess-lab (Apache-2.0). The licences above
apply to the data only; no endorsement by the data providers is implied.
"""


def render_docs_page(manifest: Mapping[str, Any]) -> str:
    total = int(manifest.get("total_size_bytes", 0)) / 1e6
    return f"""# Public datasets

euroflex-bess-lab ships a reproducible public-data layer (`euroflex_bess_lab.public_data`): downloaders with
retries and a raw cache, normalizers that map upstream fields to a snake_case contract, writers (zstd parquet +
`.meta.json` sidecar + `MANIFEST.json`) and loaders. The committed tree lives in `data/public/`
({total:.1f} MB); the raw downloads are cached in the gitignored `data/raw/`.

## Datasets

{render_dataset_table(manifest, link_prefix="data/public/")}

## Attribution

{ELIA_STATEMENT}

{ENERGY_CHARTS_STATEMENT}

{REGELLEISTUNG_STATEMENT}

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
| `load_belgium_si_forecast(kind, start, end, columns=None, on="prediction_time_utc")` | `belgium/si_forecast_{{current,next}}_qh_<YYYY>.parquet` |
| `load_day_ahead_prices(zone, start, end)` | `energy_charts/da_prices_{{BE,DE-LU}}.parquet` |
| `load_capacity_awards(product, start, end)` | `belgium/{{afrr,mfrr}}_capacity_awards.parquet` |
| `load_res_load(start, end, columns=None)` | `belgium/res_load_qh_<YYYY>.parquet` |
| `load_energy_bid_curves(start, end, direction=None)` | `belgium/energy_bid_curves_qh_<YYYY>.parquet` |
| `load_germany_reserve(product, start, end)` | `germany/{{fcr,afrr}}_capacity.parquet` (local build only) |
| `load_belgium_legacy(kind, start, end, columns=None)` | `belgium/{{qh,minute}}_<YYYY>_legacy.parquet` |
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

{render_consistency_table(manifest)}

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

{render_curve_table(manifest)}

  It is a stylised local merit order for price-impact studies (real aFRR activation is continuous and priced via
  PICASSO, mFRR via MARI), not a settlement price. Elia's own rank order (aFRR before mFRR) was tested and
  tracks MIP/MDP worse.
- **Day-ahead prices**: hourly products until delivery day 2025-09-30, 15-minute products from 2025-10-01;
  `resolution_minutes` is stored per row.
- **Germany** (regelleistung.net): FCR `price_eur_per_mw_h` = settlement price per 4-hour block divided by the
  block length; aFRR uses the German average (and marginal) capacity price and the German block demand. The
  FCR results also list BELGIUM (demand, settlement price, deficit/surplus) as an FCR-cooperation member; the
  local build keeps them in `data/raw/germany/fcr_capacity_by_country.parquet`.
"""
