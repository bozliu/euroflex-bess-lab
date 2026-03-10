# Data Provenance

For a commercial-grade BESS benchmarking tool, data provenance is part of the trust model. If a backtest shows a strong outcome, users need to know where the underlying market data came from, whether it was normalized or simplified, what it is suitable for, and what legal or operational caveats still apply.

The bundled files in `examples/data/` are frozen, normalized samples for demo, docs, CI, and benchmark reproducibility. They are intentionally small and deterministic. They are **not** live operator feeds, **not** settlement statements, and **not** a substitute for a licensed production data pipeline.

## Bundled example datasets

These samples may be truncated, normalized, or simplified so the public quickstart remains reproducible without API keys.

| File | Market | Source/operator | Status | Redistribution posture | Intended use | Known limitations |
| --- | --- | --- | --- | --- | --- | --- |
| `examples/data/belgium_day_ahead_prices.csv` | Belgium day-ahead | ENTSO-E Transparency Platform workflow | Derived, normalized sample | Bundled in this repo for demo/CI/docs; verify upstream rights before wider reuse | canonical benchmark, quickstart, docs, CI | not a full historical archive; not settlement-grade |
| `examples/data/belgium_imbalance_prices.csv` | Belgium imbalance | Elia public imbalance publications | Derived, normalized sample | Bundled in this repo for demo/CI/docs; verify upstream rights before wider reuse | validation, benchmark support, docs | frozen sample only; not a live imbalance feed |
| `examples/data/belgium_fcr_capacity_prices.csv` | Belgium FCR capacity | Belgium public market-derived benchmark curve | Derived, simplified sample | Bundled in this repo for demo/CI/docs; verify upstream rights before wider reuse | reserve benchmark demos | simplified benchmark surface, not market submission data |
| `examples/data/belgium_afrr_capacity_up_prices.csv` | Belgium aFRR capacity up | Elia-style public market publication workflow | Derived, simplified sample | Bundled in this repo for demo/CI/docs; verify upstream rights before wider reuse | canonical Belgium aFRR examples | simplified expected-value benchmark input |
| `examples/data/belgium_afrr_capacity_down_prices.csv` | Belgium aFRR capacity down | Elia-style public market publication workflow | Derived, simplified sample | Bundled in this repo for demo/CI/docs; verify upstream rights before wider reuse | canonical Belgium aFRR examples | simplified expected-value benchmark input |
| `examples/data/belgium_afrr_activation_price_up.csv` | Belgium aFRR activation up | Elia-style public market publication workflow | Derived, simplified sample | Bundled in this repo for demo/CI/docs; verify upstream rights before wider reuse | canonical Belgium aFRR examples | simplified expected-value benchmark input |
| `examples/data/belgium_afrr_activation_price_down.csv` | Belgium aFRR activation down | Elia-style public market publication workflow | Derived, simplified sample | Bundled in this repo for demo/CI/docs; verify upstream rights before wider reuse | canonical Belgium aFRR examples | simplified expected-value benchmark input |
| `examples/data/belgium_afrr_activation_ratio_up.csv` | Belgium aFRR activation ratio up | Elia-style public market publication workflow | Derived, simplified sample | Bundled in this repo for demo/CI/docs; verify upstream rights before wider reuse | canonical Belgium aFRR examples | simplified expected-value benchmark input |
| `examples/data/belgium_afrr_activation_ratio_down.csv` | Belgium aFRR activation ratio down | Elia-style public market publication workflow | Derived, simplified sample | Bundled in this repo for demo/CI/docs; verify upstream rights before wider reuse | canonical Belgium aFRR examples | simplified expected-value benchmark input |
| `examples/data/netherlands_day_ahead_prices.csv` | Netherlands day-ahead | ENTSO-E Transparency Platform workflow | Derived, normalized sample | Bundled in this repo for demo/CI/docs; verify upstream rights before wider reuse | secondary-surface example, docs, tests | secondary surface only; not a promoted live workflow |
| `examples/data/netherlands_imbalance_prices.csv` | Netherlands imbalance | TenneT public settlement-price publications | Derived, normalized sample | Bundled in this repo for demo/CI/docs; verify upstream rights before wider reuse | secondary-surface examples and tests | frozen sample only; not a live settlement feed |
| `examples/data/netherlands_fcr_capacity_prices.csv` | Netherlands FCR capacity | Netherlands public market-derived benchmark curve | Derived, simplified sample | Bundled in this repo for demo/CI/docs; verify upstream rights before wider reuse | secondary-surface reserve examples | simplified benchmark surface |

## Raw connector fixtures

The near-raw payloads under `tests/fixtures/raw/` are included to test connector normalization and schema drift behavior against representative upstream formats.

| Location | Source/operator | Purpose |
| --- | --- | --- |
| `tests/fixtures/raw/entsoe/` | ENTSO-E Transparency Platform XML | normalization coverage plus DST edge-case checks |
| `tests/fixtures/raw/elia/` | Elia Opendatasoft JSON | imbalance parsing and schema-shape checks |
| `tests/fixtures/raw/tennet/` | TenneT settlement-price JSON | NL imbalance normalization checks |

## Normalization, timezones, and missing-value handling

The public benchmark surface expects 15-minute data after normalization.

- ENTSO-E day-ahead payloads can arrive at coarser resolution. When that happens, the normalizer expands each source interval to a 15-minute grid and records the transformation in `provenance`, for example `expanded_from_60m`.
- Elia imbalance and TenneT settlement-price normalization in the current release require native 15-minute intervals. Non-15-minute payloads fail fast instead of being silently reshaped.
- Normalized frames carry both `timestamp_utc` and `timestamp_local`. Validation checks that `timestamp_local` matches the configured market timezone, that UTC timestamps are unique and sorted, and that the delivery window covers the expected 15-minute grid.
- DST matters. The repo includes dedicated ENTSO-E DST fixtures, and `euroflex validate-data` checks timezone alignment and delivery-window coverage against the configured local timezone.
- Missing or malformed payloads fail normalization or validation. The public pipeline does not silently backfill missing intervals with zeros or inferred prices.

## Live-data ingestion paths

For live or fresh historical pulls, use the built-in ingestion commands and connect them to your own storage or licensed data pipeline:

```bash
euroflex ingest entsoe-da --start 2026-01-01T00:00:00Z --end 2026-01-02T00:00:00Z --out-raw raw/entsoe.xml --out-parquet normalized/day_ahead.parquet
euroflex ingest elia-imbalance --start 2026-01-01T00:00:00Z --end 2026-01-02T00:00:00Z --out-raw raw/elia.json --out-parquet normalized/imbalance.parquet
euroflex ingest tennet-nl-imbalance --start 2026-01-01T00:00:00Z --end 2026-01-02T00:00:00Z --out-raw raw/tennet.json --out-parquet normalized/nl_imbalance.parquet
```

Current upstream connector surfaces:

- ENTSO-E day-ahead: `https://web-api.tp.entsoe.eu/api` with `documentType=A44` and an `ENTSOE_API_TOKEN`
- Elia imbalance: `https://opendata.elia.be/api/explore/v2.1/catalog/datasets/ods162/records`
- TenneT NL settlement prices: `https://api.tennet.eu/publications/v1/settlement-prices` with a `TENNET_API_KEY`

For commercial or operational use, point these commands at a governed internal data lake, warehouse, or vendor-backed feed rather than relying on the frozen CSVs in this repository.

## Usage boundary

- Bundled example data is for reproducible examples, tests, docs, and benchmarks.
- Bundled data is not a live operational feed or settlement-grade statement.
- Users are responsible for validating licensing, redistribution rights, and production suitability before use outside this public example surface.
