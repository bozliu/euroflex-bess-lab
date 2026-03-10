# Data Provenance

All bundled files in `examples/data/` are frozen samples for demo, docs, CI, and benchmark reproducibility. They are **not** authoritative production datasets and should not be treated as a substitute for licensed historical data or live operator feeds.

## Bundled example datasets

| File | Upstream style/source | Raw vs truncated vs derived | Redistribution posture | Intended use |
| --- | --- | --- | --- | --- |
| `examples/data/belgium_day_ahead_prices.csv` | ENTSO-E-style Belgium day-ahead prices | Derived normalized sample | Demo/CI redistribution only | canonical benchmark, docs, CI |
| `examples/data/belgium_imbalance_prices.csv` | Elia-style Belgium imbalance prices | Derived normalized sample | Demo/CI redistribution only | validation and benchmark support |
| `examples/data/belgium_fcr_capacity_prices.csv` | Belgium FCR capacity benchmark curve | Derived simplified sample | Demo/CI redistribution only | reserve benchmark demos |
| `examples/data/belgium_afrr_capacity_up_prices.csv` | Belgium aFRR up-capacity benchmark curve | Derived simplified sample | Demo/CI redistribution only | canonical Belgium aFRR examples |
| `examples/data/belgium_afrr_capacity_down_prices.csv` | Belgium aFRR down-capacity benchmark curve | Derived simplified sample | Demo/CI redistribution only | canonical Belgium aFRR examples |
| `examples/data/belgium_afrr_activation_price_up.csv` | Belgium aFRR up-activation benchmark curve | Derived simplified sample | Demo/CI redistribution only | canonical Belgium aFRR examples |
| `examples/data/belgium_afrr_activation_price_down.csv` | Belgium aFRR down-activation benchmark curve | Derived simplified sample | Demo/CI redistribution only | canonical Belgium aFRR examples |
| `examples/data/belgium_afrr_activation_ratio_up.csv` | Belgium aFRR up-activation ratio benchmark curve | Derived simplified sample | Demo/CI redistribution only | canonical Belgium aFRR examples |
| `examples/data/belgium_afrr_activation_ratio_down.csv` | Belgium aFRR down-activation ratio benchmark curve | Derived simplified sample | Demo/CI redistribution only | canonical Belgium aFRR examples |
| `examples/data/netherlands_day_ahead_prices.csv` | ENTSO-E-style Netherlands day-ahead prices | Derived normalized sample | Demo/CI redistribution only | secondary-surface example and tests |
| `examples/data/netherlands_imbalance_prices.csv` | TenneT-style Netherlands imbalance prices | Derived normalized sample | Demo/CI redistribution only | internal tests and secondary-surface examples |
| `examples/data/netherlands_fcr_capacity_prices.csv` | Netherlands FCR capacity benchmark curve | Derived simplified sample | Demo/CI redistribution only | secondary-surface reserve examples |

## Test fixtures

The raw payloads under `tests/fixtures/raw/` are near-raw operator-format samples used to test connector normalization and schema drift behavior.

| Location | Purpose |
| --- | --- |
| `tests/fixtures/raw/entsoe/` | ENTSO-E XML normalization and DST edge-case checks |
| `tests/fixtures/raw/elia/` | Elia JSON normalization and imbalance parsing checks |
| `tests/fixtures/raw/tennet/` | TenneT settlement-price normalization checks |

## Usage boundary

- Bundled samples are for demo, CI, docs, notebooks, and benchmark reproducibility only.
- This repository does not grant any rights to third-party upstream data beyond these frozen sample files.
- For production or commercial use, source fresh data directly from the relevant operator or a properly licensed vendor.
