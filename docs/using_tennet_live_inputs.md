# Using TenneT Live Inputs

This page documents the current live Dutch connector milestone.

## What is supported now

- live TenneT settlement-price ingestion through `euroflex ingest tennet-nl-imbalance`
- live TenneT merit-order ingestion through `euroflex ingest tennet-nl-merit-order`
- live TenneT aFRR activation-volume ingestion through `euroflex ingest tennet-nl-afrr-activations`
- derived Dutch activation price and activation-ratio series through `euroflex ingest tennet-nl-afrr-derived`
- raw payload capture plus provenance sidecars
- normalization into the internal Dutch imbalance and reserve-table schemas
- validation against the standard `validate-data` contract
- use of the normalized parquet in Dutch secondary workflows

The connector milestone is about trusted ingestion and provenance. It is not a claim of live submission readiness.

## TenneT connector contract

- endpoints:
  - settlement prices: `https://api.tennet.eu/publications/v1/settlement-prices`
  - merit order list: `https://api.tennet.eu/publications/v1/merit-order-list`
  - frequency restoration reserve activations: `https://api.tennet.eu/publications/v1/frequency-restoration-reserve-activations`
- environments:
  - `production` -> `https://api.tennet.eu/publications/v1/...`
  - `acceptance` -> `https://api.acc.tennet.eu/publications/v1/...`
  - override hooks if your TenneT portal assigns a different host:
    - `TENNET_API_BASE_URL_PRODUCTION`
    - `TENNET_API_BASE_URL_ACCEPTANCE`
    - `TENNET_API_BASE_URL` as a generic fallback
- auth:
  - `TENNET_API_KEY` as a generic fallback
  - `TENNET_API_KEY_ACCEPTANCE` and `TENNET_API_KEY_PRODUCTION` for environment-specific credentials
- selector:
  - CLI: `--env acceptance|production`
  - Python: `TenneTSettlementPricesConnector(environment="acceptance" | "production")`
  - default connector environment: `TENNET_API_ENV` or `production`
- upstream timestamp interpretation: TenneT settlement `timeInterval_*` fields are treated as UTC and then converted into `Europe/Amsterdam`
- cadence: native 15-minute rows only
- failure behavior:
  - missing or rejected credentials fail early
  - `429` rate limits are translated into connector errors
  - malformed payloads fail schema validation before normalization
  - wrapped production payloads under `Response.TimeSeries` are normalized automatically

## Live Dutch reserve path

For reserve-aware Dutch live ingestion, keep the layers explicit:

1. `euroflex ingest tennet-nl-merit-order`
2. `euroflex ingest tennet-nl-afrr-activations`
3. `euroflex ingest tennet-nl-afrr-derived`

The derived command writes:

- normalized merit-order ladders
- normalized activation-volume tables
- derived `afrr_activation_price_up`
- derived `afrr_activation_price_down`
- derived `afrr_activation_ratio_up`
- derived `afrr_activation_ratio_down`

Example:

```bash
euroflex ingest tennet-nl-afrr-derived \
  --start 2025-01-13T00:00:00Z \
  --end 2025-01-14T00:00:00Z \
  --env acceptance \
  --out-dir data/live/dutch_reserve
```

This reserve live surface is intentionally narrower than a full Dutch live `da_plus_afrr` promise. The current connector milestone does **not** claim a direct live TenneT endpoint for Dutch aFRR capacity remuneration prices, so `afrr_capacity_up` and `afrr_capacity_down` remain outside this live-ingest contract for now.

## Recommended live-input flow

Use live ENTSO-E day-ahead plus live TenneT imbalance, then point a Dutch config at the generated parquet files:

```bash
euroflex ingest entsoe-da \
  --start 2026-01-01T00:00:00Z \
  --end 2026-01-02T00:00:00Z \
  --zone 10YNL----------L \
  --out-raw data/live/netherlands_day_ahead.xml \
  --out-parquet data/live/netherlands_day_ahead.parquet

euroflex ingest tennet-nl-imbalance \
  --start 2026-01-01T00:00:00Z \
  --end 2026-01-02T00:00:00Z \
  --env acceptance \
  --out-raw data/live/netherlands_imbalance.json \
  --out-parquet data/live/netherlands_imbalance.parquet
```

Then use:

- `examples/configs/basic/netherlands_da_only_live_inputs.yaml`

and run:

```bash
euroflex validate-config examples/configs/basic/netherlands_da_only_live_inputs.yaml
euroflex validate-data examples/configs/basic/netherlands_da_only_live_inputs.yaml
euroflex backtest examples/configs/basic/netherlands_da_only_live_inputs.yaml --market netherlands --workflow da_only
euroflex reconcile artifacts/examples/<run_id> examples/configs/basic/netherlands_da_only_live_inputs.yaml
euroflex export-schedule artifacts/examples/<run_id> --profile operator
euroflex export-bids artifacts/examples/<run_id> --profile bid_planning
```

## Provenance sidecars

Both raw and normalized outputs write `.meta.json` sidecars. The metadata includes:

- `connector_id`
- `endpoint_id`
- `source_operator`
- `auth_mode`
- `environment`
- `base_url`
- `fetched_at_utc`
- `request_start_utc`
- `request_end_utc`
- `cache_hit`
- retry and timeout settings
- `normalization_name`
- `local_timezone`

For the normalized parquet, the sidecar also captures market/zone/series metadata so downstream operators can trace where the Dutch imbalance series came from.

For reserve tables and derived reserve series, the sidecars also capture the normalization step so teams can distinguish:

- direct live tables from TenneT
- derived activation series produced inside `euroflex_bess_lab`
