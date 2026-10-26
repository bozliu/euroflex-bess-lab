# Forward track record (paper)

`reports/track_record/` is an out-of-sample record of the frozen live strategies on every complete Belgian delivery
day after the end of the committed data set (from 2026-10-25). Nothing in it was available when the strategies
and their forecasters were fixed: the models were fitted on data up to 2026-10-24 and are never refitted. Each new
day is simulated minute by minute with the same engine and publication model as the backtests, for a 1 MW / 2 MWh
battery, and appended to the record. `scripts/ops/track_record.py` does all of it; the generated
`reports/track_record/README.md` shows the cumulative P&L, EUR/MW/yr to date and a chart.

> Paper money: a simulation on Elia's published data. No order or bid is submitted to any market; price taker
> (the battery's own deviation does not move the price).

## Strategies (frozen)

| Record name | Strategy | Backtest counterpart | Runs |
| --- | --- | --- | --- |
| `threshold` | price bands: charge below the 25 % and discharge above the 75 % quantile of the last 14 days of final prices, read against the running quarter-hour price | `threshold_price` | always |
| `forecast_mpc` | rolling stochastic MPC on the current-formula price forecaster, 30 % / 70 % quantile risk filter | `forecast_mpc_risk` | when the deployment models are present; until the new-formula go-live (its model prices the current formula) |
| `scales_l` | SCALES-L: mechanism-aware state forecasts priced by the settlement operator, same MPC core and risk filter; after the go-live the zero-history new-formula forecaster fused with the live running price | `prism_risk` | when the deployment models are present |

The strategies are built with the same factories as `euroflex live run` (`threshold`, `forecast-mpc`, `prism`).
The models are identified by a models id (sha256 over the forecaster files); `models/reference_manifest.json`
records the reference build. A run with different models is refused for the model strategies unless
`--allow-model-change` is passed, so the record cannot silently switch to refitted models.

## Method

- **Days.** Belgian local delivery days (23, 24 or 25 hours). A day is recorded once every quarter-hour has a
  published settlement price and the running price is published through the end of the day. An incomplete day
  waits; after `--max-wait-days` (5) it is recorded with its gaps flagged if at least half of its prices exist. A
  day without data is never recorded.
- **Engine.** `euroflex_bess_lab.realtime.simulate`: one decision per minute; a minute row is usable one minute after
  the minute ends, a final quarter-hour price five minutes after the quarter-hour, day-ahead prices from 13:00 on
  D-1. Each day is simulated on a market window that ends at the end of that day, so no later row is ever loaded.
- **Continuity.** The battery's state of charge carries over from one day to the next (exactly, from
  `daily_pnl.csv`). Each day sees a 28-day history window in which the strategy does not trade, which covers the
  threshold's 14-day lookback and the 28-day price climatology of the MPC.
- **Settlement.** The formula in force: current formula (ODS134 price) before the new-formula go-live (planned
  24 Nov 2026 10:00 Brussels time, subject to CREG approval), new formula (ODS212 price) from it. A day that
  contains the go-live is split at that instant.
- **Data.** Days covered by the committed `data/public` set are read through the public-data loaders. Newer days are
  downloaded with the public-data fetch code (read-only GETs to Elia Open Data and Energy-Charts) into the gitignored
  cache `data/raw/track_record` and normalised with the same functions that build `data/public`. Prices are taken
  as first published in Elia's historical datasets: most quarter-hours are not yet validated at that point, and
  recorded days are not restated (the counts are in `qh_not_validated`).
- **Append-only.** A recorded day is never recomputed or rewritten; re-running without a new complete day changes
  no file. `track_record.py verify` re-simulates recorded days from their recorded starting state with current data
  and reports P&L differences (for example after Elia validates prices).

Evidence, all in `tests/test_deploy_track_record.py`: appending day by day gives byte-identical files to appending
all days at once; replacing every value after a recorded day with noise leaves that day unchanged; the look-ahead
audit passes inside a day; the day-by-day state equals one continuous simulation; the forward download path
reproduces the committed tables exactly; a go-live day settles each part under its own formula; a model swap is
refused. Started from the backtest's own state of charge, the runner reproduces the committed backtest ledger of
the threshold strategy on every day from 2026-08-12 to 2026-10-24 (74 days, to the float32 precision of the stored
ledger; slow test, needs the full `data/public` set).

## Files

| File | Content |
| --- | --- |
| `daily_pnl.csv` | one row per day and strategy: `local_date, strategy, mechanism, n_qh, pnl_eur, revenue_eur, degradation_eur, discharged_mwh, charged_mwh, cycles, cumulative_pnl_eur, soc_start_mwh, soc_end_mwh, price_missing_qh, qh_not_validated, minutes_missing, data_complete, data_source, models_id, computed_at_utc` |
| `qh/<YYYY-MM>.csv` | quarter-hour ledger per month of delivery days: `timestamp_utc, strategy, mechanism, energy_mwh, discharged_mwh, charged_mwh, price_eur_mwh, price_missing, revenue_eur, degradation_eur, pnl_eur, soc_end_mwh` |
| `summary.json` | per strategy: days, P&L, EUR/MW/yr to date, cycles per day, best and worst day, drawdown; battery, publication model, settlement rule, models; inputs of the last update (dataset chunks and sha256) |
| `README.md`, `cumulative_pnl.png` | generated from the two CSV files (`track_record.py render`) |

`minutes_missing` counts the minutes of the day without a running-price row; for example Elia's minute datasets
have no rows for the two hours around the October DST switch.

## Running it

```bash
pip install -e '.[ml]'
python scripts/ops/build_models.py build --out models      # once; needed for forecast_mpc and scales_l
EUROFLEX_MODELS_DIR=models python scripts/ops/track_record.py update
python scripts/ops/track_record.py update --offline       # cached data only
python scripts/ops/track_record.py verify                 # re-simulate recorded days and compare
python scripts/ops/track_record.py render                 # regenerate README.md, summary.json and the chart
```

Without models only `threshold` advances; the model strategies catch up from their last recorded day once the
models are available (still frozen, still causal, only computed later: `computed_at_utc` shows when). A daily update
downloads the current month of eight Elia datasets plus the Energy-Charts day-ahead month (about 10 MB) and
simulates in seconds (peak memory about 0.5 GB with all three strategies). Elia adds a day to its historical
datasets one to two days later; an update before that reports the day as pending.

## Scheduling (examples, not enabled)

The record grows only when someone runs `update`. Three ways the repository owner could schedule it once a day; none
of them is enabled by this repository.

cron on any machine with a checkout (commit and push the changes yourself or add the git commands):

```cron
30 12 * * * cd /path/to/euroflex-bess-lab && EUROFLEX_MODELS_DIR=models .venv/bin/python scripts/ops/track_record.py update >> "$HOME/.cache/euroflex_bess_lab/track_record.log" 2>&1
```

launchd on macOS (`~/Library/LaunchAgents/io.github.euroflex-bess-lab.track-record.plist`, local time; replace
`/path/to/euroflex-bess-lab`):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>io.github.euroflex-bess-lab.track-record</string>
  <key>WorkingDirectory</key><string>/path/to/euroflex-bess-lab</string>
  <key>ProgramArguments</key>
  <array>
    <string>/path/to/euroflex-bess-lab/.venv/bin/python</string>
    <string>scripts/ops/track_record.py</string>
    <string>update</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict><key>EUROFLEX_MODELS_DIR</key><string>models</string></dict>
  <key>StartCalendarInterval</key><dict><key>Hour</key><integer>13</integer><key>Minute</key><integer>30</integer></dict>
</dict>
</plist>
```

GitHub Actions (a workflow file the owner would add under `.github/workflows/`):

```yaml
name: forward-track-record
on:
  schedule:
    - cron: "30 12 * * *" # daily, 12:30 UTC
  workflow_dispatch: {}
permissions:
  contents: write
concurrency:
  group: forward-track-record
jobs:
  update:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Install (library versions of models/reference_manifest.json, so the rebuilt models are the frozen ones)
        run: pip install -e '.[ml]' numpy==2.5.4 pandas==3.1.0 scipy==1.18.1 scikit-learn==1.9.1
      - name: Restore models
        id: models
        uses: actions/cache@v4
        with:
          path: models
          key: deploy-models-da8a2764093e4383
      - name: Build models
        if: steps.models.outputs.cache-hit != 'true'
        run: python scripts/ops/build_models.py build --out models
      - name: Update the track record
        env:
          EUROFLEX_MODELS_DIR: models
        run: python scripts/ops/track_record.py update
      - name: Commit the new days
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
          git add reports/track_record
          git diff --cached --quiet || git commit -m "Forward track record: $(date -u +%F)"
          git push
```

What any of these needs: Python 3.12 with `.[ml]`; the committed `data/public` set; outbound HTTPS to
`opendata.elia.be` and `api.energy-charts.info`; about 1 GB of memory; for the model strategies, a models directory
whose id matches the recorded one (the same library versions as the reference build, or the cached build). The
Actions variant also needs write permission for the workflow token on the target branch (branch protection may
require a bot or a pull request instead of a direct push).

Data: Elia Transmission Belgium SA, Elia Open Data (CC BY 4.0); day-ahead prices Bundesnetzagentur | SMARD.de via
Energy-Charts (CC BY 4.0).
