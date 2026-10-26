# Live paper trading (shadow mode)

`euroflex_bess_lab.realtime.live` runs the **same `Strategy` objects that the backtests use** against Elia's
near-real-time public feeds, minute by minute. It records every decision, settles a paper ledger per quarter-hour
on the price Elia publishes, and reconciles that ledger later with validated data. It shows that a backtested
strategy is deployable (identical decisions on identical information) and builds a live track record.

> **Safety statement.** This is paper trading only. No order, bid or nomination is ever submitted to any market
> (Elia, EPEX, a TSO platform or a broker), and the repository contains no market-submission adapter. A setpoint is
> a paper instruction that is logged (default), appended to a CSV file, or POSTed to a URL you configure
> explicitly. Every payload carries `"mode": "paper"` and `"market_submission": false`.

## Architecture

```text
 Elia Open Data (records API)        Energy-Charts /price
 ods161 ods169 ods175 ods162          day-ahead BE (15 min)
 ods136 ods147 ods211 ods212                 |
            |                                |
            v                                v
 feeds.EliaLiveFeeds: per-feed schedules, timeouts, retries/backoff + Retry-After, schema checks,
                      payload cache, revision + gap detection, first-seen latency   (feeds.py)
            |  contract-shaped rows (Plan.md section 5 names, UTC, float32)
            v
 engine.LiveEngine
   rolling MarketData window  <- data/public history + API backfill + live rows
   visibility gate            <- rows enter the window only when PublicationModel says they are public
   DecisionContext -> strategy.decide(ctx) -> battery.apply_power -> ExecutionAdapter (paper | csv | webhook)
   quarter-hour books -> settle_quarter_hours on the published price (current / new2026 by date)
   run directory: decisions.jsonl, qh_ledger.csv/.parquet, status.json, state.json, feed_log.jsonl,
                  archive/ (every row version exactly as published: raw JSON + normalized)
            |
            v
 reconcile.reconcile_run: provisional ledger vs validated prices (data/public, ods134/ods212, ods162 snapshot)
```

| Module | Role |
| --- | --- |
| `feeds.py` | `FeedSpec` per dataset, `normalize_records` / `normalize_energy_charts`, `JsonHttpClient`, `EliaLiveFeeds` |
| `replay_feeds.py` | `ReplayFeeds`: contract frames published on a simulated schedule (tests, dry runs) |
| `engine.py` | `LiveEngine`, `LiveConfig`, `PublicDataHistory` |
| `execution.py` | `PaperExecution` (default), `CsvSetpointExecution`, `WebhookExecution` |
| `reconcile.py` | `reconcile_run` |
| `dryrun.py` | `replay_check`: a `data/public` period through the live engine vs `simulate` |
| `strategies.py` | strategy registry (built-ins + lazily discovered advanced strategies) |
| `clock.py` | `SystemClock`, `ManualClock` (injected; tests never sleep) |
| `store.py` | crash-safe run-directory persistence |
| `testing.py` | `FakeEliaSession` (offline HTTP stand-in), synthetic market frames |
| `cli.py` | `live_app`: `run`, `status`, `reconcile`, `replay`, `strategies` |

## Data feeds and field mapping

All upstream fields are mapped with the column maps of `euroflex_bess_lab.public_data.normalize`, so a row seen
live and the same row downloaded later from Elia's historical dataset land in the same contract columns.

| Feed | Dataset | Contract table / prefix | Time semantics | Default poll |
| --- | --- | --- | --- | --- |
| imbalance price per minute (cumulative QH-to-date) | `ods161` (historical twin `ods133`) | minute: `si_cum_mw`, `price_cum_eur_mwh`, `mip_eur_mwh`, `mdp_eur_mwh`, `alpha_eur_mwh`, ... | row `11:49` covers 11:45-11:50, complete at 11:50:00 | 15 s |
| current system imbalance (instantaneous) | `ods169` (`ods126`) | minute: `si_mw`, `afrr_up_mw`, `igcc_up_mw`, `mfrr_sa_up_mw`, ... | interval start, complete +1 min | 15 s |
| balancing energy price components | `ods175` (`ods165`) | minute: `afrr_price_up_eur_mwh`, `mfrr_avail_price_up_eur_mwh`, `floor_price_eur_mwh`, ... | like `ods161` | 15 s |
| imbalance price per quarter-hour | `ods162` (`ods134`) | qh: `imbalance_price_eur_mwh`, `si_mw`, `alpha_eur_mwh`, ... (settlement `current`) | interval start, complete +15 min | 30 s |
| Elia SI forecast, current / next QH | `ods136`, `ods147` | minute: `elia_fc_cur_*`, `elia_fc_next_*` aligned on the prediction minute | `predictiontimeutc` = issue time | 30 s |
| new formula per minute / QH | `ods211`, `ods212` | minute `nf_*`, qh `nf_*` (settlement `new2026`) | as above | 10 min, 15/30 s from 1 h before the go-live |
| day-ahead price BE | Energy-Charts `/price?bzn=BE` | `da_price` (visible from 13:00 local on D-1) | product start | 30 min; every 5 min after 12:50 local until D+1 is complete |

The quarter-hour component and volume tables (ODS166, ODS127) have no near-real-time equivalent: those fields
exist in the live window (same names as in backtests) but stay NaN for live quarter-hours. ODS134 is published one
to two days late and `ods162` only holds the current day, so right after local midnight the previous local day is
in neither dataset until Elia's morning update (the feed status reports it as a gap, e.g. 100 quarter-hours on the
25-hour DST day).

## Same information as the backtest

The engine never lets a strategy see more than `simulate` would at the same minute:

- a minute row `j` is written into the window only when `j + 1 + minute_lag <= t`; the final price of quarter-hour
  `k` only when `15k + 15 + qh_final_lag_minutes <= t`; day-ahead prices only from 13:00 local on D-1. Rows that
  arrive earlier wait in a pending buffer; reading beyond the visible indices returns NaN;
- rows that should be visible but have not arrived are reported per decision (`missing_expected_rows`) and counted
  as `gap_decisions`;
- decisions are taken 0.5 s after each minute boundary; the minute feeds are polled on a schedule aligned so that
  one poll lands 8 s before the boundary, so no decision waits for the network;
- battery physics are `realtime.battery.apply_power` (same arithmetic as the simulator), settlement is
  `settle_quarter_hours` on the published price.

Evidence that the live loop trades exactly like the backtest:

| Check | Data | Result |
| --- | --- | --- |
| `tests/test_rt_live_engine.py` | synthetic, via `ReplayFeeds` and via the full fake-HTTP path | bit-identical power, SOC and quarter-hour ledger vs `simulate` (price and SI threshold rules; crash/resume; late start with backfill; window extension) |
| `euroflex live replay --start 2026-10-13T22:00:00Z --end 2026-10-16T22:00:00Z` | `data/public`, 3 local days | 4,320/4,320 identical decisions, 288/288 identical ledger rows (max abs P&L difference 0.0 EUR) |
| `euroflex live replay --start 2026-10-19T22:00:00Z --end 2026-10-20T22:00:00Z` | `data/public`, 1 local day | 1,440/1,440 identical decisions, P&L 157.28 EUR live and offline |
| `tests/test_rt_live_strategies.py` | `ForecastMPCStrategy` (gradient-boosting price forecaster) on the forecasting package's synthetic market | identical decisions and ledger with `replan_every="minute"` |

`euroflex live replay` reruns this check for any period and strategy (exit code 1 if a single decision differs).

### Strategies that precompute in `reset()`

Some strategies (forecast MPC, PRISM, the DQN policy) compute a plan or observation matrix for every minute in
`reset(ctx)`. In a live window the future is NaN at that time, so such a strategy would idle after a few minutes.
Two remedies:

- `--replan-every minute` (`LiveConfig(replan_every="minute")`) re-runs `reset(ctx)` before every decision on a
  fresh `MarketData` view of the window (fresh object, so identity-keyed forecast caches recompute). Correct for
  strategies whose `reset` is a pure function of visible data. Measured: the forecast MPC decides identically to
  its backtest (120/120 minutes, same P&L) with replanning and diverges after 30 minutes without it; one replan
  costs about 0.2 s on a 10-day window.
- a strategy can implement `refresh(ctx)`, which the engine calls before every decision instead. Needed when
  `reset` also clears per-quarter-hour state (the DQN policy's quarter-hour energy counter).

Forecast models must also cover the live period: a walk-forward forecaster only predicts inside the test window of
its folds, so a deployment fold (`test_start = now`, open-ended `test_end`) is required.

## Measured publication latency (calibrates `PublicationModel.minute_lag`)

Polled every 5 s (forecasts 10 s, quarter-hour prices 15 s) on 2026-10-26 from 03:29 to 04:19 UTC (night, CET).
Latency = first time a poll returned the row minus the row's completion time (minute end; quarter-hour end;
forecast issue time). Values are upper bounds within the poll interval; "mid" is the midpoint with the previous
poll. The local clock was within about 0.5 s of the server `Date` header.

| Dataset | Rows | p50 | p90 | max | min | mid p50 | Share <= 60 s | Share <= 120 s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `ods161` minute imbalance price | 50 | 17.3 s | 29.5 s | 49.5 s | 5.3 s | 14.8 s | 100 % | 100 % |
| `ods169` minute system imbalance | 49 | 27.6 s | 32.9 s | 39.1 s | 15.9 s | 24.8 s | 100 % | 100 % |
| `ods175` minute price components | 49 | 69.9 s | 92.8 s | 200.7 s | 10.6 s | 67.3 s | 14 % | 96 % |
| `ods136` SI forecast current QH | 49 | 38.9 s | 43.2 s | 50.0 s | 33.5 s | 33.8 s | 100 % | 100 % |
| `ods147` SI forecast next QH | 49 | 39.5 s | 43.7 s | 50.5 s | 33.9 s | 34.1 s | 100 % | 100 % |
| `ods162` quarter-hour price | 4 | 73.0 s | 79.5 s | 82.1 s | 56.7 s | 65.4 s | 25 % | 100 % |

Calibration. A decision for minute `t` is taken at the boundary; with lag `L` the newest usable row is `t - 1 - L`,
completed at `t - L` and needed by `t`, so it must be published within `60 L` seconds.

- `minute_lag = 0` would need publication within 0.5 s: never observed (minimum 5 s).
- `minute_lag = 1` (the core default) is met by 100 % of `ods161` and `ods169` rows (max 49.5 s and 39.1 s) and by
  all forecasts (which have 120 s of slack): it is the tightest honest lag for the price and SI signals.
- `ods175` needs `minute_lag = 2` for 96 % and 3 for 100 % of the observed rows: with lag 1 its aFRR / mFRR price
  fields are missing at most decisions (the engine flags them). Strategies using them should read them two to three
  minutes back.
- `qh_final_lag_minutes = 5` is conservative: final quarter-hour prices appeared 57-82 s after the quarter-hour.
  `ods162` rows were also revised 7-20 minutes after first publication (system imbalance changed; the price did not
  in the observed cases); the engine re-ingests revised rows and appends corrected ledger rows if a price changes.

The engine measures the same statistics in every run (`status.json` -> `feeds.*.latency_upper_s`). It is a 50-minute
night-time sample; repeat it in daytime and around quarter-hour boundaries with heavy activations before relying on
the tails.

## Settlement mechanism: by date and by which feed is live

At the go-live (planned 24 Nov 2026 10:00 Brussels time, conditional on CREG approval) Elia stops updating the
current-rule datasets (`ods161`, `ods162`, ...) and `ods211` / `ods212` start carrying live new-formula prices.
`--mechanism auto` (default) therefore combines the configured date (`--go-live`) with what the feeds actually
publish. A rule counts as *live* while its quarter-hour prices keep arriving (`stale_after_minutes`, default 60).

| Quarter-hour | Price available | Settled on | `mechanism_basis` |
| --- | --- | --- | --- |
| before the configured go-live | current-rule price | `current` (`ods162`) | `date` |
| before the configured go-live | only a new-formula price, and the current-rule feed has stopped while the new one is live | `new2026` (`ods212`) | `switch_observed` (Elia switched earlier than configured) |
| from the configured go-live | new-formula price | `new2026` (`ods212`) | `date` |
| from the configured go-live | no new-formula price at all, current-rule prices still published | `current` after 30 minutes | `fallback_current_still_live` (go-live slipped) |

Otherwise the engine waits for the price and settles the quarter-hour as `price_missing` after
`settle_timeout_minutes` (90). `DecisionContext.mechanism` follows the same evidence (date, confirmed by the
feeds), and the built-in `threshold` strategy switches its signal from `price_cum_eur_mwh` to
`nf_imbalance_price_eur_mwh`. A feed that stops publishing is flagged in `missing_expected_rows` only until it is
stale, then reported as not live in `status.json` (`mechanism_detection`, `feeds.*.live`). The new-formula feeds are
polled every 10 minutes and every 15/30 s from one hour before the configured go-live.
`--mechanism current|new2026` forces one formula (before the go-live, `new2026` has no live price: quarter-hours
stay `price_missing` until `live reconcile`).

## Lossless archive of everything published

Elia's near-real-time datasets are rolling windows (`ods161` keeps about 4.5 hours, `ods162` the current day), and
the first weeks after the reform are the data the research plan tests its pre-registered predictions on. Every row
version the engine sees (new rows, revisions, backfill) is therefore archived in the run directory:

- `archive/raw/<dataset>/<UTC day>.jsonl`: one line per row version with `received_at_utc`, `dataset_id`, `feed`,
  `kind` (`new` / `revised`), `backfill` and `record`, the upstream JSON object exactly as the API returned it
  (same fields, same order, same values; Energy-Charts products as `unix_seconds` / `price`);
- `archive/normalized/<dataset>/<UTC day>.csv`: the same rows in contract form (`RunStore.read_archive(dataset,
  latest_only=True)` gives the last published version per row).

On restart the backfill reaches back to the last journaled minute, so a downtime leaves no hole as long as Elia
still serves the rows. To capture the post-reform period without trading, run
`euroflex live run --strategy idle` continuously around the go-live.

## Paper ledger and run directory

Each closed quarter-hour becomes one ledger row with the simulator's columns (`timestamp_utc, energy_mwh,
discharged_mwh, charged_mwh, base_price_eur_mwh, price_eur_mwh, price_missing, revenue_eur, degradation_eur,
pnl_eur, soc_end_mwh`) plus `strategy, mechanism, mechanism_basis, fleet_mw, minutes_decided, minutes_missed,
minutes_outside_session, price_source, price_quality_status, price_first_seen_utc, mechanism_fallback,
provisional, revision, settled_at_utc, run_id`. Live prices are not validated (`provisional = true`); a price that
changes or arrives late produces a new `revision`.

| File | Content |
| --- | --- |
| `config.json` | strategy, battery, mechanism, publication model, execution adapter, feeds |
| `decisions.jsonl` | one line per minute: requested / applied power, SOC, visible horizon, signals, strategy state, receipt |
| `qh_ledger.csv` / `.parquet` | paper ledger (append-only CSV; parquet = latest revision per quarter-hour) |
| `status.json` | heartbeat, counters, P&L, open / awaiting quarter-hours, per-feed health and latency |
| `state.json` | snapshot for resume |
| `feed_log.jsonl`, `events.jsonl` | every poll; start / resume / stop |
| `archive/raw`, `archive/normalized` | every row version as published (see above) |
| `reconcile/` | output of `live reconcile` |

Crash safety: JSON files are replaced atomically, JSONL/CSV lines are appended and fsynced, and a torn last line is
truncated on restart. Running again on the same `--run-dir` resumes: the snapshot is rolled forward from the
journal, quarter-hours already in the ledger are not settled twice, and the minutes while the engine was down are
journaled as `missed` (idle, 0 MW). A decision that would be more than 20 s late is also recorded as missed.

## Reconciliation

`euroflex live reconcile <run>` recomputes every quarter-hour the run touched from its decision journal and compares
the provisional ledger with the best available price, trying sources in order: `data/public` (ODS134 / ODS212), the
Elia historical datasets through the API (`ods134` for the current formula and `ods212` for the new formula, so
every quarter-hour settled after the go-live is checked against `ods212`; `--current-dataset` /
`--new-formula-dataset` override the ids), then the latest `ods162` snapshot (`nrt`, labelled not validated). A
price marked `Validated` wins over any earlier candidate. Output (`<run>/reconcile/`): per quarter-hour status
(`confirmed`, `revised`, `filled`, `pending`, `unsettled_*`), price revision and P&L difference; minute-level
revisions of the signals the strategy acted on (running price seen live vs `ods133`, and the new-formula running
price vs `ods211`); missed minutes and decisions taken while a feed row was missing.

## How to run

```bash
# paper-trade the threshold rule (1 MW / 2 MWh) for 30 minutes, then wait for the final prices
euroflex live run --strategy threshold --power-mw 1 --energy-mwh 2 --max-minutes 30

euroflex live status artifacts/live/<run>          # heartbeat, paper P&L, feed health
euroflex live reconcile artifacts/live/<run>       # once validated prices are published (ods134 / ods212)
euroflex live strategies                           # what can be traded live

# dry run on data/public: the live engine must reproduce the backtest exactly
euroflex live replay --start 2026-10-19T22:00:00Z --end 2026-10-20T22:00:00Z --strategy threshold
```

Useful options: `--mechanism auto|current|new2026`, `--execution paper|csv|webhook`, `--param key=value`
(strategy parameters), `--replan-every minute`, `--minute-lag`, `--qh-final-lag`, `--poll-seconds`,
`--history-days`, `--no-public-history`, `--run-dir` (resume). Without `--max-minutes` the loop runs until Ctrl-C
or SIGTERM. Runs are written under `artifacts/live/` (gitignored).

Python:

```python
from euroflex_bess_lab.realtime import BatteryParams, ThresholdStrategy
from euroflex_bess_lab.realtime.live import EliaLiveFeeds, LiveConfig, LiveEngine, PublicDataHistory

engine = LiveEngine(
    ThresholdStrategy(),
    BatteryParams(power_mw=1.0, energy_mwh=2.0),
    source=EliaLiveFeeds(),
    run_dir="artifacts/live/demo",
    config=LiveConfig(mechanism="auto"),
    history=PublicDataHistory(),
)
summary = engine.run(max_minutes=30, finish_quarter_hour=True, drain_minutes=10)
```

Dry run through the live code path on historical data: `ReplayFeeds` (contract frames from `data/public` or
`MarketData`) with a `ManualClock` replays a day in seconds and must reproduce the backtest decisions.

Strategies are built by name: `threshold` (price bands, mechanism aware), `threshold-si`, `idle`, any strategy class
discovered in `realtime.strategies.{forecast_mpc,prism,rl,*_extra}` (skipped with a reason when the module or its
optional dependency is missing), or `module.path:ClassOrFactory`. A module can publish
`LIVE_STRATEGIES = {"name": factory}` to make a fitted model buildable by name. The perfect-foresight oracle is
refused.

## EMS integration

`--execution csv` appends `minute_utc, valid_until_utc, power_mw, requested_mw, soc_start_mwh, soc_end_mwh,
mechanism, strategy, run_id, decided_at_utc, mode` per minute to `<run-dir>/setpoints.csv` (or `--setpoint-csv`).
`--execution webhook --webhook-url URL --enable-webhook` POSTs the same fields as JSON (2 s timeout; errors are
recorded, never fatal; the query string is redacted from logs). An EMS integration would read the setpoint,
check `valid_until_utc` (fall back to idle when it expires), enforce its own site limits, and report metered power
back for reconciliation. Converting setpoints into real deviations or market orders is outside this repository and
is the operator's responsibility.

## Live session, 26 Oct 2026

`euroflex live run --strategy threshold --power-mw 1 --energy-mwh 2 --mechanism auto --max-minutes 30
--poll-seconds 5 --drain-minutes 12` from 04:03:41 to 04:45:54 UTC (05:04-05:44 CET, early Monday morning), run directory
`artifacts/live/session_20261026_threshold_1mw_2mwh` (gitignored, kept locally).

- Window: 14 days of quarter-hour prices (1,224 from `data/public`, then `ods134` and `ods162`); threshold bands from
  recent final prices: charge below 140.2, discharge above 235.5 EUR/MWh.
- Execution: 41 decisions (30 requested, finished at the quarter-hour boundary), all on time (lateness at most
  0.01 s, strategy compute 0.06 ms median), no missed minutes, no strategy errors, no feed errors in 506 polls per
  minute feed.
- Position: charging 34 minutes, discharging 3, idle 4; SOC 1.00 -> 1.49 MWh. The system was long (SI +200 to
  +310 MW early in each quarter-hour) and the running quarter-hour-to-date price started deeply negative (down to
  -138 EUR/MWh), below the charge band.
- Paper P&L: -55.45 EUR over three settled quarter-hours (revenue -55.05, degradation 0.40). The final prices were
  positive (167.85, 60.09, 111.00 EUR/MWh) because the imbalance shrank late in each quarter-hour: for this rule the
  early running price was a misleading signal, exactly the kind of gap between backtest intuition and live
  information that paper trading exposes.
- Price revision caught live: the 04:00 quarter-hour was first published at 65.53 EUR/MWh (04:16:23 UTC), its
  system imbalance was revised at 04:21:23, and its price was revised to 167.85 EUR/MWh at 04:37:24 (marginal
  decremental price; quality `DataIssue`). The engine appended ledger revision 1 (that quarter-hour's P&L
  -12.01 -> -30.77 EUR). A price that is "final" five minutes after the quarter-hour can still move by more than
  100 EUR/MWh within twenty minutes, so live strategies that learn from recent final prices see provisional values.
- Feed latency in this run (upper bounds; 5 s polls for minute feeds, 30 s otherwise): `ods161` p50 22.3 s, max
  42.4 s; `ods169` p50 27.8 s, max 42.9 s; `ods175` p50 68.2 s, max 118.8 s (missing at 34 of 41 decisions under
  `minute_lag = 1`); `ods162` 83.7 s after the quarter-hour; forecasts about 54 s (30 s poll resolution).
- Gaps: the previous local day (2026-10-25, 25 hours with the DST switch, 100 quarter-hours) was in neither
  `ods134` nor `ods162` at 04:00 UTC; one SI-forecast minute (23:35 UTC) was missing upstream.
- Reconcile the same morning: validated data for the day is not published yet, so the reference was the latest
  `ods162` snapshot; all three quarter-hours were confirmed (the ledger had already followed the revision). Re-run
  `live reconcile` once ODS134 covers 26 Oct 2026.

This session predates the archive (added the same morning). The follow-up run
`artifacts/live/session_20261026_threshold_archive` (04:52-04:59 UTC, 8 decisions, archive enabled) discharged
for four minutes into a high-price quarter-hour (settled at 263.48 EUR/MWh, +17.03 EUR) and archived 84 files
(2.4 MB, mostly the 14-day backfill); an archived `ods161` record compared with the API afterwards was identical
field for field, in the same order.

## Limitations

- Near-real-time prices are not validated; the ledger is provisional until reconciled (`live reconcile`).
- Paper P&L assumes perfect execution of every setpoint, a battery that behaves like `BatteryParams`, no metering
  error and no price impact of the trader's own deviation (use `realtime.impact` to stress that offline).
- The live sample of the latency table is short and from one night; feed outages and slow tails will occur (one
  poll in 286 hit the 10 s read timeout during the probe).
- The new-formula live datasets are assumed to be `ods211` / `ods212` from the go-live (configurable feed specs).
- Clock: decisions use the local clock; keep it NTP-synchronised (the measured offset to Elia's servers was about
  0.5 s).

Data: Elia Transmission Belgium SA, Elia Open Data (CC BY 4.0); day-ahead prices Bundesnetzagentur | SMARD.de via
Energy-Charts (CC BY 4.0).
