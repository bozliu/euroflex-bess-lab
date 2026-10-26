# Live paper-trading session (shadow mode), 26 Oct 2026

A real run of `euroflex live run` against Elia's near-real-time feeds, kept as evidence that the backtested
strategies run unchanged in real time. Paper only: no order or bid was submitted to any market. The full run
record (decisions, quarter-hour ledger including the price-revision correction, feed log, reconciliation) is in
[`live/session_20261026_threshold_1mw_2mwh/`](live/session_20261026_threshold_1mw_2mwh/).

| Item | Value |
| --- | --- |
| Window | 2026-10-26 04:04 to 04:44 UTC |
| Strategy | `ThresholdStrategy` (price mode), 1 MW / 2 MWh, efficiency 0.95 each way, degradation 8 EUR/MWh |
| Settlement | current formula, Elia ODS162 quarter-hour prices |
| Decisions | 41 on time (lateness <= 0.01 s), 0 missed, 0 strategy or execution errors |
| Price revision caught live | 04:00 quarter-hour revised from 65.53 to 167.85 EUR/MWh about 22 minutes after first publication; a corrected ledger row was appended |
| Paper P&L | -55.45 EUR over 3 settled quarter-hours |

The P&L is noise at this length: the running price started deeply negative in each quarter-hour, so the rule
charged, and the final prices came in positive. What the session shows is the engineering, not the alpha:

- the live engine makes exactly the decisions `simulate` makes on the same data (replays of 4,320 real minutes from
  `data/public` through the live engine reproduce the backtest decisions and ledger with zero difference);
- publication latency measured during the session (first poll that returned a row, relative to the end of the
  minute or quarter-hour it describes):

| Dataset | Median | 90th percentile | Max |
| --- | ---: | ---: | ---: |
| ods161 imbalance price per minute | 17.3 s | 29.5 s | 49.5 s |
| ods169 system imbalance per minute | 27.6 s | 32.9 s | 39.1 s |
| ods175 price components per minute | 69.9 s | 92.8 s | 200.7 s |
| ods136 / ods147 SI forecasts | ~39 s | ~44 s | ~50 s |
| ods162 imbalance price per quarter-hour | 73 s | 80 s | 82 s |

So `PublicationModel(minute_lag=1)`, the default in every backtest, is the tightest honest setting for ods161 and
ods169; ods175 needs a lag of 2-3 minutes. See [docs/live_paper_trading.md](../docs/live_paper_trading.md).
