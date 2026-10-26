# Real-time engine

`euroflex_bess_lab.realtime` simulates what a Belgian balance responsible party (BRP) with a battery actually does
in real time: it deviates from its nomination minute by minute and is settled per quarter-hour at Elia's single
imbalance price. It is the money-making layer of the repository, and it runs on the committed public data set
(`data/public`, Elia Open Data, CC BY 4.0).

## Layers

| Layer | Module | What it does |
| --- | --- | --- |
| State | `realtime.replay.MarketData` | Minute and quarter-hour numpy arrays built from `data/public` (system imbalance, activated aFRR/mFRR/IGCC volumes, product prices, Elia SI forecasts, day-ahead prices). |
| Mechanism | `realtime.mechanisms` | Explicit operators that map a quarter-hour state to the settlement price: `current` (marginal price +/- alpha) and `new2026` (volume-weighted aFRR/mFRR price, planned go-live 24 Nov 2026). |
| Impact | `realtime.settlement.ImpactModel` | Hook for the fleet's own effect on the price (data-driven operators live in `realtime.impact`). |
| Control | `realtime.strategies` | Policies that return a power request for the next minute. |
| Evidence | `realtime.simulator`, `realtime.audit` | Minute loop, quarter-hour ledger, summary metrics, and a perturbation audit that proves no look-ahead. |

## Settlement mechanisms (validated on Elia data)

Current formula (until the switch): if the system is short (SI < 0) the price is the marginal incremental price
plus alpha, otherwise the marginal decremental price minus alpha. `CurrentAlphaMechanism` reproduces 100 % of the
35,040 published 2025 quarter-hour prices (ODS134).

New 2026 formula: the main component is the volume-weighted average of the aFRR and mFRR prices in the direction of
the system imbalance. When Elia publishes an incentivising component, the price is `max(main_up, incentive)` for a
short system and `min(main_down, incentive)` for a long system. `NewWeightedMechanism2026` reproduces 100 % of the
20,352 simulated quarter-hour prices Elia published for Jan-Jul 2026 (ODS212).

`reconstruct_new_formula_history(qh)` reprices earlier periods under the new design from current-design
components (first-order counterfactual; it ignores how market parties would have reacted).

```bash
euroflex rt validate-mechanisms --start 2026-01-01T00:00:00Z --end 2026-08-01T00:00:00Z
```

## Information model (no look-ahead)

The decision for minute `t` is taken at the start of that minute. `PublicationModel` defines what is visible:

- a minute row describing `[j, j + 1)` is usable once `j + 1 + minute_lag <= t` (default lag 1 minute);
- the final price of quarter-hour `k` is usable once `15k + 15 + qh_final_lag_minutes <= t` (default 5 minutes);
- day-ahead prices for local day D are usable from 13:00 local time on D-1.

Strategies read data through `DecisionContext` helpers (`latest`, `latest_in_current_qh`, `minute_window`,
`settlement_window`, `da_price`, ...). `check_no_lookahead` replaces every value that is not yet published at a cut
minute with noise and checks that no earlier decision changes. The oracle strategy fails this audit by design.

Elia's minute datasets have two flavours: ODS133 rows are running quarter-hour-to-date estimates and ODS126 rows are
instantaneous values. The running estimate of the last minute is close to, but not always equal to, the final
quarter-hour price (identical in 46 % of quarter-hours since May 2024, within 1 EUR/MWh in 75 %), so settlement
always uses the quarter-hour table (ODS134 / ODS212) and minute rows are only intra-quarter-hour signals. A minute
row labelled `hh:mm` covers events up to `hh:mm:59`.

## Running a backtest

```bash
euroflex rt backtest --start 2025-01-01T00:00:00Z --end 2025-02-01T00:00:00Z --strategy threshold --mechanism current
```

```python
from euroflex_bess_lab.realtime import BatteryParams, MarketData, simulate
from euroflex_bess_lab.realtime.strategies import OracleQHStrategy, ThresholdStrategy

market = MarketData.from_public("2025-01-01T00:00:00Z", "2025-02-01T00:00:00Z")
battery = BatteryParams(power_mw=1.0, energy_mwh=2.0)
oracle = simulate(market, OracleQHStrategy(), battery, record_minutes=False)
result = simulate(market, ThresholdStrategy(), battery)
print(result.summary(oracle))
```

Sign convention: `power_mw > 0` is discharge, i.e. a long imbalance that is paid the imbalance price; charging is a
short imbalance that pays it. Degradation is charged per MWh discharged.

## What this is not

- Not connected to any market or EMS. The live loop in `realtime.live` is paper trading only.
- Prices are taken as published unless an `ImpactModel` is supplied; a single small battery is a price taker, a
  large fleet is not.
