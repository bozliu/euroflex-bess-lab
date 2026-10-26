# Contract valuation

`euroflex_bess_lab.valuation` is the contract layer of the lab. It turns simulated trading P&L into revenue
distributions, contract prices (tolls, revenue floors, partial tolls, revenue swaps) seen from both counterparties,
an auditable value attribution and portfolio risk.

Why it exists: a battery optimiser that sells tolls, floors or swaps to battery owners takes multi-year merchant
risk on its own balance sheet. Pricing those contracts needs revenue distributions that include settlement-regime
shifts (the Belgian imbalance-price formula change planned for 24 Nov 2026), market saturation as the
implicit-reaction fleet grows, and the decay of the optimiser's own edge. This package makes each of those layers
an explicit, documented input.

```text
ledgers (QH P&L)  ->  daily P&L per MW  ->  annual revenue paths  ->  contract prices + both views
   revenue.py            revenue.py           scenarios.py            contracts.py
                    \->  value waterfall (attribution.py)      \->  portfolio risk (portfolio.py)
```

Everything is reproducible from a seed, and every command writes JSON (full precision) next to a markdown summary.

## 1. Input contract

### Ledger

One row per settlement interval (the real-time simulator writes quarter-hours; backtests may use other steps).

| column | required | meaning |
| --- | --- | --- |
| `timestamp_utc` | yes | interval start, tz-aware UTC (naive values are read as UTC) |
| `pnl_eur` | see below | net P&L = `revenue_eur - degradation_eur` |
| `revenue_eur` | see below | market revenue; for imbalance settlement `energy_mwh * price_eur_mwh` |
| `degradation_eur` | no (0) | wear / degradation charge, >= 0 |
| `energy_mwh` | no | net energy, `+` = sold / long (battery discharge) |
| `price_eur_mwh` | no | settlement price of the interval |
| `discharged_mwh`, `charged_mwh` | no | gross flows (default: positive / negative part of `energy_mwh`) |
| `price_missing` | no | `True` where the interval had no price and earned 0 |

At least one of `pnl_eur`, `revenue_eur`, or `energy_mwh` + `price_eur_mwh` must be present; the others are
derived. Backtest-engine names are accepted as aliases: `realized_pnl_eur` / `total_pnl_eur` -> `pnl_eur`,
`degradation_cost_eur` -> `degradation_eur`, `delivery_start_utc` -> `timestamp_utc`,
`price_eur_per_mwh` -> `price_eur_mwh`. Site-level rows with repeated timestamps are rejected unless
`normalize_ledger(..., sum_duplicates=True)` is used. NaN P&L becomes 0 and is flagged in `pnl_was_nan`.

### Run metadata

`LedgerMetadata` travels with a ledger, on disk as a sidecar `X.meta.json` next to `X.parquet` / `X.csv`
(`X.parquet.meta.json` is also found). Without a sidecar, the `summary.json` that `SimulationResult.to_parquet`
writes next to `qh_ledger.parquet` is used (strategy, mechanism, battery, publication lags, period); the run
directory name becomes the asset id. The parser accepts flat keys, a nested `battery` block and common aliases
(`battery_power_mw`, `fleet_size_mw`, `strategy_name`, `zone`, `start` / `end`). Unknown keys are kept in `extra`.

| field | meaning |
| --- | --- |
| `asset_id`, `market` | identity; `market` (`BE`, `DE-LU`, `NL`) sets the local delivery-day timezone |
| `power_mw`, `energy_mwh` | battery spec; `power_mw` is required for every per-MW number |
| `mechanism` | settlement formula of the run: `current` or `new2026` |
| `strategy` | strategy name |
| `fleet_mw`, `impact_aware` | implicit-reaction fleet assumed in the run (0 = price-taker on published prices) |
| `availability`, `degradation_eur_per_mwh` | as simulated |
| `period_start`, `period_end`, `publication_lag` | provenance for reports |
| `synthetic` | `true` marks synthetic inputs; reports then carry a banner |

`ledger_from_simulation(result)` and `metadata_from_simulation(result, **overrides)` adapt a real-time
`SimulationResult` (its `qh` frame, `battery`, `strategy`, `mechanism`) without importing the simulator.

### Capacity-value curve

Per-MW value as a function of the total implicit-reaction fleet, produced by the impact study. File (CSV, parquet
or JSON) with columns `fleet_mw` and `value_eur_per_mw_year` (aliases: `total_fleet_mw`, `value_per_mw`,
`relative_value`), optionally a `mechanism` column with one curve per settlement formula. Only ratios enter the
scenarios, so a relative index works too. Values are interpolated linearly and extrapolated with the slope of the
last segment (or flat), floored at zero.

The fleet axis must be the same for the curve, the fleet path and `reference_fleet_mw` (the fleet already
reflected in the historical prices). If the curve is in *additional* MW on top of history, use
`reference_fleet_mw = 0` and an additional-MW fleet path.

`placeholder_capacity_value_curve()` ships a **placeholder** `V(Q) = 1 / (1 + Q / 1500 MW)` for tests and demos.
It is a shape, not an estimate; reports flag it.

## 2. Revenue (`revenue.py`)

- `daily_pnl(ledger, timezone, power_mw)` sums intervals into local delivery days (Europe/Brussels or
  Europe/Berlin). DST days have 92 or 100 quarter-hours; `expected_intervals` and `coverage` account for that.
- `complete_days(daily, min_coverage=0.98)` drops partial days (e.g. the first local day of a UTC-aligned ledger).
- Annualisation: `per MW-year = total / power_mw / days_covered * 365.25`, with `days_covered` = hours of the
  included intervals / 24 (the daily table carries `hours_covered`), so 23- and 25-hour DST days and partial
  days count pro rata. On a simulator ledger this reproduces `SimulationResult.summary()["eur_per_mw_year"]`.
- Season-weighted annual value (for ledgers covering part of a year):
  `sum over months m of mean_daily(m) * days(m)` with `days(Feb) = 28.25`; `None` unless all 12 months exist.
- `summarize_ledger` adds daily mean / std / P05 / P50 / P95 per MW, share of negative days, worst and best day,
  the share of P&L earned on the best 10 % of days, maximum drawdown and full cycles per day
  (`discharged_mwh / energy_mwh / day`).

## 3. Revenue scenarios (`scenarios.py`)

Annual revenue per MW for path `p` and contract year `y`:

```text
R[p, y] = B_r[p, y] * S_r(Q[p, y]) / S_r(Q_ref) * K[y] * F[y] * A * L[p, y]
```

| term | meaning |
| --- | --- |
| `B_r[p, y]` | sum over the real calendar days of year `y` of history days of regime `r` (bootstrap below) |
| `r = regime[p, y]` | regime draw with weights `w_r` (static or per year) and persistence `rho`: year `y` keeps the regime of `y - 1` with probability `rho`, otherwise redraws (`rho = 1`: one regime per path = model uncertainty; `rho = 0`: independent years) |
| `S_r(Q) / S_r(Q_ref)` | saturation multiplier from the capacity-value curve (per regime if given) along the fleet path |
| `Q[p, y]` | fleet path: base MW by year (linear between given years, flat outside) times a persistent lognormal build-out factor `exp(s_p z_p - s_p^2 / 2)` and optional year noise |
| `K[y]` | skill decay `(1 - share) + share * (1 - decay)^(y + offset)` on the skill share of revenue |
| `F[y]` | capacity fade `(1 - fade)^y`; `A`: availability relative to the ledger |
| `L[p, y]` | market-level shock `exp(s x[p, y] - s^2 / 2)`, `x` a unit-variance AR(1) across years with persistence `phi` (default off) |

**Bootstrap.** Each simulated day starts or continues a block of consecutive history days:

- `block`: blocks of `block_days` (7 = weekly blocks); `stationary`: Politis-Romano stationary bootstrap with
  geometric block lengths of mean `block_days`; `iid`: single days.
- A block starts on a history day within `season_window_days` (circular day-of-year distance, default +/- 30) of
  the simulated day and, with `align_weekday` (default), on the same weekday, so seasonality, weekday structure
  and multi-day volatility clusters survive. A block that meets a gap in the history ends early.
- If fewer than `min_candidates` (8) days qualify - typically a regime observed only from January to July - the
  window doubles until enough days qualify, then weekday alignment is dropped. The share of such lookups is
  reported as `seasonal_fallback_share`.

**Short regime histories.** Elia's new-formula simulation (ODS211/212) prices January-July 2026 only.
`derive_regime_from_overlap(current, new2026, method="ratio")` uses the paired overlap (the same days priced under
both formulas) to scale the long `current` history: `new = current * sum(new) / sum(current)` (or
`current + mean daily difference` with `method="delta"`); observed `new2026` days keep their observed values.
This assumes the mechanism effect is proportional and season-independent - check the overlap diagnostics.

**Joint series.** A panel with several columns (e.g. `asset` and a benchmark `index`, or two assets) is resampled
with the same days for every column, which preserves their dependence (`daily_panel` builds it).

**Reproducibility.** One `SeedSequence(seed)` spawns independent streams for regimes, fleet, level shock and each
regime's bootstrap; the same seed reproduces every path.

`ScenarioSet` exposes `annual` (primary series), `series`, `base_series` (before multipliers), regime index, fleet
and multiplier arrays, `annual_table()`, `summary()` and `to_frame()`; `ScenarioSet.from_annual(...)` wraps
external paths (for example a consultant's distribution).

## 4. Contracts (`contracts.py`)

Notation: `R[p, y]` annual merchant revenue per MW, `D_y = (1 + r)^-(y + 0.5)` mid-year discount factors at the
counterparty's rate, `A = sum D_y`, escalation `g_y = (1 + e)^y`, `G = sum D_y g_y`, `PV(X) = sum_y D_y X_y`.

### Certainty equivalents

Each counterparty values the distribution of `PV` across paths with a certainty equivalent (all three are monotone
and cash-invariant, `CE(X + c) = CE(X) + c`, which makes every reservation price closed-form):

| measure | `CE(X)` | normal closed form |
| --- | --- | --- |
| `neutral` | `E[X]` | `mu` |
| `cvar` | `(1 - w) E[X] + w CVaR_a(X)`, `CVaR_a` = mean of the worst `a` share (exact on the sample) | `CVaR_a = mu - sigma pdf(z_a) / a` |
| `exponential` | `-T ln E[exp(-M X / T)] / M` (CARA, risk tolerance `T` EUR on a deal of `M` MW) | `mu - M sigma^2 / (2 T)` |

`RiskProfile` holds the measure, `cvar_alpha`, `cvar_weight`, `risk_tolerance_eur`, `discount_rate` and a
`required_margin_eur_per_mw_year` (an annuity the counterparty wants on top). Defaults are illustrative: offtaker
mean-CVaR weight 0.3 at 10 %, owner weight 0.6 at 10 %, both discounting at 8 %.

### Prices

The *fair* price is the offtaker's reservation price - its risk-adjusted quote. The owner's reservation price
bounds the deal zone from the other side. `c` = `owner_capture`, the share of the simulated merchant revenue the
owner would capture trading on its own (route-to-market fee, skill gap).

| contract | cash flows per MW-year | offtaker reservation (fair price) | owner reservation |
| --- | --- | --- | --- |
| toll | owner `K g_y`; offtaker `R_y - K g_y` | `K = (CE_off(PV R) - m A) / G` (maximum it pays) | `K = CE_own(PV c R) / G` (minimum it accepts) |
| floor | provider pays `max(F - R_y, 0)`, receives premium `P` and `b max(R_y - H, 0)` | `P = (m A - CE_off(PV(b call - put))) / A` (minimum premium) | `P = (CE_own(PV(R + put - b call)) - CE_own(PV R)) / A` (maximum premium) |
| partial toll | owner `x K g_y + (1 - x) s R_y`; offtaker `x (R_y - K g_y) + (1 - x)(1 - s) R_y` | `K = (CE_off(PV((x + (1 - x)(1 - s)) R)) - m A) / (x G)` | `K = (CE_own(PV c R) - CE_own(PV (1 - x) s R)) / (x G)` |
| swap | owner `c Asset_y - n Ref_y + n S`; offtaker `n (Ref_y - S)` | `S = (CE_off(PV n Ref) - m A) / (n A)` | `S = (CE_own(PV c Asset) - CE_own(PV(c Asset - n Ref))) / (n A)` |

- Break-even = the risk-neutral offtaker price (zero expected margin), e.g. `E[PV R] / G` for the toll.
- The floor is a put on *annual* revenue; with `b = 0` its risk-neutral premium is the Bachelier put
  `(F - mu) Phi(d) + sigma phi(d)`, `d = (F - mu) / sigma`, for normal revenue and the Black-76 put for lognormal
  revenue (both closed forms are in the module and checked in the tests).
  `solve_floor_upside_share` finds the upside share `b` that makes the fair premium zero (a zero-cost collar).
- In a partial toll the offtaker's merchant share `(1 - x)(1 - s)` enters its valuation and raises the toll it can
  pay per tolled MW; if that share only covers trading costs, express it as a required margin.
- A swap whose reference is the asset itself is a financially settled toll; with a separate index (simulated
  jointly) the owner keeps the basis. Hedge effectiveness = `1 - Var(PV hedged) / Var(PV unhedged)`.
- Value basis: the default is net P&L (`pnl_eur`, revenue minus the ledger's degradation charge), i.e. a toll with
  a throughput charge at the ledger's wear rate. Use `--value revenue` (gross) for a pure capacity toll.

### Reading a `ContractResult`

| field | meaning |
| --- | --- |
| `price_eur_per_mw_year`, `price_eur_per_kw_month` | the fair (or quoted) price; `price_eur_per_kw_month = price / 12 / 1000` |
| `break_even_eur_per_mw_year` | risk-neutral price |
| `offtaker_reservation_...`, `owner_reservation_...`, `deal_zone_...` | direction-aware: `price_direction` says who pays |
| `expected_offtaker_margin_eur_per_mw_year` | `E[PV offtaker cash] / A` (annuity-equivalent) |
| `prob_offtaker_loss` / `prob_offtaker_loss_year` | `P(PV offtaker cash < 0)` / share of loss-making path-years |
| `offtaker`, `owner` | `CounterpartyView`: expected and CE present values, risk premium `E - CE`, CVaR, benchmark (no deal / merchant), risk-adjusted gain vs benchmark, probability of being worse off than the benchmark (the owner's *regret*), annual cash P90 / P50 / P10 |
| `merchant` | merchant revenue P90 / P50 / P10 pooled, tenor-average and by year |
| `extras` | contract-specific: floor trigger probability and payouts, swap hedge effectiveness and basis |

**P90 / P50 / P10 use the exceedance convention:** P90 is the level exceeded with 90 % probability (the 10th
percentile). Every JSON output states this in `percentile_convention`.

## 5. Value attribution (`attribution.py`)

`value_waterfall(ledgers, power_mw, timezone)` takes ledgers of the same battery over the same period, each adding
one factor to the previous one:

| key | ledger | effect shown |
| --- | --- | --- |
| `oracle` | perfect-foresight optimum on final prices | anchor (upper bound, not tradable) |
| `strategy` | tradable strategy, same mechanism | decision / forecast gap |
| `mechanism` | the strategy run under the other formula (`new2026`) on the same days | mechanism effect |
| `impact` | the same, impact-aware at fleet size `Q` | price-impact effect |
| `availability` | the same with contracted availability | availability effect |
| `degradation` | the same with the contractual cycling limit / wear cost | cycling / degradation effect |

- Effects are differences of consecutive ledgers on their **common intervals**, annualised per MW, so the
  waterfall is sequential: an effect is conditional on the steps before it. Missing ledgers are skipped.
- `basis="gross"` measures levels on revenue and adds a final *degradation cost* bar; the total is still net P&L.
- `availability_factor` adds an analytic pro-rata availability step when there is no availability ledger (assumes
  outages uncorrelated with value) and scales later steps.
- `mechanism_decomposition(current, new, new_prices)` splits the mechanism effect into **repricing** (the same
  dispatch settled at new-formula prices, `reprice_ledger`) and **re-optimisation** (the strategy re-run under
  the new formula).
- The table also reports the daily mean / std of each effect and the share of days on which it is negative;
  `WaterfallResult.daily` has the per-day detail and `plots.plot_waterfall` draws the chart.

## 6. Portfolio (`portfolio.py`)

`portfolio_risk([PortfolioAsset(name, daily, weight, market, power_mw), ...])` aligns assets on common complete
local days and reports, for daily P&L and for one jointly block-bootstrapped year:

- correlation (Pearson and rank), mean, std, `VaR_a` (the `a`-quantile of P&L) and `CVaR_a` (mean of the worst
  `a` share) - P&L levels, not losses;
- **shortfall** `E[X] - CVaR_a(X)` and the **diversification benefit** `sum_i shortfall_i - shortfall_portfolio`
  (non-negative because the shortfall is subadditive), also for the standard deviation;
- **Euler contributions**: `E[X_i | portfolio in its worst a tail]` (component CVaR, sums exactly to the portfolio
  CVaR), component shortfall and variance shares `Cov(X_i, X_p) / Var(X_p)`;
- **concentration**: HHI and effective number of assets on expected P&L, on shortfall contributions and on MW;
  share of portfolio P&L earned on the best 10 % of days.

`weight` scales a ledger's EUR P&L (e.g. contracted MW / simulated MW). Belgian and German assets share the CET
delivery day, so local dates align.

## 7. Command line

The command group is `value_app` in `euroflex_bess_lab.valuation.cli` (registered as `euroflex value`). Ledger
options take `PATH` or `NAME=PATH`; `--power-mw` overrides the sidecar. Every command writes `<command>.json` and
`<command>.md` (plus PNG figures unless `--no-figures`) into `--out-dir`. Paths in outputs are relative or file
names only.

```bash
# daily P&L and per-MW-year statistics
euroflex value summary -l runs/be_current.parquet -l runs/de.parquet -o out/summary

# revenue distribution: two settlement regimes, saturation along a fleet path, skill decay
euroflex value scenarios -l current=runs/be_current.parquet -l new2026=runs/be_new2026.parquet \
  --weights current=0.3,new2026=0.7 --capacity-curve curve.csv --fleet 2027=900,2030=2200 \
  --reference-fleet-mw 400 --skill-share 0.3 --skill-decay 0.1 --tenor-years 7 -o out/scenarios

# contracts (same scenario options as above)
euroflex value toll -l current=... -l new2026=... --offtaker-weight 0.3 --owner-weight 0.6 -o out/toll
euroflex value floor -l ... --floor 60000 --solve-upside-share --upside-threshold 100000 -o out/floor
euroflex value partial-toll -l ... --fixed-fraction 0.5 --owner-merchant-share 0.8 -o out/partial
euroflex value swap -l ... --reference-ledger runs/be_index.parquet -o out/swap

# value waterfall, with the repricing / re-optimisation split of the mechanism effect
euroflex value attribution --oracle runs/oracle.parquet --strategy runs/be_current.parquet \
  --mechanism runs/be_new2026.parquet --impact runs/be_new2026_q1500.parquet \
  --new-prices data/public/belgium/new_formula_qh_2026.parquet -o out/attribution

# portfolio of markets
euroflex value portfolio -a be=runs/be_current.parquet -a de=runs/de.parquet --weights be=2 -o out/portfolio
```

Risk options per counterparty: `--offtaker-measure/--owner-measure` (`neutral`, `cvar`, `exponential`),
`--*-alpha`, `--*-weight`, `--*-tolerance-eur`, `--*-discount`, `--*-margin`. Scenario options: `--tenor-years`,
`--start-year`, `--paths`, `--seed`, `--bootstrap`, `--block-days`, `--season-window-days` (-1 = off),
`--align-weekday/--no-align-weekday`, `--regime-persistence`, `--fleet-log-sigma`, `--capacity-fade`,
`--availability-factor`, `--level-log-sigma`, `--level-persistence`, `--value pnl|revenue`, `--write-paths`.

## 8. Demo and reports

`scripts/research/valuation_demo.py` runs the whole chain and writes `reports/valuation_demo.md` with figures
`reports/figures/valuation_demo_*.png`. Without arguments every input is **synthetic** (the seeded toy generator
in `valuation/synthetic.py`: imbalance-like prices and a toy daily-arbitrage dispatch, not market data) and the
report says so in its title. With ledger arguments (`--current`, `--new2026`, `--oracle`, `--impact`,
`--availability`, `--degradation`, `--index`, `--germany`, `--capacity-curve`, `--report`, `--figure-prefix`)
the same chain runs on simulator output; real and synthetic inputs are never mixed. A `new2026` ledger shorter
than a year is extended with the paired-overlap ratio.

## 9. Assumptions and limitations

- Paths resample historical days: they cannot contain market states absent from the history. Inter-annual
  spread uncertainty beyond that is an explicit, uncalibrated input (`level_log_sigma`).
- Saturation scales every day of a year by one capacity-value ratio; within-year shape changes from saturation are
  not modelled. The curve itself must come from the impact study; the shipped curve is a placeholder.
- The regime ratio method assumes a proportional, season-independent mechanism effect.
- Attribution effects are sequential, not Shapley values; reorder `steps` to test sensitivity.
- No credit risk, collateral, tax, inflation indexation, operating costs or contract optionality (early
  termination, availability guarantees). Risk profiles are illustrative, not calibrated to any company.
