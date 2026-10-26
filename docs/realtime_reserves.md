# Real-time reserves: aFRR capacity, aFRR energy and implicit balancing on one battery

`euroflex_bess_lab.realtime.reserve` is the reserve layer of the minute-level engine. Per 4-hour delivery block
it "virtually splits" one battery: `up_mw` / `down_mw` are sold as Belgian aFRR capacity (decided before the D-1
auction gate closure), those MW follow the TSO's activation minute by minute, and the remaining power trades
implicitly with any core `Strategy`, all under one state of charge. Settlement is per quarter-hour with one column
per revenue component.

The study built on it is `scripts/research/reserve_study.py`; its output is `reports/reserve_belgium.md`.

## Quick start

```python
from euroflex_bess_lab.realtime import BatteryParams, MarketData, ThresholdStrategy
from euroflex_bess_lab.realtime.reserve import (
    CapacityAwards,
    DeliveryBlocks,
    ReserveStudyConfig,
    run_policy_suite,
    stack_table,
)

start, end = "2025-03-01T00:00:00Z", "2025-04-01T00:00:00Z"
market = MarketData.from_public(
    start,
    end,
    include_new_formula=False,
    include_si_forecast=False,
    minute_columns=["price_cum_eur_mwh", "si_cum_mw", "afrr_up_mw", "afrr_down_mw"],
)
history = CapacityAwards.from_public("2025-02-01T00:00:00Z", end)  # published auction results
awards = history.aligned_to(DeliveryBlocks.regular(market.start_utc, market.end_utc))
suite = run_policy_suite(
    market,
    awards,
    make_inner=ThresholdStrategy,
    history_awards=history,
    config=ReserveStudyConfig(battery=BatteryParams(power_mw=1.0, energy_mwh=2.0)),
)
print(stack_table(suite.results, {"march": (start, end)}))
```

`run_policy_suite` simulates the fixed splits (0 %, 50 %, 100 % of the power reserved in both directions), the
causal forecast split and the ex-post oracle split. Lower-level pieces can be used directly:
`build_activation`, `ReservePlan.from_offers`, `simulate_reserve`, `ReserveSplitStrategy`.
`synthetic_reserve_inputs()` builds a small synthetic market + award table for experiments and tests.

## Product facts used

| Fact | Value used | Source / note |
| --- | --- | --- |
| Capacity product | per direction (up, down), six 4-hour blocks per local day (3 or 5 h on DST days) | Elia ODS125 (to 19/05/2026) and ODS207 (from 20/05/2026), 4-hour granularity, tender date D-1 |
| Auction timing | gate closure 09:00 Europe/Brussels on D-1, results assumed known 60 minutes later | public market summaries (batterydata.info, Belgium aFRR hub); Elia's DARE dimensioning publishes the next day's needs before 07:00 D-1 (Elia innovation pages); `AfrrProduct.gate_closure_hour_local`, `results_delay_minutes` |
| Capacity remuneration | pay-as-bid | Elia award tables (each bid's own price); public market summaries |
| Energy remuneration | pay-as-cleared at the cross-border marginal price of each 4-second cycle since Elia joined PICASSO (26 Nov 2024); pay-as-bid before | PICASSO TSOs' and Elia's announcements; `PICASSO_GO_LIVE_UTC`. Elia joined MARI (mFRR) on 21 May 2025 |
| Minimum bid | 1 MW | public market summaries; results are per MW of a pooled portfolio |

## Modelling choices and approximations

**Capacity revenue.** `reserved MW x capacity price (EUR/MW/h) x hours`, booked per quarter-hour
(`afrr_capacity_up_eur`, `afrr_capacity_down_eur`). Because the auction is pay-as-bid, the price a new bidder would
get is not published. `AwardModel` makes the choice explicit:

- `vwap` (default): every offered MW is awarded and paid the block's awarded-volume-weighted average price, i.e.
  a representative accepted bidder; optimistic on award certainty, neutral on price.
- `marginal`: paid the highest accepted bid; an upper bound.
- `pay_as_bid`: the battery bids at an ex-ante price (by default the causal VWAP forecast), is awarded only if the
  bid does not exceed the realised highest accepted bid, and is paid its bid. This is where award uncertainty
  costs money: forecast errors lose whole blocks.

**Headroom.** Reserving `up` / `down` MW restricts the implicit power to `-(P - down) <= u <= P - up`, and the
implicit part may not move the SOC outside

`[soc_min + up * T / eta_d,  soc_max - down * T * eta_c]`

with `T = AfrrProduct.sustain_minutes` (default 15 min). This is the repository's existing aFRR sustain convention
(`AfrrConfig.sustain_duration_minutes`), which mirrors the 15-minute limited-energy-reservoir criterion SOGL
Art. 156 sets for FCR in continental Europe. Elia's aFRR terms do not publish a battery-specific duration in the
sources checked (the BSP must deliver the requested energy throughout the block), so treat 15 minutes as a
modelling choice; the study reports a 60-minute sensitivity. `cap_reservation` scales an infeasible reservation
down to the energy band, `max_symmetric_reserve_mw` gives the largest feasible symmetric offer. When activation
pushes the SOC out of the band, the free implicit power restores it (`ReserveSettings.restore_power_frac`), and the
implicit part already respects the band of the next `preposition_qh` quarter-hours (the plan is known since the
D-1 auction results, so this is not market look-ahead).

**aFRR activation (approximation).** Provider-level activations are not public. The default `pro_rata` model
activates the reserved MW in proportion to Belgian activation:

`ratio(t) = clip(afrr_<dir>_mw(t) / awarded_<dir>_mw(block), 0, 1)`

The numerator is the ODS126 minute volume, "aFRR volumes activated in the Belgian LFC area" (it includes
activations for other TSOs via PICASSO and excludes foreign activations for Belgian needs). Do not use the ODS127
quarter-hour volume: since PICASSO it is Elia's satisfied aFRR demand, partly served by foreign bids, and several
times larger than what Belgian providers deliver (the study shows the monthly comparison). The ratio is clipped at
1 because the battery cannot deliver more than it reserved. The documented alternative `merit_order` activates the
full reserved MW whenever the quarter-hour aFRR price clears a bid at the day-ahead price +/- 10 EUR/MWh.

**aFRR energy price.** Activated energy is paid the validated quarter-hour aFRR price (ODS166
`afrr_price_<dir>_eur_mwh`; since PICASSO up and down carry the same cross-border price): upward energy earns
`price x MWh`, downward energy pays it (earns when the price is negative). `price_source="minute"` instead
reconstructs instantaneous minute prices by differencing the cumulative quarter-hour-to-date ODS165 values
(`reconstruct_minute_price`). Before PICASSO, energy was pay-as-bid: by default the battery is paid its own bid,
assumed to be the day-ahead price +/- `bid_markup_eur_mwh` (10 EUR/MWh); where no day-ahead price exists,
`bid_reference_price` uses the causal trailing mean of published imbalance prices. The published (marginal) price
is kept as an upper-bound sensitivity.

**BRP neutrality and delivery.** Settlement follows `ReserveSettings.settle_on="requested"`: the BSP is paid the
requested aFRR energy and the BRP perimeter is corrected by the same requested energy, so activated energy carries
no imbalance exposure. The ledger's `energy_mwh` is the perimeter (imbalance) energy:

`energy_mwh = metered - requested aFRR = implicit energy - undelivered aFRR energy`

Undelivered energy (only possible once activation outlasts the energy headroom or, at 100 % reservation, when no
free power is left to manage the SOC) is reported in `shortfall_mwh` and settled at the imbalance price
(`shortfall_imbalance_eur`). Activation-control and availability penalties are ignored
(`shortfall_penalty_eur_mwh = 0`, configurable). `settle_on="delivered"` (paid and corrected on delivered energy,
i.e. a battery that prices its energy bids out when it cannot deliver) is a sensitivity; in aggregate the two give
similar results after PICASSO, because the imbalance price and the aFRR price are close on average.

**The activation request is exogenous.** The TSO's request for minute `t` moves the SOC in minute `t`, but the
implicit strategy never sees market rows before their publication lag. `check_reserve_no_lookahead` holds the
activation stream fixed, perturbs every unpublished market value and checks that the implicit decisions do not
change; `ReserveSplitStrategy` also passes the core `check_no_lookahead`.

## Allocation policies

All three decide symmetric shares (`SplitCandidate(up_frac, down_frac)`; asymmetric candidates are supported):

- **Fixed splits** `fixed_0`, `fixed_50`, `fixed_100`.
- **Forecast split** (`ForecastSplitPolicy`): at the gate closure of delivery day D (09:00 on D-1) every block of D
  takes the candidate with the highest

  `value(c) = (c.up * F_up + c.down * F_down) * P * hours + M_c`

  where `F` is the capacity-price forecast (`forecast_capacity_prices`: mean of the same block of day over the last
  7 published auctions, i.e. delivery days up to D-1) and `M_c` the mean non-capacity P&L (aFRR energy, implicit
  balancing, shortfalls, degradation) the reference run of candidate `c` realised in the same block of day over
  the last 7 blocks that had settled (block end + final-price lag) before the gate closure. Ties go to the smaller
  reservation; blocks without history stay unreserved.
- **Oracle split** (`oracle_decision`): per block the candidate whose reference run realised the most in that very
  block, then re-simulated. Ex post, not tradable, and not a strict upper bound (SOC coupling).

`check_allocation_causality(build_plan, market, awards, cut_utc=...)` is the perturbation test for block decisions:
every market value unpublished at the cut and every auction result published after it is replaced by noise, the
whole pipeline (reference simulations included) is rerun, and all blocks whose gate closure is at or before the cut
must be offered identically. The forecast policy passes; the oracle fails, as it must.

## Ledger

`ReserveSimulationResult.qh` keeps the core quarter-hour ledger columns (`timestamp_utc, energy_mwh,
discharged_mwh, charged_mwh, base_price_eur_mwh, price_eur_mwh, price_missing, revenue_eur, degradation_eur,
pnl_eur, soc_end_mwh`) plus the reserve columns. Identities:

- `revenue_eur = afrr_capacity_eur + afrr_energy_eur + implicit_revenue_eur + shortfall_imbalance_eur - penalty_eur`
- `pnl_eur = revenue_eur - degradation_eur`; degradation applies to all discharged energy, aFRR included
- `implicit_revenue_eur + shortfall_imbalance_eur = energy_mwh x price_eur_mwh` (only this part depends on the
  imbalance mechanism; repricing a reserve ledger under another mechanism must keep the other components)

`save_ledger` writes `reports/ledgers/reserve_<policy>_<mechanism>_<period>.parquet` (float32, the core columns,
`strategy`, `mechanism`, `fleet_mw` and one column per component) with a JSON sidecar describing the period,
battery, settings and approximations. `revenue_stack` annualises the components per MW.

## Reproducing the study

```bash
.venv/bin/python scripts/research/reserve_study.py           # full study, about 5 minutes
.venv/bin/python scripts/research/reserve_study.py --quick --out-dir <scratch dir>   # smoke run
```

The script reads `data/public`, simulates 2024-05-22 to the last complete day (report from 2024-06-01) for a
1 MW / 2 MWh battery (efficiencies 0.95, degradation 8 EUR/MWh), runs the Jan-Jul 2026 window under the current
and the new (2026) imbalance formula, the sensitivities and the real-data causality audits, and rewrites the
report, `reports/figures/reserve_*.png`, the ledgers and `reports/ledgers/reserve_study_summary.json`.
`--implicit-factory module:callable` swaps in another implicit strategy (the callable receives the mechanism name).

## Limitations

- The pro-rata activation is an average-provider proxy; a real battery's activation depends on its energy bid in
  the merit order (bracketed by the merit-order sensitivity). Free (non-contracted) bids are ignored.
- Quarter-hour aFRR prices stand in for 4-second cross-border marginal prices.
- The battery is a price-taker in the capacity auction and its own bids do not move the clearing.
- Pre-PICASSO energy remuneration relies on an assumed pay-as-bid bid.
- At 100 % reservation there is no free power to manage the SOC, so SOC drift becomes undelivered energy; real
  operators would also restore the SOC with day-ahead or intraday trades, which are not modelled.
- No price impact, no availability tests, no energy-bid gate closure (25 minutes before the quarter-hour).
- Results scale with the implicit strategy: a stronger implicit layer raises the value of unreserved MW.
