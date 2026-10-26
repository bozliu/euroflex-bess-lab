"""Walk-forward backtest of day-ahead-only vs day-ahead + continuous-intraday re-trading (DE-LU).

Per local delivery day D (every day starts and ends at the battery's initial SOC, like ``reports/germany.md``):

1. Day-ahead stage at D-1 12:00: price-taking schedule on the persistence forecast (previous local day), held
   constant over hourly products before the 15-minute MTU (2025-10-01). Settled at realised day-ahead prices.
2. Intraday stage (strategies ``da_id_*``): trades ``z = zs - zb`` around the day-ahead position, executed at
   the intraday source price of each product (ID AEP: VWAP-execution proxy) minus / plus fee and slippage.

   * ``da_id_reopt_da`` - one re-optimisation after the day-ahead results with the day-ahead price as the
     forecast of every intraday price (no intraday information);
   * ``da_id_reopt_profile`` - same, with the trailing quarter-of-day intraday spread profile added (uses only
     days <= D-2, i.e. the published index);
   * ``da_id_rolling`` - rolling re-optimisation: the quantity of product k is frozen ``gate_closure_minutes``
     before delivery using the profile plus the decaying residual of the latest visible closing prices.

3. Oracles (upper bounds, not tradable): ``oracle_da`` (perfect day-ahead), ``oracle_id`` (perfect intraday
   prices around the persistence position), ``oracle_da_id`` (both jointly).

When the intraday price of a product is undefined (NaN; the ID AEP is undefined below 500 MW traded), the trade
decided for it is settled at the product's day-ahead price instead (``fallback_trades`` counts these); decisions
never look at whether a future price will be defined.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from ...realtime.types import BatteryParams
from .config import DEFAULT_STRATEGIES, QH_HOURS, STRATEGIES, IntradayConfig, StrategySpec
from .data import IntradayMarket
from .forecast import LiveForecaster, fit_spread_model, persistence_day_ahead
from .lp import IntradayDayLP, IntradayStep, LpError, solve_da_schedule, solve_oracle_da_id

FloatArray = NDArray[np.float64]

DAY_COLUMNS = [
    "day",
    "strategy",
    "n_qh",
    "pnl_eur",
    "da_revenue_eur",
    "id_revenue_eur",
    "da_fees_eur",
    "id_fees_eur",
    "slippage_eur",
    "degradation_eur",
    "discharged_mwh",
    "charged_mwh",
    "da_sold_mwh",
    "da_bought_mwh",
    "id_sold_mwh",
    "id_bought_mwh",
    "fallback_trades",
    "lp_fallbacks",
    "soc_violations",
    "persistence_source_lag_days",
]
SOC_TOLERANCE_MWH = 1e-6


def soc_path(schedule: DaySchedule, battery: BatteryParams, initial_soc_mwh: float) -> FloatArray:
    """SOC (MWh) at the end of every quarter-hour implied by the committed physical schedule."""

    delta = (
        schedule.charge_mw * battery.charge_efficiency - schedule.discharge_mw / battery.discharge_efficiency
    ) * QH_HOURS
    out: FloatArray = initial_soc_mwh + np.cumsum(delta)
    return out


def soc_violations(
    schedule: DaySchedule, battery: BatteryParams, initial_soc_mwh: float, terminal_soc_mwh: float
) -> int:
    """Quarter-hours outside the SOC window or power limits, plus 1 if the terminal SOC is missed."""

    soc = soc_path(schedule, battery, initial_soc_mwh)
    tol = SOC_TOLERANCE_MWH
    bad = (soc < battery.soc_min_mwh - tol) | (soc > battery.soc_max_mwh + tol)
    power = schedule.discharge_mw + schedule.charge_mw > battery.max_power_mw + tol
    balance = np.abs(schedule.discharge_mw - schedule.charge_mw - schedule.position_mw - schedule.intraday_net_mw) > tol
    terminal = abs(float(soc[-1]) - terminal_soc_mwh) > 1e-5 if len(soc) else False
    return int(np.sum(bad | power | balance)) + int(terminal)


@dataclass(frozen=True)
class DaySchedule:
    """Committed quarter-hour decisions of one strategy for one day."""

    position_mw: FloatArray
    discharge_mw: FloatArray
    charge_mw: FloatArray
    sell_mw: FloatArray
    buy_mw: FloatArray
    lp_fallbacks: int = 0

    @property
    def intraday_net_mw(self) -> FloatArray:
        out: FloatArray = self.sell_mw - self.buy_mw
        return out


@dataclass
class IntradayBacktest:
    config: IntradayConfig
    daily: pd.DataFrame
    quarter_hours: pd.DataFrame | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    def summary(self, first: date | None = None, last: date | None = None) -> pd.DataFrame:
        return summarize_daily(self.daily, self.config, first=first, last=last)


def _settle(
    schedule: DaySchedule, da: FloatArray, id_exec: FloatArray, config: IntradayConfig
) -> dict[str, float | int]:
    h = QH_HOURS
    costs = config.costs
    x = schedule.position_mw
    traded = schedule.sell_mw + schedule.buy_mw
    da_revenue = float(np.sum(x * da) * h)
    id_revenue = float(np.sum((schedule.sell_mw - schedule.buy_mw) * id_exec) * h)
    da_fees = float(np.sum(np.abs(x)) * h * costs.da_fee_eur_mwh)
    id_fees = float(np.sum(traded) * h * costs.id_fee_eur_mwh)
    slippage = float(np.sum(traded) * h * costs.slippage_eur_mwh)
    degradation = float(np.sum(schedule.discharge_mw) * h * config.battery.degradation_eur_per_mwh)
    return {
        "pnl_eur": da_revenue - da_fees + id_revenue - id_fees - slippage - degradation,
        "da_revenue_eur": da_revenue,
        "id_revenue_eur": id_revenue,
        "da_fees_eur": da_fees,
        "id_fees_eur": id_fees,
        "slippage_eur": slippage,
        "degradation_eur": degradation,
        "discharged_mwh": float(np.sum(schedule.discharge_mw) * h),
        "charged_mwh": float(np.sum(schedule.charge_mw) * h),
        "da_sold_mwh": float(np.sum(np.maximum(x, 0.0)) * h),
        "da_bought_mwh": float(np.sum(np.maximum(-x, 0.0)) * h),
        "id_sold_mwh": float(np.sum(schedule.sell_mw) * h),
        "id_bought_mwh": float(np.sum(schedule.buy_mw) * h),
    }


def _follow_position(plan_position: FloatArray, discharge: FloatArray, charge: FloatArray) -> DaySchedule:
    zeros = np.zeros(len(plan_position))
    return DaySchedule(plan_position, discharge, charge, zeros, zeros.copy())


def _static_intraday(
    x: FloatArray, prices: FloatArray, config: IntradayConfig, soc0: float, soc_end: float
) -> DaySchedule:
    lp = IntradayDayLP(
        x,
        config.battery,
        config.costs,
        initial_soc_mwh=soc0,
        terminal_soc_mwh=soc_end,
        tiebreak_eur_mwh=config.charge_tiebreak_eur_mwh,
    )
    lp.set_prices(0, prices)
    lp.solve()
    steps = [lp.step(j) for j in range(len(x))]
    return _from_steps(x, steps)


def _from_steps(x: FloatArray, steps: Sequence[IntradayStep], fallbacks: int = 0) -> DaySchedule:
    return DaySchedule(
        position_mw=x,
        discharge_mw=np.array([s.discharge_mw for s in steps]),
        charge_mw=np.array([s.charge_mw for s in steps]),
        sell_mw=np.array([s.sell_mw for s in steps]),
        buy_mw=np.array([s.buy_mw for s in steps]),
        lp_fallbacks=fallbacks,
    )


def _rolling_intraday(
    market: IntradayMarket,
    day: date,
    x: FloatArray,
    config: IntradayConfig,
    soc0: float,
    soc_end: float,
    da_discharge: FloatArray,
    da_charge: FloatArray,
) -> DaySchedule:
    lo, hi = market.day_bounds(day)
    n = hi - lo
    model = fit_spread_model(market, day, config.forecast)
    forecaster = LiveForecaster(model, market, lo, hi, config.timing.visible_offset_qh)
    lp = IntradayDayLP(
        x,
        config.battery,
        config.costs,
        initial_soc_mwh=soc0,
        terminal_soc_mwh=soc_end,
        tiebreak_eur_mwh=config.charge_tiebreak_eur_mwh,
    )
    steps: list[IntradayStep] = []
    fallbacks = 0
    for k in range(n):
        lp.set_prices(k, forecaster.forecast(k))
        try:
            lp.solve()
            step = lp.step(k)
        except LpError:
            # Numerical corner case (warm and cold start both non-optimal): commit product k as planned at the
            # previous step; that plan already satisfied every remaining constraint, so feasibility is kept.
            fallbacks += 1
            if lp.has_solution:
                step = lp.step(k)
            else:
                step = IntradayStep(float(da_discharge[k]), float(da_charge[k]), 0.0, 0.0)
        lp.fix(k, step)
        steps.append(step)
    return _from_steps(x, steps, fallbacks)


def run_day(
    market: IntradayMarket,
    day: date,
    config: IntradayConfig,
    strategies: Iterable[str] = DEFAULT_STRATEGIES,
) -> tuple[list[dict[str, Any]], dict[str, DaySchedule]]:
    """Evaluate every strategy on one local delivery day; returns day records and committed schedules."""

    specs: list[StrategySpec] = [STRATEGIES[name] for name in strategies]
    lo, hi = market.day_bounds(day)
    da = market.da_price[lo:hi]
    id_raw = market.id_price[lo:hi]
    id_exec = np.where(np.isfinite(id_raw), id_raw, da)
    fallback_mask = ~np.isfinite(id_raw)
    blocks = market.da_blocks(day)
    battery = config.battery
    soc0 = soc_end = battery.initial_soc_mwh
    forecast, source_day = persistence_day_ahead(market, day)
    lag_days = (day - source_day).days
    plan = solve_da_schedule(
        forecast,
        blocks,
        battery,
        initial_soc_mwh=soc0,
        terminal_soc_mwh=soc_end,
        fee_eur_mwh=config.costs.da_fee_eur_mwh,
        tiebreak_eur_mwh=config.charge_tiebreak_eur_mwh,
    )
    x = plan.position_mw
    schedules: dict[str, DaySchedule] = {}
    profile_model = None
    for spec in specs:
        if spec.kind == "da_only":
            schedule = _follow_position(x, plan.discharge_mw, plan.charge_mw)
        elif spec.kind == "oracle_da":
            oracle = solve_da_schedule(
                da,
                blocks,
                battery,
                initial_soc_mwh=soc0,
                terminal_soc_mwh=soc_end,
                fee_eur_mwh=config.costs.da_fee_eur_mwh,
                tiebreak_eur_mwh=config.charge_tiebreak_eur_mwh,
            )
            schedule = _follow_position(oracle.position_mw, oracle.discharge_mw, oracle.charge_mw)
        elif spec.kind == "id_static":
            if spec.forecast == "da":
                prices = da
            else:
                if profile_model is None:
                    profile_model = fit_spread_model(market, day, config.forecast)
                prices = profile_model.base(market, lo, hi)
            schedule = _static_intraday(x, prices, config, soc0, soc_end)
        elif spec.kind == "id_rolling":
            schedule = _rolling_intraday(market, day, x, config, soc0, soc_end, plan.discharge_mw, plan.charge_mw)
        elif spec.kind == "oracle_id":
            schedule = _static_intraday(x, id_exec, config, soc0, soc_end)
        elif spec.kind == "oracle_da_id":
            joint = solve_oracle_da_id(
                da,
                id_exec,
                blocks,
                battery,
                config.costs,
                initial_soc_mwh=soc0,
                terminal_soc_mwh=soc_end,
                tiebreak_eur_mwh=config.charge_tiebreak_eur_mwh,
            )
            schedule = DaySchedule(joint.position_mw, joint.discharge_mw, joint.charge_mw, joint.sell_mw, joint.buy_mw)
        else:  # pragma: no cover - guarded by StrategySpec typing
            raise ValueError(f"unknown strategy kind {spec.kind}")
        schedules[spec.name] = schedule
    records = []
    for name, schedule in schedules.items():
        settled = _settle(schedule, da, id_exec, config)
        traded = (schedule.sell_mw + schedule.buy_mw) > 1e-9
        records.append(
            {
                "day": day,
                "strategy": name,
                "n_qh": hi - lo,
                **settled,
                "fallback_trades": int(np.sum(traded & fallback_mask)),
                "lp_fallbacks": schedule.lp_fallbacks,
                "soc_violations": soc_violations(schedule, battery, soc0, soc_end),
                "persistence_source_lag_days": lag_days,
            }
        )
    return records, schedules


def schedules_to_frame(
    market: IntradayMarket, day: date, schedules: dict[str, DaySchedule], config: IntradayConfig
) -> pd.DataFrame:
    """Quarter-hour ledger of one day (local use only: it contains the intraday execution price)."""

    lo, hi = market.day_bounds(day)
    da = market.da_price[lo:hi]
    id_raw = market.id_price[lo:hi]
    id_exec = np.where(np.isfinite(id_raw), id_raw, da)
    battery = config.battery
    frames = []
    for name, s in schedules.items():
        physical = s.discharge_mw - s.charge_mw
        soc = soc_path(s, battery, battery.initial_soc_mwh)
        frames.append(
            pd.DataFrame(
                {
                    "timestamp_utc": market.timestamps[lo:hi],
                    "strategy": name,
                    "da_position_mw": s.position_mw,
                    "id_net_mw": s.intraday_net_mw,
                    "physical_mw": physical,
                    "soc_end_mwh": soc,
                    "da_price_eur_mwh": da,
                    "id_price_eur_mwh": id_raw,
                    "id_exec_price_eur_mwh": id_exec,
                }
            )
        )
    return pd.concat(frames, ignore_index=True)


def run_intraday_backtest(
    market: IntradayMarket,
    config: IntradayConfig | None = None,
    *,
    days: Sequence[date] | None = None,
    strategies: Sequence[str] = DEFAULT_STRATEGIES,
    record_quarter_hours: bool = False,
) -> IntradayBacktest:
    """Run the backtest over ``days`` (default: every market day with a previous day for persistence)."""

    cfg = config or IntradayConfig()
    unknown = [s for s in strategies if s not in STRATEGIES]
    if unknown:
        raise ValueError(f"unknown strategies {unknown}; expected {sorted(STRATEGIES)}")
    selected = list(days) if days is not None else market.days[1:]
    records: list[dict[str, Any]] = []
    qh_frames: list[pd.DataFrame] = []
    for day in selected:
        day_records, schedules = run_day(market, day, cfg, strategies)
        records.extend(day_records)
        if record_quarter_hours:
            qh_frames.append(schedules_to_frame(market, day, schedules, cfg))
    daily = pd.DataFrame(records, columns=DAY_COLUMNS)
    quarter_hours = pd.concat(qh_frames, ignore_index=True) if qh_frames else None
    return IntradayBacktest(
        config=cfg,
        daily=daily,
        quarter_hours=quarter_hours,
        meta={"id_source": market.id_source, "id_execution_label": market.id_execution_label},
    )


def _run_chunk(task: tuple[IntradayMarket, list[date], IntradayConfig, tuple[str, ...]]) -> pd.DataFrame:
    market, days, config, strategies = task
    return run_intraday_backtest(market, config, days=days, strategies=strategies).daily


def run_parallel(
    market: IntradayMarket,
    config: IntradayConfig,
    days: Sequence[date],
    *,
    strategies: Sequence[str] = DEFAULT_STRATEGIES,
    workers: int = 1,
    history_days: int = 62,
) -> pd.DataFrame:
    """Daily records for ``days``, computed month by month (in ``workers`` processes when > 1).

    Days are independent (each starts and ends at the initial SOC), so chunking does not change results; every
    chunk carries ``history_days`` earlier days for persistence and profile estimation.
    """

    from concurrent.futures import ProcessPoolExecutor

    ordered = sorted(days)
    if not ordered:
        return pd.DataFrame(columns=DAY_COLUMNS)
    months: dict[tuple[int, int], list[date]] = {}
    for day in ordered:
        months.setdefault((day.year, day.month), []).append(day)
    tasks = []
    for month_days in months.values():
        first_needed = month_days[0] - pd.Timedelta(days=history_days)
        first = next((d for d in market.days if d >= first_needed), market.days[0])
        tasks.append((market.slice_days(first, month_days[-1]), month_days, config, tuple(strategies)))
    if workers <= 1:
        frames = [_run_chunk(task) for task in tasks]
    else:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            frames = list(pool.map(_run_chunk, tasks))
    return pd.concat(frames, ignore_index=True).sort_values(["day", "strategy"], kind="stable").reset_index(drop=True)


def summarize_daily(
    daily: pd.DataFrame, config: IntradayConfig, *, first: date | None = None, last: date | None = None
) -> pd.DataFrame:
    """Per-strategy totals scaled to EUR per MW per year (x 365 / evaluated days / power)."""

    frame = daily
    if first is not None:
        frame = frame[frame["day"] >= first]
    if last is not None:
        frame = frame[frame["day"] <= last]
    if frame.empty:
        return pd.DataFrame()
    sums = frame.groupby("strategy").agg(
        days=("day", "nunique"),
        pnl_eur=("pnl_eur", "sum"),
        da_revenue_eur=("da_revenue_eur", "sum"),
        id_revenue_eur=("id_revenue_eur", "sum"),
        da_fees_eur=("da_fees_eur", "sum"),
        id_fees_eur=("id_fees_eur", "sum"),
        slippage_eur=("slippage_eur", "sum"),
        degradation_eur=("degradation_eur", "sum"),
        discharged_mwh=("discharged_mwh", "sum"),
        id_sold_mwh=("id_sold_mwh", "sum"),
        id_bought_mwh=("id_bought_mwh", "sum"),
        fallback_trades=("fallback_trades", "sum"),
        lp_fallbacks=("lp_fallbacks", "sum"),
        soc_violations=("soc_violations", "sum"),
    )
    scale = 365.0 / sums["days"] / config.battery.power_mw
    out = pd.DataFrame(index=sums.index)
    out["days"] = sums["days"]
    for column in (
        "pnl_eur",
        "da_revenue_eur",
        "id_revenue_eur",
        "da_fees_eur",
        "id_fees_eur",
        "slippage_eur",
        "degradation_eur",
    ):
        out[column.replace("_eur", "_eur_per_mw_year")] = sums[column] * scale
    out["id_volume_mwh_per_year"] = (sums["id_sold_mwh"] + sums["id_bought_mwh"]) * 365.0 / sums["days"]
    out["cycles_per_day"] = sums["discharged_mwh"] / config.battery.energy_mwh / sums["days"]
    out["fallback_trades"] = sums["fallback_trades"]
    out["lp_fallbacks"] = sums["lp_fallbacks"]
    out["soc_violations"] = sums["soc_violations"]
    if "da_only" in out.index:
        out["uplift_vs_da_only_eur_per_mw_year"] = (
            out["pnl_eur_per_mw_year"] - out.loc["da_only", "pnl_eur_per_mw_year"]
        )
    if "oracle_da_id" in out.index:
        out["capture_vs_oracle_da_id"] = out["pnl_eur_per_mw_year"] / out.loc["oracle_da_id", "pnl_eur_per_mw_year"]
    order = [name for name in STRATEGIES if name in out.index]
    return out.loc[order]


def committed_decisions(market: IntradayMarket, day: date, strategy: str, config: IntradayConfig) -> FloatArray:
    """Intraday net trade per product of ``day`` (MW) for ``strategy``; the object of the causality audit."""

    _, schedules = run_day(market, day, config, [strategy])
    return schedules[strategy].intraday_net_mw


def audit_causality(
    market: IntradayMarket,
    day: date,
    strategy: str,
    config: IntradayConfig,
    *,
    cut: int,
    seed: int = 0,
    scale_eur_mwh: float = 500.0,
) -> dict[str, Any]:
    """Perturbation test: replace intraday prices of products ``>= cut`` (local index in ``day``) and every later
    day-ahead / intraday price with noise; decisions taken before any perturbed price is visible must not change.

    For the rolling strategy product k sees products ``<= k - visible_offset``, so products
    ``k < cut + visible_offset`` must be unchanged. Static strategies decide the whole day before it starts, so
    every product of the day must be unchanged when ``cut >= 0``. ``passed`` means "behaves as designed": no
    protected decision changes for tradable strategies, while the oracles (which see realised prices) must
    change decisions *before* the cut (``lookahead_detected``).
    """

    lo, hi = market.day_bounds(day)
    rng = np.random.default_rng(seed)
    id_pert = market.id_price.copy()
    da_pert = market.da_price.copy()
    g_cut = lo + cut
    id_pert[g_cut:] = rng.normal(0.0, scale_eur_mwh, len(id_pert) - g_cut)
    da_pert[hi:] = rng.normal(0.0, scale_eur_mwh, len(da_pert) - hi)
    perturbed = market.with_id_prices(id_pert).with_da_prices(da_pert)
    base = committed_decisions(market, day, strategy, config)
    pert = committed_decisions(perturbed, day, strategy, config)
    spec = STRATEGIES[strategy]
    n = hi - lo
    if spec.kind == "id_rolling":
        protected = min(n, max(0, cut + config.timing.visible_offset_qh))
    elif spec.kind in {"id_static", "da_only"}:
        protected = n
    else:
        protected = 0
    diff = np.abs(base - pert)
    before_cut = max(0, min(cut, n))
    changed_before_cut = float(diff[:before_cut].max()) if before_cut else 0.0
    changed_protected = float(diff[:protected].max()) if protected else 0.0
    return {
        "day": f"{day}",
        "strategy": strategy,
        "tradable": spec.tradable,
        "cut": cut,
        "protected_products": int(protected),
        "max_abs_change_protected_mw": changed_protected,
        "max_abs_change_before_cut_mw": changed_before_cut,
        "max_abs_change_after_mw": float(diff[protected:].max()) if protected < n else 0.0,
        "lookahead_detected": bool(changed_before_cut > 1e-7),
        "passed": bool(changed_protected <= 1e-7) if spec.tradable else bool(changed_before_cut > 1e-7),
    }
