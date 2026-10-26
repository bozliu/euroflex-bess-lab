"""Minute-level backtest loop with quarter-hour settlement."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .metrics import daily_pnl, summarize_ledger
from .replay import DecisionContext, MarketData
from .settlement import ImpactModel, settle_quarter_hours
from .strategies.base import Strategy
from .types import MINUTE_HOURS, BatteryParams, PublicationModel


@dataclass
class SimulationResult:
    strategy: str
    mechanism: str
    battery: BatteryParams
    publication: PublicationModel
    qh: pd.DataFrame
    minute: pd.DataFrame | None
    n_minutes: int
    timezone: str
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def pnl_eur(self) -> float:
        return float(self.qh["pnl_eur"].sum())

    def summary(self, oracle: SimulationResult | float | None = None) -> dict[str, Any]:
        oracle_pnl = oracle.pnl_eur if isinstance(oracle, SimulationResult) else oracle
        out = summarize_ledger(
            self.qh, self.battery, n_minutes=self.n_minutes, oracle_pnl_eur=oracle_pnl, timezone=self.timezone
        )
        out = {"strategy": self.strategy, "mechanism": self.mechanism, **out}
        out["start_utc"] = str(self.qh["timestamp_utc"].iloc[0]) if len(self.qh) else None
        out["end_utc"] = str(self.qh["timestamp_utc"].iloc[-1] + pd.Timedelta(minutes=15)) if len(self.qh) else None
        out["battery"] = asdict(self.battery)
        out["publication"] = asdict(self.publication)
        out.update({f"meta_{k}": v for k, v in self.meta.items() if isinstance(v, str | int | float | bool)})
        return out

    def daily(self) -> pd.DataFrame:
        return daily_pnl(self.qh, timezone=self.timezone)

    def to_parquet(self, directory: str | Path, *, oracle: SimulationResult | float | None = None) -> dict[str, Path]:
        out_dir = Path(directory)
        out_dir.mkdir(parents=True, exist_ok=True)
        paths = {"qh": out_dir / "qh_ledger.parquet", "summary": out_dir / "summary.json"}
        self.qh.to_parquet(paths["qh"], index=False)
        if self.minute is not None:
            paths["minute"] = out_dir / "minute_trace.parquet"
            self.minute.to_parquet(paths["minute"], index=False)
        paths["summary"].write_text(json.dumps(self.summary(oracle), indent=2, default=str), encoding="utf-8")
        return paths


def simulate(
    market: MarketData,
    strategy: Strategy,
    battery: BatteryParams,
    *,
    mechanism: str = "current",
    publication: PublicationModel | None = None,
    impact: ImpactModel | None = None,
    fleet_mw: float | None = None,
    record_minutes: bool = True,
    meta: dict[str, Any] | None = None,
) -> SimulationResult:
    """Run ``strategy`` minute by minute over ``market`` and settle each quarter-hour under ``mechanism``.

    ``fleet_mw`` (default: the battery's own power) scales the deviation passed to ``impact``: a fleet of
    ``fleet_mw / battery.power_mw`` identical assets following the same policy moves the system imbalance together.
    """
    publication = publication or PublicationModel()
    prices = market.settlement(mechanism)
    n = market.n_minutes
    power = np.zeros(n, dtype=float)
    soc_trace = np.zeros(n, dtype=float)
    ctx = DecisionContext(market, battery, publication, mechanism)
    strategy.reset(ctx)

    p_max = battery.max_power_mw
    eta_c = battery.charge_efficiency
    eta_d = battery.discharge_efficiency
    soc_min = battery.soc_min_mwh
    soc_max = battery.soc_max_mwh
    dt = MINUTE_HOURS
    soc = battery.initial_soc_mwh
    decide = strategy.decide
    advance = ctx.advance
    for t in range(n):
        advance(t, soc)
        request = decide(ctx)
        if request != request:  # NaN -> idle
            request = 0.0
        p = p_max if request > p_max else (-p_max if request < -p_max else request)
        if p > 0.0:
            room = (soc - soc_min) * eta_d / dt
            if p > room:
                p = room if room > 0.0 else 0.0
            soc -= p * dt / eta_d
        elif p < 0.0:
            room = (soc_max - soc) / (eta_c * dt)
            if -p > room:
                p = -room if room > 0.0 else 0.0
            soc -= p * dt * eta_c
        if soc < soc_min:
            soc = soc_min
        elif soc > soc_max:
            soc = soc_max
        power[t] = p
        soc_trace[t] = soc
        ctx.last_power_mw = p

    fleet_scale = (fleet_mw / battery.power_mw) if fleet_mw is not None else 1.0
    qh = settle_quarter_hours(
        power, soc_trace, prices, market.qh_times, battery, impact=impact, fleet_scale=fleet_scale
    )
    minute = (
        pd.DataFrame({"timestamp_utc": market.minute_times, "power_mw": power, "soc_mwh": soc_trace})
        if record_minutes
        else None
    )
    run_meta = {"impact": impact.name if impact is not None else "none", "fleet_mw": fleet_mw or battery.power_mw}
    run_meta.update(meta or {})
    return SimulationResult(
        strategy=getattr(strategy, "name", type(strategy).__name__),
        mechanism=mechanism,
        battery=battery,
        publication=publication,
        qh=qh,
        minute=minute,
        n_minutes=n,
        timezone=market.timezone,
        meta=run_meta,
    )
