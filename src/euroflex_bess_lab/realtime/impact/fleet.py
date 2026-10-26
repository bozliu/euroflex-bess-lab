"""Fleet capacity-value curve: per-MW value versus the size of a fleet that follows one policy.

A fleet of ``Q`` MW is ``Q / P`` identical batteries of power ``P`` running the same policy. Its quarter-hour
deviation is ``Q / P`` times the unit's, and the settlement price is the counterfactual price of that deviation
(:class:`~euroflex_bess_lab.realtime.settlement.ImpactModel` passed to :func:`~euroflex_bess_lab.realtime.simulate`).
Price-taking policies make the same decisions for every ``Q`` (only settlement changes), so they are simulated once
and re-settled per fleet size with :func:`~euroflex_bess_lab.realtime.settle_quarter_hours`; impact-aware policies
are re-simulated per size.

Signals are *open loop* here: the fleet's running signal does not include its own deviation earlier in the
quarter-hour (it is the historical published signal). For a price taker this is the worst case (it keeps reacting
for the whole quarter-hour); the closed-loop multi-fleet engine in :mod:`.equilibrium` adds the feedback.

Overshoot share: fraction of the fleet's active quarter-hours in which its deviation is opposite to the system
imbalance and larger than ``|SI|`` (the sign of SI flips and the price branch switches against the fleet).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from ..metrics import summarize_ledger
from ..replay import MarketData
from ..settlement import ImpactModel, settle_quarter_hours
from ..simulator import simulate
from ..strategies.base import Strategy
from ..types import BatteryParams, PublicationModel
from .equilibrium import overshoot_flags

FloatArray = NDArray[np.float64]

FLEET_SIZES_MW: tuple[float, ...] = (1.0, 10.0, 25.0, 50.0, 100.0, 200.0, 400.0, 800.0, 1600.0)
CURVE_COLUMNS: tuple[str, ...] = (
    "mechanism",
    "policy",
    "fleet_mw",
    "eur_per_mw_year",
    "cycles_per_day",
    "overshoot_share",
)


@dataclass(frozen=True)
class PolicySpec:
    """A policy for the curve: ``make(fleet_mw)`` builds the strategy; price takers ignore ``fleet_mw``."""

    name: str
    make: Callable[[float], Strategy]
    fleet_dependent: bool


def resettle(
    market: MarketData,
    battery: BatteryParams,
    power_mw: FloatArray,
    soc_mwh: FloatArray,
    *,
    mechanism: str,
    impact: ImpactModel | None,
    fleet_mw: float,
) -> pd.DataFrame:
    """Quarter-hour ledger of a recorded unit power trace for a fleet of ``fleet_mw`` (same as ``simulate``)."""
    return settle_quarter_hours(
        np.asarray(power_mw, dtype=float),
        np.asarray(soc_mwh, dtype=float),
        market.settlement(mechanism),
        market.qh_times,
        battery,
        impact=impact,
        fleet_scale=fleet_mw / battery.power_mw,
    )


def resettle_ledger(
    ledger: pd.DataFrame,
    market: MarketData,
    battery: BatteryParams,
    *,
    mechanism: str,
    impact: ImpactModel,
    fleet_mw: float,
) -> pd.DataFrame:
    """Re-settle a *published* quarter-hour ledger (e.g. another agent's price-taking strategy) for a fleet.

    ``ledger`` needs ``timestamp_utc, energy_mwh, discharged_mwh, charged_mwh`` per unit battery (``battery``);
    its quarter-hours must lie in ``market``. Decisions are taken as given (valid for price takers whose decisions do
    not depend on the fleet size); prices are the counterfactual prices of the fleet's deviation.
    """
    frame = ledger.copy()
    times = pd.DatetimeIndex(pd.to_datetime(frame["timestamp_utc"], utc=True))
    pos = market.qh_times.get_indexer(times)
    if np.any(pos < 0):
        raise ValueError("ledger quarter-hours are not all inside the market period")
    energy = frame["energy_mwh"].to_numpy(dtype=float)
    base = np.asarray(market.settlement(mechanism), dtype=float)
    q_full = np.zeros(market.n_qh)
    q_full[pos] = energy / 0.25 * (fleet_mw / battery.power_mw)
    price_full = impact.adjust(np.arange(market.n_qh, dtype=np.int64), base, q_full)
    price = price_full[pos]
    missing = np.isnan(price)
    revenue = np.where(missing, 0.0, energy * np.nan_to_num(price))
    degradation = frame["discharged_mwh"].to_numpy(dtype=float) * battery.degradation_eur_per_mwh
    frame["base_price_eur_mwh"] = base[pos]
    frame["price_eur_mwh"] = price
    frame["price_missing"] = missing
    frame["revenue_eur"] = revenue
    frame["degradation_eur"] = degradation
    frame["pnl_eur"] = revenue - degradation
    if "soc_end_mwh" not in frame:
        frame["soc_end_mwh"] = np.nan
    return frame


def curve_point(
    qh: pd.DataFrame,
    battery: BatteryParams,
    *,
    n_minutes: int,
    si_mw: FloatArray,
    fleet_mw: float,
    timezone: str,
) -> dict[str, Any]:
    """Summary of one (policy, fleet size) ledger, including overshoot statistics."""
    summary = summarize_ledger(qh, battery, n_minutes=n_minutes, timezone=timezone)
    q = qh["energy_mwh"].to_numpy(dtype=float) / 0.25 * (fleet_mw / battery.power_mw)
    flags = overshoot_flags(si_mw, q)
    active = max(int(flags["active"].sum()), 1)
    impact = (qh["price_eur_mwh"] - qh["base_price_eur_mwh"]).to_numpy(dtype=float)
    deployed = np.abs(q[flags["active"]]) / max(fleet_mw * battery.availability, 1e-9)
    return {
        "eur_per_mw_year": float(summary["eur_per_mw_year"]),
        "cycles_per_day": float(summary["cycles_per_day"]),
        "overshoot_share": float(flags["overshoot"].sum() / active),
        "overshoot_qh_share": float(flags["overshoot"].mean()),
        "harmful_share": float(flags["harmful"].sum() / active),
        "active_qh_share": float(flags["active"].mean()),
        "pnl_eur": float(summary["pnl_eur"]),
        "revenue_eur": float(summary["revenue_eur"]),
        "degradation_eur": float(summary["degradation_eur"]),
        "avg_sell_price_eur_mwh": float(summary["avg_sell_price_eur_mwh"]),
        "avg_buy_price_eur_mwh": float(summary["avg_buy_price_eur_mwh"]),
        "mean_price_impact_eur_mwh": float(np.nanmean(impact[flags["active"]])) if flags["active"].any() else 0.0,
        "mean_deployed_share": float(np.mean(deployed)) if len(deployed) else 0.0,
        "daily_pnl_cvar05_eur_per_mw": float(summary["daily_pnl_cvar05_eur"]) / battery.power_mw,
        "negative_day_share": float(summary["negative_day_share"]),
    }


def capacity_value_curve(
    market: MarketData,
    mechanism: str,
    impact: ImpactModel,
    policies: Sequence[PolicySpec],
    *,
    si_mw: FloatArray,
    battery: BatteryParams,
    window: str,
    fleet_sizes: Sequence[float] = FLEET_SIZES_MW,
    publication: PublicationModel | None = None,
    keep_ledgers_for: Sequence[float] = (),
    progress: Callable[[str], None] | None = None,
) -> tuple[pd.DataFrame, list[pd.DataFrame]]:
    """Per-MW value, cycles and overshoot for every (policy, fleet size); optional QH ledgers for some sizes.

    ``si_mw`` is the mechanism's quarter-hour system imbalance (``si_mw`` for ``current``, ``nf_si_mw`` for
    ``new2026``) used for the overshoot statistics.
    """
    publication = publication or PublicationModel()
    rows: list[dict[str, Any]] = []
    ledgers: list[pd.DataFrame] = []
    period = {
        "window": window,
        "start_utc": str(market.start_utc),
        "end_utc": str(market.end_utc),
        "days": market.days,
    }

    def record(policy: str, fleet_mw: float, qh: pd.DataFrame) -> None:
        point = curve_point(
            qh, battery, n_minutes=market.n_minutes, si_mw=si_mw, fleet_mw=fleet_mw, timezone=market.timezone
        )
        rows.append({"mechanism": mechanism, "policy": policy, "fleet_mw": float(fleet_mw), **point, **period})
        if any(np.isclose(fleet_mw, s) for s in keep_ledgers_for):
            ledger = qh.copy()
            ledger["strategy"] = policy
            ledger["mechanism"] = mechanism
            ledger["fleet_mw"] = float(fleet_mw)
            ledger["window"] = window
            ledgers.append(ledger)

    for spec in policies:
        if not spec.fleet_dependent:
            if progress:
                progress(f"{window} {mechanism} {spec.name}: simulate once, re-settle {len(fleet_sizes)} sizes")
            result = simulate(market, spec.make(fleet_sizes[0]), battery, mechanism=mechanism, publication=publication)
            assert result.minute is not None
            power = result.minute["power_mw"].to_numpy(dtype=float)
            soc = result.minute["soc_mwh"].to_numpy(dtype=float)
            for q in fleet_sizes:
                qh = resettle(market, battery, power, soc, mechanism=mechanism, impact=impact, fleet_mw=q)
                record(spec.name, q, qh)
        else:
            for q in fleet_sizes:
                if progress:
                    progress(f"{window} {mechanism} {spec.name}: fleet {q:g} MW")
                result = simulate(
                    market,
                    spec.make(q),
                    battery,
                    mechanism=mechanism,
                    publication=publication,
                    impact=impact,
                    fleet_mw=q,
                    record_minutes=False,
                )
                record(spec.name, q, result.qh)
    frame = pd.DataFrame(rows)
    return frame, ledgers


def curve_sidecar(
    frame: pd.DataFrame, *, battery: BatteryParams, publication: PublicationModel, settings: dict[str, Any]
) -> dict[str, Any]:
    """JSON sidecar describing the capacity-value curve table."""
    windows = (
        frame.groupby(["window", "mechanism"])[["start_utc", "end_utc", "days"]]
        .first()
        .reset_index()
        .to_dict("records")
        if len(frame)
        else []
    )
    return {
        "table": "impact_capacity_value_curve",
        "owner": "impact",
        "columns": {
            "mechanism": "settlement mechanism: current (marginal +/- alpha) or new2026 (volume-weighted aFRR/mFRR)",
            "policy": "price_taking_threshold (ThresholdStrategy replicated across the fleet, herding), "
            "impact_aware_threshold (internalises its own impact; same signal and bands), or strat:<name> (another "
            "agent's published price-taking ledger re-settled for the fleet)",
            "fleet_mw": "fleet size Q in MW (Q / unit power identical batteries following the policy)",
            "eur_per_mw_year": "net P&L (imbalance revenue - degradation) per MW of fleet per year",
            "cycles_per_day": "discharged MWh / usable MWh / day",
            "overshoot_share": "share of active quarter-hours where the fleet deviation is opposite to SI and larger "
            "than |SI| (the sign of SI flips)",
            "overshoot_qh_share": "same, as a share of all quarter-hours",
            "harmful_share": "share of active quarter-hours where |SI + q| > |SI| (fleet made the imbalance worse)",
            "window": "evaluation window label",
        },
        "windows": windows,
        "battery": asdict(battery),
        "publication": asdict(publication),
        "settings": settings,
        "notes": [
            "Prices respond to the fleet through the fitted impact operators (euroflex_bess_lab.realtime.impact); "
            "see reports/impact.md for the fit periods, validation errors and approximations.",
            "Open-loop signals: the fleet's running signal does not contain its own deviation (worst case for the "
            "price taker); see the closed-loop multi-fleet results in reports/impact.md.",
            "No look-ahead: strategies only use rows visible under the publication model.",
        ],
    }
