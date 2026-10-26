"""State containers for the impact operators (quarter-hour finals and running minute estimates).

Every state has one row per quarter-hour (settlement) or per minute (running QH-to-date estimate visible to a
strategy). Volumes are quarter-hour average activated balancing energy in MW; Elia's system imbalance satisfies
``SI ~= -(net aFRR + net mFRR)`` (checked on ODS212: residual MAE ~11 MW, IGCC coefficient ~0), so the operators move
only aFRR and mFRR volumes when a fleet deviates.
"""

from __future__ import annotations

import warnings
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields, replace
from typing import Any, TypeVar

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from ..replay import MarketData
from ..types import MINUTES_PER_QH
from .curves import DEFAULT_REGIME_EDGES_HOURS, local_hour_regime

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]

UP_VOLUME_COLUMNS: tuple[str, ...] = ("afrr_up_mw", "mfrr_sa_up_mw", "mfrr_da_up_mw")
DOWN_VOLUME_COLUMNS: tuple[str, ...] = ("afrr_down_mw", "mfrr_sa_down_mw", "mfrr_da_down_mw")
NF = "nf_"

StateT = TypeVar("StateT", bound="_RowState")


class _RowState:
    """Shared helpers for frozen dataclasses whose array fields all have one entry per row."""

    si_mw: FloatArray

    def __len__(self) -> int:
        return len(self.si_mw)

    def take(self: StateT, rows: NDArray[np.int64] | slice) -> StateT:
        updates: dict[str, Any] = {}
        for f in fields(self):  # type: ignore[arg-type]
            value = getattr(self, f.name)
            if isinstance(value, np.ndarray):
                updates[f.name] = value[rows]
        return replace(self, **updates)  # type: ignore[type-var]


def _get(data: Mapping[str, FloatArray], name: str, n: int) -> FloatArray:
    if name in data:
        return np.asarray(data[name], dtype=float)
    return np.full(n, np.nan)


def _sum_columns(data: Mapping[str, FloatArray], names: Sequence[str], n: int) -> tuple[FloatArray, bool]:
    present = [name for name in names if name in data]
    if not present:
        return np.full(n, np.nan), False
    total = np.zeros(n, dtype=float)
    for name in present:
        total += np.nan_to_num(np.asarray(data[name], dtype=float))
    all_nan = np.all([np.isnan(np.asarray(data[name], dtype=float)) for name in present], axis=0)
    total[all_nan] = np.nan
    return total, True


def _infer_volumes(si: FloatArray, up: FloatArray, down: FloatArray) -> tuple[FloatArray, FloatArray]:
    """Fill missing activated volumes from the system imbalance (SI < 0 -> upward regulation of |SI|)."""
    up_out = np.where(np.isnan(up), np.clip(-si, 0.0, None), up)
    down_out = np.where(np.isnan(down), np.clip(si, 0.0, None), down)
    return up_out, down_out


@dataclass(frozen=True)
class CurrentState(_RowState):
    """Inputs of the current-formula price (``MIP + alpha`` if short else ``MDP - alpha``; midpoint if ``|SI| < 25``).

    ``mip_eur_mwh`` / ``mdp_eur_mwh`` are the *structural* marginal prices ``max(product prices, FLOOR)`` /
    ``min(product prices, CAP)``: Elia publishes the midpoint ``(FLOOR + CAP) / 2`` instead in the branch of a
    quarter-hour with ``|SI| < 25 MW``, which the builders undo when the product prices are available.
    """

    si_mw: FloatArray
    si_prev_mw: FloatArray
    up_mw: FloatArray
    down_mw: FloatArray
    mip_eur_mwh: FloatArray
    mdp_eur_mwh: FloatArray
    alpha_eur_mwh: FloatArray
    regime: IntArray
    curve_row: IntArray
    floor_eur_mwh: FloatArray
    cap_eur_mwh: FloatArray


@dataclass(frozen=True)
class NewFormulaState(_RowState):
    """Inputs of the 2026 formula: product volumes, product prices, published weights and main components."""

    si_mw: FloatArray
    afrr_up_mw: FloatArray
    afrr_down_mw: FloatArray
    mfrr_up_mw: FloatArray
    mfrr_down_mw: FloatArray
    afrr_price_up_eur_mwh: FloatArray
    afrr_price_down_eur_mwh: FloatArray
    mfrr_price_up_eur_mwh: FloatArray
    mfrr_price_down_eur_mwh: FloatArray
    weight_afrr: FloatArray
    weight_mfrr: FloatArray
    main_up_eur_mwh: FloatArray
    main_down_eur_mwh: FloatArray
    incentive_eur_mwh: FloatArray
    regime: IntArray


UP_PRICE_COLUMNS: tuple[str, ...] = ("afrr_price_up_eur_mwh", "mfrr_sa_price_up_eur_mwh", "mfrr_da_price_up_eur_mwh")
DOWN_PRICE_COLUMNS: tuple[str, ...] = (
    "afrr_price_down_eur_mwh",
    "mfrr_sa_price_down_eur_mwh",
    "mfrr_da_price_down_eur_mwh",
)
NEUTRAL_BAND_MW = 25.0


def _extreme(
    data: Mapping[str, FloatArray], names: Sequence[str], bound: FloatArray, n: int, *, upper: bool
) -> FloatArray:
    """``max`` (or ``min``) over the available product prices and the bound, ignoring NaN."""
    stack = [np.asarray(data[c], dtype=float) for c in names if c in data] + [bound]
    arr = np.vstack(stack) if stack else np.full((1, n), np.nan)
    with np.errstate(invalid="ignore"), warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        out = np.nanmax(arr, axis=0) if upper else np.nanmin(arr, axis=0)
    result: FloatArray = np.asarray(out, dtype=float)
    return result


def structural_marginal_prices(
    data: Mapping[str, FloatArray], si: FloatArray, *, prefix: str = ""
) -> tuple[FloatArray, FloatArray, FloatArray, FloatArray]:
    """``(MIP, MDP, FLOOR, CAP)`` with the neutral-band midpoint replaced by the structural marginal price.

    Elia's rule (verified on ODS134): ``MIP = max(aFRR, mFRR SA, mFRR DA up prices, FLOOR)``,
    ``MDP = min(aFRR, mFRR SA, mFRR DA down prices, CAP)``; in a quarter-hour with ``|SI| < 25 MW`` the published MIP
    (short) or MDP (long) is ``(FLOOR + CAP) / 2`` instead.
    """
    n = len(si)
    mip = _get(data, f"{prefix}mip_eur_mwh", n)
    mdp = _get(data, f"{prefix}mdp_eur_mwh", n)
    floor = _get(data, f"{prefix}floor_price_eur_mwh", n)
    cap = _get(data, f"{prefix}cap_price_eur_mwh", n)
    neutral = np.abs(si) < NEUTRAL_BAND_MW
    if np.isfinite(floor).any():
        mip_s = _extreme(data, [f"{prefix}{c}" for c in UP_PRICE_COLUMNS], floor, n, upper=True)
        mip = np.where(neutral & (si < 0.0) & np.isfinite(mip_s), mip_s, mip)
    if np.isfinite(cap).any():
        mdp_s = _extreme(data, [f"{prefix}{c}" for c in DOWN_PRICE_COLUMNS], cap, n, upper=False)
        mdp = np.where(neutral & (si >= 0.0) & np.isfinite(mdp_s), mdp_s, mdp)
    return mip, mdp, floor, cap


def current_state_from_arrays(
    data: Mapping[str, FloatArray],
    times: pd.DatetimeIndex,
    *,
    si_prev: FloatArray | None = None,
    regime_edges_hours: Sequence[int] = DEFAULT_REGIME_EDGES_HOURS,
    timezone: str = "Europe/Brussels",
    prefix: str = "",
) -> CurrentState:
    """Build a :class:`CurrentState` from contract-named arrays (quarter-hour table or running minute values)."""
    n = len(times)
    si = _get(data, f"{prefix}si_mw", n)
    up, _ = _sum_columns(data, [f"{prefix}{c}" for c in UP_VOLUME_COLUMNS], n)
    down, _ = _sum_columns(data, [f"{prefix}{c}" for c in DOWN_VOLUME_COLUMNS], n)
    up, down = _infer_volumes(si, up, down)
    if si_prev is None:
        si_prev = np.concatenate([[np.nan], si[:-1]]) if n else np.zeros(0)
    mip, mdp, floor, cap = structural_marginal_prices(data, si, prefix=prefix)
    return CurrentState(
        si_mw=si,
        si_prev_mw=np.asarray(si_prev, dtype=float),
        up_mw=up,
        down_mw=down,
        mip_eur_mwh=mip,
        mdp_eur_mwh=mdp,
        alpha_eur_mwh=_get(data, f"{prefix}alpha_eur_mwh", n),
        regime=local_hour_regime(times, timezone=timezone, edges_hours=regime_edges_hours),
        curve_row=np.arange(n, dtype=np.int64),
        floor_eur_mwh=floor,
        cap_eur_mwh=cap,
    )


def new_state_from_arrays(
    data: Mapping[str, FloatArray],
    times: pd.DatetimeIndex,
    *,
    prefix: str = NF,
    regime_edges_hours: Sequence[int] = DEFAULT_REGIME_EDGES_HOURS,
    timezone: str = "Europe/Brussels",
) -> NewFormulaState:
    """Build a :class:`NewFormulaState` from ``nf_``-prefixed contract arrays (ODS212 QH or ODS211 running)."""
    n = len(times)
    p = prefix
    si = _get(data, f"{p}si_mw", n)
    mfrr_up, _ = _sum_columns(data, [f"{p}mfrr_sa_up_mw", f"{p}mfrr_da_up_mw"], n)
    mfrr_down, _ = _sum_columns(data, [f"{p}mfrr_sa_down_mw", f"{p}mfrr_da_down_mw"], n)
    afrr_up = _get(data, f"{p}afrr_up_mw", n)
    afrr_down = _get(data, f"{p}afrr_down_mw", n)
    afrr_up, afrr_down = _infer_volumes(si, afrr_up, afrr_down)
    return NewFormulaState(
        si_mw=si,
        afrr_up_mw=afrr_up,
        afrr_down_mw=afrr_down,
        mfrr_up_mw=np.nan_to_num(mfrr_up),
        mfrr_down_mw=np.nan_to_num(mfrr_down),
        afrr_price_up_eur_mwh=_get(data, f"{p}afrr_price_up_eur_mwh", n),
        afrr_price_down_eur_mwh=_get(data, f"{p}afrr_price_down_eur_mwh", n),
        mfrr_price_up_eur_mwh=_get(data, f"{p}mfrr_price_up_eur_mwh", n),
        mfrr_price_down_eur_mwh=_get(data, f"{p}mfrr_price_down_eur_mwh", n),
        weight_afrr=_get(data, f"{p}weight_afrr", n),
        weight_mfrr=_get(data, f"{p}weight_mfrr", n),
        main_up_eur_mwh=_get(data, f"{p}main_up_eur_mwh", n),
        main_down_eur_mwh=_get(data, f"{p}main_down_eur_mwh", n),
        incentive_eur_mwh=_get(data, f"{p}incentive_eur_mwh", n),
        regime=local_hour_regime(times, timezone=timezone, edges_hours=regime_edges_hours),
    )


def current_state_from_market(market: MarketData, **kwargs: Any) -> CurrentState:
    """Quarter-hour state of the current formula (ODS134 components + ODS127 activated volumes)."""
    return current_state_from_arrays(market.qh, market.qh_times, timezone=market.timezone, **kwargs)


def new_state_from_market(market: MarketData, **kwargs: Any) -> NewFormulaState:
    """Quarter-hour state of the 2026 formula (ODS212 ``nf_`` fields)."""
    return new_state_from_arrays(market.qh, market.qh_times, timezone=market.timezone, **kwargs)


def qh_to_date_mean(values: FloatArray, minutes_per_qh: int = MINUTES_PER_QH) -> FloatArray:
    """Running mean within each quarter-hour of an instantaneous minute series (NaN minutes are skipped)."""
    n = len(values)
    if n % minutes_per_qh:
        raise ValueError("minute series length must be a multiple of the quarter-hour length")
    block = np.asarray(values, dtype=float).reshape(-1, minutes_per_qh)
    finite = np.isfinite(block)
    sums = np.cumsum(np.where(finite, block, 0.0), axis=1)
    counts = np.cumsum(finite, axis=1)
    with np.errstate(invalid="ignore", divide="ignore"):
        out = np.where(counts > 0, sums / np.maximum(counts, 1), np.nan)
    result: FloatArray = out.reshape(n)
    return result


def previous_qh_final(values: FloatArray, minutes_per_qh: int = MINUTES_PER_QH) -> FloatArray:
    """For each minute row, the last value of the previous quarter-hour of a QH-to-date series (NaN in QH 0)."""
    block = np.asarray(values, dtype=float).reshape(-1, minutes_per_qh)
    last = block[:, -1]
    prev = np.concatenate([[np.nan], last[:-1]])
    result: FloatArray = np.repeat(prev, minutes_per_qh)
    return result


def current_running_state(market: MarketData, **kwargs: Any) -> CurrentState:
    """Running (QH-to-date) current-formula state per minute row: ODS133 cumulative SI / MIP / MDP / alpha and the
    QH-to-date mean of the ODS126 instantaneous activated volumes. Row ``j`` only uses minute rows ``<= j``."""
    minute = market.minute
    n = market.n_minutes
    data: dict[str, FloatArray] = {}
    si_cum = _get(minute, "si_cum_mw", n)
    data["si_mw"] = si_cum
    for name in (
        "mip_eur_mwh",
        "mdp_eur_mwh",
        "alpha_eur_mwh",
        "floor_price_eur_mwh",
        "cap_price_eur_mwh",
        "afrr_price_up_eur_mwh",
        "afrr_price_down_eur_mwh",
    ):
        if name in minute:
            data[name] = _get(minute, name, n)
    for name in (*UP_VOLUME_COLUMNS, *DOWN_VOLUME_COLUMNS):
        if name in minute:
            data[name] = qh_to_date_mean(minute[name])
    state = current_state_from_arrays(
        data, market.minute_times, si_prev=previous_qh_final(si_cum), timezone=market.timezone, **kwargs
    )
    return replace(state, curve_row=np.full(n, -1, dtype=np.int64))


def new_running_state(market: MarketData, **kwargs: Any) -> NewFormulaState:
    """Running new-formula state per minute row from the ODS211 QH-to-date fields (``nf_`` minute arrays)."""
    return new_state_from_arrays(market.minute, market.minute_times, timezone=market.timezone, **kwargs)
