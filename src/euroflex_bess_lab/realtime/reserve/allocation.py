"""Per-block allocation between aFRR capacity and implicit balancing: fixed, forecast (causal) and ex-post oracle.

The forecast policy decides all six blocks of delivery day D at the capacity gate closure (09:00 local on D-1)
using only information available then:

- capacity price: :func:`~.capacity.forecast_capacity_prices` (same block of day, last ``capacity_lookback_days``
  auctions whose results were published, i.e. delivery days up to D-1);
- everything else (aFRR energy, implicit balancing, shortfalls, degradation): the realised non-capacity P&L of
  causal reference runs of each candidate split, averaged over the last ``lookback_days`` blocks of the same block
  of day that had fully settled (block end + final-price lag) before the gate closure.

``value(c) = (c.up * F_up + c.down * F_down) * P * hours + M_c`` and the best candidate is offered (ties go to
the smaller reservation). :func:`check_allocation_causality` proves the decision ignores later data.
The oracle picks, per block, the candidate whose reference run realised the highest P&L in that very block; it is
an ex-post benchmark (re-simulated, so it is not a strict upper bound once SOC coupling is accounted for).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from ..replay import MarketData
from ..types import BatteryParams, PublicationModel
from .capacity import CapacityAwards
from .plan import ReservePlan

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]


@dataclass(frozen=True)
class SplitCandidate:
    """Share of the battery power offered as aFRR capacity in each direction."""

    up_frac: float
    down_frac: float

    def __post_init__(self) -> None:
        if not (0.0 <= self.up_frac <= 1.0 and 0.0 <= self.down_frac <= 1.0):
            raise ValueError("reserved fractions must lie in [0, 1]")

    @property
    def label(self) -> str:
        if abs(self.up_frac - self.down_frac) < 1e-12:
            return f"fixed_{round(self.up_frac * 100):d}"
        return f"fixed_u{round(self.up_frac * 100):d}_d{round(self.down_frac * 100):d}"

    @property
    def mean_frac(self) -> float:
        return 0.5 * (self.up_frac + self.down_frac)


SYMMETRIC_CANDIDATES: tuple[SplitCandidate, ...] = (
    SplitCandidate(0.0, 0.0),
    SplitCandidate(0.5, 0.5),
    SplitCandidate(1.0, 1.0),
)


def offers_from_choice(
    choice: IntArray, candidates: Sequence[SplitCandidate], battery: BatteryParams
) -> tuple[FloatArray, FloatArray]:
    """Offered MW per block from candidate indices."""
    up = np.array([c.up_frac for c in candidates], dtype=float)[choice] * battery.max_power_mw
    down = np.array([c.down_frac for c in candidates], dtype=float)[choice] * battery.max_power_mw
    return up, down


@dataclass
class AllocationDecision:
    """Chosen candidate per block plus the values that justified it (for diagnostics and reports)."""

    policy: str
    candidates: tuple[SplitCandidate, ...]
    choice: IntArray
    values_eur: FloatArray  # (n_blocks, n_candidates) forecast or realised value per candidate
    meta: dict[str, object] = field(default_factory=dict)

    def reserved_share(self) -> FloatArray:
        return np.array([c.mean_frac for c in self.candidates], dtype=float)[self.choice]


@dataclass(frozen=True)
class ForecastSplitPolicy:
    """Causal per-block split from forecast capacity prices vs expected implicit/energy value (module docstring)."""

    candidates: tuple[SplitCandidate, ...] = SYMMETRIC_CANDIDATES
    lookback_days: int = 7
    min_history: int = 3
    hurdle_eur_per_mw_h: float = 0.0
    qh_final_lag_minutes: int = 5
    name: str = "forecast"

    def decide(
        self,
        awards: CapacityAwards,
        capacity_forecast: Mapping[str, FloatArray],
        reference_blocks: Mapping[SplitCandidate, pd.DataFrame],
        battery: BatteryParams,
    ) -> AllocationDecision:
        blocks = awards.blocks
        n = blocks.n
        k = len(self.candidates)
        missing = [c.label for c in self.candidates if c not in reference_blocks]
        if missing:
            raise KeyError(f"reference runs missing for candidates {missing}")
        gate = blocks.gate_closure_utc.asi8
        available = (blocks.end_utc + pd.Timedelta(minutes=self.qh_final_lag_minutes)).asi8
        hours = blocks.hours
        block_of_day = blocks.block_of_day
        power = battery.max_power_mw
        f_up = np.asarray(capacity_forecast["up"], dtype=float)
        f_down = np.asarray(capacity_forecast["down"], dtype=float)
        expected = np.full((n, k), np.nan)
        history = np.zeros(n, dtype=np.int64)
        full_qh = np.rint(hours * 4.0).astype(np.int64)
        for j, candidate in enumerate(self.candidates):
            frame = reference_blocks[candidate]
            noncap = frame["noncap_pnl_eur"].to_numpy(dtype=float)
            complete = frame["n_qh"].to_numpy() == full_qh
            for bod in np.unique(block_of_day):
                members = np.flatnonzero(block_of_day == bod)
                hist = members[complete[members]]
                order = np.argsort(available[hist], kind="stable")
                hist = hist[order]
                cumsum = np.concatenate([[0.0], np.cumsum(noncap[hist])])
                n_known = np.searchsorted(available[hist], gate[members], side="right")
                lo = np.maximum(n_known - self.lookback_days, 0)
                count = n_known - lo
                ok = count >= self.min_history
                expected[members, j] = np.where(ok, (cumsum[n_known] - cumsum[lo]) / np.maximum(count, 1), np.nan)
                if j == 0:
                    history[members] = count
        values = np.full((n, k), -np.inf)
        for j, candidate in enumerate(self.candidates):
            cap_value = np.zeros(n)
            usable = np.isfinite(expected[:, j])
            if candidate.up_frac > 0:
                cap_value += candidate.up_frac * np.nan_to_num(f_up, nan=0.0)
                usable &= np.isfinite(f_up)
            if candidate.down_frac > 0:
                cap_value += candidate.down_frac * np.nan_to_num(f_down, nan=0.0)
                usable &= np.isfinite(f_down)
            hurdle = self.hurdle_eur_per_mw_h * candidate.mean_frac * power * hours
            total = cap_value * power * hours + np.nan_to_num(expected[:, j]) - hurdle
            values[:, j] = np.where(usable, total, -np.inf)
        choice = np.argmax(values, axis=1).astype(np.int64)  # first maximum = smaller reservation on ties
        no_info = ~np.isfinite(values).any(axis=1)
        choice[no_info] = 0
        return AllocationDecision(
            policy=self.name,
            candidates=self.candidates,
            choice=choice,
            values_eur=np.where(np.isfinite(values), values, np.nan),
            meta={
                "lookback_days": self.lookback_days,
                "blocks_without_history": int(no_info.sum()),
                "history_blocks": history,
            },
        )


def oracle_decision(
    candidates: Sequence[SplitCandidate], reference_blocks: Mapping[SplitCandidate, pd.DataFrame], name: str = "oracle"
) -> AllocationDecision:
    """Ex-post best candidate per block from the realised block P&L of each reference run (not tradable)."""
    values = np.column_stack([reference_blocks[c]["pnl_eur"].to_numpy(dtype=float) for c in candidates])
    choice = np.argmax(values, axis=1).astype(np.int64)
    return AllocationDecision(policy=name, candidates=tuple(candidates), choice=choice, values_eur=values)


@dataclass(frozen=True)
class AllocationAudit:
    cut_utc: pd.Timestamp
    decided_blocks: int
    diverging_blocks: int
    first_divergence_utc: pd.Timestamp | None

    @property
    def passed(self) -> bool:
        return self.diverging_blocks == 0


def _perturb_market(market: MarketData, cut_minute: int, publication: PublicationModel, seed: int) -> MarketData:
    rng = np.random.default_rng(seed)
    m0 = publication.visible_minute_end(cut_minute)
    q0 = publication.visible_qh_end(cut_minute)

    def noise(values: FloatArray, start: int) -> FloatArray:
        out = np.array(values, dtype=float, copy=True)
        if start < len(out):
            out[start:] = np.abs(rng.normal(0.0, 300.0, size=len(out) - start))
        return out

    hidden = market.da_visible_from_minute > cut_minute
    da = market.da_price.copy()
    da[hidden] = rng.normal(0.0, 300.0, size=int(hidden.sum()))
    return MarketData(
        **{
            **market.__dict__,
            "minute": {k: noise(v, m0) for k, v in market.minute.items()},
            "qh": {k: noise(v, q0) for k, v in market.qh.items()},
            "settlement_prices": {k: noise(v, q0) for k, v in market.settlement_prices.items()},
            "da_price": da,
        }
    )


def _perturb_awards(awards: CapacityAwards, cut_utc: pd.Timestamp, seed: int) -> CapacityAwards:
    rng = np.random.default_rng(seed + 1)
    hidden = np.asarray(awards.blocks.results_published_utc > cut_utc)

    def noise(values: FloatArray, scale: float, floor: float = 0.0) -> FloatArray:
        out = np.array(values, dtype=float, copy=True)
        out[hidden] = floor + np.abs(rng.normal(0.0, scale, size=int(hidden.sum())))
        return out

    return CapacityAwards(
        blocks=awards.blocks,
        awarded_mw={d: noise(v, 100.0, floor=1.0) for d, v in awards.awarded_mw.items()},
        vwap_eur_per_mw_h={d: noise(v, 200.0) for d, v in awards.vwap_eur_per_mw_h.items()},
        marginal_eur_per_mw_h={d: noise(v, 400.0) for d, v in awards.marginal_eur_per_mw_h.items()},
        meta=dict(awards.meta),
    )


def check_allocation_causality(
    build_plan: Callable[[MarketData, CapacityAwards], ReservePlan],
    market: MarketData,
    awards: CapacityAwards,
    *,
    cut_utc: str | pd.Timestamp,
    publication: PublicationModel | None = None,
    seed: int = 5,
) -> AllocationAudit:
    """Perturbation test for block decisions.

    Every market value not yet published at ``cut_utc`` (minute rows, final quarter-hour values, settlement
    prices, hidden day-ahead prices) and every auction result published after ``cut_utc`` is replaced by noise;
    ``build_plan`` (which must rerun everything it depends on, including reference simulations) is called on both
    versions. Offers for all blocks whose gate closure is at or before ``cut_utc`` must be identical.
    """
    publication = publication or PublicationModel()
    cut = pd.Timestamp(cut_utc)
    cut = cut.tz_localize("UTC") if cut.tzinfo is None else cut.tz_convert("UTC")
    cut_minute = int(np.floor((cut - market.start_utc) / pd.Timedelta(minutes=1)))
    base = build_plan(market, awards)
    other = build_plan(_perturb_market(market, cut_minute, publication, seed), _perturb_awards(awards, cut, seed))
    decided = np.asarray(base.blocks.gate_closure_utc <= cut)
    same = np.isclose(base.offered_up_mw, other.offered_up_mw, atol=1e-9) & np.isclose(
        base.offered_down_mw, other.offered_down_mw, atol=1e-9
    )
    bad = decided & ~same
    first = pd.Timestamp(base.blocks.start_utc[np.flatnonzero(bad)[0]]) if bad.any() else None
    return AllocationAudit(
        cut_utc=cut,
        decided_blocks=int(decided.sum()),
        diverging_blocks=int(bad.sum()),
        first_divergence_utc=first,
    )
