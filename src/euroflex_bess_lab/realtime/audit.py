"""No-look-ahead audit: perturb the future and check that past decisions do not change."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import numpy as np

from .replay import MarketData
from .simulator import simulate
from .strategies.base import Strategy
from .types import MINUTES_PER_QH, BatteryParams, PublicationModel


@dataclass(frozen=True)
class LookaheadAudit:
    strategy: str
    cut_minute: int
    compared_minutes: int
    first_divergence_minute: int | None

    @property
    def passed(self) -> bool:
        return self.first_divergence_minute is None


def _perturbed(market: MarketData, *, cut_minute: int, safe_until: int, seed: int) -> MarketData:
    rng = np.random.default_rng(seed)
    q_cut = cut_minute // MINUTES_PER_QH

    def noise(values: np.ndarray, start: int) -> np.ndarray:
        out = values.copy()
        if start < len(out):
            out[start:] = rng.normal(0.0, 500.0, size=len(out) - start)
        return out

    minute = {k: noise(v, cut_minute) for k, v in market.minute.items()}
    qh = {k: noise(v, q_cut) for k, v in market.qh.items()}
    settlement = {k: noise(v, q_cut) for k, v in market.settlement_prices.items()}
    da = market.da_price.copy()
    hidden = market.da_visible_from_minute >= safe_until
    da[hidden] = rng.normal(0.0, 500.0, size=int(hidden.sum()))
    return MarketData(
        **{**market.__dict__, "minute": minute, "qh": qh, "settlement_prices": settlement, "da_price": da}
    )


def check_no_lookahead(
    make_strategy: Callable[[], Strategy],
    market: MarketData,
    battery: BatteryParams,
    *,
    cut_minute: int,
    mechanism: str = "current",
    publication: PublicationModel | None = None,
    seed: int = 7,
) -> LookaheadAudit:
    """Replace every value that is not yet published at ``cut_minute`` with noise and compare decisions.

    Decisions for minutes ``t < safe_until`` cannot legitimately depend on the perturbed values, where
    ``safe_until = min(cut + minute_lag + 1, 15 * floor(cut / 15) + 15 + qh_final_lag)``. Any divergence before that
    minute proves look-ahead.
    """
    publication = publication or PublicationModel()
    q_cut = cut_minute // MINUTES_PER_QH
    safe_until = min(
        cut_minute + publication.minute_lag + 1,
        q_cut * MINUTES_PER_QH + MINUTES_PER_QH + publication.qh_final_lag_minutes,
        market.n_minutes,
    )
    base = simulate(market, make_strategy(), battery, mechanism=mechanism, publication=publication)
    perturbed_market = _perturbed(market, cut_minute=cut_minute, safe_until=safe_until, seed=seed)
    other = simulate(perturbed_market, make_strategy(), battery, mechanism=mechanism, publication=publication)
    assert base.minute is not None and other.minute is not None
    a = base.minute["power_mw"].to_numpy()[:safe_until]
    b = other.minute["power_mw"].to_numpy()[:safe_until]
    diverged = np.flatnonzero(~np.isclose(a, b, atol=1e-9, rtol=0.0))
    return LookaheadAudit(
        strategy=base.strategy,
        cut_minute=cut_minute,
        compared_minutes=int(safe_until),
        first_divergence_minute=int(diverged[0]) if len(diverged) else None,
    )
