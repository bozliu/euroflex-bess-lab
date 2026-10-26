"""Perfect-foresight upper bound on final quarter-hour prices (not tradable; for capture-rate reporting)."""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
from numpy.typing import NDArray
from scipy import sparse
from scipy.optimize import linprog

from ..replay import DecisionContext
from ..types import QH_HOURS, BatteryParams
from .base import BaseStrategy

FloatArray = NDArray[np.float64]


def solve_qh_arbitrage(
    prices: FloatArray,
    battery: BatteryParams,
    *,
    initial_soc_mwh: float,
    charge_cost_eur_per_mwh: float = 1e-3,
) -> tuple[FloatArray, FloatArray]:
    """LP: maximise sum(price * energy) - degradation over quarter-hours with SOC dynamics.

    Returns ``(net_power_mw, soc_end_mwh)`` per quarter-hour. Missing prices are treated as zero.
    """
    q = len(prices)
    if q == 0:
        return np.zeros(0), np.zeros(0)
    price = np.nan_to_num(np.asarray(prices, dtype=float))
    h = QH_HOURS
    eta_c, eta_d = battery.charge_efficiency, battery.discharge_efficiency
    # variables: d (q), c (q), s (q)
    cost = np.concatenate(
        [
            -price * h + battery.degradation_eur_per_mwh * h,
            price * h + charge_cost_eur_per_mwh * h,
            np.zeros(q),
        ]
    )
    rows = np.arange(q)
    eye = sparse.identity(q, format="csr")
    shift = sparse.csr_matrix((np.ones(q - 1), (rows[1:], rows[:-1])), shape=(q, q))
    a_eq = sparse.hstack([eye * (h / eta_d), eye * (-h * eta_c), eye - shift], format="csr")
    b_eq = np.zeros(q)
    b_eq[0] = initial_soc_mwh
    p_max = battery.max_power_mw
    bounds = [(0.0, p_max)] * (2 * q) + [(battery.soc_min_mwh, battery.soc_max_mwh)] * q
    result = linprog(cost, A_eq=a_eq, b_eq=b_eq, bounds=bounds, method="highs")
    if not result.success:
        raise RuntimeError(f"oracle LP failed: {result.message}")
    x = result.x
    return x[:q] - x[q : 2 * q], x[2 * q :]


@dataclass
class OracleQHStrategy(BaseStrategy):
    """Knows every final settlement price in advance; solves rolling LP windows with a look-ahead overlap.

    This deliberately violates the publication model. Use it only as the upper bound for capture rates.
    """

    window_days: int = 7
    overlap_days: int = 1
    name: str = "oracle_qh"
    plan_mw: FloatArray = field(default_factory=lambda: np.zeros(0), init=False, repr=False)

    def reset(self, ctx: DecisionContext) -> None:
        prices = ctx.market.settlement(ctx.mechanism)
        n_qh = len(prices)
        window, overlap = 96 * self.window_days, 96 * self.overlap_days
        plan = np.zeros(n_qh)
        soc = ctx.battery.initial_soc_mwh
        start = 0
        while start < n_qh:
            stop = min(n_qh, start + window + overlap)
            power, soc_path = solve_qh_arbitrage(prices[start:stop], ctx.battery, initial_soc_mwh=soc)
            commit = min(window, n_qh - start)
            plan[start : start + commit] = power[:commit]
            soc = float(soc_path[commit - 1])
            start += commit
        self.plan_mw = plan

    def decide(self, ctx: DecisionContext) -> float:
        return float(self.plan_mw[ctx.qh_index])
