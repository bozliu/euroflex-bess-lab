"""Settlement-period simulator for GB battery strategies (wholesale on MID, NIV chasing, dynamic response).

One clock step per SP ``t`` (decision time = start of SP ``t``, ``T_t``):

1. **EAC allocation** (if ``response`` is on and ``T_t`` is 14:00 UK): choose MW per EFA block and product for
   the EFA day starting at 23:00 with forecasts only, submit them at a reserve price equal to their expected
   energy cost; at 15:00 (``t + 2``) the clearing prices reveal which offers were accepted (pay-as-clear).
2. **Wholesale** (if ``wholesale == "mpc"``): fix the MID-traded position of SP ``t + wholesale_lead_sps``
   (default 18 SPs = 9 h, the opening of the MID trading window) with a rolling LP on MID forecasts.
3. **Imbalance / baseline** (``imbalance``): at gate closure (``t + imbalance_lead_sps``, default 2 SPs = 60 min)
   fix the physical baseline of that SP. ``"mpc"`` re-optimises on system-price forecasts (NIV chasing: the
   deviation from the wholesale position is settled at the single imbalance price); ``"soc_rule"`` only steers
   the SOC back into the band required by the response contracts.
4. **Physics and settlement** of SP ``t`` with realised data: response energy from frequency moves the SOC,
   the baseline is curtailed if the SOC window would be left, availability payments are withheld in SPs where
   the State of Energy requirement (Service Terms 6.11) is not met.

Perfect-foresight oracles produce complete position arrays first and are then settled by the same step 4
(``replay``). Money is GBP; degradation (EUR per MWh discharged) is converted at the delivery day's ECB rate.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import numpy as np
import pandas as pd
from numpy.typing import NDArray
from scipy.optimize import linprog

from ...realtime.types import BatteryParams
from .data import GBMarket
from .forecast import (
    ForecastSettings,
    PriceForecaster,
    eac_persistence_forecast,
    expected_response_energy,
    realised_block_prices,
    replacement_prices,
)
from .lp import StackProblem, StorageLP, rolling_oracle, solve_stack_lp
from .rules import (
    DELIVERY_DURATION_HOURS,
    ENERGY_RECOVERY_DELAY_SPS,
    ENERGY_RECOVERY_SHARE_PER_SP,
    HIGH_PRODUCTS,
    LOW_PRODUCTS,
    RESPONSE_PRODUCTS,
    SP_HOURS,
    StackingRules,
)

FloatArray = NDArray[np.float64]
SLOTS = 48
H = SP_HOURS


@dataclass(frozen=True)
class GBRunConfig:
    """Strategy switches (see module docstring)."""

    name: str
    wholesale: Literal["none", "mpc"] = "none"
    imbalance: Literal["none", "mpc", "soc_rule"] = "none"
    response: Literal["none", "forecast"] = "none"
    wholesale_lead_sps: int = 18
    imbalance_lead_sps: int = 2
    horizon_sps: int = 48
    rules: StackingRules = field(default_factory=StackingRules)
    forecast: ForecastSettings = field(default_factory=ForecastSettings)
    reserve_price: Literal["energy_cost", "floor"] = "energy_cost"
    floor_price_gbp: float = -100.0
    soc_margin_mwh: float = 0.1
    soc_rule_deadband_mwh: float = 0.02
    energy_window_days: int = 28
    planning_hurdle_gbp_mwh: float = 0.0
    planning_margin_mwh: float = 0.1
    replacement_pricing: bool = True

    def __post_init__(self) -> None:
        if self.wholesale == "mpc" and self.imbalance == "mpc" and self.wholesale_lead_sps <= self.imbalance_lead_sps:
            raise ValueError("the wholesale position must be fixed before the imbalance (baseline) decision")
        if self.response == "forecast" and self.imbalance == "none" and self.wholesale == "none":
            raise ValueError("response needs a baseline controller (imbalance='soc_rule' or 'mpc')")


@dataclass
class ReplayPlan:
    """Complete positions for a perfect-foresight run (settled without decisions)."""

    wholesale_mw: FloatArray
    baseline_mw: FloatArray
    contracts_mw: dict[str, FloatArray]


@dataclass
class GBResult:
    name: str
    ledger: pd.DataFrame
    battery: BatteryParams
    config: dict[str, object]
    diagnostics: dict[str, object]


def _degradation_gbp(market: GBMarket, battery: BatteryParams) -> FloatArray:
    return battery.degradation_eur_per_mwh * market.gbp_per_eur


class GBSimulator:
    def __init__(self, market: GBMarket, battery: BatteryParams, config: GBRunConfig) -> None:
        self.m = market
        self.b = battery
        self.cfg = config
        self.n = market.n
        self.fc = PriceForecaster(market, config.forecast)
        self.deg = _degradation_gbp(market, battery)
        # decisions use the previous day's ECB rate (today's is set at 16:00 CET)
        self.deg_plan = np.concatenate([np.full(SLOTS, self.deg[0]), self.deg[:-SLOTS]]) if self.n > SLOTS else self.deg
        self.wholesale = np.zeros(self.n)
        self.wholesale_set = np.zeros(self.n, dtype=bool)
        self.baseline = np.zeros(self.n)
        self.baseline_set = np.zeros(self.n, dtype=bool)
        self.accepted = {p: np.zeros(self.n) for p in RESPONSE_PRODUCTS}
        self.view = {p: np.zeros(self.n) for p in RESPONSE_PRODUCTS}
        self.offer_price = {p: np.full(self.n, np.nan) for p in RESPONSE_PRODUCTS}
        self.offered = {p: np.zeros(self.n) for p in RESPONSE_PRODUCTS}
        self.last_viewed_sp = -1
        self.pending_reveal: list[tuple[int, int]] = []  # (reveal SP index, EFA day index)
        self._lp = StorageLP(config.horizon_sps, battery)
        self._energy_day = -1
        self._energy: dict[str, FloatArray] = {}
        self.diag: dict[str, object] = {"lp_solves": 0, "allocations": 0, "soc_lp_violation_mwh": 0.0}

    # ------------------------------------------------------------------------------------------ helpers
    def expected_energy_sp(self, t: int, ks: NDArray[np.int64]) -> dict[str, FloatArray]:
        day = t // SLOTS
        if day != self._energy_day:
            self._energy = expected_response_energy(self.m, t, self.cfg.energy_window_days)
            self._energy_day = day
        blocks = self.m.efa_block[np.clip(ks, 0, self.n - 1)] - 1
        return {p: self._energy[p][blocks] for p in RESPONSE_PRODUCTS}

    def contract_view(self, ks: NDArray[np.int64]) -> dict[str, FloatArray]:
        """Contracts the planner assumes for SPs ``ks``: accepted/offered where known, otherwise the contracts of
        the same SP one or more days earlier (persistence of the own allocation)."""

        ks = np.asarray(ks, dtype=np.int64)
        src = ks.copy()
        beyond = src > self.last_viewed_sp
        if beyond.any():
            back = np.ceil((src[beyond] - self.last_viewed_sp) / SLOTS).astype(np.int64)
            src[beyond] = src[beyond] - SLOTS * back
        valid = (src >= 0) & (src < self.n)
        out: dict[str, FloatArray] = {}
        for p in RESPONSE_PRODUCTS:
            arr = np.zeros(len(ks))
            arr[valid] = self.view[p][src[valid]]
            out[p] = arr
        return out

    def plan_arrays(
        self, t: int, ks: NDArray[np.int64]
    ) -> tuple[FloatArray, FloatArray, FloatArray, FloatArray, FloatArray]:
        """``(net_up, net_down, soc_lb, soc_ub, expected_drift)`` for SPs ``ks`` from the contract view."""

        b = self.b
        eta_c, eta_d = b.charge_efficiency, b.discharge_efficiency
        if self.cfg.response == "none":
            n = len(ks)
            return (
                np.full(n, b.max_power_mw),
                np.full(n, b.max_power_mw),
                np.full(n, b.soc_min_mwh),
                np.full(n, b.soc_max_mwh),
                np.zeros(n),
            )
        view = self.contract_view(np.append(ks, ks[-1] + 1))
        energy = self.expected_energy_sp(t, ks)
        low = sum(view[p] for p in LOW_PRODUCTS)
        high = sum(view[p] for p in HIGH_PRODUCTS)
        rev_l = sum(view[p] * DELIVERY_DURATION_HOURS[p] for p in LOW_PRODUCTS)
        rev_h = sum(view[p] * DELIVERY_DURATION_HOURS[p] for p in HIGH_PRODUCTS)
        assert isinstance(low, np.ndarray) and isinstance(high, np.ndarray)
        assert isinstance(rev_l, np.ndarray) and isinstance(rev_h, np.ndarray)
        scale_l = 1.0 / eta_d if self.cfg.rules.apply_efficiency_to_energy_volume else 1.0
        scale_h = eta_c if self.cfg.rules.apply_efficiency_to_energy_volume else 1.0
        req_l = np.maximum(rev_l[:-1], rev_l[1:]) * scale_l
        req_h = np.maximum(rev_h[:-1], rev_h[1:]) * scale_h
        margin = self.cfg.planning_margin_mwh
        req_l = np.where(req_l > 0, req_l + margin, 0.0)
        req_h = np.where(req_h > 0, req_h + margin, 0.0)
        drift = np.zeros(len(ks))
        for p in RESPONSE_PRODUCTS:
            e = energy[p] * view[p][:-1]
            drift += e * eta_c if p in HIGH_PRODUCTS else -e / eta_d
        return (
            b.max_power_mw - low[:-1],
            b.max_power_mw - high[:-1],
            b.soc_min_mwh + req_l,
            b.soc_max_mwh - req_h,
            drift,
        )

    def project_soc(self, soc: float, t: int, j: int) -> float:
        """Expected SOC at the start of SP ``j`` from the SOC at the start of SP ``t`` and committed baselines."""

        if j <= t:
            return soc
        ks = np.arange(t, j, dtype=np.int64)
        base = np.where(self.baseline_set[ks], self.baseline[ks], self.wholesale[ks])
        eta_c, eta_d = self.b.charge_efficiency, self.b.discharge_efficiency
        delta = -H * np.clip(base, 0.0, None) / eta_d + H * np.clip(-base, 0.0, None) * eta_c
        _, _, _, _, drift = self.plan_arrays(t, ks)
        return float(soc + delta.sum() + drift.sum())

    def _horizon(self, j: int) -> NDArray[np.int64]:
        return np.arange(j, min(self.n, j + self.cfg.horizon_sps), dtype=np.int64)

    # -------------------------------------------------------------------------------------- decisions
    def decide_wholesale(self, t: int, soc: float) -> None:
        j = t + self.cfg.wholesale_lead_sps
        if j >= self.n:
            return
        ks = self._horizon(j)
        price = self.fc.mid_forecast(t, ks)
        up, down, lb, ub, drift = self.plan_arrays(t, ks)
        soc0 = float(np.clip(self.project_soc(soc, t, j), 0.0, self.b.energy_mwh))
        plan = self._lp.solve(
            price,
            soc0=soc0,
            drift=drift,
            net_up=up,
            net_down=down,
            soc_lb=lb,
            soc_ub=ub,
            degradation=self.deg_plan[ks] + self.cfg.planning_hurdle_gbp_mwh,
            terminal_value=max(float(np.mean(price)), 0.0) * self.b.discharge_efficiency,
            deburn=True,
        )
        self.diag["lp_solves"] = int(self.diag["lp_solves"]) + 1  # type: ignore[call-overload]
        self.wholesale[j] = float(plan.net_mw[0])
        self.wholesale_set[j] = True
        if min(plan.discharge_mw[0], plan.charge_mw[0]) > 1e-6:
            self.diag["simultaneous_first_period"] = int(self.diag.get("simultaneous_first_period", 0)) + 1  # type: ignore[call-overload]

    def decide_baseline_mpc(self, t: int, soc: float) -> None:
        j = t + self.cfg.imbalance_lead_sps
        if j >= self.n:
            return
        ks = self._horizon(j)
        if self.cfg.wholesale == "mpc":
            fixed = self.wholesale_set[ks]
            price = np.where(fixed, self.fc.sip_forecast(t, ks), self.fc.mid_forecast(t, ks))
        else:
            price = self.fc.sip_forecast(t, ks)
        up, down, lb, ub, drift = self.plan_arrays(t, ks)
        soc0 = float(np.clip(self.project_soc(soc, t, j), 0.0, self.b.energy_mwh))
        plan = self._lp.solve(
            price,
            soc0=soc0,
            drift=drift,
            net_up=up,
            net_down=down,
            soc_lb=lb,
            soc_ub=ub,
            degradation=self.deg_plan[ks] + self.cfg.planning_hurdle_gbp_mwh,
            terminal_value=max(float(np.mean(price)), 0.0) * self.b.discharge_efficiency,
            deburn=True,
        )
        self.diag["lp_solves"] = int(self.diag["lp_solves"]) + 1  # type: ignore[call-overload]
        self.diag["soc_lp_violation_mwh"] = float(self.diag["soc_lp_violation_mwh"]) + plan.soc_violation_mwh  # type: ignore[arg-type]
        self.baseline[j] = float(plan.net_mw[0])
        self.baseline_set[j] = True
        if min(plan.discharge_mw[0], plan.charge_mw[0]) > 1e-6:
            self.diag["simultaneous_first_period"] = int(self.diag.get("simultaneous_first_period", 0)) + 1  # type: ignore[call-overload]

    def decide_baseline_rule(self, t: int, soc: float) -> None:
        """Steer the SOC to the middle of the band required by the contracts (response-only operation)."""

        j = t + self.cfg.imbalance_lead_sps
        if j >= self.n:
            return
        ks = np.array([j], dtype=np.int64)
        up, down, lb, ub, drift = self.plan_arrays(t, ks)
        s = self.project_soc(soc, t, j) + float(drift[0])
        lo, hi = float(lb[0]) + self.cfg.soc_margin_mwh / 2, float(ub[0]) - self.cfg.soc_margin_mwh / 2
        target = (lo + hi) / 2 if lo <= hi else (float(lb[0]) + float(ub[0])) / 2
        error = s - target
        power = 0.0
        if abs(error) > self.cfg.soc_rule_deadband_mwh:
            if error > 0:
                power = min(error * self.b.discharge_efficiency / H, float(up[0]))
            else:
                power = -min(-error / (self.b.charge_efficiency * H), float(down[0]))
        self.baseline[j] = max(power, -self.b.max_power_mw) if power < 0 else min(power, self.b.max_power_mw)
        self.baseline_set[j] = True

    # ------------------------------------------------------------------------------------- allocation
    def energy_cost_per_mw_h(
        self, energy_block: dict[str, FloatArray], price: float | dict[str, float], deg: float
    ) -> dict[str, FloatArray]:
        """Expected cost (GBP per MW and hour of availability) of the energy a product moves: exported energy
        (low) has to be bought back, imported energy (high) can be sold later. ``price`` may be one price or a
        replacement price per product."""

        eta = self.b.charge_efficiency * self.b.discharge_efficiency
        out: dict[str, FloatArray] = {}
        for p in RESPONSE_PRODUCTS:
            per_hour = energy_block[p] / H
            value = price[p] if isinstance(price, dict) else price
            if p in LOW_PRODUCTS:
                out[p] = per_hour * (value / eta + deg)
            else:
                out[p] = -per_hour * eta * (value - deg)
        return out

    def allocate(self, t: int, soc: float) -> None:
        start = t + 18  # 23:00 UK, start of the EFA day being auctioned
        if start >= self.n:
            return
        day = int(self.m.efa_day[start])
        sps = self.m.day_sps(day)
        sps = sps[sps >= start]
        if len(sps) == 0:
            return
        forecast_prices = eac_persistence_forecast(self.m, day)
        energy_block = expected_response_energy(self.m, t, self.cfg.energy_window_days)
        deg = float(self.deg_plan[t])
        if self.cfg.replacement_pricing:
            repl, mean_sip = replacement_prices(self.m, t, days=self.cfg.energy_window_days)
        else:
            mean_sip = self.fc.trailing_mean("sip", t, days=self.cfg.energy_window_days)
            repl = {p: mean_sip for p in RESPONSE_PRODUCTS}
        if self.cfg.wholesale == "mpc":
            # the baseline plan values energy at forecast MID; add the timing premium of replacing response energy
            ref_mid = float(np.mean(self.fc.mid_forecast(t, sps)))
            premium = {p: repl[p] - mean_sip for p in RESPONSE_PRODUCTS}
            cost = self.energy_cost_per_mw_h(energy_block, {p: ref_mid + premium[p] for p in RESPONSE_PRODUCTS}, deg)
            timing = self.energy_cost_per_mw_h(energy_block, premium, 0.0)
            adjusted = [
                {p: prices[p] - float(timing[p][b]) for p in RESPONSE_PRODUCTS}
                for b, prices in enumerate(forecast_prices)
            ]
            offers = self._allocate_with_baseline(t, soc, sps, adjusted)
        else:
            cost = self.energy_cost_per_mw_h(energy_block, repl, deg)
            offers = self._allocate_blocks(sps, forecast_prices, cost)
        self.diag["allocations"] = int(self.diag["allocations"]) + 1  # type: ignore[call-overload]
        blocks = self.m.efa_block[sps]
        for p in RESPONSE_PRODUCTS:
            mw = np.array([offers[int(b) - 1][p] for b in blocks])
            mw = np.where(mw > 1e-4, mw, 0.0)
            if self.cfg.reserve_price == "energy_cost":
                price = np.round(cost[p][blocks - 1], 2)
            else:
                price = np.full(len(sps), self.cfg.floor_price_gbp)
            self.offered[p][sps] = mw
            self.offer_price[p][sps] = price
            self.view[p][sps] = mw
        self.last_viewed_sp = max(self.last_viewed_sp, int(sps[-1]))
        self.pending_reveal.append((t + 2, day))

    def reveal(self, t: int) -> None:
        keep: list[tuple[int, int]] = []
        for when, day in self.pending_reveal:
            if when > t:
                keep.append((when, day))
                continue
            sps = self.m.day_sps(day)
            sps = sps[self.offered["DCL"][sps] >= 0]
            for p in RESPONSE_PRODUCTS:
                clearing = self.m.eac_price[p][sps]
                ok = np.isfinite(clearing) & (clearing >= self.offer_price[p][sps] - 1e-9)
                self.accepted[p][sps] = np.where(ok, self.offered[p][sps], 0.0)
                self.view[p][sps] = self.accepted[p][sps]
        self.pending_reveal = keep

    def _allocate_blocks(
        self, sps: NDArray[np.int64], prices: list[dict[str, float]], cost: dict[str, FloatArray]
    ) -> list[dict[str, float]]:
        """Response-only allocation: per EFA block, an LP over the six products (procurement reserve, energy
        volumes within the SOC window minus a margin)."""

        b = self.b
        eta_c, eta_d = b.charge_efficiency, b.discharge_efficiency
        products = list(RESPONSE_PRODUCTS)
        dur = np.array([DELIVERY_DURATION_HOURS[p] for p in products])
        is_low = np.array([p in LOW_PRODUCTS for p in products])
        usable = b.soc_max_mwh - b.soc_min_mwh - self.cfg.soc_margin_mwh
        energy_row = np.where(is_low, dur / eta_d, dur * eta_c)
        procurement = self.cfg.rules.procurement_rows(
            products, b.max_power_mw, b.max_power_mw, charge_efficiency=eta_c, discharge_efficiency=eta_d
        )
        a_ub = np.vstack([*(coef for coef, _ in procurement), energy_row])
        b_ub = np.array([*(rhs for _, rhs in procurement), usable])
        out: list[dict[str, float]] = []
        blocks_present = set(int(x) for x in self.m.efa_block[sps])
        for block in range(1, 7):
            if block not in blocks_present:
                out.append({p: 0.0 for p in products})
                continue
            value = np.array([prices[block - 1][p] - float(cost[p][block - 1]) for p in products])
            finite = np.isfinite(value)
            value = np.where(finite, value, 0.0)
            bounds = [(0.0, b.max_power_mw if f else 0.0) for f in finite]
            result = linprog(-value, A_ub=a_ub, b_ub=b_ub, bounds=bounds, method="highs")
            x = result.x if result.success else np.zeros(len(products))
            out.append({p: float(x[i]) for i, p in enumerate(products)})
        return out

    def _allocate_with_baseline(
        self, t: int, soc: float, sps: NDArray[np.int64], prices: list[dict[str, float]]
    ) -> list[dict[str, float]]:
        """Combined stack: response MW per block chosen jointly with a forecast-MID baseline plan for the day."""

        start = int(sps[0])
        soc0 = float(np.clip(self.project_soc(soc, t, start), self.b.soc_min_mwh, self.b.soc_max_mwh))
        price = self.fc.mid_forecast(t, sps)
        energy = self.expected_energy_sp(t, sps)
        blocks = self.m.efa_block[sps] - 1
        problem = StackProblem(
            price_sell=price,
            price_buy=price,
            free_block=blocks.astype(np.int64),
            block_price=prices,
            block_hours=np.array([float((blocks == b).sum()) * H for b in range(6)]),
            energy=energy,
            soc0=soc0,
            degradation=self.deg_plan[sps] + self.cfg.planning_hurdle_gbp_mwh,
            terminal_value=max(float(np.mean(price)), 0.0) * self.b.discharge_efficiency,
        )
        solution = solve_stack_lp(problem, self.b, self.cfg.rules)
        return solution.block_mw

    # ------------------------------------------------------------------------------------------- run
    def run(self, replay: ReplayPlan | None = None, preset_contracts: dict[str, FloatArray] | None = None) -> GBResult:
        """Simulate; ``replay`` settles complete positions, ``preset_contracts`` fixes the response contracts
        (e.g. a perfect-foresight allocation) while the baseline is still decided by the configured controller."""

        m, b, cfg = self.m, self.b, self.cfg
        n = self.n
        eta_c, eta_d = b.charge_efficiency, b.discharge_efficiency
        mid = m.mid_settle
        sip = m.sip
        if replay is not None:
            self.wholesale = replay.wholesale_mw.copy()
            self.baseline = replay.baseline_mw.copy()
            self.baseline_set[:] = True
            self.accepted = {p: replay.contracts_mw[p].copy() for p in RESPONSE_PRODUCTS}
        if preset_contracts is not None:
            self.accepted = {p: np.asarray(preset_contracts[p], dtype=float).copy() for p in RESPONSE_PRODUCTS}
            self.view = {p: self.accepted[p].copy() for p in RESPONSE_PRODUCTS}
            self.last_viewed_sp = n - 1
        allocate_here = cfg.response == "forecast" and replay is None and preset_contracts is None
        gate_sps = set(int(x) for x in m.eac_gate_sps()) if allocate_here else set()
        soc = b.initial_soc_mwh
        cols = {
            name: np.zeros(n)
            for name in (
                "wholesale_mw",
                "baseline_mw",
                "imbalance_mw",
                "response_export_mwh",
                "response_import_mwh",
                "response_undelivered_mwh",
                "discharged_mwh",
                "charged_mwh",
                "soc_start_mwh",
                "soc_end_mwh",
                "revenue_wholesale_gbp",
                "revenue_imbalance_gbp",
                "revenue_response_gbp",
                "degradation_gbp",
            )
        }
        rev_product = {p: np.zeros(n) for p in RESPONSE_PRODUCTS}
        avail = {"low": np.ones(n, dtype=bool), "high": np.ones(n, dtype=bool)}
        # Minimum State of Energy Requirement tracking (Service Terms 6.11)
        msoer = {"low": 0.0, "high": 0.0}
        erav_due = {"low": np.zeros(n + 8), "high": np.zeros(n + 8)}
        prev_contract_key: tuple[float, ...] | None = None
        prev_block = (-1, -1)
        delivered_prev = {"low": 0.0, "high": 0.0}
        for t in range(n):
            if replay is None:
                self.reveal(t)
                if t in gate_sps:
                    self.allocate(t, soc)
                if cfg.wholesale == "mpc":
                    self.decide_wholesale(t, soc)
                if cfg.imbalance == "mpc":
                    self.decide_baseline_mpc(t, soc)
                elif cfg.imbalance == "soc_rule":
                    self.decide_baseline_rule(t, soc)
            # ------------------------------------------------------------------- physics of SP t
            v = {p: self.accepted[p][t] for p in RESPONSE_PRODUCTS}
            low_mw = sum(v[p] for p in LOW_PRODUCTS)
            high_mw = sum(v[p] for p in HIGH_PRODUCTS)
            crev = {
                "low": sum(v[p] * DELIVERY_DURATION_HOURS[p] for p in LOW_PRODUCTS),
                "high": sum(v[p] * DELIVERY_DURATION_HOURS[p] for p in HIGH_PRODUCTS),
            }
            block_key = (int(m.efa_day[t]), int(m.efa_block[t]))
            contract_key = tuple(round(v[p], 6) for p in RESPONSE_PRODUCTS)
            new_period = block_key != prev_block or contract_key != prev_contract_key
            for side in ("low", "high"):
                if new_period:
                    msoer[side] = crev[side]
                else:
                    msoer[side] = min(crev[side], msoer[side] - delivered_prev[side] + erav_due[side][t])
                    msoer[side] = max(msoer[side], 0.0)
                # Energy Recovery Adjustment Volume for this post-activation SP, applied 4 SPs later
                if not new_period and crev[side] > 0:
                    scheduled = erav_due[side][t + 1 : t + ENERGY_RECOVERY_DELAY_SPS + 1].sum()
                    erav = min(
                        ENERGY_RECOVERY_SHARE_PER_SP * crev[side], max(crev[side] - msoer[side] - scheduled, 0.0)
                    )
                    erav_due[side][t + ENERGY_RECOVERY_DELAY_SPS + 1] += erav
            prev_block, prev_contract_key = block_key, contract_key
            tol = 1e-3
            avail["low"][t] = (soc - b.soc_min_mwh) * eta_d >= msoer["low"] - tol if crev["low"] > 0 else True
            avail["high"][t] = (b.soc_max_mwh - soc) / eta_c >= msoer["high"] - tol if crev["high"] > 0 else True
            cols["soc_start_mwh"][t] = soc
            base = self.baseline[t] if self.baseline_set[t] else self.wholesale[t]
            base = float(np.clip(base, -(b.max_power_mw - high_mw), b.max_power_mw - low_mw))
            e_low = sum(v[p] * m.response_energy[p][t] for p in LOW_PRODUCTS)
            e_high = sum(v[p] * m.response_energy[p][t] for p in HIGH_PRODUCTS)
            s_resp = soc - e_low / eta_d + e_high * eta_c
            max_dis = max(0.0, (s_resp - b.soc_min_mwh) * eta_d / H)
            max_chg = max(0.0, (b.soc_max_mwh - s_resp) / (eta_c * H))
            base = float(np.clip(base, -max_chg, max_dis))
            soc_new = s_resp - H * max(base, 0.0) / eta_d + H * max(-base, 0.0) * eta_c
            undelivered = 0.0
            if soc_new < 0.0:
                undelivered = -soc_new * eta_d
                soc_new = 0.0
            elif soc_new > b.energy_mwh:
                undelivered = (soc_new - b.energy_mwh) / eta_c
                soc_new = b.energy_mwh
            delivered_prev = {"low": e_low, "high": e_high}
            x = float(self.wholesale[t])
            cols["wholesale_mw"][t] = x
            cols["baseline_mw"][t] = base
            cols["imbalance_mw"][t] = base - x
            cols["response_export_mwh"][t] = e_low
            cols["response_import_mwh"][t] = e_high
            cols["response_undelivered_mwh"][t] = undelivered
            cols["discharged_mwh"][t] = H * max(base, 0.0) + e_low
            cols["charged_mwh"][t] = H * max(-base, 0.0) + e_high
            cols["soc_end_mwh"][t] = soc_new
            cols["revenue_wholesale_gbp"][t] = x * H * mid[t]
            cols["revenue_imbalance_gbp"][t] = (base - x) * H * sip[t]
            total_resp = 0.0
            for p in RESPONSE_PRODUCTS:
                side = "low" if p in LOW_PRODUCTS else "high"
                price = m.eac_price[p][t]
                if v[p] > 0 and np.isfinite(price) and avail[side][t]:
                    rev_product[p][t] = v[p] * H * price
                    total_resp += rev_product[p][t]
            cols["revenue_response_gbp"][t] = total_resp
            cols["degradation_gbp"][t] = cols["discharged_mwh"][t] * self.deg[t]
            soc = soc_new
        ledger = pd.DataFrame(
            {
                "timestamp_utc": m.ts,
                "settlement_date": m.settlement_date,
                "settlement_period": m.settlement_period,
                **cols,
                **{f"mw_{p.lower()}": self.accepted[p] for p in RESPONSE_PRODUCTS},
                **{f"revenue_{p.lower()}_gbp": rev_product[p] for p in RESPONSE_PRODUCTS},
                "available_low": avail["low"],
                "available_high": avail["high"],
                "mid_gbp_mwh": m.mid,
                "system_price_gbp_mwh": sip,
                "gbp_per_eur": m.gbp_per_eur,
            }
        )
        ledger["pnl_gbp"] = (
            ledger["revenue_wholesale_gbp"]
            + ledger["revenue_imbalance_gbp"]
            + ledger["revenue_response_gbp"]
            - ledger["degradation_gbp"]
        )
        ledger["pnl_eur"] = ledger["pnl_gbp"] / ledger["gbp_per_eur"]
        ledger["strategy"] = cfg.name
        diag = dict(self.diag)
        diag["offers"] = {
            p: {"offered_mwh": float(self.offered[p].sum() * H), "accepted_mwh": float(self.accepted[p].sum() * H)}
            for p in RESPONSE_PRODUCTS
        }
        return GBResult(cfg.name, ledger, b, _config_dict(cfg), diag)


def _config_dict(cfg: GBRunConfig) -> dict[str, object]:
    return {
        "name": cfg.name,
        "wholesale": cfg.wholesale,
        "imbalance": cfg.imbalance,
        "response": cfg.response,
        "wholesale_lead_sps": cfg.wholesale_lead_sps,
        "imbalance_lead_sps": cfg.imbalance_lead_sps,
        "horizon_sps": cfg.horizon_sps,
        "reserved_capacity": cfg.rules.reserved_capacity,
        "reserve_mode": cfg.rules.reserve_mode,
        "reserve_price": cfg.reserve_price,
        "level_decay_per_sp": cfg.forecast.level_decay_per_sp,
        "use_spread": cfg.forecast.use_spread,
    }


# -------------------------------------------------------------------------------------------- oracles


def oracle_single_market(market: GBMarket, battery: BatteryParams, which: Literal["mid", "sip"], name: str) -> GBResult:
    """Perfect foresight on one price (MID or system price), settled through the common ledger."""

    price = market.mid_settle if which == "mid" else market.sip
    plan = rolling_oracle(price, battery, degradation=_degradation_gbp(market, battery))
    zeros = np.zeros(market.n)
    if which == "mid":
        replay = ReplayPlan(plan.net_mw, plan.net_mw, {p: zeros.copy() for p in RESPONSE_PRODUCTS})
    else:
        replay = ReplayPlan(zeros.copy(), plan.net_mw, {p: zeros.copy() for p in RESPONSE_PRODUCTS})
    sim = GBSimulator(market, battery, GBRunConfig(name=name))
    return sim.run(replay=replay)


def oracle_stack(
    market: GBMarket,
    battery: BatteryParams,
    *,
    name: str,
    trading: bool = True,
    rules: StackingRules | None = None,
    window_days: int = 7,
    overlap_days: int = 1,
) -> GBResult:
    """Perfect-foresight stack: response MW per EFA block at realised clearing prices and realised response energy,
    plus (``trading``) a baseline traded at the better of MID and the system price in each SP. Without trading the
    baseline only restores the SOC (valued at the system price)."""

    rules = rules or StackingRules()
    n = market.n
    deg = _degradation_gbp(market, battery)
    days = sorted(set(int(d) for d in market.efa_day))
    wholesale = np.zeros(n)
    baseline = np.zeros(n)
    contracts = {p: np.zeros(n) for p in RESPONSE_PRODUCTS}
    soc = battery.initial_soc_mwh
    hi_price = np.maximum(market.mid_settle, market.sip)
    lo_price = np.minimum(market.mid_settle, market.sip)
    i = 0
    while i < len(days):
        commit_days = days[i : i + window_days]
        look_days = days[i : i + window_days + overlap_days]
        sps = np.flatnonzero(np.isin(market.efa_day, look_days))
        commit_mask = np.isin(market.efa_day[sps], commit_days)
        keys = list(zip(market.efa_day[sps], market.efa_block[sps], strict=True))
        uniq = sorted(set(keys))
        index = {key: k for k, key in enumerate(uniq)}
        free = np.array([index[key] for key in keys], dtype=np.int64)
        block_price = []
        block_hours = []
        for d, blk in uniq:
            first = market.block_sps(d, blk)
            block_price.append({p: float(market.eac_price[p][first[0]]) for p in RESPONSE_PRODUCTS})
            block_hours.append(float(np.isin(sps, first).sum()) * H)
        problem = StackProblem(
            price_sell=hi_price[sps] if trading else market.sip[sps],
            price_buy=lo_price[sps] if trading else market.sip[sps],
            free_block=free,
            block_price=block_price,
            block_hours=np.asarray(block_hours),
            energy={p: market.response_energy[p][sps] for p in RESPONSE_PRODUCTS},
            soc0=soc,
            degradation=deg[sps],
        )
        solution = solve_stack_lp(problem, battery, rules)
        commit = sps[commit_mask]
        net = solution.discharge_mw - solution.charge_mw
        baseline[commit] = net[commit_mask]
        for k_local, k in zip(np.flatnonzero(commit_mask), commit, strict=True):
            for p in RESPONSE_PRODUCTS:
                contracts[p][k] = solution.block_mw[free[k_local]][p]
        if trading:
            use_mid = np.where(
                net > 0, market.mid_settle[sps] >= market.sip[sps], market.mid_settle[sps] <= market.sip[sps]
            )
            wholesale[commit] = np.where(use_mid, net, 0.0)[commit_mask]
        last_local = int(np.flatnonzero(commit_mask)[-1])
        soc = float(solution.soc_end_mwh[last_local])
        i += window_days
    for p in RESPONSE_PRODUCTS:
        contracts[p] = np.where(contracts[p] > 1e-4, contracts[p], 0.0)
    replay = ReplayPlan(wholesale, baseline, contracts)
    sim = GBSimulator(market, battery, GBRunConfig(name=name))
    sim.cfg = GBRunConfig(name=name, rules=rules)
    return sim.run(replay=replay)


def oracle_response_allocation(market: GBMarket, battery: BatteryParams, config: GBRunConfig) -> dict[str, FloatArray]:
    """Perfect-foresight response allocation: each EFA block's LP of :meth:`GBSimulator._allocate_blocks` with the
    realised clearing prices and the realised mean response energy of the block (valued at the block's mean system
    price). Upper bound for the causal allocation rule (same feasibility rules, no rejected offers)."""

    sim = GBSimulator(market, battery, config)
    contracts = {p: np.zeros(market.n) for p in RESPONSE_PRODUCTS}
    deg = _degradation_gbp(market, battery)
    for day in sorted(set(int(d) for d in market.efa_day)):
        sps = market.day_sps(day)
        prices = realised_block_prices(market, day)
        blocks = market.efa_block[sps] - 1
        energy = {
            p: np.array(
                [
                    float(market.response_energy[p][sps[blocks == b]].mean()) if (blocks == b).any() else 0.0
                    for b in range(6)
                ]
            )
            for p in RESPONSE_PRODUCTS
        }
        later = np.mean(np.stack([market.sip[np.clip(sps + lag, 0, market.n - 1)] for lag in (2, 3, 4)]), axis=0)
        cost_by_block: dict[str, list[float]] = {p: [] for p in RESPONSE_PRODUCTS}
        for b in range(6):
            in_block = blocks == b
            for p in RESPONSE_PRODUCTS:
                weights = market.response_energy[p][sps[in_block]]
                repl = (
                    float((later[in_block] * weights).sum() / weights.sum())
                    if in_block.any() and weights.sum() > 0
                    else float(np.nanmean(market.sip[sps]))
                )
                single = sim.energy_cost_per_mw_h(
                    {q: energy[q][b : b + 1] for q in RESPONSE_PRODUCTS}, repl, float(deg[sps[0]])
                )
                cost_by_block[p].append(float(single[p][0]))
        cost = {p: np.asarray(cost_by_block[p]) for p in RESPONSE_PRODUCTS}
        offers = sim._allocate_blocks(sps, prices, cost)
        for p in RESPONSE_PRODUCTS:
            mw = np.array([offers[int(b)][p] for b in blocks])
            contracts[p][sps] = np.where(mw > 1e-4, mw, 0.0)
    return contracts
