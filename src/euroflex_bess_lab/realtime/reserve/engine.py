"""Minute-level simulation of a battery split between aFRR (capacity + energy) and implicit balancing.

One battery, one SOC, two virtual parts:

- the reserved part follows the aFRR activation request ``a(t) = up_mw * ratio_up(t) - down_mw * ratio_down(t)``
  (an exogenous real-time instruction from the TSO, see :mod:`.activation`);
- the implicit part is any core :class:`~euroflex_bess_lab.realtime.strategies.base.Strategy`, wrapped by
  :class:`ReserveSplitStrategy` so it only uses the power and SOC headroom the reservation leaves free.

Settlement per quarter-hour (``settle_on="requested"``, the Elia design assumed here): the BSP is paid the
requested aFRR energy at the aFRR energy price and the BRP perimeter is corrected by the same requested energy, so
activated energy carries no imbalance exposure. The BRP imbalance energy is ``metered - requested activation``
= implicit energy - delivery shortfall; it is settled at the single imbalance price of the chosen mechanism. A
delivery shortfall (only possible when activation outlasts the energy headroom) is therefore settled as imbalance;
activation-control and availability penalties are ignored by default (``shortfall_penalty_eur_mwh=0``).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass, field, replace
from typing import Any, Literal

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from ..metrics import daily_pnl, summarize_ledger
from ..replay import DecisionContext, MarketData
from ..strategies.base import BaseStrategy, Strategy
from ..types import MINUTE_HOURS, MINUTES_PER_QH, QH_HOURS, BatteryParams, PublicationModel
from .activation import ActivationSeries
from .plan import ReservePlan
from .products import AfrrProduct

FloatArray = NDArray[np.float64]

COMPONENT_COLUMNS = (
    "afrr_capacity_up_eur",
    "afrr_capacity_down_eur",
    "afrr_energy_up_eur",
    "afrr_energy_down_eur",
    "implicit_revenue_eur",
    "shortfall_imbalance_eur",
    "penalty_eur",
    "degradation_eur",
)


@dataclass(frozen=True)
class ReserveSettings:
    """Operating and settlement assumptions of the reserve layer.

    ``restore_power_frac``: when activation pushes the SOC outside the reserve band, the implicit part is forced
    to move it back with up to this share of the battery power (bounded by the free implicit power).
    ``preposition_qh``: the implicit part already respects the reserve band of the next ``preposition_qh``
    quarter-hours (the plan is known since the D-1 auction results, so this uses no market look-ahead).
    """

    restore_power_frac: float = 1.0
    preposition_qh: int = 2
    settle_on: Literal["requested", "delivered"] = "requested"
    shortfall_penalty_eur_mwh: float = 0.0

    def __post_init__(self) -> None:
        if not 0.0 <= self.restore_power_frac <= 1.0:
            raise ValueError("restore_power_frac must be in [0, 1]")
        if self.preposition_qh < 0:
            raise ValueError("preposition_qh must be non-negative")
        if self.shortfall_penalty_eur_mwh < 0:
            raise ValueError("shortfall_penalty_eur_mwh must be non-negative")


class ReserveSplitStrategy(BaseStrategy):
    """Implicit trading restricted to the headroom a reservation leaves free (a core ``Strategy``).

    Per quarter-hour ``q`` with reserved ``up_mw[q]`` / ``down_mw[q]``:

    - power: ``-(P - down_mw) <= u <= P - up_mw`` (the reserve can be added on top of ``u`` at any time);
    - energy: ``u`` never moves the SOC outside ``[soc_min + up_mw*T/eta_d, soc_max - down_mw*T*eta_c]`` where
      ``T`` is the sustain duration (:func:`~.products.reserve_soc_band`);
    - restoration: if activation left the SOC outside that band, ``u`` pushes it back (``restore_power_frac``).

    The wrapped strategy sees a :class:`BatteryParams` view with the free power and the reserve band as its SOC
    limits (swapped into ``ctx.battery`` for the call), so it plans with what it really has. Strategies must read
    ``ctx.battery`` at decision time rather than caching it in ``reset``.
    """

    def __init__(
        self,
        inner: Strategy,
        up_mw: FloatArray,
        down_mw: FloatArray,
        *,
        sustain_hours: float,
        restore_power_frac: float = 1.0,
        preposition_qh: int = 2,
        name: str | None = None,
    ) -> None:
        self.inner = inner
        self.up_mw = np.asarray(up_mw, dtype=float)
        self.down_mw = np.asarray(down_mw, dtype=float)
        if self.up_mw.shape != self.down_mw.shape:
            raise ValueError("up_mw and down_mw must have the same length")
        self.sustain_hours = float(sustain_hours)
        self.restore_power_frac = float(restore_power_frac)
        self.preposition_qh = int(preposition_qh)
        self.name = name or f"reserve_split[{getattr(inner, 'name', type(inner).__name__)}]"
        self._dis: list[float] = []
        self._chg: list[float] = []
        self._lo: list[float] = []
        self._hi: list[float] = []
        self._view_index: list[int] = []
        self._views: list[BatteryParams | None] = []
        self._restore_mw = 0.0
        self._eta_c = 1.0
        self._eta_d = 1.0

    def _prepare(self, battery: BatteryParams, n_qh: int) -> None:
        if len(self.up_mw) < n_qh:
            raise ValueError("reservation arrays are shorter than the market's quarter-hour grid")
        up = np.clip(self.up_mw[:n_qh], 0.0, battery.max_power_mw)
        down = np.clip(self.down_mw[:n_qh], 0.0, battery.max_power_mw)
        lo = battery.soc_min_mwh + up * self.sustain_hours / battery.discharge_efficiency
        hi = battery.soc_max_mwh - down * self.sustain_hours * battery.charge_efficiency
        bad = lo > hi
        if bad.any():  # infeasible reservation: keep the SOC at the midpoint, the implicit part cannot trade
            mid = 0.5 * (lo + hi)
            lo, hi = np.where(bad, mid, lo), np.where(bad, mid, hi)
        lo_eff, hi_eff = lo.copy(), hi.copy()
        for k in range(1, self.preposition_qh + 1):
            if k >= n_qh:
                break
            nxt_lo = np.concatenate([lo[k:], np.full(k, -np.inf)])
            nxt_hi = np.concatenate([hi[k:], np.full(k, np.inf)])
            lo_eff = np.maximum(lo_eff, nxt_lo)
            hi_eff = np.minimum(hi_eff, nxt_hi)
        clash = lo_eff > hi_eff
        lo_eff = np.where(clash, lo, lo_eff)
        hi_eff = np.where(clash, hi, hi_eff)
        dis = np.maximum(battery.max_power_mw - up, 0.0)
        chg = np.maximum(battery.max_power_mw - down, 0.0)
        views: dict[tuple[float, float, float, float], int] = {}
        self._views = []
        index: list[int] = []
        energy = battery.energy_mwh
        for q in range(n_qh):
            key = (
                round(float(dis[q]), 9),
                round(float(chg[q]), 9),
                round(float(lo_eff[q]), 9),
                round(float(hi_eff[q]), 9),
            )
            if key not in views:
                power = max(key[0], key[1])
                view: BatteryParams | None = None
                if power > 1e-9 and key[3] - key[2] > 1e-9:
                    lo_frac = max(key[2] / energy, 0.0)
                    hi_frac = min(key[3] / energy, 1.0)
                    view = replace(
                        battery,
                        power_mw=power,
                        availability=1.0,
                        soc_min_frac=lo_frac,
                        soc_max_frac=hi_frac,
                        initial_soc_frac=min(max(battery.initial_soc_frac, lo_frac), hi_frac),
                    )
                views[key] = len(self._views)
                self._views.append(view)
            index.append(views[key])
        self._dis = dis.tolist()
        self._chg = chg.tolist()
        self._lo = lo_eff.tolist()
        self._hi = hi_eff.tolist()
        self._view_index = index
        self._restore_mw = self.restore_power_frac * battery.max_power_mw
        self._eta_c = battery.charge_efficiency
        self._eta_d = battery.discharge_efficiency

    def reset(self, ctx: DecisionContext) -> None:
        self._prepare(ctx.battery, ctx.market.n_qh)
        self.inner.reset(ctx)

    def band(self, qh_index: int) -> tuple[float, float]:
        return self._lo[qh_index], self._hi[qh_index]

    def decide(self, ctx: DecisionContext) -> float:
        q = ctx.qh_index
        dis = self._dis[q]
        chg = self._chg[q]
        lo = self._lo[q]
        hi = self._hi[q]
        soc = ctx.soc_mwh
        u = 0.0
        view = self._views[self._view_index[q]]
        if view is not None:
            full = ctx.battery
            ctx.battery = view
            try:
                u = float(self.inner.decide(ctx))
            finally:
                ctx.battery = full
            if u != u:  # NaN -> idle
                u = 0.0
        if u > dis:
            u = dis
        elif u < -chg:
            u = -chg
        dt = MINUTE_HOURS
        if u > 0.0:
            room = (soc - lo) * self._eta_d / dt
            if u > room:
                u = room if room > 0.0 else 0.0
        elif u < 0.0:
            room = (hi - soc) / (self._eta_c * dt)
            if -u > room:
                u = -room if room > 0.0 else 0.0
        restore = self._restore_mw
        if restore > 0.0:
            if soc < lo - 1e-9 and chg > 0.0:
                target = -min(restore, chg, (lo - soc) / (self._eta_c * dt))
                if u > target:
                    u = target
            elif soc > hi + 1e-9 and dis > 0.0:
                target = min(restore, dis, (soc - hi) * self._eta_d / dt)
                if u < target:
                    u = target
        return u


def _qh_sum(values: FloatArray, n_qh: int) -> FloatArray:
    return np.asarray(values, dtype=float).reshape(n_qh, MINUTES_PER_QH).sum(axis=1)


def settle_reserve_quarter_hours(
    *,
    qh_times: pd.DatetimeIndex,
    power_mw: FloatArray,
    implicit_mw: FloatArray,
    act_up_mw: FloatArray,
    act_down_mw: FloatArray,
    act_delivered_mw: FloatArray,
    soc_mwh: FloatArray,
    imbalance_price: FloatArray,
    afrr_price_up: FloatArray,
    afrr_price_down: FloatArray,
    reserved_up_mw: FloatArray,
    reserved_down_mw: FloatArray,
    capacity_price_up: FloatArray,
    capacity_price_down: FloatArray,
    battery: BatteryParams,
    settings: ReserveSettings,
) -> pd.DataFrame:
    """Quarter-hour ledger with one column per revenue component (see module docstring for the rules)."""
    n_qh = len(qh_times)
    if len(power_mw) != n_qh * MINUTES_PER_QH:
        raise ValueError("minute arrays must cover 15 x number of quarter-hours")
    h = MINUTE_HOURS
    requested = act_up_mw - act_down_mw
    shortfall = requested - act_delivered_mw  # sign of the request: + missing discharge, - missing charge
    short_up = np.clip(shortfall, 0.0, None)
    short_down = np.clip(-shortfall, 0.0, None)
    if settings.settle_on == "requested":
        paid_up, paid_down = act_up_mw, act_down_mw
        perimeter = power_mw - requested
    else:
        paid_up = np.clip(act_up_mw - short_up, 0.0, None)
        paid_down = np.clip(act_down_mw - short_down, 0.0, None)
        perimeter = power_mw - act_delivered_mw
    up_missing = (paid_up > 0) & np.isnan(afrr_price_up)
    down_missing = (paid_down > 0) & np.isnan(afrr_price_down)
    energy_up_eur = _qh_sum(paid_up * np.nan_to_num(afrr_price_up) * h, n_qh)
    energy_down_eur = -_qh_sum(paid_down * np.nan_to_num(afrr_price_down) * h, n_qh)
    metered = _qh_sum(power_mw * h, n_qh)
    discharged = _qh_sum(np.clip(power_mw, 0.0, None) * h, n_qh)
    charged = _qh_sum(np.clip(-power_mw, 0.0, None) * h, n_qh)
    implicit_energy = _qh_sum(implicit_mw * h, n_qh)
    perimeter_energy = _qh_sum(perimeter * h, n_qh)
    shortfall_energy = implicit_energy - perimeter_energy  # net shortfall settled as imbalance (0 if delivered)
    price = np.asarray(imbalance_price, dtype=float)
    missing = np.isnan(price)
    p = np.nan_to_num(price)
    implicit_revenue = np.where(missing, 0.0, implicit_energy * p)
    shortfall_imbalance = np.where(missing, 0.0, -shortfall_energy * p)
    short_up_mwh = _qh_sum(short_up * h, n_qh)
    short_down_mwh = _qh_sum(short_down * h, n_qh)
    penalty = settings.shortfall_penalty_eur_mwh * (short_up_mwh + short_down_mwh)
    cap_up = reserved_up_mw * capacity_price_up * QH_HOURS
    cap_down = reserved_down_mw * capacity_price_down * QH_HOURS
    degradation = discharged * battery.degradation_eur_per_mwh
    imbalance_revenue = implicit_revenue + shortfall_imbalance
    revenue = cap_up + cap_down + energy_up_eur + energy_down_eur + imbalance_revenue - penalty
    return pd.DataFrame(
        {
            "timestamp_utc": qh_times,
            "energy_mwh": perimeter_energy,
            "discharged_mwh": discharged,
            "charged_mwh": charged,
            "base_price_eur_mwh": price,
            "price_eur_mwh": price,
            "price_missing": missing,
            "revenue_eur": revenue,
            "degradation_eur": degradation,
            "pnl_eur": revenue - degradation,
            "soc_end_mwh": soc_mwh.reshape(n_qh, MINUTES_PER_QH)[:, -1],
            "metered_energy_mwh": metered,
            "implicit_energy_mwh": implicit_energy,
            "afrr_up_reserved_mw": reserved_up_mw,
            "afrr_down_reserved_mw": reserved_down_mw,
            "afrr_capacity_price_up_eur_mw_h": capacity_price_up,
            "afrr_capacity_price_down_eur_mw_h": capacity_price_down,
            "afrr_up_energy_mwh": _qh_sum(act_up_mw * h, n_qh),
            "afrr_down_energy_mwh": _qh_sum(act_down_mw * h, n_qh),
            "shortfall_up_mwh": short_up_mwh,
            "shortfall_down_mwh": short_down_mwh,
            "afrr_capacity_up_eur": cap_up,
            "afrr_capacity_down_eur": cap_down,
            "afrr_capacity_eur": cap_up + cap_down,
            "afrr_energy_up_eur": energy_up_eur,
            "afrr_energy_down_eur": energy_down_eur,
            "afrr_energy_eur": energy_up_eur + energy_down_eur,
            "implicit_revenue_eur": implicit_revenue,
            "shortfall_imbalance_eur": shortfall_imbalance,
            "imbalance_revenue_eur": imbalance_revenue,
            "penalty_eur": penalty,
            "afrr_price_missing_minutes": _qh_sum((up_missing | down_missing).astype(float), n_qh).astype(np.int32),
        }
    )


def revenue_stack(qh: pd.DataFrame, battery: BatteryParams, *, n_minutes: int | None = None) -> dict[str, float]:
    """Revenue components in EUR per MW (battery power) per year; costs are negative."""
    minutes = n_minutes if n_minutes is not None else len(qh) * MINUTES_PER_QH
    years = minutes / 1440.0 / 365.25
    scale = 1.0 / (battery.power_mw * years) if years > 0 else float("nan")
    totals = {c: float(qh[c].sum()) for c in COMPONENT_COLUMNS}
    stack = {
        "afrr_capacity": (totals["afrr_capacity_up_eur"] + totals["afrr_capacity_down_eur"]) * scale,
        "afrr_capacity_up": totals["afrr_capacity_up_eur"] * scale,
        "afrr_capacity_down": totals["afrr_capacity_down_eur"] * scale,
        "afrr_energy": (totals["afrr_energy_up_eur"] + totals["afrr_energy_down_eur"]) * scale,
        "afrr_energy_up": totals["afrr_energy_up_eur"] * scale,
        "afrr_energy_down": totals["afrr_energy_down_eur"] * scale,
        "implicit_balancing": totals["implicit_revenue_eur"] * scale,
        "shortfall_imbalance": totals["shortfall_imbalance_eur"] * scale,
        "penalty": -totals["penalty_eur"] * scale,
        "degradation": -totals["degradation_eur"] * scale,
    }
    stack["total"] = float(qh["pnl_eur"].sum()) * scale
    days = minutes / 1440.0
    stack["cycles_per_day"] = float(qh["discharged_mwh"].sum()) / battery.usable_energy_mwh / days if days else 0.0
    stack["mean_up_reserved_mw"] = float(qh["afrr_up_reserved_mw"].mean()) if len(qh) else 0.0
    stack["mean_down_reserved_mw"] = float(qh["afrr_down_reserved_mw"].mean()) if len(qh) else 0.0
    stack["shortfall_mwh_per_mw_year"] = (
        float(qh["shortfall_up_mwh"].sum() + qh["shortfall_down_mwh"].sum()) * scale if years > 0 else float("nan")
    )
    return stack


@dataclass
class ReserveSimulationResult:
    policy: str
    strategy: str
    mechanism: str
    battery: BatteryParams
    publication: PublicationModel
    settings: ReserveSettings
    product: AfrrProduct
    activation_model: str
    plan: ReservePlan
    qh: pd.DataFrame
    minute: pd.DataFrame | None
    n_minutes: int
    timezone: str
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def pnl_eur(self) -> float:
        return float(self.qh["pnl_eur"].sum())

    def stack(self, start: str | pd.Timestamp | None = None, end: str | pd.Timestamp | None = None) -> dict[str, float]:
        frame = self.window(start, end)
        return revenue_stack(frame, self.battery, n_minutes=len(frame) * MINUTES_PER_QH)

    def window(self, start: str | pd.Timestamp | None = None, end: str | pd.Timestamp | None = None) -> pd.DataFrame:
        """Quarter-hour ledger rows with ``start <= timestamp_utc < end``."""
        return ledger_window(self.qh, start, end)

    def summary(self) -> dict[str, Any]:
        out: dict[str, Any] = {"policy": self.policy, "strategy": self.strategy, "mechanism": self.mechanism}
        out.update(summarize_ledger(self.qh, self.battery, n_minutes=self.n_minutes, timezone=self.timezone))
        out["stack_eur_per_mw_year"] = revenue_stack(self.qh, self.battery, n_minutes=self.n_minutes)
        out["activation_model"] = self.activation_model
        out["battery"] = asdict(self.battery)
        out["publication"] = asdict(self.publication)
        out["settings"] = asdict(self.settings)
        out["product"] = asdict(self.product)
        out["start_utc"] = str(self.qh["timestamp_utc"].iloc[0]) if len(self.qh) else None
        out["end_utc"] = str(self.qh["timestamp_utc"].iloc[-1] + pd.Timedelta(minutes=15)) if len(self.qh) else None
        return out

    def daily(self) -> pd.DataFrame:
        return daily_pnl(self.qh, timezone=self.timezone)

    def block_values(self) -> pd.DataFrame:
        """P&L per delivery block: realised capacity revenue and everything else (inputs to allocation)."""
        blocks = self.plan.blocks
        index = blocks.index_of(pd.DatetimeIndex(self.qh["timestamp_utc"]))
        inside = index >= 0
        pnl = np.bincount(index[inside], weights=self.qh["pnl_eur"].to_numpy()[inside], minlength=blocks.n)
        capacity = np.bincount(
            index[inside], weights=self.qh["afrr_capacity_eur"].to_numpy()[inside], minlength=blocks.n
        )
        count = np.bincount(index[inside], minlength=blocks.n)
        return pd.DataFrame(
            {
                "delivery_start_utc": blocks.start_utc,
                "delivery_end_utc": blocks.end_utc,
                "pnl_eur": pnl,
                "capacity_eur": capacity,
                "noncap_pnl_eur": pnl - capacity,
                "n_qh": count,
            }
        )

    def ledger_for_export(
        self, start: str | pd.Timestamp | None = None, end: str | pd.Timestamp | None = None
    ) -> pd.DataFrame:
        frame = self.window(start, end).copy()
        frame.insert(1, "strategy", f"reserve_{self.policy}")
        frame.insert(2, "mechanism", self.mechanism)
        frame.insert(3, "fleet_mw", float(self.battery.power_mw))
        return frame


def simulate_reserve(
    market: MarketData,
    inner: Strategy,
    battery: BatteryParams,
    plan: ReservePlan,
    activation: ActivationSeries,
    *,
    mechanism: str = "current",
    publication: PublicationModel | None = None,
    settings: ReserveSettings | None = None,
    product: AfrrProduct | None = None,
    record_minutes: bool = False,
    meta: dict[str, Any] | None = None,
) -> ReserveSimulationResult:
    """Run the split battery minute by minute and settle every quarter-hour (see module docstring).

    ``inner`` trades implicitly on the free headroom; ``activation`` supplies the TSO request ratios and aFRR
    energy prices; ``plan`` the reserved MW per block. The activation request of minute ``t`` is an exogenous
    real-time instruction: it moves the SOC but is never visible to ``inner`` before its publication lag.
    """
    product = product or plan.blocks.product
    settings = settings or ReserveSettings()
    publication = publication or PublicationModel()
    n = market.n_minutes
    if activation.n_minutes != n:
        raise ValueError("activation series does not match the market's minute grid")
    prices = market.settlement(mechanism)
    reserve_qh = plan.qh_arrays(market.qh_times)
    up_q = reserve_qh["up_mw"]
    down_q = reserve_qh["down_mw"]
    split = ReserveSplitStrategy(
        inner,
        up_q,
        down_q,
        sustain_hours=product.sustain_hours,
        restore_power_frac=settings.restore_power_frac,
        preposition_qh=settings.preposition_qh,
    )
    ctx = DecisionContext(market, battery, publication, mechanism)
    split.reset(ctx)

    power = np.zeros(n)
    implicit = np.zeros(n)
    delivered = np.zeros(n)
    soc_trace = np.zeros(n)
    up_minute = np.repeat(up_q, MINUTES_PER_QH)
    down_minute = np.repeat(down_q, MINUTES_PER_QH)
    act_up = up_minute * activation.ratio_up
    act_down = down_minute * activation.ratio_down
    requested = (act_up - act_down).tolist()

    p_max = battery.max_power_mw
    eta_c = battery.charge_efficiency
    eta_d = battery.discharge_efficiency
    soc_min = battery.soc_min_mwh
    soc_max = battery.soc_max_mwh
    dt = MINUTE_HOURS
    soc = battery.initial_soc_mwh
    decide = split.decide
    advance = ctx.advance
    for t in range(n):
        advance(t, soc)
        u = decide(ctx)
        a = requested[t]
        p_hi = (soc - soc_min) * eta_d / dt
        if p_hi > p_max:
            p_hi = p_max
        elif p_hi < 0.0:
            p_hi = 0.0
        p_lo = -(soc_max - soc) / (eta_c * dt)
        if p_lo < -p_max:
            p_lo = -p_max
        elif p_lo > 0.0:
            p_lo = 0.0
        a_del = p_hi if a > p_hi else (p_lo if a < p_lo else a)
        u_lo = p_lo - a_del
        u_hi = p_hi - a_del
        if u > u_hi:
            u = u_hi
        elif u < u_lo:
            u = u_lo
        p = u + a_del
        if p > 0.0:
            soc -= p * dt / eta_d
        elif p < 0.0:
            soc -= p * dt * eta_c
        if soc < soc_min:
            soc = soc_min
        elif soc > soc_max:
            soc = soc_max
        power[t] = p
        implicit[t] = u
        delivered[t] = a_del
        soc_trace[t] = soc
        ctx.last_power_mw = u

    qh = settle_reserve_quarter_hours(
        qh_times=market.qh_times,
        power_mw=power,
        implicit_mw=implicit,
        act_up_mw=act_up,
        act_down_mw=act_down,
        act_delivered_mw=delivered,
        soc_mwh=soc_trace,
        imbalance_price=prices,
        afrr_price_up=activation.price_up,
        afrr_price_down=activation.price_down,
        reserved_up_mw=up_q,
        reserved_down_mw=down_q,
        capacity_price_up=reserve_qh["price_up_eur_per_mw_h"],
        capacity_price_down=reserve_qh["price_down_eur_per_mw_h"],
        battery=battery,
        settings=settings,
    )
    minute = (
        pd.DataFrame(
            {
                "timestamp_utc": market.minute_times,
                "power_mw": power,
                "implicit_mw": implicit,
                "afrr_requested_mw": act_up - act_down,
                "afrr_delivered_mw": delivered,
                "soc_mwh": soc_trace,
            }
        )
        if record_minutes
        else None
    )
    run_meta = {"activation": activation.meta, **(meta or {})}
    return ReserveSimulationResult(
        policy=plan.policy,
        strategy=getattr(inner, "name", type(inner).__name__),
        mechanism=mechanism,
        battery=battery,
        publication=publication,
        settings=settings,
        product=product,
        activation_model=activation.model.label(),
        plan=plan,
        qh=qh,
        minute=minute,
        n_minutes=n,
        timezone=market.timezone,
        meta=run_meta,
    )


@dataclass(frozen=True)
class ReserveLookaheadAudit:
    cut_minute: int
    compared_minutes: int
    first_divergence_minute: int | None

    @property
    def passed(self) -> bool:
        return self.first_divergence_minute is None


def check_reserve_no_lookahead(
    make_inner: Callable[[], Strategy],
    market: MarketData,
    battery: BatteryParams,
    plan: ReservePlan,
    activation: ActivationSeries,
    *,
    cut_minute: int,
    mechanism: str = "current",
    publication: PublicationModel | None = None,
    settings: ReserveSettings | None = None,
    seed: int = 11,
) -> ReserveLookaheadAudit:
    """Perturb every market value not yet published at ``cut_minute`` and compare the implicit decisions.

    The activation stream (the TSO's real-time instruction) and the plan are held fixed: they are inputs to the
    physics, not information the implicit strategy may read. Implicit power before
    ``safe_until = min(cut + minute_lag + 1, end of the cut's quarter-hour + qh_final_lag)`` must be identical.
    """
    publication = publication or PublicationModel()
    rng = np.random.default_rng(seed)
    q_cut = cut_minute // MINUTES_PER_QH
    safe_until = min(
        cut_minute + publication.minute_lag + 1,
        q_cut * MINUTES_PER_QH + MINUTES_PER_QH + publication.qh_final_lag_minutes,
        market.n_minutes,
    )

    def noise(values: FloatArray, start: int) -> FloatArray:
        out = np.array(values, dtype=float, copy=True)
        if start < len(out):
            out[start:] = rng.normal(0.0, 500.0, size=len(out) - start)
        return out

    hidden_da = market.da_visible_from_minute >= safe_until
    da = market.da_price.copy()
    da[hidden_da] = rng.normal(0.0, 500.0, size=int(hidden_da.sum()))
    perturbed = MarketData(
        **{
            **market.__dict__,
            "minute": {k: noise(v, cut_minute) for k, v in market.minute.items()},
            "qh": {k: noise(v, q_cut) for k, v in market.qh.items()},
            "settlement_prices": {k: noise(v, q_cut) for k, v in market.settlement_prices.items()},
            "da_price": da,
        }
    )
    base = simulate_reserve(
        market,
        make_inner(),
        battery,
        plan,
        activation,
        mechanism=mechanism,
        publication=publication,
        settings=settings,
        record_minutes=True,
    )
    other = simulate_reserve(
        perturbed,
        make_inner(),
        battery,
        plan,
        activation,
        mechanism=mechanism,
        publication=publication,
        settings=settings,
        record_minutes=True,
    )
    assert base.minute is not None and other.minute is not None
    a = base.minute["implicit_mw"].to_numpy()[:safe_until]
    b = other.minute["implicit_mw"].to_numpy()[:safe_until]
    diverged = np.flatnonzero(~np.isclose(a, b, atol=1e-9, rtol=0.0))
    return ReserveLookaheadAudit(
        cut_minute=cut_minute,
        compared_minutes=int(safe_until),
        first_divergence_minute=int(diverged[0]) if len(diverged) else None,
    )


def to_utc(value: str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def ledger_window(
    qh: pd.DataFrame, start: str | pd.Timestamp | None = None, end: str | pd.Timestamp | None = None
) -> pd.DataFrame:
    """Rows of a quarter-hour ledger with ``start <= timestamp_utc < end`` (open bounds when ``None``)."""
    times = pd.to_datetime(qh["timestamp_utc"], utc=True)
    mask = np.ones(len(qh), dtype=bool)
    if start is not None:
        mask &= np.asarray(times >= to_utc(start))
    if end is not None:
        mask &= np.asarray(times < to_utc(end))
    return qh.loc[mask].reset_index(drop=True)
