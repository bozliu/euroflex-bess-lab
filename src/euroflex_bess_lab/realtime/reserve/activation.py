"""aFRR energy activation model (approximation, explicitly labelled as such in every report).

Elia does not publish per-provider activations, so the battery's activation is inferred from public system data:

- ``pro_rata`` (default): the reserved MW in a direction are activated in proportion to Belgian aFRR activation
  in that direction, ``ratio(t) = clip(afrr_<dir>_mw(t) / awarded_<dir>_mw(block), 0, 1)`` with the minute volumes
  of ODS126 and the block's total awarded capacity. This is the "average contracted provider" view. Since PICASSO
  the published aFRR volume can exceed the contracted volume (cross-border exchange, non-contracted bids); the
  ratio is clipped at 1 because the battery cannot deliver more than it reserved.
- ``merit_order`` (documented alternative, used as a sensitivity): the battery bids its reserved MW at the
  day-ahead price plus/minus ``bid_markup_eur_mwh`` and is fully activated in every minute with Belgian activation
  in that direction while the quarter-hour aFRR price clears its bid. It brackets the pro-rata view from above in
  activated volume.

Remuneration: the activated energy of a minute is paid the published aFRR energy price of its quarter-hour
(``afrr_price_<dir>_eur_mwh`` from ODS166, validated; ``price_source="minute"`` instead reconstructs an
instantaneous minute price by differencing the cumulative quarter-hour-to-date ODS165 value). Upward energy earns
``price_up * E``; downward energy pays ``price_down * E`` (it earns when the price is negative). Before PICASSO
(26 Nov 2024) Belgian aFRR energy was pay-as-bid: with ``pay_as_bid_until_utc`` set (default) the battery is paid
its own bid, assumed to be the day-ahead price plus/minus ``bid_markup_eur_mwh`` (where the day-ahead price is
missing, :func:`bid_reference_price` falls back to a causal trailing mean of published imbalance prices); the
published (marginal) price remains available as an upper-bound sensitivity.

Data note: use the minute volumes of ODS126 ("aFRR volumes activated in the Belgian LFC area", including
activations for other TSOs) as the numerator, not the quarter-hour ODS127 volumes, which since PICASSO are Elia's
satisfied aFRR demand (partly served by foreign bids) and are several times larger.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from ..replay import NEW_FORMULA_PREFIX, MarketData
from ..types import MINUTE_HOURS, MINUTES_PER_QH
from .capacity import CapacityAwards
from .products import PICASSO_GO_LIVE_UTC

FloatArray = NDArray[np.float64]
ActivationKind = Literal["pro_rata", "merit_order"]
PriceSource = Literal["qh", "minute"]
PRICE_LIMIT_EUR_MWH = 15_000.0


@dataclass(frozen=True)
class ActivationModel:
    kind: ActivationKind = "pro_rata"
    price_source: PriceSource = "qh"
    pay_as_bid_until_utc: pd.Timestamp | None = PICASSO_GO_LIVE_UTC
    bid_markup_eur_mwh: float = 10.0
    max_ratio: float = 1.0

    def __post_init__(self) -> None:
        if not 0.0 < self.max_ratio <= 1.0:
            raise ValueError("max_ratio must be in (0, 1]")

    def label(self) -> str:
        pab = "pab-da" if self.pay_as_bid_until_utc is not None else "published"
        return f"{self.kind}/{self.price_source}/{pab}"


@dataclass
class ActivationSeries:
    """Per-minute activation ratios (share of the reserved MW requested) and energy prices (EUR/MWh)."""

    ratio_up: FloatArray
    ratio_down: FloatArray
    price_up: FloatArray
    price_down: FloatArray
    raw_ratio_up: FloatArray
    raw_ratio_down: FloatArray
    model: ActivationModel
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def n_minutes(self) -> int:
        return len(self.ratio_up)

    def slice_minutes(self, m0: int, m1: int) -> ActivationSeries:
        return ActivationSeries(
            ratio_up=self.ratio_up[m0:m1],
            ratio_down=self.ratio_down[m0:m1],
            price_up=self.price_up[m0:m1],
            price_down=self.price_down[m0:m1],
            raw_ratio_up=self.raw_ratio_up[m0:m1],
            raw_ratio_down=self.raw_ratio_down[m0:m1],
            model=self.model,
            meta=dict(self.meta),
        )


def _qh_field(market: MarketData, name: str) -> FloatArray:
    if name in market.qh:
        values = np.asarray(market.qh[name], dtype=float)
        nf_name = f"{NEW_FORMULA_PREFIX}{name}"
        if nf_name in market.qh and np.isnan(values).any():
            values = np.where(np.isnan(values), market.qh[nf_name], values)
        return values
    nf_name = f"{NEW_FORMULA_PREFIX}{name}"
    if nf_name in market.qh:
        return np.asarray(market.qh[nf_name], dtype=float)
    raise KeyError(f"quarter-hour field `{name}` (or `{nf_name}`) is needed for aFRR energy prices")


def reconstruct_minute_price(cumulative: FloatArray) -> FloatArray:
    """Instantaneous minute value from a quarter-hour-to-date running mean (``p_k = k m_k - (k - 1) m_{k-1}``).

    The grid must start on a quarter-hour boundary. Values are clipped to +/- 15,000 EUR/MWh (the harmonised
    balancing-energy price limit) because differencing amplifies revisions of the non-validated running mean.
    """
    values = np.asarray(cumulative, dtype=float)
    n = len(values)
    k = (np.arange(n) % MINUTES_PER_QH + 1).astype(float)
    prev = np.concatenate([[np.nan], values[:-1]])
    out = np.where(k == 1.0, values, k * values - (k - 1.0) * prev)
    return np.clip(out, -PRICE_LIMIT_EUR_MWH, PRICE_LIMIT_EUR_MWH)


def _minute_prices(market: MarketData, direction: str, source: PriceSource) -> FloatArray:
    qh_price = np.repeat(_qh_field(market, f"afrr_price_{direction}_eur_mwh"), MINUTES_PER_QH)
    if source == "qh":
        return qh_price
    name = f"afrr_price_{direction}_eur_mwh"
    if name not in market.minute:
        raise KeyError(f"minute field `{name}` is needed for price_source='minute'")
    reconstructed = reconstruct_minute_price(market.minute[name])
    return np.where(np.isnan(reconstructed), qh_price, reconstructed)


def bid_reference_price(market: MarketData) -> tuple[FloatArray, int]:
    """Opportunity-cost reference for energy bids per quarter-hour: the day-ahead price where available.

    Where the day-ahead price is missing, a causal fallback is used: the mean of the last 96 published
    settlement prices, lagged by two quarter-hours so the final price is public. Returns ``(prices, n_fallback)``.
    """
    da = np.asarray(market.da_price, dtype=float)
    missing = ~np.isfinite(da)
    if not missing.any():
        return da, 0
    base = market.settlement_prices.get("current")
    if base is None and market.settlement_prices:
        base = next(iter(market.settlement_prices.values()))
    if base is None:
        return da, 0
    trailing = pd.Series(np.asarray(base, dtype=float)).shift(2).rolling(96, min_periods=8).mean().to_numpy()
    return np.where(missing, trailing, da), int(missing.sum())


def build_activation(
    market: MarketData, awards: CapacityAwards, model: ActivationModel | None = None
) -> ActivationSeries:
    """Per-minute activation request ratios and energy prices for ``market`` (see module docstring).

    ``awards`` must be aligned to blocks covering the market period (``CapacityAwards.aligned_to``).
    """
    model = model or ActivationModel()
    minute_block = awards.blocks.index_of(market.minute_times)
    inside = minute_block >= 0
    safe_block = np.clip(minute_block, 0, None)
    reference_qh, n_fallback = bid_reference_price(market)
    da_minute = np.repeat(reference_qh, MINUTES_PER_QH)
    ratios: dict[str, FloatArray] = {}
    raw: dict[str, FloatArray] = {}
    prices: dict[str, FloatArray] = {}
    meta: dict[str, Any] = {"model": model.label(), "bid_reference_fallback_qh": n_fallback}
    for direction, sign in (("up", 1.0), ("down", -1.0)):
        volume = np.abs(np.nan_to_num(market.require_minute(f"afrr_{direction}_mw")))
        contracted = np.where(inside, np.asarray(awards.awarded_mw[direction], dtype=float)[safe_block], np.nan)
        with np.errstate(divide="ignore", invalid="ignore"):
            raw_ratio = np.where(contracted > 0.0, volume / contracted, np.nan)
        price = _minute_prices(market, direction, model.price_source)
        bid = da_minute + sign * model.bid_markup_eur_mwh
        if model.kind == "pro_rata":
            ratio = np.clip(np.nan_to_num(raw_ratio), 0.0, model.max_ratio)
        elif model.kind == "merit_order":
            clears = (price >= bid) if direction == "up" else (price <= bid)  # False where a price is missing
            ratio = np.where((volume > 0.0) & clears & (contracted > 0.0), model.max_ratio, 0.0)
        else:  # pragma: no cover - guarded by the Literal type
            raise ValueError(f"unknown activation model {model.kind!r}")
        if model.pay_as_bid_until_utc is not None:
            cutoff = pd.Timestamp(model.pay_as_bid_until_utc)
            pay_as_bid = np.asarray(market.minute_times < cutoff)
            price = np.where(pay_as_bid, bid, price)
            meta[f"pay_as_bid_minutes_{direction}"] = int(pay_as_bid.sum())
        ratios[direction] = ratio.astype(float)
        raw[direction] = raw_ratio.astype(float)
        prices[direction] = np.asarray(price, dtype=float)
        meta[f"minutes_outside_blocks_{direction}"] = int((~inside).sum())
    return ActivationSeries(
        ratio_up=ratios["up"],
        ratio_down=ratios["down"],
        price_up=prices["up"],
        price_down=prices["down"],
        raw_ratio_up=raw["up"],
        raw_ratio_down=raw["down"],
        model=model,
        meta=meta,
    )


def activation_diagnostics(market: MarketData, awards: CapacityAwards, series: ActivationSeries) -> dict[str, Any]:
    """Validation statistics of the activation approximation (per direction, per reserved MW)."""
    out: dict[str, Any] = {"model": series.model.label(), "days": market.days}
    minute_block = awards.blocks.index_of(market.minute_times)
    for direction in ("up", "down"):
        volume = np.abs(market.require_minute(f"afrr_{direction}_mw"))
        ratio = series.ratio_up if direction == "up" else series.ratio_down
        raw_ratio = series.raw_ratio_up if direction == "up" else series.raw_ratio_down
        price = series.price_up if direction == "up" else series.price_down
        sign = 1.0 if direction == "up" else -1.0
        contracted = np.asarray(awards.awarded_mw[direction], dtype=float)
        valid_raw = np.isfinite(raw_ratio)
        energy_per_mw = float(np.sum(ratio) * MINUTE_HOURS)
        revenue_per_mw = float(np.nansum(sign * ratio * price) * MINUTE_HOURS)
        weighted_price = float(np.nansum(ratio * price) / np.sum(ratio)) if np.sum(ratio) > 0 else float("nan")
        out[direction] = {
            "minutes_with_activation_share": float(np.mean(np.nan_to_num(volume) > 0.0)),
            "mean_activated_mw": float(np.nanmean(volume)),
            "mean_contracted_mw": float(np.nanmean(contracted[minute_block[minute_block >= 0]]))
            if (minute_block >= 0).any()
            else float("nan"),
            "mean_raw_ratio": float(np.nanmean(raw_ratio)) if valid_raw.any() else float("nan"),
            "raw_ratio_above_one_share": float(np.mean(raw_ratio[valid_raw] > 1.0)) if valid_raw.any() else 0.0,
            "mean_ratio": float(np.mean(ratio)),
            "energy_mwh_per_reserved_mw_day": energy_per_mw / market.days if market.days else float("nan"),
            "activation_weighted_price_eur_mwh": weighted_price,
            "energy_revenue_eur_per_reserved_mw_day": revenue_per_mw / market.days if market.days else float("nan"),
            "price_missing_while_active_minutes": int(np.sum((ratio > 0) & np.isnan(price))),
        }
    qh_name = "afrr_up_mw"
    if qh_name in market.qh:
        minute_qh = np.abs(market.require_minute("afrr_up_mw")).reshape(-1, MINUTES_PER_QH).mean(axis=1)
        published = np.abs(np.asarray(market.qh[qh_name], dtype=float))
        ok = np.isfinite(minute_qh) & np.isfinite(published)
        if ok.sum() > 2:
            out["minute_vs_qh_volume_up"] = {
                "corr": float(np.corrcoef(minute_qh[ok], published[ok])[0, 1]),
                "mean_minute_mw": float(np.mean(minute_qh[ok])),
                "mean_qh_mw": float(np.mean(published[ok])),
            }
    return out
