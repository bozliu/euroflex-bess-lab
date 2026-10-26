"""aFRR capacity: delivery blocks, published awards, award/clearing model and causal capacity-price forecasts.

Clearing proxy. Belgian aFRR capacity is pay-as-bid, so the price a new bidder would earn is not published. The
reserve layer offers three explicit choices (:class:`AwardModel`):

- ``"vwap"`` (default): every offered MW is awarded and paid the block's awarded-volume-weighted average bid
  price (VWAP). This treats the battery as a representative accepted bidder; it ignores the risk of not being
  selected, so it is optimistic on award certainty and neutral on price.
- ``"marginal"``: awarded and paid the highest accepted bid price. An upper bound (what a perfectly informed
  pay-as-bid bidder could at most earn); not tradable.
- ``"pay_as_bid"``: the battery bids at an ex-ante price (by default the causal VWAP forecast); it is awarded only
  if its bid is at or below the realised marginal accepted price, and is paid its own bid. This is the honest
  version of the award uncertainty: forecast errors lose whole blocks.
"""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from .products import DIRECTIONS, AfrrProduct

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]
ClearingProxy = Literal["vwap", "marginal", "pay_as_bid"]


def _utc(value: str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def utc_ns(times: pd.DatetimeIndex | pd.Series | NDArray[Any]) -> pd.DatetimeIndex:
    """UTC ``DatetimeIndex`` in nanoseconds (pandas 3 defaults to microseconds; never compare ``asi8`` across units)."""
    index = pd.DatetimeIndex(times)
    index = index.tz_localize("UTC") if index.tz is None else index.tz_convert("UTC")
    return index.as_unit("ns")


@dataclass(frozen=True)
class DeliveryBlocks:
    """Ordered, non-overlapping capacity delivery blocks with their auction timing."""

    start_utc: pd.DatetimeIndex
    end_utc: pd.DatetimeIndex
    product: AfrrProduct = field(default_factory=AfrrProduct)

    def __post_init__(self) -> None:
        object.__setattr__(self, "start_utc", utc_ns(self.start_utc))
        object.__setattr__(self, "end_utc", utc_ns(self.end_utc))
        if len(self.start_utc) != len(self.end_utc):
            raise ValueError("start_utc and end_utc must have the same length")
        if len(self.start_utc):
            if not (self.end_utc > self.start_utc).all():
                raise ValueError("every block must end after it starts")
            if not self.start_utc.is_monotonic_increasing:
                raise ValueError("blocks must be sorted by start")
            if (self.start_utc[1:] < self.end_utc[:-1]).any():
                raise ValueError("delivery blocks overlap")

    @property
    def n(self) -> int:
        return len(self.start_utc)

    @property
    def hours(self) -> FloatArray:
        return np.asarray((self.end_utc - self.start_utc) / pd.Timedelta(hours=1), dtype=float)

    @property
    def local_start(self) -> pd.DatetimeIndex:
        return self.start_utc.tz_convert(self.product.timezone)

    @property
    def delivery_day(self) -> pd.DatetimeIndex:
        """Local delivery date of each block (naive midnight)."""
        return self.local_start.tz_localize(None).normalize()

    @property
    def block_of_day(self) -> IntArray:
        return np.asarray(self.local_start.hour // self.product.block_hours, dtype=np.int64)

    @property
    def gate_closure_utc(self) -> pd.DatetimeIndex:
        """Capacity auction gate closure: ``gate_closure_hour_local`` on the local day before delivery."""
        naive = self.delivery_day - pd.Timedelta(days=1) + pd.Timedelta(hours=self.product.gate_closure_hour_local)
        return utc_ns(naive.tz_localize(self.product.timezone, ambiguous="raise", nonexistent="shift_forward"))

    @property
    def results_published_utc(self) -> pd.DatetimeIndex:
        return self.gate_closure_utc + pd.Timedelta(minutes=self.product.results_delay_minutes)

    def index_of(self, times: pd.DatetimeIndex) -> IntArray:
        """Block index containing each timestamp (``-1`` outside every block)."""
        if self.n == 0:
            return np.full(len(times), -1, dtype=np.int64)
        starts = self.start_utc.asi8
        ends = self.end_utc.asi8
        values = utc_ns(times).asi8
        pos = np.searchsorted(starts, values, side="right") - 1
        inside = (pos >= 0) & (values < ends[np.clip(pos, 0, None)])
        return np.where(inside, pos, -1).astype(np.int64)

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "delivery_start_utc": self.start_utc,
                "delivery_end_utc": self.end_utc,
                "delivery_day_local": self.delivery_day.date,
                "block_of_day": self.block_of_day,
                "hours": self.hours,
                "gate_closure_utc": self.gate_closure_utc,
            }
        )

    @classmethod
    def regular(
        cls, start: str | pd.Timestamp, end: str | pd.Timestamp, product: AfrrProduct | None = None
    ) -> DeliveryBlocks:
        """Local-time aligned blocks (00-04, 04-08, ... local) overlapping ``[start, end)``."""
        product = product or AfrrProduct()
        lo, hi = _utc(start), _utc(end)
        first_day = lo.tz_convert(product.timezone).tz_localize(None).normalize() - pd.Timedelta(days=1)
        last_day = hi.tz_convert(product.timezone).tz_localize(None).normalize() + pd.Timedelta(days=1)
        days = pd.date_range(first_day, last_day, freq="D")
        offsets = pd.to_timedelta(np.arange(0, 24 + product.block_hours, product.block_hours), unit="h")
        edges_naive = pd.DatetimeIndex(
            sorted({day + off for day in days for off in offsets if day + off <= last_day + pd.Timedelta(days=1)})
        )
        edges = edges_naive.tz_localize(product.timezone, ambiguous="raise", nonexistent="shift_forward").tz_convert(
            "UTC"
        )
        starts, ends = edges[:-1], edges[1:]
        keep = (ends > lo) & (starts < hi)
        return cls(start_utc=starts[keep], end_utc=ends[keep], product=product)


@dataclass(frozen=True)
class CapacityAwards:
    """Published aFRR capacity auction results aligned on :class:`DeliveryBlocks` (one value per block/direction).

    ``awarded_mw`` is Elia's total awarded volume, ``vwap_eur_per_mw_h`` the awarded-volume-weighted average bid
    price and ``marginal_eur_per_mw_h`` the highest accepted bid price (Plan.md section 5). Missing blocks are NaN.
    """

    blocks: DeliveryBlocks
    awarded_mw: Mapping[str, FloatArray]
    vwap_eur_per_mw_h: Mapping[str, FloatArray]
    marginal_eur_per_mw_h: Mapping[str, FloatArray]
    meta: dict[str, Any] = field(default_factory=dict)

    def price(self, direction: str, proxy: Literal["vwap", "marginal"] = "vwap") -> FloatArray:
        if direction not in DIRECTIONS:
            raise ValueError(f"direction must be one of {DIRECTIONS}")
        source = self.vwap_eur_per_mw_h if proxy == "vwap" else self.marginal_eur_per_mw_h
        return np.asarray(source[direction], dtype=float)

    def available(self, direction: str) -> NDArray[np.bool_]:
        """Blocks where an award (positive volume and finite prices) was published."""
        awarded = np.asarray(self.awarded_mw[direction], dtype=float)
        return (
            np.isfinite(awarded)
            & (awarded > 0.0)
            & np.isfinite(self.vwap_eur_per_mw_h[direction])
            & np.isfinite(self.marginal_eur_per_mw_h[direction])
        )

    def to_frame(self) -> pd.DataFrame:
        frame = self.blocks.to_frame()
        for direction in DIRECTIONS:
            frame[f"awarded_{direction}_mw"] = self.awarded_mw[direction]
            frame[f"vwap_{direction}_eur_per_mw_h"] = self.vwap_eur_per_mw_h[direction]
            frame[f"marginal_{direction}_eur_per_mw_h"] = self.marginal_eur_per_mw_h[direction]
        return frame

    def aligned_to(self, blocks: DeliveryBlocks) -> CapacityAwards:
        """Re-index onto ``blocks`` by block start (unmatched blocks become NaN)."""
        position = pd.Index(self.blocks.start_utc.asi8).get_indexer(blocks.start_utc.asi8)
        found = position >= 0
        same_end = np.zeros(blocks.n, dtype=bool)
        same_end[found] = self.blocks.end_utc.asi8[position[found]] == blocks.end_utc.asi8[found]
        found &= same_end

        def take(values: FloatArray) -> FloatArray:
            out = np.full(blocks.n, np.nan)
            out[found] = np.asarray(values, dtype=float)[position[found]]
            return out

        return CapacityAwards(
            blocks=blocks,
            awarded_mw={d: take(self.awarded_mw[d]) for d in DIRECTIONS},
            vwap_eur_per_mw_h={d: take(self.vwap_eur_per_mw_h[d]) for d in DIRECTIONS},
            marginal_eur_per_mw_h={d: take(self.marginal_eur_per_mw_h[d]) for d in DIRECTIONS},
            meta={**self.meta, "aligned_blocks": int(blocks.n), "matched_blocks": int(found.sum())},
        )

    @classmethod
    def from_frame(cls, frame: pd.DataFrame, *, product: AfrrProduct | None = None) -> CapacityAwards:
        """Build from the contract table ``belgium/afrr_capacity_awards`` (long: one row per block and direction)."""
        product = product or AfrrProduct()
        required = {"delivery_start_utc", "delivery_end_utc", "direction", "awarded_mw", "vwap_eur_per_mw_h"}
        missing = required - set(frame.columns)
        if missing:
            raise KeyError(f"capacity awards frame is missing columns {sorted(missing)}")
        data = frame.copy()
        data["delivery_start_utc"] = pd.to_datetime(data["delivery_start_utc"], utc=True)
        data["delivery_end_utc"] = pd.to_datetime(data["delivery_end_utc"], utc=True)
        data["direction"] = data["direction"].astype(str).str.lower()
        data = data[data["direction"].isin(DIRECTIONS)]
        if "marginal_eur_per_mw_h" not in data.columns:
            data["marginal_eur_per_mw_h"] = data["vwap_eur_per_mw_h"]
        data["awarded_mw"] = pd.to_numeric(data["awarded_mw"], errors="coerce").astype(float)
        data["vwap_eur_per_mw_h"] = pd.to_numeric(data["vwap_eur_per_mw_h"], errors="coerce").astype(float)
        data["marginal_eur_per_mw_h"] = pd.to_numeric(data["marginal_eur_per_mw_h"], errors="coerce").astype(float)
        data["value"] = data["awarded_mw"] * data["vwap_eur_per_mw_h"]
        keys = ["delivery_start_utc", "delivery_end_utc", "direction"]
        grouped = data.groupby(keys, sort=True)
        agg = pd.DataFrame(
            {
                "awarded_mw": grouped["awarded_mw"].sum(min_count=1),
                "value": grouped["value"].sum(min_count=1),
                "marginal": grouped["marginal_eur_per_mw_h"].max(),
            }
        ).reset_index()
        agg["vwap"] = np.where(
            agg["awarded_mw"] > 0, agg["value"] / agg["awarded_mw"].where(agg["awarded_mw"] > 0), np.nan
        )
        spans = agg[["delivery_start_utc", "delivery_end_utc"]].drop_duplicates().sort_values("delivery_start_utc")
        overlapping = spans["delivery_start_utc"].duplicated(keep="first")
        dropped = int(overlapping.sum())
        spans = spans.loc[~overlapping]
        blocks = DeliveryBlocks(
            start_utc=pd.DatetimeIndex(spans["delivery_start_utc"]),
            end_utc=pd.DatetimeIndex(spans["delivery_end_utc"]),
            product=product,
        )
        index = pd.MultiIndex.from_arrays([blocks.start_utc, blocks.end_utc])

        def pivot(column: str, direction: str) -> FloatArray:
            part = agg[agg["direction"] == direction].set_index(["delivery_start_utc", "delivery_end_utc"])[column]
            return part.reindex(index).to_numpy(dtype=float, na_value=np.nan)

        return cls(
            blocks=blocks,
            awarded_mw={d: pivot("awarded_mw", d) for d in DIRECTIONS},
            vwap_eur_per_mw_h={d: pivot("vwap", d) for d in DIRECTIONS},
            marginal_eur_per_mw_h={d: pivot("marginal", d) for d in DIRECTIONS},
            meta={"rows": int(len(frame)), "blocks": int(blocks.n), "dropped_overlapping_spans": dropped},
        )

    @classmethod
    def from_public(
        cls,
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
        *,
        product: AfrrProduct | None = None,
    ) -> CapacityAwards:
        """Load ``belgium/afrr_capacity_awards`` through :mod:`euroflex_bess_lab.public_data`."""
        pdata: Any = importlib.import_module("euroflex_bess_lab.public_data")
        frame = pdata.load_capacity_awards("afrr", _utc(start), _utc(end))
        awards = cls.from_frame(frame, product=product)
        awards.meta["source"] = "data/public/belgium/afrr_capacity_awards.parquet"
        return awards


@dataclass(frozen=True)
class AwardModel:
    """Turns offered MW and bid prices into awarded MW and the capacity price actually paid (module docstring)."""

    clearing: ClearingProxy = "vwap"

    def settle(
        self,
        offered_mw: FloatArray,
        awards: CapacityAwards,
        direction: str,
        bid_eur_per_mw_h: FloatArray | None = None,
    ) -> tuple[FloatArray, FloatArray]:
        """``(awarded_mw, paid_eur_per_mw_h)`` per block; zero where nothing is awarded."""
        offered = np.clip(np.nan_to_num(np.asarray(offered_mw, dtype=float)), 0.0, None)
        valid = awards.available(direction)
        if self.clearing == "vwap":
            paid = awards.price(direction, "vwap")
            accepted = valid
        elif self.clearing == "marginal":
            paid = awards.price(direction, "marginal")
            accepted = valid
        elif self.clearing == "pay_as_bid":
            if bid_eur_per_mw_h is None:
                raise ValueError("pay_as_bid clearing needs bid prices")
            bid = np.asarray(bid_eur_per_mw_h, dtype=float)
            marginal = awards.price(direction, "marginal")
            accepted = valid & np.isfinite(bid) & (bid <= np.nan_to_num(marginal, nan=-np.inf))
            paid = bid
        else:  # pragma: no cover - guarded by the Literal type
            raise ValueError(f"unknown clearing proxy {self.clearing!r}")
        awarded = np.where(accepted, offered, 0.0)
        paid_price = np.where(accepted & (awarded > 0.0), np.nan_to_num(paid), 0.0)
        return awarded, paid_price


def forecast_capacity_prices(
    awards: CapacityAwards,
    *,
    proxy: Literal["vwap", "marginal"] = "vwap",
    lookback_days: int = 7,
    min_history: int = 1,
    target_blocks: DeliveryBlocks | None = None,
) -> dict[str, FloatArray]:
    """Causal per-block capacity-price forecast: mean of the same block-of-day over recent known auctions.

    For block ``i`` only blocks whose results were published at or before block ``i``'s gate closure are used
    (i.e. delivery days up to D-1), so the forecast is available when the D-1 bid is due. Pass the full award
    history as ``awards`` and the simulated blocks as ``target_blocks`` to forecast from auctions held before the
    simulated window; the result is then aligned on ``target_blocks`` (NaN where a block is not in ``awards``).
    """
    if target_blocks is not None:
        full = forecast_capacity_prices(awards, proxy=proxy, lookback_days=lookback_days, min_history=min_history)
        position = pd.Index(awards.blocks.start_utc.asi8).get_indexer(target_blocks.start_utc.asi8)
        found = position >= 0
        aligned: dict[str, FloatArray] = {}
        for direction, values in full.items():
            out_values = np.full(target_blocks.n, np.nan)
            out_values[found] = values[position[found]]
            aligned[direction] = out_values
        return aligned
    blocks = awards.blocks
    gate = blocks.gate_closure_utc.asi8
    published = blocks.results_published_utc.asi8
    block_of_day = blocks.block_of_day
    out: dict[str, FloatArray] = {}
    for direction in DIRECTIONS:
        prices = awards.price(direction, proxy)
        forecast = np.full(blocks.n, np.nan)
        for bod in np.unique(block_of_day):
            members = np.flatnonzero(block_of_day == bod)
            known = np.isfinite(prices[members])
            if not known.any():
                continue
            hist_idx = members[known]
            hist_pub = published[hist_idx]
            order = np.argsort(hist_pub, kind="stable")
            hist_idx, hist_pub = hist_idx[order], hist_pub[order]
            cumsum = np.concatenate([[0.0], np.cumsum(prices[hist_idx])])
            n_known = np.searchsorted(hist_pub, gate[members], side="right")
            lo = np.maximum(n_known - lookback_days, 0)
            count = n_known - lo
            ok = count >= max(min_history, 1)
            values = np.where(ok, (cumsum[n_known] - cumsum[lo]) / np.maximum(count, 1), np.nan)
            forecast[members] = values
        out[direction] = forecast
    return out
