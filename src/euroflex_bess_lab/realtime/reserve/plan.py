"""Per-block reservation plans: offered and awarded aFRR MW per direction and the capacity price paid."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from ..types import BatteryParams
from .capacity import AwardModel, CapacityAwards, DeliveryBlocks
from .products import AfrrProduct, cap_reservation

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]


@dataclass
class ReservePlan:
    """What the battery sold per delivery block (after the award model) and at which capacity price."""

    policy: str
    blocks: DeliveryBlocks
    offered_up_mw: FloatArray
    offered_down_mw: FloatArray
    up_mw: FloatArray
    down_mw: FloatArray
    price_up_eur_per_mw_h: FloatArray
    price_down_eur_per_mw_h: FloatArray
    bid_up_eur_per_mw_h: FloatArray
    bid_down_eur_per_mw_h: FloatArray
    meta: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        n = self.blocks.n
        for name in (
            "offered_up_mw",
            "offered_down_mw",
            "up_mw",
            "down_mw",
            "price_up_eur_per_mw_h",
            "price_down_eur_per_mw_h",
            "bid_up_eur_per_mw_h",
            "bid_down_eur_per_mw_h",
        ):
            values = np.asarray(getattr(self, name), dtype=float)
            if len(values) != n:
                raise ValueError(f"{name} must have one value per block")
            setattr(self, name, values)

    @classmethod
    def from_offers(
        cls,
        policy: str,
        awards: CapacityAwards,
        battery: BatteryParams,
        *,
        offered_up_mw: FloatArray,
        offered_down_mw: FloatArray,
        award_model: AwardModel | None = None,
        bid_up_eur_per_mw_h: FloatArray | None = None,
        bid_down_eur_per_mw_h: FloatArray | None = None,
        product: AfrrProduct | None = None,
        meta: dict[str, Any] | None = None,
    ) -> ReservePlan:
        """Cap offers to power/energy feasibility, then apply the award model per direction."""
        product = product or awards.blocks.product
        award_model = award_model or AwardModel()
        n = awards.blocks.n
        up_req = np.broadcast_to(np.asarray(offered_up_mw, dtype=float), (n,)).copy()
        down_req = np.broadcast_to(np.asarray(offered_down_mw, dtype=float), (n,)).copy()
        for i in range(n):
            up_req[i], down_req[i] = cap_reservation(battery, up_req[i], down_req[i], product.sustain_hours)
        bid_up = np.full(n, np.nan) if bid_up_eur_per_mw_h is None else np.asarray(bid_up_eur_per_mw_h, dtype=float)
        bid_down = (
            np.full(n, np.nan) if bid_down_eur_per_mw_h is None else np.asarray(bid_down_eur_per_mw_h, dtype=float)
        )
        up, price_up = award_model.settle(up_req, awards, "up", None if bid_up_eur_per_mw_h is None else bid_up)
        down, price_down = award_model.settle(
            down_req, awards, "down", None if bid_down_eur_per_mw_h is None else bid_down
        )
        return cls(
            policy=policy,
            blocks=awards.blocks,
            offered_up_mw=up_req,
            offered_down_mw=down_req,
            up_mw=up,
            down_mw=down,
            price_up_eur_per_mw_h=price_up,
            price_down_eur_per_mw_h=price_down,
            bid_up_eur_per_mw_h=bid_up,
            bid_down_eur_per_mw_h=bid_down,
            meta={"clearing": award_model.clearing, **(meta or {})},
        )

    @classmethod
    def empty(cls, blocks: DeliveryBlocks, policy: str = "none") -> ReservePlan:
        zeros = np.zeros(blocks.n)
        nans = np.full(blocks.n, np.nan)
        return cls(policy, blocks, zeros, zeros, zeros, zeros, zeros, zeros, nans, nans)

    def qh_arrays(self, qh_times: pd.DatetimeIndex) -> dict[str, NDArray[Any]]:
        """Reservation per quarter-hour (zero outside blocks)."""
        index = self.blocks.index_of(qh_times)
        inside = index >= 0
        safe = np.clip(index, 0, None)

        def take(values: FloatArray) -> FloatArray:
            return np.where(inside, values[safe], 0.0)

        return {
            "block": index,
            "up_mw": take(self.up_mw),
            "down_mw": take(self.down_mw),
            "price_up_eur_per_mw_h": take(self.price_up_eur_per_mw_h),
            "price_down_eur_per_mw_h": take(self.price_down_eur_per_mw_h),
        }

    def to_frame(self) -> pd.DataFrame:
        frame = self.blocks.to_frame()
        frame["policy"] = self.policy
        frame["offered_up_mw"] = self.offered_up_mw
        frame["offered_down_mw"] = self.offered_down_mw
        frame["up_mw"] = self.up_mw
        frame["down_mw"] = self.down_mw
        frame["price_up_eur_per_mw_h"] = self.price_up_eur_per_mw_h
        frame["price_down_eur_per_mw_h"] = self.price_down_eur_per_mw_h
        frame["bid_up_eur_per_mw_h"] = self.bid_up_eur_per_mw_h
        frame["bid_down_eur_per_mw_h"] = self.bid_down_eur_per_mw_h
        return frame
