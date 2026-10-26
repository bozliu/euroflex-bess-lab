"""Great Britain market rules encoded by the GB backtest (with the public sources they come from).

Settlement and imbalance (Elexon, BSC):

* 30-minute settlement periods (SPs); settlement day = Europe/London calendar day (46/50 SPs on clock changes).
* Single imbalance ("cash-out") price since BSC Modification P305 (5 Nov 2015): the system sell price equals the
  system buy price, so a party that is long receives and a party that is short pays the same price. Verified on
  every SP of the 2024-2026 data (``gb/system_prices.meta.json`` ``validation.ssp_ne_sbp_rows == 0``).
* Net imbalance volume (NIV) > 0 means the system is short.
* BMRS publishes the indicative system price shortly after each SP: on the SPs still carrying their first
  publication in our snapshot, ``createdDateTime`` is a median 22 minutes after the SP end (p95 23 minutes); most
  SPs are re-issued about 24 hours later. We use a conservative 30-minute publication lag: the price of SP k is
  usable from the end of SP k + 30 minutes.
* Gate closure is 60 minutes before the start of each SP (Physical Notifications / operational baselines are fixed
  then; NESO Dynamic Response Services Provider Guidance). A battery chasing NIV through its baseline therefore
  fixes its position for SP j at ``T_j - 60 min``.

Wholesale (Market Index Data, BSC Market Index Definition Statement v9, effective 18 April 2019):

* The Market Index Price is the volume-weighted price of half-hour, 1-, 2- and 4-hour products traded within the
  eight hours before gate closure (day-ahead auction excluded), i.e. trades in ``[T_j - 9 h, T_j - 1 h]``. The
  backtest uses it as the **traded-price proxy** for within-day wholesale trading and fixes the wholesale position
  for SP j at the opening of that window, ``T_j - 9 h``. MID values of SP k are treated as usable from the end of
  SP k (BMRS shows them earlier).

Dynamic response (NESO Response Services Service Terms v6 and Procurement Rules v5; Provider Guidance June 2026):

* Products DC, DM, DR, each procured separately for low frequency (L: export) and high frequency (H: import) in a
  day-ahead pay-as-clear EAC auction per 4-hour EFA block; gate closure 14:00 UK on the day before the EFA day,
  results at 15:00 UK, delivery from 23:00 UK. Payments are for availability only: clearing price (GBP/MW/h,
  may be negative) x contracted MW x 0.5 h per SP, withheld for SPs in which the unit is unavailable.
* Delivery duration (Response Energy Volume = contracted MW x duration): DC 15 min, DM 30 min, DR 60 min. An energy
  limited unit must hold that energy (export side) / headroom (import side) from the start of every contracted
  period; after a delivery the requirement is relaxed by the delivered energy and must be recovered at 20 % of
  the volume per SP, the first increase applying in the fifth SP after the delivery (Service Terms 6.11 and its
  worked example).
* Reserved Capacity: an energy-limited unit must keep at least 10 % (DC), 20 % (DM) or 40 % (DR) of the offered
  MW unoffered, and offered plus reserved capacity of all stacked products (both directions) must not exceed the
  unit's registered capacity (Procurement Rules definition and 8.3.3.2). We read the registered capacity of a
  battery as export plus import capability, which matches observed contracts (see :class:`StackingRules`); the
  strategies additionally keep offered plus reserved MW within each direction's rating (self-imposed limit).
* A MW cannot serve two services at once: per direction, contracted response MW plus the baseline (wholesale or
  imbalance) power in that direction must fit in the battery's power rating; the response energy volumes must fit
  in the SOC window together with the planned baseline trajectory.
* Delivered response energy is not paid and, through ABSVD, does not create imbalance for the provider (Provider
  Guidance, Settlements). It still moves the battery's state of charge, so its value is lost (low products) or
  gained (high products) through the trades that restore the SOC.

Units: GB prices are GBP. The repository-wide degradation cost (8 EUR per MWh discharged) is converted to GBP with
the ECB reference rate of each delivery date; EUR results are GBP results converted the same way.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

import numpy as np
from numpy.typing import NDArray

SP_HOURS = 0.5
SPS_PER_DAY = 48
GATE_CLOSURE_MINUTES = 60
MID_WINDOW_OPEN_MINUTES = 9 * 60  # MID trades: within 8 h before gate closure -> window opens 9 h before SP start
SYSTEM_PRICE_PUBLICATION_LAG_MINUTES = 30  # after SP end (observed first publication: median 22, p95 23 minutes)
MID_PUBLICATION_LAG_MINUTES = 0  # after SP end (conservative)
EAC_GATE_CLOSURE_HOUR_LOCAL = 14
EAC_RESULTS_HOUR_LOCAL = 15

RESPONSE_PRODUCTS: tuple[str, ...] = ("DCL", "DCH", "DML", "DMH", "DRL", "DRH")
LOW_PRODUCTS: tuple[str, ...] = ("DCL", "DML", "DRL")
HIGH_PRODUCTS: tuple[str, ...] = ("DCH", "DMH", "DRH")
DELIVERY_DURATION_HOURS: dict[str, float] = {
    "DCL": 0.25,
    "DCH": 0.25,
    "DML": 0.5,
    "DMH": 0.5,
    "DRL": 1.0,
    "DRH": 1.0,
}
RESERVED_CAPACITY_SHARE: dict[str, float] = {
    "DCL": 0.10,
    "DCH": 0.10,
    "DML": 0.20,
    "DMH": 0.20,
    "DRL": 0.40,
    "DRH": 0.40,
}
ENERGY_RECOVERY_SHARE_PER_SP = 0.20
ENERGY_RECOVERY_DELAY_SPS = 3

SOURCES: dict[str, str] = {
    "bsc_p305_single_price": "https://www.elexon.co.uk/bsc/mod-proposal/p305/",
    "bmrs_system_prices": "https://bmrs.elexon.co.uk/system-prices",
    "market_index_definition_statement": (
        "https://bscdocs.elexon.co.uk/bsc-module/category-3-documents/market-index-definition-statement"
    ),
    "mids_review_2024": "https://www.elexon.co.uk/bsc/article/market-index-definition-statement-review-2024/",
    "neso_dynamic_services": (
        "https://www.neso.energy/industry-information/balancing-services/frequency-response-services/"
        "dynamic-services-dcdmdr"
    ),
    "neso_provider_guidance": "https://www.neso.energy/document/276606/download",
    "neso_service_terms_v6": "https://www.neso.energy/document/384606/download",
    "neso_procurement_rules_v5": "https://www.neso.energy/document/378246/download",
    "eac_auction_timing": (
        "https://sandbox.eac.neso.production.n-side.com/docs/market-participant/api/auction-sessions/"
        "auction-session-lifecycle"
    ),
    "eac_results": "https://www.neso.energy/data-portal/eac-auction-results",
}


def is_low(product: str) -> bool:
    if product not in RESPONSE_PRODUCTS:
        raise KeyError(f"unknown response product {product!r}")
    return product in LOW_PRODUCTS


ReserveMode = Literal["import_plus_export", "per_direction", "export_only"]


@dataclass(frozen=True)
class StackingRules:
    """Feasibility rules for offering response products (Procurement Rules 8.3.3.2 and "Reserved Capacity").

    Offered MW per direction never exceed the battery's power in that direction. ``reserve_mode`` says what the
    offered plus reserved capacity (10 / 20 / 40 % of the offered MW for DC / DM / DR) is compared with:

    * ``"import_plus_export"``: both directions together within export + import capability - our reading of the
      market rule. It is consistent with observed battery contracts: in a sample of 506 battery unit-blocks of
      the 2026 EAC results by unit, offered + reserved MW exceeded export + import capability in 1 % of cases but
      the export capability alone in 64 %.
    * ``"per_direction"`` (default, used by the strategies): each direction within its own power rating. This is
      stricter than the market rule and acts as a self-imposed SOC-risk limit; it was chosen on the development
      window (December 2023 - June 2024).
    * ``"export_only"``: both directions together within the export rating (literal and strictest reading).
    """

    reserved_capacity: bool = True
    reserve_mode: ReserveMode = "per_direction"
    recovery_headroom: bool = False
    apply_efficiency_to_energy_volume: bool = True

    def procurement_rows(
        self,
        products: Sequence[str],
        export_mw: float,
        import_mw: float,
        *,
        charge_efficiency: float = 0.95,
        discharge_efficiency: float = 0.95,
    ) -> list[tuple[NDArray[np.float64], float]]:
        """Linear constraints ``coef @ offered_mw <= rhs`` for one EFA block.

        With ``recovery_headroom`` the battery must also be able to restore 20 % of the Response Energy Volume of
        one direction per SP (Service Terms 6.11 energy recovery) through its baseline in the opposite direction
        while still holding that direction's response: ``H + 0.4 h^-1 x REV_low / (eta_c eta_d) <= P_import`` and
        ``L + 0.4 h^-1 x REV_high x eta_c eta_d <= P_export`` (0.2 per SP of 0.5 h)."""

        low = np.array([p in LOW_PRODUCTS for p in products], dtype=float)
        high = 1.0 - low
        reserve = np.array([RESERVED_CAPACITY_SHARE[p] if self.reserved_capacity else 0.0 for p in products])
        coef = 1.0 + reserve
        rows: list[tuple[NDArray[np.float64], float]] = [(low, export_mw), (high, import_mw)]
        if self.recovery_headroom:
            duration = np.array([DELIVERY_DURATION_HOURS[p] for p in products])
            rate = ENERGY_RECOVERY_SHARE_PER_SP / SP_HOURS
            eta = charge_efficiency * discharge_efficiency
            rows.append((high + low * rate * duration / eta, import_mw))
            rows.append((low + high * rate * duration * eta, export_mw))
        if self.reserve_mode == "import_plus_export":
            rows.append((coef, export_mw + import_mw))
        elif self.reserve_mode == "per_direction":
            rows += [(coef * low, export_mw), (coef * high, import_mw)]
        elif self.reserve_mode == "export_only":
            rows.append((coef, export_mw))
        else:  # pragma: no cover - guarded by the Literal type
            raise ValueError(f"unknown reserve_mode {self.reserve_mode!r}")
        return rows
