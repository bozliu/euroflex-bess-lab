"""Nordic market rules encoded in the research backtest, with the public sources they come from.

Power and energy reservations per MW of capacity sold (``C``), both directions measured at the grid connection:

=================  =====================  =====================  ======================  ======================
Product            power up (discharge)   power down (charge)    energy up               energy down
=================  =====================  =====================  ======================  ======================
FCR-N              1.34 C                 1.34 C                 1 h x C                 1 h x C
FCR-D up           1.00 C                 0.20 C                 1/3 h x C               0
FCR-D down         0.20 C                 1.00 C                 0                       1/3 h x C
FCR (DK1, CE)      1.25 C                 1.25 C                 0.5 h x C               0.5 h x C
aFRR up / down     1.00 C (+1.00 C in DK) (opposite direction)   1 h x C (own direction)
mFRR up / down     1.00 C (+1.00 C in DK) (opposite direction)   1 h x C (own direction)
=================  =====================  =====================  ======================  ======================

* FCR-N / FCR-D (DK2, SE): "Technical Requirements for Frequency Containment Reserve Provision in the Nordic
  Synchronous Area" (Nordic TSOs, version 14.5.2025), section 3.5, Table 10 (LER entities; the columns are summed
  when several products are provided at once). SvK lists the endurance as 1 hour (FCR-N) and at least 20 minutes
  (FCR-D).
* FCR DK1: Energinet, "Prequalification of Units and Aggregated Portfolios" (November 2025), section 10.1.1,
  Table 36 (25 % power and 30 minutes of energy reserved in both directions for NEM).
* aFRR / mFRR: SvK gives an endurance of 1 hour for aFRR; an mFRR capacity award obliges energy bids in all four
  quarter-hours of the awarded hour, so one hour is reserved in the product's direction. Energinet's LER rules for
  aFRR/mFRR capacity require that sold capacity is deliverable in capacity and energy, forbid SOC management via
  imbalances and describe an intraday-based energy management that reserves the sold MW in the opposite
  direction; that opposite-direction reservation is applied in DK1/DK2 (``opposite_power_share = 1``).

All capacity products are pay-as-cleared (marginal price) in the data period used for settlement; see
``GATE_CLOSURES`` for the auction timing and ``RULE_SOURCES`` for the URLs.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

NORDIC_FCR_REQUIREMENTS_URL = (
    "https://www.fingrid.fi/globalassets/dokumentit/fi/sahkomarkkinat/reservit/"
    "technical-requirements-for-frequency-containment-reserve-provision-in-the-nordic-synchronous-area_14.5.2025.pdf"
)
ENERGINET_PREQUALIFICATION_URL = (
    "https://en.energinet.dk/media/fm1pgsez/prequalification-of-units-and-agggegated-portfolios-november-2025.pdf"
)
ENERGINET_PRODUCT_OVERVIEW_URL = "https://en.energinet.dk/media/z4ilcceo/product-overview-dk1-and-dk2.pdf"
SVK_FCR_N_URL = "https://www.svk.se/aktorsportalen/bidra-med-reserver/om-olika-reserver/fcr-n/"
SVK_FCR_D_UP_URL = "https://www.svk.se/aktorsportalen/bidra-med-reserver/om-olika-reserver/fcr-d-upp/"
SVK_AFRR_URL = "https://www.svk.se/aktorsportalen/bidra-med-reserver/om-olika-reserver/afrr/"
SVK_MFRR_URL = "https://www.svk.se/aktorsportalen/bidra-med-reserver/om-olika-reserver/mfrr/"
NORDIC_AFRR_HANDBOOK_URL = (
    "https://nordicbalancingmodel.net/wp-content/uploads/2022/08/Nordic-Handbook-aFRR-Capacity-Market.pdf"
)
ENERGINET_AFRR_MARKET_URL = "https://en.energinet.dk/electricity/balancing-and-ancillary-services/market-overview-and-tender/afrr-capacity-market/"
ENERGINET_IMBALANCE_DESIGN_URL = (
    "https://en.energinet.dk/electricity/balancing-and-ancillary-services/imbalance-price-design/"
)
NORDIC_BALANCING_PHILOSOPHY_URL = (
    "https://www.fingrid.fi/globalassets/dokumentit/fi/tiedotteet/ajankohtaista/nordic-balancing-philosophy-may24.pdf"
)
ENERGINET_C1_URL = "https://energinet.dk/media/d45j2s2m/forskrift-c1.pdf"
FINGRID_FCR_N_DATASET_URL = "https://data.fingrid.fi/en/datasets/317"

RULE_SOURCES: dict[str, str] = {
    "nordic_fcr_technical_requirements": NORDIC_FCR_REQUIREMENTS_URL,
    "energinet_prequalification": ENERGINET_PREQUALIFICATION_URL,
    "energinet_product_overview": ENERGINET_PRODUCT_OVERVIEW_URL,
    "svk_fcr_n": SVK_FCR_N_URL,
    "svk_fcr_d_up": SVK_FCR_D_UP_URL,
    "svk_afrr": SVK_AFRR_URL,
    "svk_mfrr": SVK_MFRR_URL,
    "nordic_afrr_handbook": NORDIC_AFRR_HANDBOOK_URL,
    "energinet_afrr_market": ENERGINET_AFRR_MARKET_URL,
    "energinet_imbalance_design": ENERGINET_IMBALANCE_DESIGN_URL,
    "nordic_balancing_philosophy": NORDIC_BALANCING_PHILOSOPHY_URL,
    "energinet_c1": ENERGINET_C1_URL,
}


@dataclass(frozen=True)
class ReserveProduct:
    """One capacity product as seen by the battery LP (prices in EUR per MW and hour of availability)."""

    key: str
    label: str
    table_product: str  # product name in the Nordic data tables / loader
    direction: str  # "symmetric", "up" or "down" (data direction filter)
    power_up: float  # MW of discharge headroom per MW sold
    power_down: float  # MW of charge headroom per MW sold
    energy_up_hours: float  # hours of full delivery that must be stored above the SOC floor
    energy_down_hours: float  # hours of full absorption that must fit below the SOC ceiling
    block_hours: int = 1  # product length (DK1 FCR trades 4-hour blocks)
    source: str = ""

    def with_opposite_reservation(self, share: float) -> ReserveProduct:
        """aFRR/mFRR with ``share`` x C reserved in the opposite direction (Energinet LER energy management)."""

        if share <= 0:
            return self
        if self.direction == "up":
            return replace(self, power_down=self.power_down + share)
        if self.direction == "down":
            return replace(self, power_up=self.power_up + share)
        return self


FCR_N = ReserveProduct("fcr_n", "FCR-N", "fcr_n", "symmetric", 1.34, 1.34, 1.0, 1.0, source=NORDIC_FCR_REQUIREMENTS_URL)
FCR_D_UP = ReserveProduct(
    "fcr_d_up", "FCR-D up", "fcr_d_up", "up", 1.0, 0.2, 1.0 / 3.0, 0.0, source=NORDIC_FCR_REQUIREMENTS_URL
)
FCR_D_DOWN = ReserveProduct(
    "fcr_d_down", "FCR-D down", "fcr_d_down", "down", 0.2, 1.0, 0.0, 1.0 / 3.0, source=NORDIC_FCR_REQUIREMENTS_URL
)
FCR_DK1 = ReserveProduct(
    "fcr_dk1",
    "FCR (DK1)",
    "fcr",
    "symmetric",
    1.25,
    1.25,
    0.5,
    0.5,
    block_hours=4,
    source=ENERGINET_PREQUALIFICATION_URL,
)
AFRR_UP = ReserveProduct("afrr_up", "aFRR up", "afrr", "up", 1.0, 0.0, 1.0, 0.0, source=SVK_AFRR_URL)
AFRR_DOWN = ReserveProduct("afrr_down", "aFRR down", "afrr", "down", 0.0, 1.0, 0.0, 1.0, source=SVK_AFRR_URL)
MFRR_UP = ReserveProduct("mfrr_up", "mFRR up", "mfrr", "up", 1.0, 0.0, 1.0, 0.0, source=SVK_MFRR_URL)
MFRR_DOWN = ReserveProduct("mfrr_down", "mFRR down", "mfrr", "down", 0.0, 1.0, 0.0, 1.0, source=SVK_MFRR_URL)

PRODUCTS: dict[str, ReserveProduct] = {
    p.key: p for p in (FCR_N, FCR_D_UP, FCR_D_DOWN, FCR_DK1, AFRR_UP, AFRR_DOWN, MFRR_UP, MFRR_DOWN)
}
PRODUCT_ORDER = tuple(PRODUCTS)

# Opposite-direction power reserved for aFRR/mFRR energy management (Energinet LER rules; 0 elsewhere).
OPPOSITE_POWER_SHARE = {"DK1": 1.0, "DK2": 1.0, "SE3": 0.0, "SE4": 0.0, "FI": 0.0}

ZONE_PRODUCTS: dict[str, tuple[str, ...]] = {
    "DK1": ("fcr_dk1", "afrr_up", "afrr_down", "mfrr_up", "mfrr_down"),
    "DK2": ("fcr_n", "fcr_d_up", "fcr_d_down", "afrr_up", "afrr_down", "mfrr_up", "mfrr_down"),
    "SE3": ("fcr_n", "fcr_d_up", "fcr_d_down", "afrr_up", "afrr_down", "mfrr_up", "mfrr_down"),
    "SE4": ("fcr_n", "fcr_d_up", "fcr_d_down", "afrr_up", "afrr_down", "mfrr_up", "mfrr_down"),
    # Finland: Fingrid's FCR and mFRR prices need an API key, so only the Nordic aFRR market is modelled.
    "FI": ("afrr_up", "afrr_down"),
}
ZONE_COUNTRY = {"DK1": "Denmark", "DK2": "Denmark", "SE3": "Sweden", "SE4": "Sweden", "FI": "Finland"}


def zone_products(zone: str, keys: tuple[str, ...] | None = None) -> list[ReserveProduct]:
    """Products available in ``zone`` (optionally restricted to ``keys``) with the zone's opposite reservation."""

    allowed = ZONE_PRODUCTS[zone]
    chosen = allowed if keys is None else tuple(k for k in keys if k in allowed)
    share = OPPOSITE_POWER_SHARE.get(zone, 0.0)
    out = []
    for key in chosen:
        product = PRODUCTS[key]
        if key.startswith(("afrr", "mfrr")):
            product = product.with_opposite_reservation(share)
        out.append(product)
    return out


@dataclass(frozen=True)
class GateClosure:
    market: str
    zones: str
    gate_closure: str
    pricing: str
    source: str


GATE_CLOSURES: tuple[GateClosure, ...] = (
    GateClosure(
        "FCR-N, FCR-D up/down, auction 1 ('D-1 early', former D-2)",
        "SE1-SE4, DK2 (joint market)",
        "D-1 00:30 (CET/CEST)",
        "marginal price per auction (pay-as-cleared since 2024-02-01)",
        SVK_FCR_N_URL,
    ),
    GateClosure(
        "FCR-N, FCR-D up/down, auction 2 ('D-1 late')",
        "SE1-SE4, DK2",
        "D-1 18:00",
        "marginal price (not used: the backtest offers in auction 1, where most volume is procured)",
        SVK_FCR_N_URL,
    ),
    GateClosure(
        "aFRR capacity (Nordic aFRR CM; DK1 local market from 2024-10-02)",
        "DK1, DK2, SE, FI",
        "D-1 07:30 CET",
        "marginal price",
        NORDIC_AFRR_HANDBOOK_URL,
    ),
    GateClosure(
        "mFRR capacity (trilateral DK-SE-FI market from 2024-11-19)",
        "DK1, DK2, SE, FI",
        "D-1 07:30",
        "marginal price",
        SVK_MFRR_URL,
    ),
    GateClosure(
        "FCR (Continental Europe FCR cooperation, 4-hour products)",
        "DK1",
        "D-1 08:00",
        "marginal price",
        ENERGINET_PRODUCT_OVERVIEW_URL,
    ),
    GateClosure(
        "Day-ahead (SDAC; 15-minute products from delivery day 2025-10-01)",
        "all",
        "D-1 12:00 CET",
        "marginal price per bidding zone",
        "https://www.energidataservice.dk/tso-electricity/DayAheadPrices",
    ),
)
