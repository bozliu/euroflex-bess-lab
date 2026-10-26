"""Upstream sources, licences and dataset identifiers of the Nordic public-data layer.

Every source used here publishes under Creative Commons Attribution 4.0 and is downloaded without an account or
API key:

* Energinet, Energi Data Service (``api.energidataservice.dk``): terms at
  https://www.energidataservice.dk/terms-and-conditions ("Data published on Energi Data Service is subject to the
  conditions of Creative Commons CC-BY 4.0. Users may copy, change and distribute data freely, even for commercial
  purposes"), suggested credit "Source: Energinet (www.energidataservice.dk)".
* Svenska kraftnät, SVK Data Service (``data.svk.se``, CKAN datastore): every dataset used is tagged
  ``license_id = CC-BY-4.0`` in the CKAN metadata, attribution "Produced by Svk business units".

Not used (checked 2026-10-26): Fingrid open data (``data.fingrid.fi``, CC BY 4.0) needs an API key or a signed-in
session for every download, so Finnish FCR and mFRR prices are not part of the committed data; Energy-Charts
day-ahead prices for SE3 and FI carry a "private and internal use only" notice and are not used either.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

ENERGINET_API = "https://api.energidataservice.dk/dataset"
ENERGINET_META_API = "https://api.energidataservice.dk/meta/dataset"
ENERGINET_PAGE = "https://www.energidataservice.dk/tso-electricity/{name}"
ENERGINET_LICENCE = "CC BY 4.0"
ENERGINET_LICENCE_URL = "https://creativecommons.org/licenses/by/4.0/"
ENERGINET_TERMS_URL = "https://www.energidataservice.dk/terms-and-conditions"
ENERGINET_ATTRIBUTION = "Source: Energinet (www.energidataservice.dk)"
ENERGINET_SOURCE = "Energinet, Energi Data Service (api.energidataservice.dk)"

SVK_BASE = "https://data.svk.se"
SVK_PACKAGE_API = f"{SVK_BASE}/api/3/action/package_show"
SVK_DUMP = f"{SVK_BASE}/datastore/dump/{{resource_id}}"
SVK_PAGE = f"{SVK_BASE}/dataset/{{package}}"
SVK_LICENCE = "CC BY 4.0"
SVK_LICENCE_URL = "https://creativecommons.org/licenses/by/4.0/"
SVK_ATTRIBUTION = "Svenska kraftnät (SVK Data Service, data.svk.se), produced by Svk business units"
SVK_SOURCE = "Svenska kraftnät, SVK Data Service (data.svk.se)"

FINGRID_DATASET_PAGE = "https://data.fingrid.fi/en/datasets/{dataset_id}"

# Delivery days of every Nordic market used here (SDAC, FCR, aFRR and mFRR capacity auctions) are CET/CEST days.
MARKET_TZ = "Europe/Copenhagen"
DEFAULT_START_UTC = "2023-12-01T00:00:00Z"  # one month before the 2024 target window (persistence history)
DA_15MIN_GO_LIVE_UTC = pd.Timestamp("2025-09-30T22:00:00Z")  # SDAC 15-minute MTU, delivery day 2025-10-01
FCR_MARGINAL_PRICING_UTC = pd.Timestamp("2024-01-31T23:00:00Z")  # SE/DK2 FCR pay-as-cleared from 2024-02-01
DK1_AFRR_MARKET_UTC = pd.Timestamp("2024-10-01T22:00:00Z")  # new DK1 aFRR capacity market from 2024-10-02
MFRR_EAM_GO_LIVE_UTC = pd.Timestamp("2025-03-04T12:00:00Z")  # Nordic mFRR EAM, new Danish imbalance price design

DA_ZONES = ("DK1", "DK2", "SE3", "SE4", "FI")
ENERGINET_DA_ZONES = ("DK1", "DK2", "SE3", "SE4")
SVK_DA_ZONES = ("FI", "SE3", "SE4")  # FI is the reason; SE3/SE4 are kept only for the cross-check
FCR_NORDIC_ZONES = ("DK2", "SE3", "SE4")  # modelled zones (the dataset also has SE1/SE2)
AFRR_ZONES = ("DK1", "DK2", "FI", "SE3", "SE4")  # modelled zones (the dataset covers all Nordic zones)
MFRR_ZONES = ("DK1", "DK2", "SE3", "SE4")
DK_AREAS = ("DK1", "DK2")


@dataclass(frozen=True)
class EnerginetDataset:
    """One Energi Data Service dataset (``api.energidataservice.dk/dataset/<name>``)."""

    name: str
    time_column: str
    title: str
    columns: tuple[str, ...]
    first_utc: str = DEFAULT_START_UTC
    end_utc: str | None = None  # discontinued datasets: exclusive end of the published history
    price_areas: tuple[str, ...] = ()  # server-side filter on PriceArea (empty: no filter)
    chunk: str = "year"  # "year" (one request per calendar year) or "all" (one request for the whole range)

    @property
    def page_url(self) -> str:
        return ENERGINET_PAGE.format(name=self.name)

    @property
    def api_url(self) -> str:
        return f"{ENERGINET_API}/{self.name}"


ENERGINET_DATASETS: dict[str, EnerginetDataset] = {
    d.name: d
    for d in (
        EnerginetDataset(
            "Elspotprices",
            "HourUTC",
            "Elspot Prices (hourly day-ahead, discontinued after 2025-09-30)",
            ("HourUTC", "PriceArea", "SpotPriceEUR"),
            end_utc="2025-10-01T00:00:00Z",
            price_areas=ENERGINET_DA_ZONES,
            chunk="all",
        ),
        EnerginetDataset(
            "DayAheadPrices",
            "TimeUTC",
            "Day-Ahead Prices (15-minute, from 2025-10-01)",
            ("TimeUTC", "PriceArea", "DayAheadPriceEUR"),
            first_utc="2025-09-30T00:00:00Z",
            price_areas=ENERGINET_DA_ZONES,
        ),
        EnerginetDataset(
            "FcrNdDK2",
            "HourUTC",
            "FCR N and D, Frequency Containment Reserves, DK2 (joint SE-DK2 auctions, all SE zones included)",
            (
                "HourUTC",
                "HourDK",
                "PriceArea",
                "ProductName",
                "AuctionType",
                "PurchasedVolumeLocal",
                "PurchasedVolumeTotal",
                "PriceTotalEUR",
            ),
            price_areas=FCR_NORDIC_ZONES,
        ),
        EnerginetDataset(
            "FcrDK1",
            "HourUTC",
            "FCR, Frequency Containment Reserves, DK1",
            ("HourUTC", "HourDK", "FCRdomestic_MW", "FCRabroad_MW", "FCRcross_EUR", "FCRdk_EUR"),
        ),
        EnerginetDataset(
            "AfrrReservesNordic",
            "TimeUTC",
            "aFRR, Automatic Frequency Restoration Reserves, Capacity Market (all Nordic zones)",
            (
                "TimeUTC",
                "PriceArea",
                "UpDemandMW",
                "UpProcuredMW",
                "UpPriceEUR",
                "DownDemandMW",
                "DownProcuredMW",
                "DownPriceEUR",
            ),
            price_areas=AFRR_ZONES,
        ),
        EnerginetDataset(
            "mFRRCapacityMarket",
            "TimeUTC",
            "mFRR, manual Frequency Restoration Reserves, Capacity Market (daily auctions)",
            (
                "TimeUTC",
                "PriceArea",
                "UpDemandMW",
                "UpProcuredMW",
                "UpPriceEUR",
                "DownDemandMW",
                "DownProcuredMW",
                "DownPriceEUR",
            ),
        ),
        EnerginetDataset(
            "ImbalancePrice",
            "TimeUTC",
            "Imbalance Price (15-minute, from 2025-03-04)",
            (
                "TimeUTC",
                "PriceArea",
                "SatisfiedDemand",
                "ImbalancePriceEUR",
                "SpotPriceEUR",
                "DominatingDirection",
                "aFRRUpMW",
                "aFRRVWAUpEUR",
                "aFRRDownMW",
                "aFRRVWADownEUR",
                "mFRRMarginalPriceUpEUR",
                "mFRRMarginalPriceDownEUR",
            ),
            first_utc="2025-03-01T00:00:00Z",
        ),
        EnerginetDataset(
            "RegulatingBalancePowerdata",
            "HourUTC",
            "Regulating and Balance Power, Overall Data (hourly, discontinued after 2025-03-04)",
            (
                "HourUTC",
                "PriceArea",
                "mFRRUpActBal",
                "mFRRDownActBal",
                "ImbalanceMWh",
                "ImbalancePriceEUR",
                "BalancingPowerPriceUpEUR",
                "BalancingPowerPriceDownEUR",
            ),
            end_utc="2025-03-05T00:00:00Z",
            price_areas=DK_AREAS,
            chunk="all",
        ),
    )
}


@dataclass(frozen=True)
class SvkResource:
    """One SVK Data Service datastore resource (CKAN), fetched as a per-zone CSV dump."""

    key: str
    package: str
    resource_id: str  # known id; re-resolved through package_show when online
    title: str
    fields: tuple[str, ...]

    @property
    def page_url(self) -> str:
        return SVK_PAGE.format(package=self.package)


SVK_RESOURCES: dict[str, SvkResource] = {
    r.key: r
    for r in (
        SvkResource(
            "day_ahead",
            "market_data_day_ahead_area_prices",
            "cf392ef3-7345-4e98-970e-83088c185570",
            "Market data day-ahead prices (SE1-SE4 and neighbours, incl. FI)",
            ("start_time_utc", "bidding_zone", "price", "price_unit"),
        ),
        SvkResource(
            "mfrr_capacity",
            "mfrr_capacity_market",
            "0c56e30d-8fce-4c27-afc8-621c230ae34d",
            "mFRR Capacity Market (SE1-SE4 marginal prices and contracted volumes)",
            (
                "start_time_utc",
                "bidding_zone",
                "reserve_product",
                "reserve_direction",
                "price",
                "price_unit",
                "volume",
                "volume_unit",
            ),
        ),
    )
}

FETCH_SCRIPT = "python scripts/research/nordic_fetch.py"


def fetch_hint(table: str) -> str:
    """Command that (re)builds a Nordic table; used in ``FileNotFoundError`` messages."""

    return f"{FETCH_SCRIPT} --tables {table}"
