"""Upstream sources, licences and attribution statements for the Great Britain (GB) public data tables.

All three sources permit redistribution with attribution, so the normalized tables are committed under
``data/public/gb``:

* Elexon BMRS / Insights Solution API (no key needed): system prices (DISEBSP), Market Index Data (MID) and
  15-second system frequency (FREQ). Licence: "Licence to use BMRS open data" (worldwide, royalty-free, perpetual,
  non-exclusive; copy, publish, distribute, adapt and exploit commercially) with the mandatory attribution
  statement below.
* NESO data portal (CKAN API): Enduring Auction Capability (EAC) day-ahead auction results for the dynamic
  response products (DC/DM/DR) and the reserve products (QR/SR/BR). Licence: NESO Open Data Licence v1.0 (based
  on the Open Government Licence v3.0; copy, publish, distribute, adapt, exploit commercially) with the mandatory
  attribution statement below.
* European Central Bank euro foreign exchange reference rates (GBP per EUR), used only to express GBP results in
  EUR and to convert the repository-wide 8 EUR/MWh degradation cost into GBP. Reuse is free provided the ECB is
  cited as the source and modifications are stated.
"""

from __future__ import annotations

ELEXON_API_BASE = "https://data.elexon.co.uk/bmrs/api/v1"
ELEXON_SYSTEM_PRICES_URL = ELEXON_API_BASE + "/balancing/settlement/system-prices/{settlement_date}"
ELEXON_MID_STREAM_URL = ELEXON_API_BASE + "/datasets/MID/stream"
ELEXON_FREQUENCY_STREAM_URL = ELEXON_API_BASE + "/system/frequency/stream"
ELEXON_DATASET_PAGES = {
    "DISEBSP": "https://bmrs.elexon.co.uk/system-prices",
    "MID": "https://bmrs.elexon.co.uk/market-index-prices",
    "FREQ": "https://bmrs.elexon.co.uk/system-frequency",
}
BMRS_LICENCE = "Elexon BMRS open data licence"
BMRS_LICENCE_URL = "https://www.elexon.co.uk/bsc/data/balancing-mechanism-reporting-agent/copyright-licence-bmrs-data/"
BMRS_ATTRIBUTION_TEMPLATE = "Contains BMRS data © Elexon Limited copyright and database right {year}"
BMRS_LICENCE_TERMS = (
    "Worldwide, royalty-free, perpetual, non-exclusive licence: copy, publish, distribute and transmit, adapt and "
    "exploit the BMRS Data (including commercially) provided the attribution statement is included and, where "
    "possible, a link to the licence; no implication of official status or endorsement; do not misrepresent the "
    "data. Excludes third-party rights that Elexon is not authorised to license."
)

NESO_API_BASE = "https://api.neso.energy"
NESO_PACKAGE_SHOW_URL = NESO_API_BASE + "/api/3/action/package_show"
NESO_DATASTORE_DUMP_URL = NESO_API_BASE + "/datastore/dump/{resource_id}"
NESO_EAC_PACKAGE = "eac-auction-results"
NESO_EAC_PAGE = "https://www.neso.energy/data-portal/eac-auction-results"
NESO_LICENCE = "NESO Open Data Licence v1.0"
NESO_LICENCE_URL = "https://www.neso.energy/data-portal/neso-open-licence"
NESO_ATTRIBUTION = "Supported by National Energy SO Open Data"
NESO_LICENCE_TERMS = (
    "Based on the Open Government Licence v3.0: copy, publish, distribute and transmit, adapt and exploit "
    "commercially and non-commercially, provided NESO is acknowledged with the statement 'Supported by National "
    "Energy SO Open Data'."
)

ECB_EXR_URL = "https://data-api.ecb.europa.eu/service/data/EXR/D.GBP.EUR.SP00.A"
ECB_DATASET_PAGE = "https://data.ecb.europa.eu/data/datasets/EXR/EXR.D.GBP.EUR.SP00.A"
ECB_LICENCE = "ECB terms of use (free reuse with source citation)"
ECB_LICENCE_URL = "https://www.ecb.europa.eu/services/disclaimer/html/index.en.html"
ECB_ATTRIBUTION = "Source: European Central Bank (euro foreign exchange reference rates)"
ECB_LICENCE_TERMS = (
    "Free use of information obtained from the ECB website provided it appears accurately and the ECB is cited as "
    "the source; modifications (here: as-of join of daily rates onto delivery dates) must be stated."
)

MID_PROVIDERS = ("APXMIDP", "N2EXMIDP")


def bmrs_attribution(*years: int) -> str:
    """BMRS attribution statement for the copyright years covered by a table (e.g. ``2024-2026``)."""

    if not years:
        raise ValueError("at least one year is required")
    lo, hi = min(years), max(years)
    return BMRS_ATTRIBUTION_TEMPLATE.format(year=str(lo) if lo == hi else f"{lo}-{hi}")
