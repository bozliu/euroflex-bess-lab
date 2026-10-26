"""Nordic public data: Danish, Swedish and Finnish day-ahead, reserve-capacity and Danish imbalance prices.

Sources (all CC BY 4.0, no account or key): Energinet's Energi Data Service and Svenska kraftnät's SVK Data
Service. Typical use::

    from euroflex_bess_lab.public_data.nordic import load_nordic_day_ahead, load_nordic_reserve

    da = load_nordic_day_ahead("SE3", "2025-06-10", "2025-06-12", quarter_hourly=True)
    fcr_d = load_nordic_reserve("fcr_d_up", "SE3", "2025-06-10", "2025-06-12")

Build or refresh the tables with ``python scripts/research/nordic_fetch.py``; see ``docs/nordics.md``.
"""

from __future__ import annotations

from .build import TABLE_NAMES, NordicContext, build_tables
from .loaders import (
    FCR_AUCTIONS,
    RESERVE_PRODUCTS,
    load_dk_imbalance,
    load_nordic_day_ahead,
    load_nordic_reserve,
    nordic_table_path,
)
from .sources import AFRR_ZONES, DA_ZONES, DK_AREAS, FCR_NORDIC_ZONES, MFRR_ZONES

__all__ = [
    "AFRR_ZONES",
    "DA_ZONES",
    "DK_AREAS",
    "FCR_AUCTIONS",
    "FCR_NORDIC_ZONES",
    "MFRR_ZONES",
    "RESERVE_PRODUCTS",
    "TABLE_NAMES",
    "NordicContext",
    "build_tables",
    "load_dk_imbalance",
    "load_nordic_day_ahead",
    "load_nordic_reserve",
    "nordic_table_path",
]
