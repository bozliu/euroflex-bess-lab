"""Great Britain public data: Elexon BMRS (system prices, MID, frequency), NESO EAC auction results, ECB FX.

Typical use::

    from euroflex_bess_lab.public_data.gb import load_gb_market_index, load_gb_system_prices

    prices = load_gb_system_prices("2025-06-10", "2025-06-13")
    mid = load_gb_market_index("2025-06-10", "2025-06-13")

Tables live in ``data/public/gb`` (override the root with ``EUROFLEX_DATA_DIR``); rebuild them with
``.venv/bin/python scripts/research/gb_fetch_data.py --target all``. See ``docs/great_britain.md``.
"""

from __future__ import annotations

from .loaders import (
    gb_table_path,
    load_gb_frequency,
    load_gb_fx,
    load_gb_market_index,
    load_gb_reserve_auctions,
    load_gb_response_auctions,
    load_gb_response_energy,
    load_gb_system_prices,
)
from .tables import FETCH_TARGETS, GB_TABLES

__all__ = [
    "FETCH_TARGETS",
    "GB_TABLES",
    "gb_table_path",
    "load_gb_frequency",
    "load_gb_fx",
    "load_gb_market_index",
    "load_gb_reserve_auctions",
    "load_gb_response_auctions",
    "load_gb_response_energy",
    "load_gb_system_prices",
]
