"""Reproducible public-data layer: Elia Open Data, Energy-Charts day-ahead prices, regelleistung.net results.

Typical use::

    from euroflex_bess_lab.public_data import load_belgium_minute, load_belgium_qh

    minute = load_belgium_minute("2025-06-10", "2025-06-13")
    qh = load_belgium_qh("2025-06-10", "2025-06-13")

The data tree lives in ``data/public`` (override with ``EUROFLEX_DATA_DIR``); rebuild it with
``python scripts/data/fetch_all_public.py`` or ``euroflex data fetch all``. See ``docs/public_datasets.md``.
"""

from __future__ import annotations

from .loaders import (
    load_belgium_legacy,
    load_belgium_minute,
    load_belgium_new_formula_minute,
    load_belgium_new_formula_qh,
    load_belgium_qh,
    load_belgium_si_forecast,
    load_capacity_awards,
    load_day_ahead_prices,
    load_energy_bid_curves,
    load_germany_reserve,
    load_res_load,
    read_manifest,
    table_path,
)
from .paths import data_root, raw_root
from .registry import TABLES
from .validate import last_minute_vs_qh

__all__ = [
    "TABLES",
    "data_root",
    "last_minute_vs_qh",
    "load_belgium_legacy",
    "load_belgium_minute",
    "load_belgium_new_formula_minute",
    "load_belgium_new_formula_qh",
    "load_belgium_qh",
    "load_belgium_si_forecast",
    "load_capacity_awards",
    "load_day_ahead_prices",
    "load_energy_bid_curves",
    "load_germany_reserve",
    "load_res_load",
    "raw_root",
    "read_manifest",
    "table_path",
]
