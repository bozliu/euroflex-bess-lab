"""Full committed GB data set (``data/public/gb``): coverage, integrity, budget, and an end-to-end look-ahead check
on a real month. Marked slow: runs only with ``EUROFLEX_RUN_SLOW=1``."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.markets.gb import GBMarket, GBSimulator
from euroflex_bess_lab.markets.gb.strategies import DEFAULT_BATTERY, MAIN_CONFIGS
from euroflex_bess_lab.public_data.gb import GB_TABLES, load_gb_market_index, load_gb_system_prices
from euroflex_bess_lab.public_data.http import sha256_file

REPO_ROOT = Path(__file__).resolve().parents[1]
GB = REPO_ROOT / "data" / "public" / "gb"


@pytest.mark.slow
def test_gb_tables_are_complete_and_match_their_sidecars() -> None:
    total = 0
    for meta_path in sorted(GB.glob("*.meta.json")):
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        path = GB / Path(meta["path"]).name
        assert path.exists(), meta["path"]
        assert sha256_file(path) == meta["sha256"], meta["path"]
        assert meta["licence"] and meta["attribution"] and meta["redistributed"] is True
        total += path.stat().st_size
        assert path.stat().st_size < 50 * 1024 * 1024
    assert total < 60 * 1024 * 1024
    assert {Path(GB_TABLES[k].pattern).name for k in GB_TABLES if "{year}" not in GB_TABLES[k].pattern} <= {
        p.name for p in GB.glob("*.parquet")
    }


@pytest.mark.slow
def test_gb_price_grid_has_no_gaps_and_a_single_cash_out_price() -> None:
    sp = load_gb_system_prices("2024-01-01", "2026-10-25")
    expected = pd.date_range("2024-01-01", "2026-10-25", freq="30min", tz="UTC", inclusive="left")
    assert len(sp) == len(expected) and sp["timestamp_utc"].is_unique
    assert (sp["system_sell_price_gbp_mwh"] == sp["system_buy_price_gbp_mwh"]).all()
    mid = load_gb_market_index("2024-01-01", "2026-10-25", ["mid_price_gbp_mwh"])
    assert mid["mid_price_gbp_mwh"].isna().mean() < 0.001


@pytest.mark.slow
def test_combined_stack_does_not_look_ahead_on_real_data() -> None:
    market = GBMarket.from_public("2025-01-01", "2025-02-01")
    cut = market.index_of("2025-01-20T09:30Z")
    shocked = GBMarket(**{**market.__dict__})
    shocked.mid = market.mid.copy()
    shocked.sip = market.sip.copy()
    shocked.mid[cut:] = np.where(np.arange(market.n - cut) % 2 == 0, 3000.0, -3000.0)
    shocked.sip[cut:] = -shocked.mid[cut:]
    config = MAIN_CONFIGS["combined_stack"]
    base = GBSimulator(market, DEFAULT_BATTERY, config)
    base.run()
    test = GBSimulator(shocked, DEFAULT_BATTERY, config)
    test.run()
    assert np.allclose(
        base.wholesale[: cut + config.wholesale_lead_sps + 1], test.wholesale[: cut + config.wholesale_lead_sps + 1]
    )
    assert np.allclose(
        base.baseline[: cut + config.imbalance_lead_sps + 1], test.baseline[: cut + config.imbalance_lead_sps + 1]
    )
    assert not np.allclose(base.wholesale, test.wholesale)
