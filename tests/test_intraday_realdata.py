"""Real-data checks of the intraday study (slow; need the local ID AEP table, which is not redistributed)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.markets.intraday import (
    IntradayConfig,
    IntradayMarket,
    audit_causality,
    run_parallel,
    summarize_daily,
)

pytestmark = pytest.mark.slow
LEDGER = Path(__file__).resolve().parents[1] / "reports" / "ledgers" / "intraday_daily.parquet"


@pytest.fixture(scope="module")
def market() -> IntradayMarket:
    from euroflex_bess_lab.public_data.intraday import intraday_index_path

    if not intraday_index_path().exists():
        pytest.skip("local ID AEP table missing: python -m euroflex_bess_lab.public_data.intraday fetch")
    return IntradayMarket.from_public("2026-03-01", "2026-03-31", history_days=62)


def test_march_2026_reproduces_the_committed_ledger(market: IntradayMarket) -> None:
    days = [d for d in market.days if d >= date(2026, 3, 1)]
    strategies = ("da_only", "da_id_reopt_da", "da_id_rolling", "oracle_da")
    daily = run_parallel(market, IntradayConfig(), days, strategies=strategies, workers=2)
    assert (daily["soc_violations"] == 0).all() and len(days) == 31
    summary = summarize_daily(daily, IntradayConfig())
    assert summary.loc["da_id_rolling", "pnl_eur_per_mw_year"] > summary.loc["da_only", "pnl_eur_per_mw_year"]
    assert summary.loc["oracle_da", "pnl_eur_per_mw_year"] >= summary.loc["da_only", "pnl_eur_per_mw_year"]
    if LEDGER.exists():
        committed = pd.read_parquet(LEDGER)
        committed = committed[
            committed["strategy"].isin(strategies) & committed["day"].between("2026-03-01", "2026-03-31")
        ]
        rerun = daily.assign(day=pd.to_datetime(daily["day"]).dt.strftime("%Y-%m-%d"))
        merged = committed.merge(rerun, on=["day", "strategy"], suffixes=("_committed", "_rerun"))
        assert len(merged) == 31 * len(strategies)
        assert np.allclose(merged["pnl_eur_committed"], merged["pnl_eur_rerun"], atol=1e-6)


@pytest.mark.parametrize("cut", [10, 50])
def test_rolling_strategy_is_causal_on_real_days(market: IntradayMarket, cut: int) -> None:
    record = audit_causality(market, date(2026, 3, 18), "da_id_rolling", IntradayConfig(), cut=cut, seed=cut)
    assert record["passed"] and record["max_abs_change_protected_mw"] == 0.0
