"""Full-data checks on data/public/nordic and the committed report ledgers (run with EUROFLEX_RUN_SLOW=1)."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from euroflex_bess_lab.markets.nordic.backtest import ZoneMarket, annualise, local_day_bounds, run_zone
from euroflex_bess_lab.public_data.http import sha256_file

REPO_ROOT = Path(__file__).resolve().parents[1]
NORDIC = REPO_ROOT / "data" / "public" / "nordic"
LEDGERS = REPO_ROOT / "reports" / "ledgers"

pytestmark = pytest.mark.slow


def _meta(name: str) -> dict:
    return json.loads((NORDIC / f"{name}.meta.json").read_text(encoding="utf-8"))


def test_committed_nordic_tables_match_their_sidecars_and_budget() -> None:
    sidecars = sorted(NORDIC.glob("*.meta.json"))
    assert len(sidecars) == 10
    total = 0
    for path in sidecars:
        meta = json.loads(path.read_text(encoding="utf-8"))
        file = REPO_ROOT / "data" / "public" / meta["path"]
        assert meta["sha256"] == sha256_file(file), meta["path"]
        assert meta["licence"] == "CC BY 4.0" and meta["redistributed"] is True
        assert meta["size_bytes"] <= 50 * 1024 * 1024
        total += meta["size_bytes"]
    assert total < 60 * 1024 * 1024


def test_validation_facts_recorded_in_sidecars() -> None:
    for zone in ("DK1", "DK2", "SE3", "SE4"):
        v = _meta(f"da_prices_{zone}")["validation"]
        assert v["grid_60min_until_2025_09_30"]["missing_intervals"] == 0
        assert v["grid_15min_from_2025_10_01"]["missing_intervals"] == 0
    assert _meta("da_prices_SE3")["validation"]["cross_check_svk_data_service"]["mismatch_delivery_days"] == [
        "2024-10-15"
    ]
    assert _meta("fcr_nordic")["validation"]["zone_price_identical_share"] == 1.0
    assert _meta("fcr_dk1")["validation"]["constant_within_4h_blocks_share"] == 1.0
    rule = _meta("dk_imbalance")["validation"]["price_rule_check_from_2025_03_20"]
    assert rule["share_matching_rule_idle_afrr_as_missing"] > 0.999


def test_headline_numbers_are_reproduced_from_committed_data() -> None:
    summary = json.loads((LEDGERS / "nordic_summary.json").read_text(encoding="utf-8"))
    first, last = summary["periods"]["SE3"]["latest12"]
    start, end = local_day_bounds(pd.Timestamp(first))[0], local_day_bounds(pd.Timestamp(last))[1]
    market = ZoneMarket.from_public("SE3", start, end)
    result = run_zone(market, start, end, workflows=["full_stack"])
    for mode in ("persistence", "oracle"):
        net = annualise(result.daily.loc[result.daily["mode"] == mode])["net_eur"]
        assert net == pytest.approx(summary["latest12"]["SE3"]["full_stack"][mode]["net_eur"], rel=1e-6)


def test_committed_ledgers_respect_size_limit_and_contract() -> None:
    for path in sorted(LEDGERS.glob("nordic_*.parquet")):
        assert path.stat().st_size <= 5 * 1024 * 1024, path.name
    daily = pd.read_parquet(LEDGERS / "nordic_daily.parquet")
    pivot = daily.pivot_table(index=["zone", "day", "workflow"], columns="mode", values="net_eur")
    assert (pivot["oracle"] >= pivot["persistence"] - 1e-6).all()
    ledger = pd.read_parquet(LEDGERS / "nordic_qh_DK2_full_stack.parquet")
    assert ((ledger["revenue_eur"] - ledger["degradation_eur"] - ledger["pnl_eur"]).abs() < 1e-9).all()
