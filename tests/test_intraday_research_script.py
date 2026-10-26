"""scripts/research/intraday_backtest.py end to end on a SYNTHETIC market (no network, no ID AEP data)."""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest

from euroflex_bess_lab.markets.intraday import IntradayConfig, synthetic_market

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "research" / "intraday_backtest.py"
LOCAL_PREFIX = "/" + "Users" + "/"


def _load_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("intraday_backtest_script", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def script() -> ModuleType:
    return _load_script()


def test_periods_follow_the_germany_report_convention(script: ModuleType) -> None:
    periods = {p.key: p for p in script.define_periods(date(2024, 3, 1), date(2026, 10, 25))}
    assert (periods["latest_12m"].first, periods["latest_12m"].last) == (date(2025, 10, 1), date(2026, 9, 30))
    assert (periods["quarter_hour_era"].first, periods["quarter_hour_era"].last) == (
        date(2025, 10, 1),
        date(2026, 10, 25),
    )
    assert (periods["previous_12m"].first, periods["previous_12m"].last) == (date(2024, 10, 1), date(2025, 9, 30))
    assert periods["full"].first == date(2024, 3, 1)


def test_full_pipeline_on_synthetic_data(tmp_path: Path, script: ModuleType) -> None:
    market = synthetic_market("2025-07-25", 73, hourly_until="2025-10-01", seed=21)
    start, end = date(2025, 9, 28), date(2025, 10, 5)
    out = script.run_all(
        market,
        start=start,
        end=end,
        config=IntradayConfig(),
        workers=1,
        report_path=tmp_path / "reports" / "intraday.md",
        figures_dir=tmp_path / "reports" / "figures",
        ledgers_dir=tmp_path / "reports" / "ledgers",
        audit_days=2,
    )
    report = out["report"]
    for marker in ("## Key findings", "VWAP-execution proxy", "not redistributed", "## Causality audit", "## Scope"):
        assert marker in report
    assert sorted(p.name for p in out["figures"]) == [
        "intraday_cost_sensitivity.png",
        "intraday_cumulative_net_revenue.png",
        "intraday_monthly_uplift.png",
        "intraday_spread_profile.png",
    ]
    ledger = pd.read_parquet(tmp_path / "reports" / "ledgers" / "intraday_daily.parquet")
    assert not [c for c in ledger.columns if "price" in c], "committed ledgers must not carry intraday prices"
    assert {"timestamp_utc", "strategy", "pnl_eur", "revenue_eur", "degradation_eur"} <= set(ledger.columns)
    assert ledger["day"].nunique() == 8 and ledger["soc_violations"].sum() == 0
    audit = json.loads((tmp_path / "reports" / "ledgers" / "intraday_audit.json").read_text(encoding="utf-8"))
    tradable = [r for r in audit["causality_audit"] if r["tradable"]]
    assert tradable and all(r["passed"] for r in tradable)
    sensitivity = pd.read_parquet(tmp_path / "reports" / "ledgers" / "intraday_sensitivity.parquet")
    assert set(sensitivity["group"]) == {"slippage", "id_fee", "gate_closure", "visibility_lag"}
    for path in (tmp_path / "reports").rglob("*"):
        if path.suffix in {".md", ".json"}:
            text = path.read_text(encoding="utf-8")
            assert LOCAL_PREFIX not in text and str(tmp_path) not in text, path.name
