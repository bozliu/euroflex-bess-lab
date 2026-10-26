"""scripts/research/gb_backtest.py helpers on the fixture days: summaries, ledgers, figures and the report text.

Nothing is written outside ``tmp_path`` and no network access is needed."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest

from euroflex_bess_lab.markets.gb import GBMarket, run_strategy
from euroflex_bess_lab.public_data.gb import load_gb_response_auctions

REPO_ROOT = Path(__file__).resolve().parents[1]
PUBLIC = REPO_ROOT / "tests" / "fixtures" / "gb" / "public"
LOCAL_PREFIX = "/" + "Users" + "/"


@pytest.fixture(scope="module")
def script() -> ModuleType:
    path = REPO_ROOT / "scripts" / "research" / "gb_backtest.py"
    spec = importlib.util.spec_from_file_location("gb_backtest_under_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def market() -> GBMarket:
    return GBMarket.from_public("2025-06-03", "2025-06-13", root=PUBLIC)


@pytest.fixture(scope="module")
def ledgers(script: ModuleType, market: GBMarket) -> dict[str, pd.DataFrame]:
    names = [*script.MAIN, *script.ORACLES, *script.SENSITIVITIES]
    return {name: run_strategy(name, market).ledger for name in names}


def test_period_bounds_and_summary(script: ModuleType, ledgers: dict[str, pd.DataFrame]) -> None:
    end = pd.Timestamp("2026-10-26", tz="UTC")
    periods = script.period_bounds(end)
    labels = [p[0] for p in periods]
    assert labels == ["2024", "2025", "2026 to 25 Oct", "Latest 12 months"]
    assert periods[-1][1] == pd.Timestamp("2025-10-26", tz="UTC")
    lo, hi = pd.Timestamp("2025-06-04", tz="UTC"), pd.Timestamp("2025-06-13", tz="UTC")
    s = script.summarize(ledgers["combined_stack"], lo, hi)
    assert s["days"] == pytest.approx(9.0)
    parts = s["wholesale"] + s["imbalance"] + s["response"] + s["degradation"]
    assert s["net_gbp"] == pytest.approx(parts)
    assert s["response"] == pytest.approx(s["DC"] + s["DM"] + s["DR"])
    assert 0 <= s["soe_ok_low"] <= 1 and s["cycles_per_day"] > 0


def test_ledgers_and_figures_are_small_and_portable(
    script: ModuleType, ledgers: dict[str, pd.DataFrame], market: GBMarket, tmp_path: Path
) -> None:
    path, size = script.write_ledger("combined_stack", ledgers["combined_stack"], tmp_path, {"period": "fixture"})
    assert size < 5_000_000
    frame = pd.read_parquet(path)
    for column in ("timestamp_utc", "pnl_eur", "pnl_gbp", "strategy", "market", "fleet_mw", "energy_mwh"):
        assert column in frame.columns
    sidecar = json.loads((tmp_path / "gb_combined_stack.json").read_text(encoding="utf-8"))
    assert sidecar["strategy"] == "combined_stack" and sidecar["rows"] == len(frame)
    daily_path, daily_size = script.write_daily_ledger(
        {"combined_stack": ledgers["combined_stack"], "fr_oracle": ledgers["fr_oracle"]}, tmp_path, {}
    )
    daily = pd.read_parquet(daily_path)
    assert set(daily["strategy"]) == {"combined_stack", "fr_oracle"} and daily_size < 1_000_000
    per_day = daily[daily["strategy"] == "combined_stack"]["pnl_gbp"].sum()
    report_start = pd.Timestamp(script.REPORT_START, tz="UTC")
    led = ledgers["combined_stack"]
    assert per_day == pytest.approx(float(led.loc[led["timestamp_utc"] >= report_start, "pnl_gbp"].sum()), rel=1e-5)
    start = pd.Timestamp("2025-06-03", tz="UTC")
    script.figure_monthly({"combined_stack": script.monthly(ledgers["combined_stack"], start)}, tmp_path / "m.png")
    script.figure_cumulative({"fr_stack": ledgers["fr_stack"]}, start, tmp_path / "c.png")
    lo, hi = start, pd.Timestamp("2025-06-13", tz="UTC")
    stack = script.summarize(ledgers["combined_stack"], lo, hi)
    script.figure_stack({"a": stack, "b": stack}, tmp_path / "s.png", "test")
    auctions = load_gb_response_auctions("2025-06-01", "2025-06-15", root=PUBLIC)
    script.figure_market(auctions, market, start, tmp_path / "k.png")
    assert all((tmp_path / f).stat().st_size > 10_000 for f in ("m.png", "c.png", "s.png", "k.png"))


def test_report_renders_every_section(script: ModuleType, ledgers: dict[str, pd.DataFrame], market: GBMarket) -> None:
    lo, hi = pd.Timestamp("2025-06-04", tz="UTC"), pd.Timestamp("2025-06-13", tz="UTC")
    labels = ["2024", "2025", "Latest 12 months"]
    summary = {name: {label: script.summarize(led, lo, hi) for label in labels} for name, led in ledgers.items()}
    auctions = load_gb_response_auctions("2025-06-01", "2025-06-15", root=PUBLIC)
    context = {label: script.market_context(market, auctions, lo, hi) for label in labels}
    ctx = {
        "end_label": "2025-06-12",
        "periods": labels,
        "summary": summary,
        "full": {name: rows["2025"] for name, rows in summary.items()},
        "market": context,
        "deg_gbp_mean": 6.8,
        "rules": ["rule one"],
        "data_notes": ["note one"],
        "scope": ["scope one"],
    }
    text = script.render(ctx)
    for heading in ("## Key findings", "## Latest 12 months", "## Sensitivities", "## Market context", "## Scope"):
        assert heading in text
    assert "Combined stack" in text and "Modo" in text and "traded-price proxy" in text
    assert LOCAL_PREFIX not in text
