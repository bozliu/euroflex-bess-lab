from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from euroflex_bess_lab.valuation import write_ledger
from euroflex_bess_lab.valuation.cli import value_app
from euroflex_bess_lab.valuation.synthetic import synthetic_ledger_set, synthetic_prices

START, END = "2025-10-01", "2026-02-01"
FAST = ["--paths", "200", "--tenor-years", "3"]


@pytest.fixture(scope="module")
def ledgers(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("ledgers")
    for key, (ledger, metadata) in synthetic_ledger_set(START, END).items():
        write_ledger(ledger, root / f"{key}.parquet", metadata)
    prices = synthetic_prices(START, END, seed=7)
    prices.rename(columns={"price_new2026_eur_mwh": "imbalance_price_eur_mwh"}).to_parquet(root / "new_prices.parquet")
    return root


def run(*args: str) -> str:
    result = CliRunner().invoke(value_app, list(args))
    assert result.exit_code == 0, result.output
    return result.output


def no_absolute_paths(path: Path, tmp_path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    assert str(tmp_path) not in text
    assert "/Users/" not in text


def test_toll_command_with_regimes_saturation_and_skill(ledgers: Path, tmp_path: Path) -> None:
    out = tmp_path / "toll"
    output = run(
        "toll",
        "-l",
        f"current={ledgers / 'strategy.parquet'}",
        "-l",
        f"new2026={ledgers / 'mechanism.parquet'}",
        "--weights",
        "current=0.4,new2026=0.6",
        "--capacity-curve",
        "placeholder",
        "--fleet",
        "2027=900,2029=2000",
        "--reference-fleet-mw",
        "400",
        "--skill-share",
        "0.3",
        "--skill-decay",
        "0.1",
        "--level-log-sigma",
        "0.1",
        "--offtaker-measure",
        "exponential",
        "--offtaker-tolerance-eur",
        "500000",
        "-o",
        str(out),
        *FAST,
    )
    assert "Toll valuation" in output
    payload = json.loads((out / "toll.json").read_text(encoding="utf-8"))
    contract = payload["contract"]
    assert contract["contract"] == "toll" and contract["priced"] is True
    assert contract["years"] == [2027, 2028, 2029]
    assert {"offtaker", "owner", "deal_zone_eur_per_mw_year", "prob_offtaker_loss"} <= set(contract)
    assert payload["scenarios"]["config"]["saturation"]["curve"]["placeholder"] is True
    assert set(payload["scenarios"]["regime_shares"]) == {"current", "new2026"}
    assert len(payload["toll_sensitivity"]) == 9
    markdown = out / "toll.md"
    assert "SYNTHETIC INPUTS" in markdown.read_text(encoding="utf-8")
    assert (out / "toll_annual_distribution.png").exists() and (out / "toll_sensitivity.png").exists()
    no_absolute_paths(markdown, tmp_path)
    no_absolute_paths(out / "toll.json", tmp_path)


def test_floor_partial_toll_and_swap_commands(ledgers: Path, tmp_path: Path) -> None:
    strategy = str(ledgers / "strategy.parquet")
    run("floor", "-l", strategy, "--floor", "150000", "--solve-upside-share", "-o", str(tmp_path / "floor"), *FAST)
    floor = json.loads((tmp_path / "floor" / "floor.json").read_text(encoding="utf-8"))["contract"]
    assert floor["price_eur_per_mw_year"] == pytest.approx(0.0, abs=1.0)
    assert 0.0 <= floor["terms"]["upside_share"] <= 1.0

    run(
        "partial-toll",
        "-l",
        strategy,
        "--fixed-fraction",
        "0.6",
        "--owner-merchant-share",
        "0.75",
        "--no-figures",
        "-o",
        str(tmp_path / "pt"),
        *FAST,
    )
    partial = json.loads((tmp_path / "pt" / "partial_toll.json").read_text(encoding="utf-8"))["contract"]
    assert partial["terms"]["fixed_fraction"] == 0.6

    run(
        "swap",
        "-l",
        strategy,
        "--reference-ledger",
        str(ledgers / "index.parquet"),
        "--write-paths",
        "-o",
        str(tmp_path / "swap"),
        *FAST,
    )
    swap = json.loads((tmp_path / "swap" / "swap.json").read_text(encoding="utf-8"))["contract"]
    assert swap["terms"]["reference_series"] == "reference"
    assert 0.0 < swap["extras"]["hedge_effectiveness_pv_variance"] < 1.0
    paths = pd.read_csv(tmp_path / "swap" / "swap_annual_paths.csv")
    assert {"asset", "reference"} <= set(paths.columns) and len(paths) == 600


def test_scenarios_summary_and_portfolio_commands(ledgers: Path, tmp_path: Path) -> None:
    run(
        "scenarios",
        "-l",
        str(ledgers / "strategy.parquet"),
        "--bootstrap",
        "stationary",
        "-o",
        str(tmp_path / "sc"),
        *FAST,
    )
    scenarios = json.loads((tmp_path / "sc" / "scenarios.json").read_text(encoding="utf-8"))["scenarios"]
    assert scenarios["config"]["bootstrap"]["method"] == "stationary"
    assert (tmp_path / "sc" / "scenarios_annual_distribution.png").exists()

    run(
        "summary",
        "-l",
        str(ledgers / "strategy.parquet"),
        "-l",
        str(ledgers / "germany.parquet"),
        "-o",
        str(tmp_path / "sum"),
    )
    summary = json.loads((tmp_path / "sum" / "summary.json").read_text(encoding="utf-8"))
    assert [item["name"] for item in summary["ledgers"]] == ["strategy", "germany"]
    assert (tmp_path / "sum" / "daily_germany.csv").exists()

    run(
        "portfolio",
        "-a",
        f"be={ledgers / 'strategy.parquet'}",
        "-a",
        f"de={ledgers / 'germany.parquet'}",
        "--weights",
        "be=2",
        "--paths",
        "200",
        "-o",
        str(tmp_path / "pf"),
    )
    portfolio = json.loads((tmp_path / "pf" / "portfolio.json").read_text(encoding="utf-8"))["portfolio"]
    assert portfolio["assets"] == ["be", "de"]
    assert portfolio["diversification"]["annual_shortfall_benefit_eur"] >= 0.0
    no_absolute_paths(tmp_path / "pf" / "portfolio.md", tmp_path)


def test_attribution_command_with_mechanism_decomposition(ledgers: Path, tmp_path: Path) -> None:
    out = tmp_path / "attr"
    output = run(
        "attribution",
        "--oracle",
        str(ledgers / "oracle.parquet"),
        "--strategy",
        str(ledgers / "strategy.parquet"),
        "--mechanism",
        str(ledgers / "mechanism.parquet"),
        "--impact",
        str(ledgers / "impact.parquet"),
        "--availability",
        str(ledgers / "availability.parquet"),
        "--degradation",
        str(ledgers / "degradation.parquet"),
        "--new-prices",
        str(ledgers / "new_prices.parquet"),
        "-o",
        str(out),
    )
    assert "attribution:" in output
    payload = json.loads((out / "attribution.json").read_text(encoding="utf-8"))
    keys = [step["key"] for step in payload["waterfall"]["steps"]]
    assert keys == ["oracle", "strategy", "mechanism", "impact", "availability", "degradation", "total"]
    decomposition = payload["mechanism_decomposition"]["steps"]
    assert [step["key"] for step in decomposition] == ["current", "repriced", "new", "total"]
    assert (out / "attribution_waterfall.png").exists() and (out / "attribution_daily.csv").exists()
    no_absolute_paths(out / "attribution.md", tmp_path)


def test_cli_errors_are_reported_cleanly(ledgers: Path, tmp_path: Path) -> None:
    bare = tmp_path / "bare.parquet"
    pd.read_parquet(ledgers / "strategy.parquet").to_parquet(bare)
    result = CliRunner().invoke(value_app, ["toll", "-l", str(bare), "-o", str(tmp_path / "x"), *FAST])
    assert result.exit_code != 0 and "power_mw" in result.output
    result = CliRunner().invoke(value_app, ["toll", "-l", str(tmp_path / "missing.parquet"), "-o", str(tmp_path / "x")])
    assert result.exit_code != 0 and "not found" in result.output
    result = CliRunner().invoke(
        value_app,
        ["toll", "-l", str(ledgers / "strategy.parquet"), "--capacity-curve", "placeholder", "-o", str(tmp_path)],
    )
    assert result.exit_code != 0 and "--fleet" in result.output
    ok = CliRunner().invoke(value_app, ["toll", "-l", str(bare), "--power-mw", "1", "-o", str(tmp_path / "ok"), *FAST])
    assert ok.exit_code == 0, ok.output
