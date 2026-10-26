"""scripts/research/germany_backtest.py: periods, DST-safe persistence, pay-as-bid acceptance, aggregation, report.

Runs on the committed DST fixture (real Energy-Charts DE-LU prices, CC BY 4.0) and synthetic reserve blocks; no
network access and nothing is written outside ``tmp_path``.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from types import ModuleType

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.markets.adapters.germany import energy_charts_payload_to_frame, normalize_day_ahead_frame

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE = REPO_ROOT / "tests" / "fixtures" / "germany" / "energy_charts_de_lu_2025-10-25_2025-10-27.json"


@pytest.fixture(scope="module")
def research() -> ModuleType:
    path = REPO_ROOT / "scripts" / "research" / "germany_backtest.py"
    spec = importlib.util.spec_from_file_location("germany_backtest_under_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _blocks(first: date, last: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    fcr_rows, afrr_rows = [], []
    day = first
    while day <= last:
        for block in range(6):
            start = pd.Timestamp(f"{day} {block * 4:02d}:00", tz="Europe/Berlin").tz_convert("UTC")
            end_local = (
                pd.Timestamp(f"{day} {block * 4 + 4:02d}:00", tz="Europe/Berlin")
                if block < 5
                else pd.Timestamp(f"{day + timedelta(days=1)} 00:00", tz="Europe/Berlin")
            )
            common = {"delivery_start_utc": start, "delivery_end_utc": end_local.tz_convert("UTC")}
            fcr_price = [8.0, 12.0, 20.0, 11.0, 24.0, 7.0][block]
            fcr_rows.append(
                {
                    **common,
                    "product": "FCR",
                    "direction": "symmetric",
                    "price_eur_per_mw_h": fcr_price,
                    "marginal_price_eur_per_mw_h": fcr_price,
                }
            )
            for direction, prices in (
                ("up", [3.0, 9.0, 5.0, 2.0, 16.0, 6.0]),
                ("down", [1.5, 2.0, 12.0, 14.0, 3.0, 1.0]),
            ):
                # Marginal below average in block 4 for "up": a persistence bid there is rejected.
                marginal = prices[block] * (0.5 if (direction == "up" and block == 4) else 1.3)
                afrr_rows.append(
                    {
                        **common,
                        "product": "aFRR",
                        "direction": direction,
                        "price_eur_per_mw_h": prices[block],
                        "marginal_price_eur_per_mw_h": marginal,
                    }
                )
        day += timedelta(days=1)
    return pd.DataFrame(fcr_rows), pd.DataFrame(afrr_rows)


@pytest.fixture(scope="module")
def tables(research: ModuleType):
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    day_ahead = normalize_day_ahead_frame(energy_charts_payload_to_frame(payload), source="energy_charts")
    fcr, afrr = _blocks(date(2025, 10, 25), date(2025, 10, 27))
    return research.MarketTables(
        day_ahead=day_ahead, fcr_blocks=fcr, afrr_blocks=afrr, sources={"day_ahead": "fixture", "reserves": "synthetic"}
    )


def test_periods_follow_the_latest_full_months(research: ModuleType) -> None:
    periods = {period.key: period for period in research.define_periods(date(2026, 10, 25))}
    assert (periods["latest_12m"].first, periods["latest_12m"].last) == (date(2025, 10, 1), date(2026, 9, 30))
    assert (periods["quarter_hour_era"].first, periods["quarter_hour_era"].last) == (
        date(2025, 10, 1),
        date(2026, 10, 25),
    )
    assert (periods["previous_12m"].first, periods["previous_12m"].last) == (date(2024, 10, 1), date(2025, 9, 30))
    month_end = {period.key: period for period in research.define_periods(date(2026, 9, 30))}
    assert (month_end["latest_12m"].first, month_end["latest_12m"].last) == (date(2025, 10, 1), date(2026, 9, 30))


def test_build_days_handles_dst_and_falls_back_after_the_25_hour_day(research: ModuleType, tables) -> None:
    days, coverage = research.build_days(tables, date(2025, 10, 26), date(2025, 10, 27))
    assert [day.day for day in days] == [date(2025, 10, 26), date(2025, 10, 27)]
    assert len(days[0].timestamps_utc) == 100
    assert np.bincount(days[0].block).tolist() == [20, 16, 16, 16, 16, 16]
    # The repository provider aligns the 100-quarter-hour day with its 96-quarter-hour predecessor ...
    assert days[0].persistence_source_day == date(2025, 10, 25)
    # ... and, since the DST fix in PersistenceForecastProvider, also the day after it (no fallback needed).
    assert days[1].persistence_source_day == date(2025, 10, 26)
    assert coverage["persistence_fallback_days"] == []
    np.testing.assert_allclose(days[1].forecast["perfect_foresight"]["day_ahead"], days[1].actual["day_ahead"])
    np.testing.assert_allclose(days[0].forecast["persistence"]["fcr_capacity"], days[0].actual["fcr_capacity"])


def test_evaluate_day_settles_every_workflow_and_oracle_dominates(research: ModuleType, tables) -> None:
    days, _ = research.build_days(tables, date(2025, 10, 26), date(2025, 10, 26))
    records = pd.DataFrame(research.evaluate_day((days[0], (research.BASE,))))
    assert len(records) == 6
    for workflow in research.WORKFLOWS:
        rows = records.loc[records["workflow"] == workflow].set_index("provider")
        assert rows.loc["perfect_foresight", "net_eur"] >= rows.loc["persistence", "net_eur"] - 1e-6
    fcr = records.loc[(records["workflow"] == "da_plus_fcr") & (records["provider"] == "perfect_foresight")].iloc[0]
    assert fcr["reserve_revenue_eur"] > 0.0
    total = fcr["da_revenue_eur"] + fcr["reserve_revenue_eur"] - fcr["degradation_eur"]
    assert fcr["net_eur"] == pytest.approx(total)
    assert fcr["degradation_eur"] == pytest.approx(8.0 * fcr["discharged_mwh"])


def test_pay_as_bid_rejects_bids_above_the_marginal_price(research: ModuleType, tables) -> None:
    days, _ = research.build_days(tables, date(2025, 10, 26), date(2025, 10, 26))
    day = days[0]
    variant = next(variant for variant in research.SENSITIVITIES if variant.name == "afrr_pay_as_bid_acceptance")
    record = research._pay_as_bid(day, variant)
    assert record["workflow"] == "da_plus_afrr"
    assert 0.0 <= record["afrr_accepted_share"] < 1.0
    stage_one = research._solve(day, variant, "da_plus_afrr", day.forecast["persistence"])
    rejected_block = day.block == 4
    assert stage_one.afrr_up_mw[rejected_block].max() > 0.0
    # Nothing is paid for the rejected up block: reserve revenue equals the accepted volumes times own bids.
    accepted_up = np.where(rejected_block, 0.0, stage_one.afrr_up_mw)
    expected = (
        np.sum(
            accepted_up * day.forecast["persistence"]["afrr_capacity_up"]
            + stage_one.afrr_down_mw * day.forecast["persistence"]["afrr_capacity_down"]
        )
        * 0.25
    )
    assert record["reserve_revenue_eur"] == pytest.approx(expected, rel=1e-6)


def test_summaries_annualise_and_report_renders(research: ModuleType, tables, tmp_path: Path) -> None:
    days, coverage = research.build_days(tables, date(2025, 10, 26), date(2025, 10, 27))
    results = research.run_backtest(days, workers=1, sensitivity_start=date(2025, 10, 1))
    period = research.Period("window", "Fixture window", date(2025, 10, 26), date(2025, 10, 27))
    summary = research.summarise(results, period).set_index(["provider", "workflow"])
    row = summary.loc[("persistence", "da_only")]
    assert row["days"] == 2
    assert row["net_eur_per_mw_year"] == pytest.approx(row["net_eur"] * 365.0 / 2.0)
    assert row["cycles_per_day"] == pytest.approx(row["discharged_mwh"] / 2.0 / 2.0)
    assert summary.loc[("perfect_foresight", "da_plus_fcr"), "capture_vs_oracle"] == pytest.approx(1.0)
    table, month_days = research.monthly(results, period)
    assert month_days.to_dict() == {"2025-10": 2}
    assert table[("persistence", "da_only")].iloc[0] == pytest.approx(row["net_eur"])

    periods = [
        research.Period("latest_12m", "Latest", date(2025, 10, 26), date(2025, 10, 27)),
        research.Period("quarter_hour_era", "Era", date(2025, 10, 26), date(2025, 10, 27)),
        research.Period("previous_12m", "Previous", date(2025, 10, 26), date(2025, 10, 26)),
    ]
    figures = research.make_figures(results, tables, periods, tmp_path / "figures")
    assert [path.name for path in figures] == [
        "germany_monthly_net_revenue.png",
        "germany_cumulative_net_revenue.png",
        "germany_reserve_capacity_prices.png",
    ]
    assert all(path.stat().st_size > 10_000 for path in figures)
    report = research.render_report(
        results=results,
        tables=tables,
        coverage=coverage,
        periods=periods,
        check={"status": "skipped", "reason": "unit test"},
        figures=figures,
        start=date(2025, 10, 26),
        end=date(2025, 10, 27),
        runtime_seconds=1.0,
    )
    assert "## Key findings" in report and "## Assumptions and limitations" in report
    assert "2025-10-27 (persistence from 2025-10-25)" not in report
    assert "/Users/" not in report
