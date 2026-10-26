"""Tiny engine backtests for Germany on real DE-LU day-ahead fixtures (Energy-Charts, CC BY 4.0).

The window 2025-09-30 .. 2025-10-02 crosses the SDAC switch from hourly to 15-minute products. Reserve block
prices are synthetic (regelleistung.net results are not redistributed).
"""

from __future__ import annotations

import json
import shutil
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.backtesting.engine import run_walk_forward
from euroflex_bess_lab.config import BacktestConfig, load_config
from euroflex_bess_lab.validation import validate_config_file, validate_data_file

REPO_ROOT = Path(__file__).resolve().parents[1]
DAY_AHEAD_FIXTURE = REPO_ROOT / "tests" / "fixtures" / "germany" / "energy_charts_de_lu_2025-09-15_2025-10-12.json"


def _write_blocks(path: Path, first: date, last: date) -> Path:
    fcr = [9.0, 14.0, 21.0, 12.0, 25.0, 8.0]
    up = [4.0, 11.0, 6.0, 3.0, 19.0, 7.0]
    down = [2.0, 3.0, 15.0, 18.0, 4.0, 1.5]
    rows: list[dict[str, Any]] = []
    day = first
    while day <= last:
        bump = 0.5 * (day - first).days
        for block in range(6):
            start = pd.Timestamp(f"{day} {block * 4:02d}:00", tz="Europe/Berlin")
            end = (
                pd.Timestamp(f"{day} {block * 4 + 4:02d}:00", tz="Europe/Berlin")
                if block < 5
                else pd.Timestamp(f"{day + timedelta(days=1)} 00:00", tz="Europe/Berlin")
            )
            common = {"delivery_start_utc": start.tz_convert("UTC"), "delivery_end_utc": end.tz_convert("UTC")}
            rows.append({**common, "product": "FCR", "direction": "symmetric", "price_eur_per_mw_h": fcr[block] + bump})
            rows.append({**common, "product": "aFRR", "direction": "up", "price_eur_per_mw_h": up[block] + bump})
            rows.append({**common, "product": "aFRR", "direction": "down", "price_eur_per_mw_h": down[block] + bump})
        day += timedelta(days=1)
    frame = pd.DataFrame(rows)
    frame.loc[frame["product"] == "FCR"].to_csv(path / "fcr_blocks.csv", index=False)
    frame.loc[frame["product"] == "aFRR"].to_csv(path / "afrr_blocks.csv", index=False)
    return path


def _config(tmp_path: Path, workflow: str, *, start: str, end: str, **extra: Any) -> BacktestConfig:
    blocks = _write_blocks(tmp_path, date(2025, 9, 28), date(2025, 10, 3))
    payload: dict[str, Any] = {
        "schema_version": 4,
        "run_name": f"germany-test-{workflow}",
        "market": {"id": "germany"},
        "workflow": workflow,
        "forecast_provider": {"name": "persistence"},
        "timing": {
            "timezone": "Europe/Berlin",
            "day_ahead_gate_closure_local": "12:00",
            "delivery_start_date": start,
            "delivery_end_date": end,
        },
        "site": {"id": "de-test-site", "poi_import_limit_mw": 1.0, "poi_export_limit_mw": 1.0},
        "assets": [
            {
                "id": "de-bess",
                "battery": {
                    "power_mw": 1.0,
                    "energy_mwh": 2.0,
                    "initial_soc_mwh": 1.0,
                    "terminal_soc_mwh": 1.0,
                    "soc_min_mwh": 0.1,
                    "soc_max_mwh": 1.9,
                    "charge_efficiency": 0.95,
                    "discharge_efficiency": 0.95,
                },
            }
        ],
        "degradation": {"mode": "throughput_linear", "throughput_cost_eur_per_mwh": 4.0},
        "data": {"day_ahead": {"actual_path": str(DAY_AHEAD_FIXTURE)}},
        "artifacts": {
            "root_dir": str(tmp_path / "runs"),
            "save_inputs": False,
            "save_plots": False,
            "save_forecast_snapshots": False,
        },
    }
    if workflow == "da_plus_fcr":
        payload["data"]["fcr_capacity"] = {"actual_path": str(blocks / "fcr_blocks.csv")}
        payload["fcr"] = {"sustain_duration_minutes": 30}
    if workflow == "da_plus_afrr":
        payload["data"]["afrr_capacity_up"] = {"actual_path": str(blocks / "afrr_blocks.csv")}
        payload["data"]["afrr_capacity_down"] = {"actual_path": str(blocks / "afrr_blocks.csv")}
        payload["afrr"] = {
            "sustain_duration_minutes": 60,
            "settlement_mode": "capacity_only",
            "activation_mode": "none",
        }
    payload.update(extra)
    return BacktestConfig.model_validate(payload)


def _assert_physical(dispatch: pd.DataFrame) -> None:
    assert (np.minimum(dispatch["charge_mw"], dispatch["discharge_mw"]) <= 1e-9).all()
    assert dispatch["soc_mwh"].between(0.1 - 1e-6, 1.9 + 1e-6).all()


def test_da_only_backtest_crosses_the_15_minute_mtu_switch(tmp_path: Path) -> None:
    result = run_walk_forward(_config(tmp_path, "da_only", start="2025-09-30", end="2025-10-02"))
    dispatch = result.site_dispatch
    assert len(dispatch) == 3 * 96
    assert len(result.decision_log) == 3
    assert result.decision_log["decision_time_local"].astype(str).str.endswith("12:00:00+02:00").all()
    _assert_physical(dispatch)
    hourly_day = dispatch.loc[pd.to_datetime(dispatch["timestamp_utc"], utc=True) < pd.Timestamp("2025-09-30T22:00Z")]
    per_hour = hourly_day.groupby(pd.to_datetime(hourly_day["timestamp_utc"], utc=True).dt.floor("h"))[
        "day_ahead_actual_price_eur_per_mwh"
    ].nunique()
    assert per_hour.eq(1).all()
    assert result.oracle is not None
    assert result.oracle.total_pnl_eur >= result.pnl.total_pnl_eur - 1e-6
    assert result.pnl.reserve_capacity_revenue_eur == 0.0


def test_da_plus_fcr_backtest_reserves_with_energy_buffer(tmp_path: Path) -> None:
    result = run_walk_forward(_config(tmp_path, "da_plus_fcr", start="2025-10-01", end="2025-10-02"))
    dispatch = result.site_dispatch
    _assert_physical(dispatch)
    assert result.pnl.reserve_capacity_revenue_eur > 0.0
    assert dispatch["fcr_reserved_mw"].between(0.0, 1.0 + 1e-9).all()
    # Engine sustain duration 30 min: SOC at interval start keeps the FCR energy buffer in both directions.
    assert (dispatch["soc_start_mwh"] - dispatch["fcr_reserved_mw"] * 0.5 / 0.95 >= 0.1 - 1e-6).all()
    assert (dispatch["soc_start_mwh"] + dispatch["fcr_reserved_mw"] * 0.5 * 0.95 <= 1.9 + 1e-6).all()
    assert result.decision_log["decision_time_local"].astype(str).str.endswith("08:00:00+02:00").all()
    assert result.output_dir is not None
    summary = json.loads((Path(result.output_dir) / "summary.json").read_text(encoding="utf-8"))
    assert summary["decision_time_basis"] == "fcr_gate_closure"
    assert summary["reserve_settlement_mode"] == "capacity_only"
    assert summary["reserve_block_constraint_in_engine"] is False
    assert summary["reserve_product_block_minutes"] == 240


def test_da_plus_afrr_capacity_only_backtest(tmp_path: Path) -> None:
    result = run_walk_forward(_config(tmp_path, "da_plus_afrr", start="2025-10-01", end="2025-10-02"))
    dispatch = result.site_dispatch
    _assert_physical(dispatch)
    assert result.pnl.reserve_capacity_revenue_eur > 0.0
    assert result.pnl.reserve_activation_revenue_eur == 0.0
    assert dispatch["afrr_up_reserved_mw"].max() > 0.0 and dispatch["afrr_down_reserved_mw"].max() > 0.0
    assert result.decision_log["decision_time_local"].astype(str).str.endswith("09:00:00+02:00").all()
    assert result.output_dir is not None
    summary = json.loads((Path(result.output_dir) / "summary.json").read_text(encoding="utf-8"))
    assert summary["reserve_activation_mode"] == "none"
    assert summary["market_id"] == "germany"


def test_schedule_revision_wraps_germany_da_only(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        "schedule_revision",
        start="2025-10-01",
        end="2025-10-01",
        revision={"base_workflow": "da_only", "revision_checkpoints_local": ["10:00", "16:00"]},
    )
    result = run_walk_forward(config)
    assert result.workflow == "schedule_revision"
    assert result.revision_schedule is not None
    assert len(result.site_dispatch) == 96
    _assert_physical(result.site_dispatch)


@pytest.mark.parametrize("name", ["germany_da_only_base", "germany_da_plus_fcr_base", "germany_da_plus_afrr_base"])
def test_example_configs_validate_and_run(name: str, tmp_path: Path) -> None:
    path = REPO_ROOT / "examples" / "configs" / "basic" / f"{name}.yaml"
    assert validate_config_file(path).ok
    assert validate_data_file(path).ok
    config = load_config(path)
    config.artifacts.root_dir = tmp_path / "runs"
    result = run_walk_forward(config)
    assert len(result.site_dispatch) == 96
    assert result.oracle is not None and result.oracle.total_pnl_eur >= result.pnl.total_pnl_eur - 1e-6
    shutil.rmtree(tmp_path / "runs", ignore_errors=True)
