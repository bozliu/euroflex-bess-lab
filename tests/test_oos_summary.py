"""Schema of the out-of-sample summary, the committed artefacts, and the fixed design of the evaluation script."""

from __future__ import annotations

import argparse
import copy
import importlib
import json
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.realtime import BatteryParams
from euroflex_bess_lab.realtime.forecasting.holdout import (
    SUMMARY_SCHEMA_VERSION,
    evaluate_holdout,
    holdout_split,
    to_jsonable,
    validate_summary,
)

REPO = Path(__file__).resolve().parents[1]
LEDGERS = REPO / "reports" / "ledgers"
SUMMARY = LEDGERS / "oos_summary.json"
QH_COLUMNS = {
    "timestamp_utc",
    "energy_mwh",
    "discharged_mwh",
    "charged_mwh",
    "base_price_eur_mwh",
    "price_eur_mwh",
    "price_missing",
    "revenue_eur",
    "degradation_eur",
    "pnl_eur",
    "soc_end_mwh",
    "strategy",
    "mechanism",
    "fleet_mw",
}


def _ledger(times: pd.DatetimeIndex, pnl: float) -> pd.DataFrame:
    n = len(times)
    return pd.DataFrame(
        {
            "timestamp_utc": times,
            "energy_mwh": np.zeros(n),
            "discharged_mwh": np.full(n, 0.1),
            "charged_mwh": np.full(n, 0.1),
            "base_price_eur_mwh": np.full(n, 60.0),
            "price_eur_mwh": np.full(n, 60.0),
            "price_missing": np.zeros(n, dtype=bool),
            "revenue_eur": np.full(n, pnl + 0.8),
            "degradation_eur": np.full(n, 0.8),
            "pnl_eur": np.full(n, pnl),
            "soc_end_mwh": np.ones(n),
        }
    )


def _summary() -> dict[str, Any]:
    split = holdout_split("2026-03-31", "2026-05-20")
    times = pd.date_range(split.cutoff_utc, split.test_end_utc, freq="15min", inclusive="left")
    values = {
        "oracle_qh": 3.0,
        "threshold_price": 1.0,
        "threshold_si": 0.7,
        "prism_risk_frozen": 1.4,
        "prism_risk_wf": 1.5,
    }
    block = evaluate_holdout(
        {k: _ledger(times, v) for k, v in values.items()},
        split,
        battery=BatteryParams(power_mw=1.0, energy_mwh=2.0),
        mechanism="current",
        frozen_arms=["prism_risk_frozen"],
        walk_forward={"prism_risk_frozen": "prism_risk_wf"},
        thresholds=["threshold_price", "threshold_si"],
        n_boot=100,
    )
    return to_jsonable(
        {
            "schema_version": SUMMARY_SCHEMA_VERSION,
            "generated_by": "tests",
            "data_source": "synthetic",
            "battery": {"power_mw": 1.0},
            "publication": {"minute_lag": 1, "qh_final_lag_minutes": 5},
            "settings": {},
            "primary_arms": {"current": "prism_risk_frozen"},
            "evaluations": [block],
            "frozen_training_audit": [
                {"model": "frozen_prism_current", "passed": True, "cutoff_utc": "x", "n_folds": 1}
            ],
            "walk_forward_models": {},
            "lookahead_audit": {
                "rows": [
                    {
                        "slice": "s",
                        "mechanism": "current",
                        "arm": "a",
                        "cut_minute": 37,
                        "passed": True,
                        "tradable": True,
                    }
                ],
                "all_tradable_passed": True,
                "tradable_arms_audited": ["current_2026-03-31:a"],
            },
            "headline": ["a sentence"],
            "runtime_seconds": {},
        }
    )


def test_summary_schema_accepts_a_complete_summary() -> None:
    assert validate_summary(_summary()) == []


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda s: s.pop("evaluations"), "missing `evaluations`"),
        (lambda s: s.update(schema_version=99), "schema_version"),
        (lambda s: s["evaluations"][0]["comparisons"]["prism_risk_frozen"].pop("vs_walk_forward"), "vs_walk_forward"),
        (
            lambda s: s["evaluations"][0]["comparisons"]["prism_risk_frozen"]["vs_best_threshold"].pop("ratio_ci"),
            "ratio_ci",
        ),
        (lambda s: s["evaluations"][0]["monthly"][0].pop("eur_per_mw_year"), "eur_per_mw_year"),
        (lambda s: s["evaluations"][0].update(best_threshold="oracle_qh"), "best_threshold"),
        (lambda s: s["primary_arms"].update(current="prism_risk_other"), "primary arm"),
        (lambda s: s["evaluations"].append(copy.deepcopy(s["evaluations"][0])), "not unique"),
        (lambda s: s["lookahead_audit"]["rows"][0].pop("passed"), "passed"),
        (lambda s: s["frozen_training_audit"][0].pop("n_folds"), "n_folds"),
        (lambda s: s.update(headline=[1]), "headline"),
    ],
)
def test_summary_schema_rejects_broken_summaries(mutate: Any, message: str) -> None:
    summary = _summary()
    mutate(summary)
    errors = validate_summary(summary)
    assert errors and any(message in e for e in errors), errors


def _load_script() -> Any:
    path = REPO / "scripts" / "research"
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
    return importlib.import_module("out_of_sample")


def test_script_design_is_the_preregistered_one(tmp_path: Path) -> None:
    oos = _load_script()
    args = argparse.Namespace(
        out=str(tmp_path), cache=str(tmp_path / "cache"), wf_models=str(tmp_path), quick=False, workers=1, threads=1
    )
    args.force, args.ablation = False, False
    s = oos.make_settings(args)
    evs = {ev.key: ev for ev in oos.evaluations(s)}
    assert set(evs) == {"current_2025-06-30", "current_2025-12-31", "current_2026-03-31", "new2026_2025-12-31"}
    assert evs["current_2025-06-30"].split.cutoff_utc == pd.Timestamp("2025-06-30T22:00", tz="UTC")
    assert evs["current_2026-03-31"].split.test_end_utc == pd.Timestamp("2026-10-24T22:00", tz="UTC")
    assert evs["new2026_2025-12-31"].split.test_end_utc == pd.Timestamp("2026-07-31T22:00", tz="UTC")
    assert all(ev.split.train_days > 180 for ev in evs.values())  # six local calendar months
    for mechanism, primary in oos.PRIMARY.items():
        assert primary in oos.ARMS[mechanism] and primary.endswith("_frozen")
        assert oos.walk_forward_pairs(mechanism, oos.ARMS[mechanism])[primary] in oos.ARMS[mechanism]
    assert not any(a.endswith("_plan") for arms in oos.ARMS.values() for a in arms)  # ablation is opt-in
    battery = oos.battery_params()
    assert (battery.power_mw, battery.energy_mwh, battery.degradation_eur_per_mwh) == (1.0, 2.0, 8.0)
    assert (battery.charge_efficiency, battery.discharge_efficiency) == (0.95, 0.95)
    assert (oos.publication().minute_lag, oos.publication().qh_final_lag_minutes) == (1, 5)
    assert (oos.BLOCK_DAYS, oos.N_BOOT, oos.CONFIDENCE) == (7, 10_000, 0.95)


# --------------------------------------------------------------------------------------------------------------
# committed artefacts (skipped until the report has been generated)


@pytest.fixture
def committed() -> dict[str, Any]:
    if not SUMMARY.exists():
        pytest.skip("reports/ledgers/oos_summary.json not generated yet")
    return json.loads(SUMMARY.read_text(encoding="utf-8"))


def test_committed_summary_is_valid_and_consistent(committed: dict[str, Any]) -> None:
    assert validate_summary(committed) == []
    assert all(
        a["passed"] and a["n_folds"] == 1 and a["fit_market_ends_by_cutoff"] for a in committed["frozen_training_audit"]
    )
    for audit in committed["frozen_training_audit"]:
        for row in audit["training_rows"]:
            assert pd.Timestamp(row["last_target_published_utc"]) <= pd.Timestamp(audit["cutoff_utc"])
    audit = committed["lookahead_audit"]
    assert audit["all_tradable_passed"] and audit["white_box_passed"] == audit["white_box_audits"]
    audited = {(r["evaluation"], r["arm"]) for r in audit["rows"] if r["passed"]}
    for block in committed["evaluations"]:
        for arm in block["arms"]:
            if arm != "oracle_qh":
                assert (block["id"], arm) in audited, (block["id"], arm)  # every tradable run is audited
        for arm, wf in block["walk_forward"].items():
            comp = block["comparisons"][arm]["vs_walk_forward"]
            assert comp["ratio"] == pytest.approx(block["arms"][arm]["pnl_eur"] / block["arms"][wf]["pnl_eur"])
            assert comp["ratio_ci"][0] <= comp["ratio"] <= comp["ratio_ci"][1]
        best = block["best_threshold"]
        assert all(block["arms"][best]["pnl_eur"] >= block["arms"][t]["pnl_eur"] for t in block["thresholds"])
    primary = committed["primary_arms"]["current"]
    for block, sentence in zip(
        [b for b in committed["evaluations"] if b["mechanism"] == "current"], committed["headline"], strict=False
    ):
        assert f"{block['arms'][primary]['eur_per_mw_year']:,.0f} EUR/MW/yr" in sentence


def test_committed_ledgers_match_the_summary(committed: dict[str, Any]) -> None:
    blocks = {b["id"]: b for b in committed["evaluations"]}
    stems = [s for s in committed["ledgers_written"] if s != "oos_daily_pnl"]
    assert stems
    for stem in stems:
        path = LEDGERS / f"{stem}.parquet"
        assert path.stat().st_size <= 5 * 1024 * 1024
        sidecar = json.loads((LEDGERS / f"{stem}.json").read_text(encoding="utf-8"))
        frame = pd.read_parquet(path)
        assert QH_COLUMNS <= set(frame.columns)
        assert set(frame["strategy"]) == {sidecar["strategy"]} and sidecar["frozen"] is True
        block = blocks[sidecar["evaluation"]]
        assert pd.Timestamp(frame["timestamp_utc"].min()) == pd.Timestamp(block["split"]["cutoff_utc"])
        expected = block["arms"][sidecar["strategy"]]["pnl_eur"]
        assert float(frame["pnl_eur"].astype(float).sum()) == pytest.approx(expected, rel=1e-4)
    daily = pd.read_parquet(LEDGERS / "oos_daily_pnl.parquet")
    totals = daily.groupby(["evaluation", "arm"])["pnl_eur"].sum()
    for block in committed["evaluations"]:
        for arm, values in block["arms"].items():
            assert totals[(block["id"], arm)] == pytest.approx(values["pnl_eur"], rel=1e-9)
