"""scripts/ops/track_record.py on the committed fixture tree: idempotent appends, causality, continuity, formula
switch, the forward fetch path (offline raw cache), frozen models, and the consistency of the committed record.

No network: every source is the fixture ``data/public`` slice or a raw cache written by the test (offline mode).
"""

from __future__ import annotations

import importlib.util
import json
import math
import pickle
import shutil
import sys
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab import public_data
from euroflex_bess_lab.public_data.normalize import (
    ODS126_MAP,
    ODS127_MAP,
    ODS133_MAP,
    ODS134_MAP,
    ODS165_MAP,
    ODS166_MAP,
    SI_FORECAST_MAP,
)
from euroflex_bess_lab.realtime import MarketData, check_no_lookahead, simulate

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE = REPO_ROOT / "tests" / "fixtures" / "public"
RECORD = REPO_ROOT / "reports" / "track_record"
DAY1, DAY2 = date(2026, 3, 3), date(2026, 3, 4)  # complete local days of the fixture (2026-03-02..04 UTC)
NOW = pd.Timestamp("2026-03-05T12:00:00Z")
FAR = pd.Timestamp("2030-01-01T00:00:00Z")  # go-live outside the fixture: current formula throughout
SPLIT = pd.Timestamp("2026-03-04T09:00:00Z")  # fake go-live inside DAY2 (the fixture has ODS211/212 rows)


@pytest.fixture(scope="module")
def tr() -> ModuleType:
    path = REPO_ROOT / "scripts" / "ops" / "track_record.py"
    spec = importlib.util.spec_from_file_location("ops_track_record_under_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def fixture_data(monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("EUROFLEX_DATA_DIR", str(FIXTURE))
    return FIXTURE


def _cfg(tr: ModuleType, out: Path, **overrides: Any) -> Any:
    settings: dict[str, Any] = {
        "out_dir": out,
        "track_start": DAY1,
        "warmup_days": 3,
        "go_live_utc": FAR,
        "strategies": ("threshold",),
        "models_dir": None,
    }
    settings.update(overrides)
    return tr.TrackConfig(**settings)


def _source(tr: ModuleType, tmp_path: Path, **overrides: Any) -> Any:
    settings: dict[str, Any] = {"raw_dir": tmp_path / "raw", "offline": True, "go_live_utc": FAR}
    settings.update(overrides)
    return tr.PublicForwardSource(**settings)


def _snapshot(directory: Path) -> dict[str, bytes]:
    return {p.relative_to(directory).as_posix(): p.read_bytes() for p in sorted(directory.rglob("*")) if p.is_file()}


class _Tampered:
    """Replaces every value from ``cut`` on with noise (what a look-ahead would pick up)."""

    def __init__(self, base: Any, cut: pd.Timestamp, tr: ModuleType) -> None:
        self.base, self.cut, self.tr = base, cut, tr

    def load(self, start: pd.Timestamp, end: pd.Timestamp) -> Any:
        window = self.base.load(start, end)
        market = window.market
        m = max(0, self.tr.minute_offset(market, self.cut))
        q = m // 15
        rng = np.random.default_rng(3)

        def noisy(values: np.ndarray, at: int) -> np.ndarray:
            out = values.copy()
            out[at:] = rng.normal(0.0, 300.0, size=max(0, len(out) - at))
            return out

        tampered = MarketData(
            **{
                **market.__dict__,
                "minute": {k: noisy(v, m) for k, v in market.minute.items()},
                "qh": {k: noisy(v, q) for k, v in market.qh.items()},
                "settlement_prices": {k: noisy(v, q) for k, v in market.settlement_prices.items()},
                "da_price": noisy(market.da_price, q),
            }
        )
        return replace(window, market=tampered)


# ------------------------------------------------------------------------------------------------------- calendar


def test_local_days_and_formula_segments(tr: ModuleType) -> None:
    start, end = tr.day_bounds(date(2026, 10, 25))
    assert (start, end) == (pd.Timestamp("2026-10-24T22:00:00Z"), pd.Timestamp("2026-10-25T23:00:00Z"))
    assert (end - start) / pd.Timedelta(minutes=15) == 100  # the 25-hour DST day
    start, end = tr.day_bounds(date(2026, 3, 29))
    assert (end - start) / pd.Timedelta(minutes=15) == 92
    go_live = tr.NEW_FORMULA_GO_LIVE_UTC
    assert go_live == pd.Timestamp("2026-11-24T09:00:00Z")
    d0, d1 = tr.day_bounds(date(2026, 11, 24))
    assert tr.day_segments(d0, d1, go_live) == [(d0, go_live, "current"), (go_live, d1, "new2026")]
    assert tr.day_segments(*tr.day_bounds(date(2026, 11, 23)), go_live)[0][2] == "current"
    assert tr.day_segments(*tr.day_bounds(date(2026, 11, 25)), go_live)[0][2] == "new2026"
    assert tr.TRACK_START == date(2026, 10, 25)
    assert tr.BATTERY.power_mw == 1.0 and tr.BATTERY.energy_mwh == 2.0


# ------------------------------------------------------------------------------------------------ append semantics


def test_update_appends_each_day_once_and_a_rerun_is_a_noop(tr: ModuleType, fixture_data: Path, tmp_path: Path) -> None:
    cfg = _cfg(tr, tmp_path / "out")
    report = tr.update(cfg, _source(tr, tmp_path), now=NOW)
    assert [(r["local_date"], r["strategy"]) for r in report.appended] == [
        ("2026-03-03", "threshold"),
        ("2026-03-04", "threshold"),
    ]
    assert report.pending is None and not report.errors
    files = _snapshot(cfg.out_dir)
    assert set(files) == {"README.md", "cumulative_pnl.png", "daily_pnl.csv", "qh/2026-03.csv", "summary.json"}
    again = tr.update(cfg, _source(tr, tmp_path), now=NOW + pd.Timedelta(days=1))
    assert again.appended == [] and again.pending is not None and again.pending["day"] == "2026-03-05"
    assert _snapshot(cfg.out_dir) == files  # nothing new: no file touched
    daily = tr.TrackStore(cfg.out_dir).read_daily()
    ledger = tr.TrackStore(cfg.out_dir).read_ledger()
    assert len(daily) == 2 and len(ledger) == 192
    assert daily["n_qh"].tolist() == [96, 96] and daily["data_source"].tolist() == ["public", "public"]
    assert daily["soc_start_mwh"].iloc[0] == tr.BATTERY.initial_soc_mwh
    assert daily["soc_start_mwh"].iloc[1] == daily["soc_end_mwh"].iloc[0]  # state carried over exactly
    assert daily["cumulative_pnl_eur"].iloc[1] == pytest.approx(daily["pnl_eur"].sum(), abs=1e-6)
    assert float(ledger["pnl_eur"].sum()) == pytest.approx(float(daily["pnl_eur"].sum()), abs=0.01)


def test_day_by_day_equals_all_at_once(tr: ModuleType, fixture_data: Path, tmp_path: Path) -> None:
    stepwise, at_once = _cfg(tr, tmp_path / "a"), _cfg(tr, tmp_path / "b")
    assert len(tr.update(stepwise, _source(tr, tmp_path), now=NOW, until=DAY1).appended) == 1
    assert len(tr.update(stepwise, _source(tr, tmp_path), now=NOW).appended) == 1
    tr.update(at_once, _source(tr, tmp_path), now=NOW)
    for name in ("daily_pnl.csv", "qh/2026-03.csv"):
        assert (stepwise.out_dir / name).read_bytes() == (at_once.out_dir / name).read_bytes(), name


def test_existing_rows_are_never_rewritten(tr: ModuleType, fixture_data: Path, tmp_path: Path) -> None:
    cfg = _cfg(tr, tmp_path / "out")
    tr.update(cfg, _source(tr, tmp_path), now=NOW, until=DAY1)
    store = tr.TrackStore(cfg.out_dir)
    first = store.read_daily()
    doctored = first.assign(pnl_eur=first["pnl_eur"] + 1000.0)
    store.append(store.read_ledger(), doctored)  # an attempt to restate an existing day
    assert store.read_daily()["pnl_eur"].tolist() == first["pnl_eur"].tolist()


# ------------------------------------------------------------------------------------------------------- causality


def test_recorded_day_ignores_everything_after_it(tr: ModuleType, fixture_data: Path, tmp_path: Path) -> None:
    clean, dirty = _cfg(tr, tmp_path / "clean"), _cfg(tr, tmp_path / "dirty")
    end_day1 = tr.day_bounds(DAY1)[1]
    tr.update(clean, _source(tr, tmp_path), now=NOW)
    tr.update(dirty, _Tampered(_source(tr, tmp_path), end_day1, tr), now=NOW)
    a, b = tr.TrackStore(clean.out_dir).read_ledger(), tr.TrackStore(dirty.out_dir).read_ledger()
    day1 = a["timestamp_utc"] < tr.iso_z(end_day1)
    pd.testing.assert_frame_equal(a[day1].reset_index(drop=True), b[day1].reset_index(drop=True))
    assert not a[~day1].reset_index(drop=True).equals(b[~day1].reset_index(drop=True))  # the tampering is real


def test_decisions_inside_a_day_only_use_published_data(tr: ModuleType, fixture_data: Path, tmp_path: Path) -> None:
    source = _source(tr, tmp_path)
    d0, d1 = tr.day_bounds(DAY1)
    market = source.load(d0 - pd.Timedelta(days=3), d1).market
    start = tr.minute_offset(market, d0)
    for cut in (start + 7, start + 601, start + 1399):
        audit = check_no_lookahead(
            lambda: tr.DeferredStrategy(tr.build_strategy("threshold", "current", None), start),
            market,
            tr.BATTERY,
            cut_minute=cut,
            mechanism="current",
            publication=tr.PUBLICATION,
        )
        assert audit.passed and audit.compared_minutes >= cut


def test_day_by_day_state_equals_one_continuous_simulation(tr: ModuleType, fixture_data: Path, tmp_path: Path) -> None:
    cfg = _cfg(tr, tmp_path / "out")
    source = _source(tr, tmp_path)
    tr.update(cfg, source, now=NOW)
    d0, d2 = tr.day_bounds(DAY1)[0], tr.day_bounds(DAY2)[1]
    market = source.load(d0 - pd.Timedelta(days=3), d2).market
    strategy = tr.DeferredStrategy(tr.build_strategy("threshold", "current", None), tr.minute_offset(market, d0))
    continuous = simulate(market, strategy, tr.BATTERY, publication=tr.PUBLICATION, record_minutes=False).qh
    continuous = continuous[continuous["timestamp_utc"] >= d0].reset_index(drop=True)
    ledger = tr.TrackStore(cfg.out_dir).read_ledger()
    assert len(ledger) == len(continuous) == 192
    for column, atol in (("energy_mwh", 1e-6), ("soc_end_mwh", 1e-6), ("pnl_eur", 1e-4), ("price_eur_mwh", 1e-4)):
        np.testing.assert_allclose(ledger[column], continuous[column], atol=atol, err_msg=column)
    assert float(np.abs(ledger["energy_mwh"]).sum()) > 1.0  # the strategy traded


# ------------------------------------------------------------------------------------------- formula and data gaps


def test_go_live_day_settles_each_part_under_its_formula(tr: ModuleType, fixture_data: Path, tmp_path: Path) -> None:
    cfg = _cfg(tr, tmp_path / "out", go_live_utc=SPLIT)
    report = tr.update(cfg, _source(tr, tmp_path, go_live_utc=SPLIT), now=NOW)
    assert len(report.appended) == 2 and not report.errors
    daily = tr.TrackStore(cfg.out_dir).read_daily()
    assert daily["mechanism"].tolist() == ["current", "current+new2026"]
    ledger = tr.TrackStore(cfg.out_dir).read_ledger()
    ledger["ts"] = pd.to_datetime(ledger["timestamp_utc"], utc=True)
    d0, d1 = tr.day_bounds(DAY2)
    current = public_data.load_belgium_qh(d0, SPLIT).set_index("timestamp_utc")["imbalance_price_eur_mwh"]
    new = public_data.load_belgium_new_formula_qh(SPLIT, d1).set_index("timestamp_utc")["imbalance_price_eur_mwh"]
    before = ledger[(ledger["ts"] >= d0) & (ledger["ts"] < SPLIT)].set_index("ts")
    after = ledger[ledger["ts"] >= SPLIT].set_index("ts")
    assert (before["mechanism"] == "current").all() and (after["mechanism"] == "new2026").all()
    np.testing.assert_allclose(before["price_eur_mwh"], current.reindex(before.index).astype(float), atol=1e-4)
    np.testing.assert_allclose(after["price_eur_mwh"], new.reindex(after.index).astype(float), atol=1e-4)
    assert len(before) + len(after) == 96


class _Gappy:
    """Hides the settlement price of the last quarter-hour of DAY2 (Elia not finished publishing)."""

    def __init__(self, base: Any, tr: ModuleType) -> None:
        self.base, self.tr = base, tr

    def load(self, start: pd.Timestamp, end: pd.Timestamp) -> Any:
        window = self.base.load(start, end)
        prices = dict(window.market.settlement_prices)
        current = prices["current"].copy()
        k = self.tr.minute_offset(window.market, self.tr.day_bounds(DAY2)[1]) // 15 - 1
        if 0 <= k < len(current):
            current[k] = np.nan
        prices["current"] = current
        return replace(window, market=MarketData(**{**window.market.__dict__, "settlement_prices": prices}))


def test_incomplete_day_waits_then_is_recorded_with_its_gap(tr: ModuleType, fixture_data: Path, tmp_path: Path) -> None:
    cfg = _cfg(tr, tmp_path / "out")
    source = _Gappy(_source(tr, tmp_path), tr)
    report = tr.update(cfg, source, now=NOW)
    assert [r["local_date"] for r in report.appended] == ["2026-03-03"]
    assert report.pending == {"day": "2026-03-04", "reason": "1 quarter-hours without a published settlement price"}
    late = tr.day_bounds(DAY2)[1] + pd.Timedelta(days=cfg.max_wait_days, hours=1)
    report = tr.update(cfg, source, now=late)
    assert [r["local_date"] for r in report.appended] == ["2026-03-04"]
    assert report.pending is not None and report.pending["day"] == "2026-03-05"  # no data at all: never forced
    row = tr.TrackStore(cfg.out_dir).read_daily().iloc[-1]
    assert not bool(row["data_complete"]) and int(row["price_missing_qh"]) == 1


# --------------------------------------------------------------------------------------------- forward fetch path


def _inverse(frame: pd.DataFrame, mapping: dict[str, str], time_column: str = "timestamp_utc") -> pd.DataFrame:
    out = pd.DataFrame({"datetime": frame[time_column]})
    for upstream, contract in mapping.items():
        out[upstream] = frame[contract]
    return out


def _write_raw_cache(raw: Path) -> None:
    """Upstream-shaped copies of the fixture tables, at the paths the public-data fetch code caches them."""
    lo, hi = "2026-03-01", "2026-04-01"
    minute = public_data.load_belgium_minute(lo, hi)
    qh = public_data.load_belgium_qh(lo, hi)
    tables = {
        ("ods133", "2026-03-01__2026-04-01"): _inverse(minute, ODS133_MAP),
        ("ods126", "2026-03-01__2026-04-01"): _inverse(minute, ODS126_MAP),
        ("ods165", "2026-03-01__2026-04-01"): _inverse(minute, ODS165_MAP),
        ("ods134", "2026-01-01__2027-01-01"): _inverse(qh, ODS134_MAP),
        ("ods166", "2026-01-01__2027-01-01"): _inverse(qh, ODS166_MAP),
        ("ods127", "2026-01-01__2027-01-01"): _inverse(qh, ODS127_MAP),
    }
    for dataset, kind in (("ods136", "current"), ("ods147", "next")):
        si = public_data.load_belgium_si_forecast(kind, lo, hi)
        upstream = pd.DataFrame(
            {"predictiontimeutc": si["prediction_time_utc"], "predictions_forecastedtimeutc": si["target_qh_utc"]}
        )
        for name, contract in SI_FORECAST_MAP.items():
            upstream[name] = si[contract]
        tables[(dataset, "2026-03-01__2026-04-01")] = upstream
    for (dataset, label), frame in tables.items():
        path = raw / "elia" / dataset / f"{dataset}__{label}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)
    da = public_data.load_day_ahead_prices("BE", lo, hi)
    payload = {
        "unix_seconds": [int(ts.timestamp()) for ts in da["timestamp_utc"]],
        "price": [float(v) for v in da["price_eur_mwh"]],
    }
    path = raw / "energy_charts" / "BE" / "price_BE_2026-03.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_forward_fetch_path_reproduces_the_committed_tables(tr: ModuleType, fixture_data: Path, tmp_path: Path) -> None:
    raw = tmp_path / "raw_forward"
    _write_raw_cache(raw)
    cut = tr.day_bounds(DAY2)[0]  # pretend the committed data set ends after DAY1: DAY2 comes from the raw cache
    forward = _source(tr, tmp_path, raw_dir=raw, committed_end=cut)
    committed = _source(tr, tmp_path)
    d0, d1 = tr.day_bounds(DAY2)
    a = committed.load(d0 - pd.Timedelta(days=3), d1).market
    b = forward.load(d0 - pd.Timedelta(days=3), d1).market
    # The Jan-Jul 2026 new-formula simulation (nf_*) only exists in the committed set; days after it never need it.
    core_minute = {k for k in a.minute if not k.startswith("nf_")}
    core_qh = {k for k in a.qh if not k.startswith("nf_")}
    assert core_minute == {k for k in b.minute if not k.startswith("nf_")} and len(core_minute) > 40
    assert core_qh == {k for k in b.qh if not k.startswith("nf_")}
    for name in sorted(core_minute):
        np.testing.assert_array_equal(a.minute[name], b.minute[name], err_msg=name)
    for name in sorted(core_qh):
        np.testing.assert_array_equal(a.qh[name], b.qh[name], err_msg=name)
    np.testing.assert_array_equal(a.settlement("current"), b.settlement("current"))
    np.testing.assert_array_equal(a.da_price, b.da_price)
    assert {r["dataset_id"] for r in forward.records} >= {"ods133", "ods134", "ods136", "energy_charts_price_BE"}
    via_public, via_forward = _cfg(tr, tmp_path / "p"), _cfg(tr, tmp_path / "f")
    tr.update(via_public, committed, now=NOW)
    tr.update(via_forward, forward, now=NOW)
    assert (via_public.out_dir / "qh" / "2026-03.csv").read_bytes() == (
        via_forward.out_dir / "qh" / "2026-03.csv"
    ).read_bytes()
    sources = tr.TrackStore(via_forward.out_dir).read_daily()["data_source"].tolist()
    assert sources == ["public", "forward"]


def test_offline_source_without_cache_leaves_new_days_pending(
    tr: ModuleType, fixture_data: Path, tmp_path: Path
) -> None:
    source = _source(tr, tmp_path, committed_end=tr.day_bounds(DAY2)[0])  # empty raw cache, offline
    report = tr.update(_cfg(tr, tmp_path / "out"), source, now=NOW)
    assert [r["local_date"] for r in report.appended] == ["2026-03-03"]
    assert report.pending is not None and report.pending["day"] == "2026-03-04"


# --------------------------------------------------------------------------------------------------- frozen models


def _fit_models(directory: Path, *, seed_params: dict[str, Any] | None = None) -> None:
    from euroflex_bess_lab.realtime.forecasting import (
        GBMParams,
        PriceForecaster,
        PriceForecasterConfig,
        PrismForecaster,
        PrismForecasterConfig,
    )
    from euroflex_bess_lab.realtime.forecasting.deploy import DEPLOY_FILES, FUSION_FILE, deployment_fold

    pytest.importorskip("sklearn")
    from threadpoolctl import threadpool_limits

    data_start = pd.Timestamp("2026-03-02T00:00:00Z")
    data_end = pd.Timestamp("2026-03-02T23:00:00Z")
    market = MarketData.from_public(data_start, data_end)
    fold = deployment_fold(data_end, data_start=data_start)
    fast = GBMParams(**{"max_iter": 8, "max_leaf_nodes": 15, "min_samples_leaf": 20, **(seed_params or {})})
    models = {
        "price_current": PriceForecaster(PriceForecasterConfig(params=fast, stride=4)),
        "prism_current": PrismForecaster(PrismForecasterConfig(params=fast, stride=4)),
        "prism_new2026": PrismForecaster(
            PrismForecasterConfig(mechanism="new2026", components="reconstructed", params=fast, stride=4)
        ),
    }
    directory.mkdir(parents=True, exist_ok=True)
    for key, model in models.items():
        with threadpool_limits(limits=1):  # tiny data: OpenMP start-up would dominate the fit
            model.fit(market, [fold])
        with open(directory / DEPLOY_FILES[key], "wb") as handle:
            pickle.dump(model, handle)
    (directory / FUSION_FILE).write_text(json.dumps({"variance": list(np.linspace(4000.0, 100.0, 16))}))


@pytest.fixture(scope="module")
def frozen_models(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Tiny deployment forecasters fitted on the first fixture day (test window: everything after it)."""
    directory = tmp_path_factory.mktemp("frozen_models")
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("EUROFLEX_DATA_DIR", str(FIXTURE))
        _fit_models(directory)
    return directory


def test_model_strategies_use_frozen_models_and_refuse_a_swap(
    tr: ModuleType, fixture_data: Path, tmp_path: Path, frozen_models: Path
) -> None:
    models = tmp_path / "models"
    shutil.copytree(frozen_models, models)
    cfg = _cfg(tr, tmp_path / "out", strategies=("threshold", "forecast_mpc", "scales_l"), models_dir=models)
    report = tr.update(cfg, _source(tr, tmp_path), now=NOW, until=DAY1)
    assert sorted(r["strategy"] for r in report.appended) == ["forecast_mpc", "scales_l", "threshold"]
    assert not report.errors and report.skipped == {}
    daily = tr.TrackStore(cfg.out_dir).read_daily()
    ids = dict(zip(daily["strategy"], daily["models_id"], strict=True))
    assert ids["threshold"] == "" and ids["forecast_mpc"] == ids["scales_l"] == tr.inspect_models(models).models_id
    for name in ("forecast_mpc", "scales_l"):
        row = daily[daily["strategy"] == name].iloc[0]
        assert math.isfinite(row["pnl_eur"]) and row["discharged_mwh"] + row["charged_mwh"] > 0
    _fit_models(models, seed_params={"max_iter": 9})  # someone refits: no longer the frozen models
    report = tr.update(cfg, _source(tr, tmp_path), now=NOW)
    assert [r["strategy"] for r in report.appended] == ["threshold"]
    assert set(report.skipped) == {"forecast_mpc", "scales_l"} and "models changed" in report.skipped["scales_l"]
    report = tr.update(replace(cfg, allow_model_change=True), _source(tr, tmp_path), now=NOW)
    assert sorted(r["strategy"] for r in report.appended) == ["forecast_mpc", "scales_l"]


def test_go_live_retires_forecast_mpc_and_switches_scales_l(
    tr: ModuleType, fixture_data: Path, tmp_path: Path, frozen_models: Path
) -> None:
    strategies = ("threshold", "forecast_mpc", "scales_l")
    cfg = _cfg(tr, tmp_path / "out", strategies=strategies, models_dir=frozen_models, go_live_utc=SPLIT)
    report = tr.update(cfg, _source(tr, tmp_path, go_live_utc=SPLIT), now=NOW)
    assert not report.errors and len(report.appended) == 6
    daily = tr.TrackStore(cfg.out_dir).read_daily()
    day2 = daily[daily["local_date"] == DAY2.isoformat()].set_index("strategy")
    before = int((SPLIT - tr.day_bounds(DAY2)[0]) / pd.Timedelta(minutes=15))
    assert day2.loc["forecast_mpc", "mechanism"] == "current" and day2.loc["forecast_mpc", "n_qh"] == before
    assert day2.loc["scales_l", "mechanism"] == "current+new2026" and day2.loc["scales_l", "n_qh"] == 96
    assert day2.loc["threshold", "mechanism"] == "current+new2026"
    later = tr.update(cfg, _source(tr, tmp_path, go_live_utc=SPLIT), now=NOW + pd.Timedelta(days=1))
    assert "retired at the new-formula go-live" in later.skipped["forecast_mpc"]


def test_missing_models_skip_only_the_model_strategies(tr: ModuleType, fixture_data: Path, tmp_path: Path) -> None:
    cfg = _cfg(tr, tmp_path / "out", strategies=("threshold", "scales_l"), models_dir=tmp_path / "absent")
    report = tr.update(cfg, _source(tr, tmp_path), now=NOW)
    assert [r["strategy"] for r in report.appended] == ["threshold", "threshold"]
    assert "models not available" in report.skipped["scales_l"]
    readme = (cfg.out_dir / "README.md").read_text(encoding="utf-8")
    assert "| SCALES-L (risk filter) | 0 | not run yet: models not available" in readme


# ------------------------------------------------------------------------------------------------ outputs and CLI


def test_generated_outputs(tr: ModuleType, fixture_data: Path, tmp_path: Path) -> None:
    cfg = _cfg(tr, tmp_path / "out")
    tr.update(cfg, _source(tr, tmp_path), now=NOW)
    readme = (cfg.out_dir / "README.md").read_text(encoding="utf-8")
    for fragment in ("Cumulative P&L (EUR)", "EUR/MW/yr to date", "Threshold (price bands)", "cumulative_pnl.png"):
        assert fragment in readme
    assert "No order or bid is submitted to any market" in readme
    assert (cfg.out_dir / "cumulative_pnl.png").read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"
    summary = json.loads((cfg.out_dir / "summary.json").read_text(encoding="utf-8"))
    entry = summary["strategies"]["threshold"]
    daily = tr.TrackStore(cfg.out_dir).read_daily()
    assert entry["days"] == 2 and entry["pnl_eur"] == pytest.approx(float(daily["pnl_eur"].sum()), abs=0.01)
    assert summary["paper_only"] is True and summary["market_submission"] is False
    expected_rate = entry["pnl_eur"] / 1.0 / (2.0 / 365.25)
    assert entry["eur_per_mw_year_to_date"] == pytest.approx(expected_rate, abs=1.0)
    for path in cfg.out_dir.rglob("*"):
        if path.suffix in {".md", ".json", ".csv"}:
            text = path.read_text(encoding="utf-8")
            assert str(tmp_path) not in text and "/" + "Users/" not in text, path.name


def test_cli_update_render_verify(tr: ModuleType, fixture_data: Path, tmp_path: Path) -> None:
    common = ["--out", str(tmp_path / "out"), "--strategies", "threshold", "--track-start", "2026-03-03"]
    common += ["--warmup-days", "3", "--go-live", "2030-01-01T00:00:00Z"]
    raw = ["--raw-dir", str(tmp_path / "raw"), "--offline"]
    assert tr.main(["update", *common, *raw, "--now", "2026-03-05T12:00:00Z"]) == 0
    assert tr.TrackStore(tmp_path / "out").read_daily()["local_date"].tolist() == ["2026-03-03", "2026-03-04"]
    before = _snapshot(tmp_path / "out")
    assert tr.main(["render", *common]) == 0
    assert _snapshot(tmp_path / "out") == before  # rendering is deterministic
    assert tr.main(["verify", *common, *raw]) == 0
    with pytest.raises(SystemExit):
        tr.main(["update", "--strategies", "nope"])


# ------------------------------------------------------------------------------------- the committed record (repo)


def test_committed_track_record_is_consistent(tr: ModuleType) -> None:
    store = tr.TrackStore(RECORD)
    daily, ledger = store.read_daily(), store.read_ledger()
    assert len(daily) >= 1 and set(daily["strategy"]) <= set(tr.STRATEGIES)
    assert min(daily["local_date"]) >= tr.TRACK_START.isoformat()
    ledger["local_date"] = (
        pd.to_datetime(ledger["timestamp_utc"], utc=True).dt.tz_convert(tr.TZ).dt.strftime("%Y-%m-%d")
    )
    sums = ledger.groupby(["local_date", "strategy"])["pnl_eur"].agg(["sum", "size"])
    reference = json.loads((REPO_ROOT / "models" / "reference_manifest.json").read_text(encoding="utf-8"))
    for row in daily.itertuples(index=False):
        total, size = sums.loc[(row.local_date, row.strategy)]
        assert size == row.n_qh and total == pytest.approx(row.pnl_eur, abs=0.01)
        assert row.data_source in {"public", "forward", "public+forward"}
        if tr.STRATEGIES[row.strategy].needs_models:
            assert row.models_id == reference["models_id"]  # frozen models only
    for name, rows in daily.groupby("strategy"):
        rows = rows.sort_values("local_date")
        assert rows["soc_start_mwh"].iloc[0] == tr.BATTERY.initial_soc_mwh or rows["local_date"].iloc[0] > "2026-10-25"
        np.testing.assert_allclose(rows["soc_start_mwh"].iloc[1:], rows["soc_end_mwh"].iloc[:-1], atol=0.0)
        np.testing.assert_allclose(rows["cumulative_pnl_eur"], rows["pnl_eur"].cumsum(), atol=1e-6)
        days = [date.fromisoformat(d) for d in rows["local_date"]]
        assert all(b - a == timedelta(days=1) for a, b in zip(days, days[1:], strict=False)), name
    summary = json.loads((RECORD / "summary.json").read_text(encoding="utf-8"))
    assert summary["schema"] == tr.SCHEMA and summary["last_day"] == max(daily["local_date"])
    for name, entry in summary["strategies"].items():
        recorded = daily[daily["strategy"] == name]
        assert entry["days"] == len(recorded)
        if len(recorded):
            assert entry["pnl_eur"] == pytest.approx(float(recorded["pnl_eur"].sum()), abs=0.01)
    readme = (RECORD / "README.md").read_text(encoding="utf-8")
    assert "cumulative_pnl.png" in readme and (RECORD / "cumulative_pnl.png").is_file()
    assert readme == tr.render_readme(summary, daily)  # the README is exactly what the script generates


@pytest.mark.slow
def test_track_record_days_reproduce_the_committed_backtest_ledger(tr: ModuleType, tmp_path: Path) -> None:
    """Started from the backtest's own SOC, a recorded day equals the continuous backtest (float32 storage)."""
    ledger = pd.read_parquet(REPO_ROOT / "reports" / "ledgers" / "realtime_threshold_price_current_C.parquet")
    source = tr.PublicForwardSource(raw_dir=tmp_path / "raw", offline=True)
    strategies = tr.StrategyCache(None)
    for day in pd.date_range("2026-08-12", "2026-10-24", freq="D").date:
        d0, d1 = tr.day_bounds(day)
        start_soc = float(ledger.loc[ledger["timestamp_utc"] < d0, "soc_end_mwh"].iloc[-1])
        window = source.load(d0 - pd.Timedelta(days=tr.WARMUP_DAYS), d1)
        qh = tr.run_day(window.market, day, "threshold", start_soc, strategies)
        expected = ledger[(ledger["timestamp_utc"] >= d0) & (ledger["timestamp_utc"] < d1)]
        np.testing.assert_allclose(qh["energy_mwh"], expected["energy_mwh"].astype(float), atol=1e-5, err_msg=str(day))
