"""Offline end-to-end builds from a tiny synthetic raw cache (no network)."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.public_data.build import BuildContext, as_of_snapshot, build_target, snapshot_end_utc
from euroflex_bess_lab.public_data.normalize import MINUTE_COLUMNS, NEW_FORMULA_MAP, QH_COLUMNS
from euroflex_bess_lab.public_data.writers import build_manifest, sidecar_path, verify_manifest

START = pd.Timestamp("2026-03-02T00:00:00Z")
END = pd.Timestamp("2026-03-03T00:00:00Z")


def _write_raw(raw: Path, dataset_id: str, label: str, frame: pd.DataFrame) -> None:
    path = raw / "elia" / dataset_id / f"{dataset_id}__{label}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _qh_raw() -> dict[str, pd.DataFrame]:
    ts = pd.Series(pd.date_range(START, END, freq="15min", tz="UTC", inclusive="left"))
    n = len(ts)
    price = np.linspace(50, 150, n)
    return {
        "ods134": pd.DataFrame(
            {
                "datetime": ts,
                "resolutioncode": "PT15M",
                "qualitystatus": "Validated",
                "ace": 0.0,
                "systemimbalance": np.linspace(-100, 100, n),
                "alpha": 0.0,
                "alpha_prime": 0.0,
                "marginalincrementalprice": price,
                "marginaldecrementalprice": price - 10,
                "imbalanceprice": price,
            }
        ),
        "ods166": pd.DataFrame({"datetime": ts, "afrrpriceup": price, "floorprice": 40.0, "cap": 200.0}),
        "ods127": pd.DataFrame({"datetime": ts, "afrrvolumeup": 30.0, "afrrvolumedown": 5.0}),
    }


def _minute_raw(qh: pd.DataFrame) -> dict[str, pd.DataFrame]:
    ts = pd.Series(pd.date_range(START, END, freq="1min", tz="UTC", inclusive="left"))
    qh_start = ts.dt.floor("15min")
    final = qh.set_index("datetime")["imbalanceprice"].reindex(qh_start).to_numpy()
    minute_in_qh = ((ts - qh_start).dt.total_seconds() // 60).to_numpy()
    price = np.where(minute_in_qh == 14, final, final + 3.0)  # minute 14 equals the final QH price
    common = {"datetime": ts, "resolutioncode": "PT1M", "quarterhour": qh_start, "qualitystatus": "NotValidated"}
    return {
        "ods133": pd.DataFrame(
            {
                **common,
                "ace": 0.0,
                "systemimbalance": -10.0,
                "alpha": 0.0,
                "alpha_prime": 0.0,
                "marginalincrementalprice": price,
                "marginaldecrementalprice": price - 10,
                "imbalanceprice": price,
            }
        ),
        "ods126": pd.DataFrame({"datetime": ts, "systemimbalance": -12.0, "ace": 1.0, "afrrvolumeup": 20.0}),
        "ods165": pd.DataFrame({**common, "r2priceup": price, "floorprice": 40.0, "cap": 200.0}),
    }


@pytest.fixture
def raw_cache(tmp_path: Path) -> Path:
    raw = tmp_path / "raw"
    qh = _qh_raw()
    for dataset_id, frame in qh.items():
        _write_raw(raw, dataset_id, "2026-01-01__2027-01-01", frame)
    for dataset_id, frame in _minute_raw(qh["ods134"]).items():
        _write_raw(raw, dataset_id, "2026-03-01__2026-04-01", frame)
        # Year partitions are always rebuilt whole: the other months of 2026 exist (empty) in the cache.
        for label in ("2026-01-01__2026-02-01", "2026-02-01__2026-03-01"):
            _write_raw(raw, dataset_id, label, frame.iloc[0:0])
    nf_ts = pd.Series(pd.date_range(START, END, freq="15min", tz="UTC", inclusive="left"))
    nf = pd.DataFrame({"datetime": nf_ts, "resolutioncode": "PT15M", "qualitystatus": "NotValidated"})
    for upstream in NEW_FORMULA_MAP:
        if upstream != "qualitystatus":
            nf[upstream] = 1.0
    nf["incentivizingcomponent"] = np.nan
    _write_raw(raw, "ods212", "all", nf.drop(columns=["frcerolling", "systemimbalancerolling"]))
    minute_ts = pd.Series(pd.date_range(START, END, freq="1min", tz="UTC", inclusive="left"))
    nf_min = pd.DataFrame({"datetime": minute_ts, "resolutioncode": "PT1M", "qualitystatus": "NotValidated"})
    for upstream in NEW_FORMULA_MAP:
        if upstream != "qualitystatus":
            nf_min[upstream] = 1.0
    _write_raw(raw, "ods211", "all", nf_min)
    return raw


def _ctx(tmp_path: Path, raw: Path) -> BuildContext:
    return BuildContext(root=tmp_path / "public", raw_dir=raw, offline=True, now=pd.Timestamp("2026-03-05T00:00:00Z"))


def test_snapshot_boundaries_are_brussels_midnights() -> None:
    assert snapshot_end_utc(pd.Timestamp("2026-10-26T03:00:00Z")) == pd.Timestamp("2026-10-25T23:00:00Z")
    assert snapshot_end_utc(pd.Timestamp("2026-07-01T21:59:00Z")) == pd.Timestamp("2026-06-30T22:00:00Z")
    assert as_of_snapshot("2026-10-26") == pd.Timestamp("2026-10-25T23:00:00Z")


def test_offline_build_of_qh_and_minute_tables(tmp_path: Path, raw_cache: Path) -> None:
    ctx = _ctx(tmp_path, raw_cache)
    written = build_target(ctx, "belgium-qh", start="2026-03-01", end="2026-04-01")
    written += build_target(ctx, "belgium-minute", start="2026-03-01", end="2026-04-01")
    assert [w.relpath for w in written] == ["belgium/qh_2026.parquet", "belgium/minute_2026.parquet"]
    qh = pd.read_parquet(written[0].path)
    minute = pd.read_parquet(written[1].path)
    assert list(qh.columns) == QH_COLUMNS and len(qh) == 96
    assert list(minute.columns) == MINUTE_COLUMNS and len(minute) == 1440
    sidecar = json.loads(sidecar_path(written[1].path).read_text(encoding="utf-8"))
    assert sidecar["licence"] == "CC BY 4.0" and sidecar["attribution"] == "Elia Transmission Belgium SA"
    assert sidecar["dataset_ids"] == ["ods133", "ods126", "ods165"]
    assert (
        sidecar["coverage_start_utc"] == "2026-03-02T00:00:00Z"
        and sidecar["coverage_end_utc"] == "2026-03-03T00:00:00Z"
    )
    assert sidecar["snapshot_end_utc"] == "2026-03-05T00:00:00Z"
    assert sidecar["fetch_command"] == "euroflex data fetch belgium-minute --start 2026-01-01 --end 2027-01-01"
    check = sidecar["validation"]["last_minute_vs_qh"]["price_cum_eur_mwh_vs_imbalance_price_eur_mwh"]
    assert check["match_rate"] == pytest.approx(1.0) and check["compared"] == 96
    assert all(url.startswith("https://opendata.elia.be/") for url in sidecar["source_urls"])
    manifest = build_manifest(ctx.root)
    ok, issues = verify_manifest(ctx.root, manifest)
    assert issues == [] and len(ok) == 2


def test_offline_build_of_new_formula_tables(tmp_path: Path, raw_cache: Path) -> None:
    written = build_target(_ctx(tmp_path, raw_cache), "belgium-new-formula")
    assert sorted(w.relpath for w in written) == [
        "belgium/new_formula_minute_2026.parquet",
        "belgium/new_formula_qh_2026.parquet",
    ]
    by_path = {w.relpath: w for w in written}
    qh_meta = by_path["belgium/new_formula_qh_2026.parquet"].sidecar
    assert qh_meta["simulated"] is True
    assert qh_meta["validation"]["decomposition"]["price_equals_main_by_si_sign_plus_incentive"] == pytest.approx(1.0)
    minute_meta = by_path["belgium/new_formula_minute_2026.parquet"].sidecar
    assert minute_meta["validation"]["last_minute_vs_ods212"]["qh_compared"] == 96


def test_offline_build_fails_cleanly_without_cache(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path, tmp_path / "empty_raw")
    with pytest.raises(FileNotFoundError, match="offline"):
        build_target(ctx, "belgium-qh", start="2026-03-01", end="2026-04-01")
    with pytest.raises(ValueError, match="Unknown target"):
        build_target(ctx, "belgium-everything")
