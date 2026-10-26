"""GB public-data layer: parsers on real payload excerpts, loaders on the fixture tree, and a full build with a
fake HTTP session (no network; outputs only under ``tmp_path``)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.public_data.gb import (
    GB_TABLES,
    ecb,
    elexon,
    load_gb_frequency,
    load_gb_fx,
    load_gb_market_index,
    load_gb_reserve_auctions,
    load_gb_response_auctions,
    load_gb_response_energy,
    load_gb_system_prices,
    neso,
)
from euroflex_bess_lab.public_data.gb.build import GBBuildContext, build_gb
from euroflex_bess_lab.public_data.gb.sources import (
    BMRS_LICENCE_URL,
    NESO_ATTRIBUTION,
    NESO_LICENCE,
    bmrs_attribution,
)
from euroflex_bess_lab.public_data.http import HttpClient, HttpConfig

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "gb"
PUBLIC = FIXTURES / "public"
RAW = FIXTURES / "raw"
LOCAL_PREFIX = "/" + "Users" + "/"


def _json(name: str) -> Any:
    return json.loads((RAW / name).read_text(encoding="utf-8"))


# ------------------------------------------------------------------------------------------------- parsers


def test_system_prices_single_price_niv_and_publication_lag() -> None:
    frame = elexon.parse_system_prices([_json("system_prices_2025-06-10.json")])
    assert len(frame) == 48
    assert frame["timestamp_utc"].iloc[0] == pd.Timestamp("2025-06-09T23:00Z")
    assert frame["settlement_period"].tolist() == list(range(1, 49))
    assert (frame["system_sell_price_gbp_mwh"] == frame["system_buy_price_gbp_mwh"]).all()
    assert np.allclose(frame["system_price_gbp_mwh"], frame["system_sell_price_gbp_mwh"])
    assert frame["timestamp_utc"].is_monotonic_increasing
    lag = frame["publication_lag_minutes"]
    assert (lag > 0).all()  # published after the SP ends
    assert frame["system_price_gbp_mwh"].dtype == np.float32


def test_system_prices_on_the_long_day_have_50_periods() -> None:
    frame = elexon.parse_system_prices([_json("system_prices_2025-10-26.json")])
    assert len(frame) == 50
    assert frame["settlement_period"].max() == 50
    assert frame["timestamp_utc"].diff().dropna().eq(pd.Timedelta(minutes=30)).all()


def test_market_index_treats_zero_volume_provider_rows_as_missing() -> None:
    frame = elexon.parse_market_index([_json("mid_2025-06-10.json")])
    assert len(frame) == 48
    assert frame["n2ex_price_gbp_mwh"].isna().all()  # N2EX reports price 0 / volume 0
    assert np.allclose(frame["mid_price_gbp_mwh"], frame["apx_price_gbp_mwh"])
    rows = [
        {"startTime": "2025-06-10T00:00:00Z", "dataProvider": "APXMIDP", "price": 50.0, "volume": 100.0},
        {"startTime": "2025-06-10T00:00:00Z", "dataProvider": "N2EXMIDP", "price": 80.0, "volume": 300.0},
        {"startTime": "2025-06-10T00:30:00Z", "dataProvider": "APXMIDP", "price": 0.0, "volume": 0.0},
    ]
    frame = elexon.parse_market_index([rows])
    assert frame["mid_price_gbp_mwh"].iloc[0] == pytest.approx(72.5)  # volume weighted
    assert np.isnan(frame["mid_price_gbp_mwh"].iloc[1])


def test_frequency_parser_is_lossless_and_drops_invalid_samples() -> None:
    payload = _json("frequency_2025-06-10T12.json")
    frame, stats = elexon.parse_frequency_with_stats(
        [payload + [{"measurementTime": "2025-06-10T13:00:00Z", "frequency": 0.0}]]
    )
    assert stats["invalid_samples_dropped"] == 1
    assert len(frame) == len(payload)
    assert frame["frequency_dev_mhz"].dtype == np.int16
    original = np.array([r["frequency"] for r in payload], dtype=float)
    assert np.allclose(50.0 + frame["frequency_dev_mhz"].to_numpy() / 1000.0, original, atol=1e-9)


def test_eac_normalisation_splits_products_dedupes_and_maps_efa_blocks() -> None:
    raw = pd.read_csv(RAW / "eac_results_summary_excerpt.csv")
    response, reserve, stats = neso.normalize_eac([raw, raw])  # duplicated input
    assert stats["duplicates_dropped"] >= len(raw) // 2
    assert set(response["product"]) <= {"DCL", "DCH", "DML", "DMH", "DRL", "DRH"}
    assert set(reserve["service"]) <= {"QR", "SR", "BR"}
    assert not response.duplicated(["product", "delivery_start_utc"]).any()
    day = response[response["efa_date"] == "2025-06-10"]
    assert sorted(set(day["efa_block"])) == [1, 2, 3, 4, 5, 6]
    first = day[day["efa_block"] == 1]["delivery_start_utc"].iloc[0]
    assert first == pd.Timestamp("2025-06-09T22:00Z")  # 23:00 BST, UTC in the source
    assert (day["duration_hours"] == 4.0).all()
    assert response.loc[response["product"] == "DRL", "direction"].eq("low").all()


def test_ecb_parser_and_as_of_join() -> None:
    frame = ecb.parse_ecb_csv((RAW / "ecb_gbp_eur_excerpt.csv").read_text(encoding="utf-8"))
    assert frame["gbp_per_eur"].between(0.7, 1.0).all()
    saturday = pd.DatetimeIndex(["2025-06-07T12:00Z"])
    friday_rate = frame.loc[frame["timestamp_utc"] == pd.Timestamp("2025-06-06", tz="UTC"), "gbp_per_eur"].iloc[0]
    assert ecb.gbp_per_eur_on(saturday, frame)[0] == pytest.approx(friday_rate)


def test_attribution_statement_years() -> None:
    assert bmrs_attribution(2024, 2026) == "Contains BMRS data © Elexon Limited copyright and database right 2024-2026"
    assert bmrs_attribution(2025).endswith("right 2025")


# ------------------------------------------------------------------------------------------------- loaders


def test_loaders_read_the_fixture_tree_with_half_open_bounds() -> None:
    sp = load_gb_system_prices("2025-06-10", "2025-06-11", root=PUBLIC)
    assert len(sp) == 48
    assert sp["timestamp_utc"].min() == pd.Timestamp("2025-06-10T00:00Z")
    assert sp["timestamp_utc"].max() == pd.Timestamp("2025-06-10T23:30Z")
    assert str(sp["timestamp_utc"].dtype) == "datetime64[ns, UTC]"
    mid = load_gb_market_index("2025-06-10", "2025-06-11", ["mid_price_gbp_mwh"], root=PUBLIC)
    assert list(mid.columns) == ["timestamp_utc", "mid_price_gbp_mwh"]
    energy = load_gb_response_energy("2025-06-10", "2025-06-11", root=PUBLIC)
    assert (energy["drl_mwh_per_mw"] >= energy["dml_mwh_per_mw"]).mean() > 0.9  # DR delivers most energy
    auctions = load_gb_response_auctions("2025-06-10", "2025-06-11", products=["dcl"], root=PUBLIC)
    assert set(auctions["product"]) == {"DCL"}
    assert len(load_gb_reserve_auctions("2025-06-10", "2025-06-11", root=PUBLIC)) > 0
    freq = load_gb_frequency("2025-06-10", "2025-06-11", root=PUBLIC)
    assert len(freq) > 5000 and freq["frequency_hz"].between(49.5, 50.5).all()
    assert len(load_gb_fx("2025-06-01", "2025-06-14", root=PUBLIC)) > 5
    with pytest.raises(KeyError):
        load_gb_market_index("2025-06-10", "2025-06-11", ["nope"], root=PUBLIC)
    with pytest.raises(ValueError):
        load_gb_system_prices("2025-06-11", "2025-06-10", root=PUBLIC)


def test_missing_table_names_the_fetch_command(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="gb_fetch_data.py --target system-prices"):
        load_gb_system_prices("2025-06-10", "2025-06-11", root=tmp_path)
    assert "euroflex data gb fetch eac" in GB_TABLES["gb_response_auctions"].fetch_command()


def test_fixture_sidecars_carry_licence_and_no_local_paths() -> None:
    for meta_path in PUBLIC.rglob("*.meta.json"):
        text = meta_path.read_text(encoding="utf-8")
        assert LOCAL_PREFIX not in text
        meta = json.loads(text)
        assert meta["licence"] and meta["attribution"] and meta["redistributed"] is True


# --------------------------------------------------------------------------------------- build (fake HTTP)


class _Response:
    def __init__(self, body: bytes, status: int = 200) -> None:
        self.status_code = status
        self._body = body
        self.headers: dict[str, str] = {}

    @property
    def text(self) -> str:
        return self._body.decode("utf-8", errors="replace")

    def iter_content(self, chunk_size: int = 1) -> Any:
        for i in range(0, len(self._body), max(1, chunk_size)):
            yield self._body[i : i + chunk_size]

    def close(self) -> None:
        return None


class RoutingSession:
    """Serves canned payloads by URL; records every request."""

    def __init__(self, routes: dict[str, bytes]) -> None:
        self.routes = routes
        self.calls: list[str] = []
        self.headers: dict[str, str] = {}

    def get(self, url: str, params: dict[str, str] | None = None, **_: Any) -> _Response:
        full = f"{url}?{urlencode(params)}" if params else url
        self.calls.append(full)
        for key, body in self.routes.items():
            if key in full:
                return _Response(body)
        return _Response(b'{"data": []}')


def _routes() -> dict[str, bytes]:
    sp_day = _json("system_prices_2025-06-10.json")
    mid_day = _json("mid_2025-06-10.json")
    freq_hour = _json("frequency_2025-06-10T12.json")
    eac_csv = (RAW / "eac_results_summary_excerpt.csv").read_bytes()
    package = {
        "success": True,
        "result": {
            "resources": [
                {"id": "res-archive", "name": "NESO Response-Reserve Results Summary FY2025 (Archive)"},
                {"id": "res-live", "name": "NESO Response-Reserve Results Summary"},
                {"id": "res-orders", "name": "NESO Response-Reserve Sell Orders"},
            ]
        },
    }
    return {
        "system-prices/2025-06-10": json.dumps(sp_day).encode(),
        "datasets/MID/stream": json.dumps(mid_day).encode(),
        "system/frequency/stream": json.dumps(freq_hour).encode(),
        "package_show": json.dumps(package).encode(),
        "datastore/dump/res-archive": eac_csv,
        "datastore/dump/res-live": eac_csv,
        "data-api.ecb.europa.eu": (RAW / "ecb_gbp_eur_excerpt.csv").read_bytes(),
    }


def test_build_writes_tables_and_sidecars_from_mocked_http(tmp_path: Path) -> None:
    session = RoutingSession(_routes())
    ctx = GBBuildContext(
        root=tmp_path / "public",
        raw_dir=tmp_path / "raw",
        client=HttpClient(config=HttpConfig(max_retries=0), session=session, sleep=lambda _: None),
        now=pd.Timestamp("2025-06-10T23:00Z"),
        start=pd.Timestamp("2025-06-09T23:00Z"),
    )
    written = build_gb(["all"], ctx)
    paths = {w.relpath for w in written}
    assert {"gb/system_prices.parquet", "gb/market_index.parquet", "gb/response_auctions.parquet"} <= paths
    assert {"gb/frequency_15s_2025.parquet", "gb/response_energy.parquet", "gb/ecb_gbp_per_eur.parquet"} <= paths
    sp_meta = json.loads((tmp_path / "public" / "gb" / "system_prices.meta.json").read_text(encoding="utf-8"))
    assert sp_meta["licence_url"] == BMRS_LICENCE_URL
    assert sp_meta["attribution"].startswith("Contains BMRS data © Elexon Limited")
    assert sp_meta["validation"]["grid"]["missing_intervals"] == 0
    assert sp_meta["validation"]["ssp_ne_sbp_rows"] == 0
    assert sp_meta["rows"] == 48 and len(sp_meta["sha256"]) == 64
    eac_meta = json.loads((tmp_path / "public" / "gb" / "response_auctions.meta.json").read_text(encoding="utf-8"))
    assert eac_meta["licence"] == NESO_LICENCE and eac_meta["attribution"] == NESO_ATTRIBUTION
    assert not any("res-orders" in call for call in session.calls)  # only results-summary resources
    energy = pd.read_parquet(tmp_path / "public" / "gb" / "response_energy.parquet")
    hour = energy[
        (energy["timestamp_utc"] >= pd.Timestamp("2025-06-10T12:00Z"))
        & (energy["timestamp_utc"] < pd.Timestamp("2025-06-10T13:00Z"))
    ]
    assert hour["n_samples"].tolist() == [120, 120]
    assert energy["drl_mwh_per_mw"].isna().sum() == len(energy) - 2  # only the hour with samples is defined
    for meta_path in (tmp_path / "public").rglob("*.meta.json"):
        assert LOCAL_PREFIX not in meta_path.read_text(encoding="utf-8")
    for record in (tmp_path / "raw").rglob("*.source.json"):
        assert LOCAL_PREFIX not in record.read_text(encoding="utf-8")
    # a second offline build reuses the raw cache and reproduces the same table
    offline = GBBuildContext(
        root=tmp_path / "public2",
        raw_dir=tmp_path / "raw",
        client=HttpClient(config=HttpConfig(max_retries=0), session=RoutingSession({}), sleep=lambda _: None),
        offline=True,
        now=ctx.now,
        start=ctx.start,
    )
    again = build_gb(["system-prices"], offline)
    assert again[0].sidecar["sha256"] == sp_meta["sha256"]
