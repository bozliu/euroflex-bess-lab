"""Nordic public-data layer: normalisers, cached fetching (mocked HTTP), offline build, loaders on fixtures."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from euroflex_bess_lab.public_data.http import HttpClient, HttpConfig, sha256_file
from euroflex_bess_lab.public_data.nordic import (
    NordicContext,
    build_tables,
    energinet,
    load_dk_imbalance,
    load_nordic_day_ahead,
    load_nordic_reserve,
    svk,
)
from euroflex_bess_lab.public_data.nordic.normalize import (
    dk_imbalance_rule_check,
    fix_fcr_dst_labels,
    normalize_afrr,
    normalize_dk_imbalance,
    normalize_energinet_day_ahead,
    normalize_fcr_nordic,
    normalize_mfrr,
    normalize_svk_day_ahead,
)
from euroflex_bess_lab.public_data.nordic.sources import ENERGINET_DATASETS, fetch_hint

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "nordic" / "public"
LOCAL_PREFIX = "/" + "Users" + "/"


# --- normalisers --------------------------------------------------------------------------------------------------


def test_day_ahead_splices_hourly_elspot_and_quarter_hour_day_ahead_at_go_live() -> None:
    elspot = pd.DataFrame(
        {
            "HourUTC": ["2025-09-30T20:00:00", "2025-09-30T21:00:00", "2025-09-30T21:00:00"],
            "PriceArea": ["SE3", "SE3", "DK1"],
            "SpotPriceEUR": [40.0, 41.0, 99.0],
        }
    )
    quarter = pd.DataFrame(
        {
            "TimeUTC": ["2025-09-30T21:45:00", "2025-09-30T22:00:00", "2025-09-30T22:15:00"],
            "PriceArea": ["SE3", "SE3", "SE3"],
            "DayAheadPriceEUR": [1.0, 50.0, 51.0],
        }
    )
    frame, stats = normalize_energinet_day_ahead(elspot, quarter, "SE3")
    assert frame["timestamp_utc"].dt.tz is not None and str(frame["timestamp_utc"].dt.tz) == "UTC"
    assert frame["resolution_minutes"].tolist() == [60, 60, 15, 15]
    assert frame["price_eur_mwh"].tolist() == [40.0, 41.0, 50.0, 51.0]  # the 21:45 quarter is before go-live
    assert stats == {"null_prices_dropped": 0, "duplicates_dropped": 0}


def test_svk_day_ahead_resolution_follows_the_sdac_era_and_drops_off_grid_rows() -> None:
    raw = pd.DataFrame(
        {
            "start_time_utc": [
                "2025-09-30T20:00:00",
                "2025-09-30T20:15:00",
                "2025-09-30T22:00:00",
                "2025-09-30T22:45:00",
            ],
            "bidding_zone": ["FI"] * 4,
            "price": [10.0, 11.0, 12.0, 13.0],
            "price_unit": ["EUR-MWh"] * 4,
        }
    )
    frame, stats = normalize_svk_day_ahead(raw, "FI")
    assert frame["resolution_minutes"].tolist() == [60, 15, 15]
    assert stats["off_grid_rows_dropped"] == 1
    with pytest.raises(ValueError, match="units"):
        normalize_svk_day_ahead(raw.assign(price_unit="SEK-MWh"), "FI")


def _fcr_rows(times: list[tuple[str, str]], auction: str = "D-1 late") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "HourUTC": [t[0] for t in times],
            "HourDK": [t[1] for t in times],
            "PriceArea": "DK2",
            "ProductName": "FCR-N",
            "AuctionType": auction,
            "PurchasedVolumeLocal": 1.0,
            "PurchasedVolumeTotal": 10.0,
            "PriceTotalEUR": np.arange(len(times), dtype=float) + 20.0,
        }
    )


def test_fcr_dst_2024_labelling_error_is_repaired() -> None:
    # Energinet: HourUTC 2024-10-27 00:00 is published as 01:00 and 01:00 as 01:01 (both labelled HourDK 02:xx).
    raw = _fcr_rows(
        [
            ("2024-10-26T23:00:00", "2024-10-27T01:00:00"),
            ("2024-10-27T01:00:00", "2024-10-27T02:00:00"),
            ("2024-10-27T01:01:00", "2024-10-27T02:01:00"),
            ("2024-10-27T02:00:00", "2024-10-27T03:00:00"),
        ]
    )
    fixed, repaired = fix_fcr_dst_labels(raw)
    assert repaired == 2
    assert [f"{t:%H:%M}" for t in fixed["_ts"]] == ["23:00", "00:00", "01:00", "02:00"]
    frame, stats = normalize_fcr_nordic(raw)
    assert frame["delivery_start_utc"].is_unique and stats["dst_label_rows_repaired"] == 2
    assert (frame["delivery_end_utc"] - frame["delivery_start_utc"] == pd.Timedelta(hours=1)).all()


def test_fcr_products_auctions_and_directions_are_mapped() -> None:
    rows = []
    for product in ("FCR-N", "FCR-D upp", "FCR-D ned", "FFR"):
        for auction in ("D-1 early", "D-1 late", "Total"):
            rows.append(
                {
                    "HourUTC": "2025-06-10T10:00:00",
                    "HourDK": "2025-06-10T12:00:00",
                    "PriceArea": "SE3",
                    "ProductName": product,
                    "AuctionType": auction,
                    "PurchasedVolumeLocal": 5.0,
                    "PurchasedVolumeTotal": 50.0,
                    "PriceTotalEUR": 7.5,
                }
            )
    frame, stats = normalize_fcr_nordic(pd.DataFrame(rows))
    assert set(frame["product"]) == {"fcr_n", "fcr_d_up", "fcr_d_down"}
    assert set(frame["auction"]) == {"early", "late", "total"}
    assert dict(zip(frame["product"], frame["direction"], strict=True)) == {
        "fcr_n": "symmetric",
        "fcr_d_up": "up",
        "fcr_d_down": "down",
    }
    assert stats["unknown_products_dropped"] == ["FFR"]


def test_afrr_and_mfrr_are_long_format_with_sources() -> None:
    raw = pd.DataFrame(
        {
            "TimeUTC": ["2025-06-10T10:00:00", "2025-06-10T11:00:00"],
            "PriceArea": ["FI", "FI"],
            "UpDemandMW": [None, None],
            "UpProcuredMW": [10.0, 12.0],
            "UpPriceEUR": [5.0, 6.0],
            "DownDemandMW": [None, None],
            "DownProcuredMW": [8.0, 9.0],
            "DownPriceEUR": [3.0, 4.0],
        }
    )
    afrr, _ = normalize_afrr(raw)
    assert len(afrr) == 4 and set(afrr["direction"]) == {"up", "down"} and set(afrr["source"]) == {"energinet"}
    assert afrr["price_eur_per_mw_h"].dtype == np.float32
    svk_raw = pd.DataFrame(
        {
            "start_time_utc": pd.to_datetime(["2025-06-10T10:00:00Z", "2025-06-10T10:00:00Z"], utc=True),
            "bidding_zone": ["SE3", "SE3"],
            "reserve_product": ["mFRRCapacityMarket"] * 2,
            "reserve_direction": ["up", "down"],
            "price": [30.0, 2.0],
            "price_unit": ["EUR-MW"] * 2,
            "volume": [100.0, 50.0],
            "volume_unit": ["MW"] * 2,
        }
    )
    mfrr, stats = normalize_mfrr(raw.assign(PriceArea="DK2"), {"SE3": svk_raw})
    assert set(mfrr["zone"]) == {"DK2", "SE3"} and set(mfrr["source"]) == {"energinet", "svk"}
    assert stats["per_zone"]["SE3"]["rows"] == 2
    with pytest.raises(ValueError, match="units"):
        normalize_mfrr(pd.DataFrame(), {"SE3": svk_raw.assign(price_unit="SEK-MW")})


def test_dk_imbalance_hourly_then_quarter_hour_and_rule_check() -> None:
    hourly = pd.DataFrame(
        {
            "HourUTC": ["2025-03-04T10:00:00", "2025-03-04T11:00:00"],
            "PriceArea": ["DK1", "DK1"],
            "mFRRUpActBal": [0.0, 0.0],
            "mFRRDownActBal": [1.0, 2.0],
            "ImbalanceMWh": [-1.0, -2.0],
            "ImbalancePriceEUR": [10.0, 11.0],
            "BalancingPowerPriceUpEUR": [12.0, 13.0],
            "BalancingPowerPriceDownEUR": [9.0, 8.0],
        }
    )
    quarter = pd.DataFrame(
        {
            "TimeUTC": ["2025-03-04T11:15:00", "2025-03-04T11:30:00", "2025-03-04T11:45:00", "2025-03-04T12:00:00"],
            "PriceArea": ["DK1"] * 4,
            "SatisfiedDemand": [-5.0, 5.0, 0.0, 5.0],
            "ImbalancePriceEUR": [40.0, 80.0, 50.0, 70.0],
            "SpotPriceEUR": [50.0] * 4,
            "DominatingDirection": [-1, 1, 0, 1],
            "aFRRUpMW": [0.0, 1.0, 0.0, 0.0],
            "aFRRVWAUpEUR": [0.0, 80.0, 0.0, 0.0],
            "aFRRDownMW": [-1.0, 0.0, 0.0, 0.0],
            "aFRRVWADownEUR": [40.0, 0.0, 0.0, 0.0],
            "mFRRMarginalPriceUpEUR": [60.0, 70.0, 50.0, 70.0],
            "mFRRMarginalPriceDownEUR": [45.0, 50.0, 50.0, 50.0],
        }
    )
    cutover = pd.Timestamp("2025-03-04T11:15:00Z")
    frame, stats = normalize_dk_imbalance(quarter, hourly, cutover)
    assert frame["resolution_minutes"].tolist() == [60, 15, 15, 15, 15]
    assert stats["rows_per_resolution"] == {"15": 4, "60": 1}
    check = dk_imbalance_rule_check(frame)
    assert check["rows_checked"] == 4
    assert check["share_matching_rule_idle_afrr_as_missing"] == 1.0
    assert check["share_matching_rule"] == 1.0


# --- fetching (mocked HTTP) ---------------------------------------------------------------------------------------


class _Response:
    def __init__(self, status: int, body: bytes, headers: dict[str, str] | None = None) -> None:
        self.status_code = status
        self._body = body
        self.headers = headers or {}
        self.text = body.decode("utf-8", errors="replace")

    def iter_content(self, chunk_size: int = 1 << 20) -> Any:
        yield self._body

    def close(self) -> None:
        return None

    def json(self) -> Any:
        return json.loads(self._body)


class _Session:
    def __init__(self, responses: list[_Response]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict[str, str] | None]] = []

    def get(self, url: str, params: dict[str, str] | None = None, **_: Any) -> _Response:
        self.calls.append((url, params))
        return self.responses.pop(0)


def test_energinet_chunks_are_yearly_or_whole_range_and_requests_are_utc() -> None:
    spec = ENERGINET_DATASETS["FcrNdDK2"]
    windows = energinet.dataset_chunks(spec, pd.Timestamp("2023-12-01", tz="UTC"), pd.Timestamp("2025-02-01", tz="UTC"))
    assert [(f"{a:%Y-%m-%d}", f"{b:%Y-%m-%d}") for a, b in windows] == [
        ("2023-12-01", "2024-01-01"),
        ("2024-01-01", "2025-01-01"),
        ("2025-01-01", "2026-01-01"),
    ]
    elspot = ENERGINET_DATASETS["Elspotprices"]
    assert (
        len(
            energinet.dataset_chunks(elspot, pd.Timestamp("2023-12-01", tz="UTC"), pd.Timestamp("2026-10-01", tz="UTC"))
        )
        == 1
    )
    params = energinet.chunk_params(spec, *windows[1])
    assert params["timezone"] == "UTC" and params["limit"] == "0"
    assert json.loads(params["filter"]) == {"PriceArea": ["DK2", "SE3", "SE4"]}
    assert "PriceTotalEUR" in params["columns"].split(",")


def test_energinet_fetch_honours_retry_after_and_caches(tmp_path: Path) -> None:
    payload = json.dumps({"total": 1, "records": [{"HourUTC": "2025-01-01T00:00:00"}]}).encode()
    session = _Session([_Response(429, b"{}", {"Retry-After": "216"}), _Response(200, payload)])
    sleeps: list[float] = []
    client = HttpClient(config=HttpConfig(max_retries=2, backoff_cap_s=900.0), session=session, sleep=sleeps.append)
    month = pd.Timestamp("2025-01-01", tz="UTC")
    chunk = energinet.fetch_chunk(client, tmp_path, "FcrDK1", month, pd.Timestamp("2026-01-01", tz="UTC"))
    assert sleeps == [216.0]  # the server's Retry-After is honoured, not capped at a short backoff
    assert chunk.path.name == "FcrDK1_2025.json" and chunk.path.exists()
    record = json.loads(chunk.path.with_name(chunk.path.name + ".source.json").read_text())
    assert record["params"]["timezone"] == "UTC" and LOCAL_PREFIX not in json.dumps(record)
    again = energinet.fetch_chunk(client, tmp_path, "FcrDK1", month, pd.Timestamp("2026-01-01", tz="UTC"), offline=True)
    assert again.record.from_cache and len(session.calls) == 2
    bad = tmp_path / "bad.json"
    bad.write_text('{"no_records": []}')
    with pytest.raises(Exception, match="records"):
        energinet.validate_energinet_json(bad)


def test_svk_dump_is_validated_and_resource_resolution_falls_back(tmp_path: Path) -> None:
    csv = b"start_time_utc,bidding_zone,price,price_unit\n2025-06-10T10:00:00,FI,20.5,EUR-MWh\n"

    class _Broken:
        def get(self, *_: Any, **__: Any) -> Any:
            raise ConnectionError("offline")

    broken = HttpClient(config=HttpConfig(max_retries=0), session=_Broken(), sleep=lambda s: None)
    assert svk.resolve_resource_id(broken, svk.SVK_RESOURCES["day_ahead"]) == svk.SVK_RESOURCES["day_ahead"].resource_id
    session = _Session([_Response(200, csv)])
    client = HttpClient(config=HttpConfig(max_retries=0), session=session, sleep=lambda s: None)
    dump = svk.fetch_dump(
        client, tmp_path, "day_ahead", "FI", snapshot_end_utc=pd.Timestamp("2026-01-01", tz="UTC"), resource_id="abc"
    )
    assert session.calls[0][0].endswith("/datastore/dump/abc")
    assert json.loads(session.calls[0][1]["filters"]) == {"bidding_zone": "FI"}
    frame = svk.read_dump(dump)
    assert frame["start_time_utc"].dt.tz is not None and frame["price"].iloc[0] == 20.5
    validator = svk.make_csv_validator(("start_time_utc", "price", "volume"))
    with pytest.raises(Exception, match="lacks columns"):
        validator(dump.path)


# --- offline build from a synthetic raw tree ----------------------------------------------------------------------


def _write_raw(raw: Path, dataset: str, name: str, records: list[dict[str, Any]]) -> None:
    path = raw / "nordic" / "energinet" / dataset / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"total": len(records), "records": records}), encoding="utf-8")


def test_offline_build_writes_zstd_tables_with_cc_by_sidecars(tmp_path: Path) -> None:
    raw, root = tmp_path / "raw", tmp_path / "public"
    hours = pd.date_range("2025-06-09T22:00Z", periods=48, freq="1h")
    stamps = [f"{h:%Y-%m-%dT%H:%M:%S}" for h in hours]
    _write_raw(
        raw,
        "Elspotprices",
        "Elspotprices_20231201_20251001.json",
        [
            {"HourUTC": s, "PriceArea": z, "SpotPriceEUR": 40.0 + i % 24}
            for i, s in enumerate(stamps)
            for z in ("DK1", "SE3")
        ],
    )
    _write_raw(raw, "DayAheadPrices", "DayAheadPrices_2025.json", [])
    _write_raw(raw, "DayAheadPrices", "DayAheadPrices_2026.json", [])
    _write_raw(
        raw,
        "AfrrReservesNordic",
        "AfrrReservesNordic_2025.json",
        [
            {
                "TimeUTC": s,
                "PriceArea": z,
                "UpDemandMW": None,
                "UpProcuredMW": 10.0,
                "UpPriceEUR": 5.0,
                "DownDemandMW": None,
                "DownProcuredMW": 10.0,
                "DownPriceEUR": 4.0,
            }
            for s in stamps
            for z in ("FI", "SE3", "NO1")
        ],
    )
    for year in (2023, 2024, 2026):
        _write_raw(raw, "AfrrReservesNordic", f"AfrrReservesNordic_{year}.json", [])
    ctx = NordicContext(
        root=root,
        raw_dir=raw,
        client=HttpClient(config=HttpConfig(max_retries=0), session=_Session([]), sleep=lambda s: None),
        offline=True,
        start_utc=pd.Timestamp("2023-12-01", tz="UTC"),
        end_utc=pd.Timestamp("2025-06-11T22:00Z"),
    )
    from euroflex_bess_lab.public_data.nordic.build import build_afrr, build_day_ahead

    written = build_day_ahead(ctx, zones=("DK1",)) + build_afrr(ctx)
    paths = {w.relpath for w in written}
    assert paths == {"nordic/da_prices_DK1.parquet", "nordic/afrr_capacity.parquet"}
    for item in written:
        meta = json.loads((root / item.relpath.replace(".parquet", ".meta.json")).read_text())
        assert meta["licence"] == "CC BY 4.0" and meta["redistributed"] is True
        assert meta["attribution"].startswith("Source: Energinet")
        assert meta["sha256"] == sha256_file(root / item.relpath)
        assert pq.ParquetFile(root / item.relpath).metadata.row_group(0).column(0).compression == "ZSTD"
        assert LOCAL_PREFIX not in json.dumps(meta)
    afrr = pd.read_parquet(root / "nordic/afrr_capacity.parquet")
    assert set(afrr["zone"]) == {"FI", "SE3"}  # NO1 is outside the modelled zones
    assert afrr["delivery_start_utc"].max() < ctx.end_utc
    da = pd.read_parquet(root / "nordic/da_prices_DK1.parquet")
    assert len(da) == 48 and (da["resolution_minutes"] == 60).all()
    with pytest.raises(ValueError, match="Unknown Nordic tables"):
        build_tables(ctx, ["nonsense"])


# --- loaders on the committed fixture tree ------------------------------------------------------------------------


def test_day_ahead_loader_expands_hourly_products_on_fixture_tree() -> None:
    hourly = load_nordic_day_ahead("SE3", "2025-06-10", "2025-06-11", root=FIXTURES)
    assert len(hourly) == 24 and (hourly["resolution_minutes"] == 60).all()
    quarter = load_nordic_day_ahead("SE3", "2025-06-10", "2025-06-11", quarter_hourly=True, root=FIXTURES)
    assert len(quarter) == 96
    assert (quarter["price_eur_mwh"].to_numpy()[:4] == hourly["price_eur_mwh"].iloc[0]).all()
    mtu = load_nordic_day_ahead("DK1", "2025-09-30T21:00Z", "2025-09-30T23:00Z", quarter_hourly=True, root=FIXTURES)
    assert mtu["resolution_minutes"].tolist() == [60] * 4 + [15] * 4
    with pytest.raises(ValueError, match="bidding zone"):
        load_nordic_day_ahead("NO2", "2025-06-10", "2025-06-11", root=FIXTURES)
    with pytest.raises(ValueError, match="half-open"):
        load_nordic_day_ahead("SE3", "2025-06-11", "2025-06-10", root=FIXTURES)


def test_reserve_loader_filters_products_zones_auctions_and_directions() -> None:
    early = load_nordic_reserve("fcr_d_up", "SE3", "2025-06-10", "2025-06-11", root=FIXTURES)
    assert len(early) == 24 and set(early["auction"]) == {"early"} and set(early["direction"]) == {"up"}
    both = load_nordic_reserve("fcr_n", "DK2", "2025-06-10", "2025-06-11", auction=None, root=FIXTURES)
    assert set(both["auction"]) == {"early", "late", "total"}
    dk1 = load_nordic_reserve("fcr", "DK1", "2025-06-10", "2025-06-11", root=FIXTURES)
    assert len(dk1) == 24 and set(dk1["direction"]) == {"symmetric"}
    afrr = load_nordic_reserve("afrr", "FI", "2025-06-10", "2025-06-11", direction="down", root=FIXTURES)
    assert len(afrr) == 24 and set(afrr["direction"]) == {"down"}
    mfrr = load_nordic_reserve("mfrr", "SE4", "2025-06-10", "2025-06-11", root=FIXTURES)
    assert set(mfrr["source"]) == {"svk"}
    with pytest.raises(ValueError, match="only published for DK1"):
        load_nordic_reserve("fcr", "SE3", "2025-06-10", "2025-06-11", root=FIXTURES)
    with pytest.raises(ValueError, match="FCR_NORDIC|DK2"):
        load_nordic_reserve("fcr_n", "FI", "2025-06-10", "2025-06-11", root=FIXTURES)
    with pytest.raises(ValueError, match="Unknown Nordic reserve product"):
        load_nordic_reserve("ffr", "DK2", "2025-06-10", "2025-06-11", root=FIXTURES)


def test_imbalance_loader_and_missing_file_hint(tmp_path: Path) -> None:
    frame = load_dk_imbalance("DK2", "2025-06-10", "2025-06-11", root=FIXTURES)
    assert len(frame) == 96 and set(frame["area"]) == {"DK2"} and (frame["resolution_minutes"] == 15).all()
    with pytest.raises(FileNotFoundError) as excinfo:
        load_dk_imbalance("DK1", "2025-06-10", "2025-06-11", root=tmp_path)
    assert fetch_hint("dk_imbalance") in str(excinfo.value)


def test_fixture_sidecars_match_files() -> None:
    sidecars = sorted((FIXTURES / "nordic").glob("*.meta.json"))
    assert len(sidecars) == 10
    for path in sidecars:
        meta = json.loads(path.read_text(encoding="utf-8"))
        assert meta["sha256"] == sha256_file(FIXTURES / meta["path"])
        assert meta["licence"] == "CC BY 4.0" and meta["redistributed"] is True


def test_licence_section_lists_nordic_files_per_publisher() -> None:
    from euroflex_bess_lab.public_data.nordic.docs import render_nordic_licence_section
    from euroflex_bess_lab.public_data.writers import collect_sidecars

    section = render_nordic_licence_section({"files": collect_sidecars(FIXTURES)})
    energinet_part, svk_part = section.split("Svenska kraftnät (SVK Data Service), Sweden")
    assert "`nordic/fcr_nordic.parquet`" in energinet_part and "CC BY 4.0" in energinet_part
    assert "`nordic/da_prices_FI.parquet`" in svk_part and "`nordic/mfrr_capacity.parquet`" in svk_part
    assert LOCAL_PREFIX not in section
