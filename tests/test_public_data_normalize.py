from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.public_data import aggregate, energy_charts, regelleistung
from euroflex_bess_lab.public_data.normalize import (
    MINUTE_COLUMNS,
    NEW_FORMULA_MAP,
    NEW_FORMULA_MINUTE_COLUMNS,
    NEW_FORMULA_QH_COLUMNS,
    QH_COLUMNS,
    RES_LOAD_COLUMNS,
    SI_FORECAST_COLUMNS,
    normalize_legacy,
    normalize_minute,
    normalize_new_formula,
    normalize_qh,
    normalize_res_load,
    normalize_si_forecast,
)

T0 = pd.Timestamp("2026-03-02T00:00:00Z")


def _minutes(n: int, start: pd.Timestamp = T0) -> pd.Series:
    return pd.Series(pd.date_range(start, periods=n, freq="1min", tz="UTC"))


def _ods133(n: int = 30) -> pd.DataFrame:
    ts = _minutes(n)
    return pd.DataFrame(
        {
            "datetime": ts,
            "resolutioncode": "PT1M",
            "quarterhour": ts.dt.floor("15min"),
            "qualitystatus": "NotValidated",
            "ace": np.linspace(-10, 10, n),
            "systemimbalance": np.linspace(-100, 50, n),
            "alpha": 0.0,
            "alpha_prime": 0.0,
            "marginalincrementalprice": np.linspace(80, 120, n),
            "marginaldecrementalprice": 60.0,
            "imbalanceprice": np.linspace(80, 120, n),
        }
    )


def _ods126(n: int = 30) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "datetime": _minutes(n),
            "resolutioncode": "PT1M",
            "qualitystatus": "Non Validated",
            "systemimbalance": np.arange(n, dtype=float),
            "ace": 1.0,
            "igccvolumeup": 2.0,
            "afrrvolumeup": 3.0,
            "mfrrsaup": np.nan,
            "mfrrdaup": np.nan,
            "reserve_sharing_import": np.nan,
            "igccvolumedown": 0.0,
            "afrrvolumedown": 0.5,
            "mfrrsadown": np.nan,
            "mfrrdadown": np.nan,
            "reserve_sharing_export": np.nan,
        }
    )


def _ods165(n: int = 30) -> pd.DataFrame:
    ts = _minutes(n)
    return pd.DataFrame(
        {
            "datetime": ts,
            "resolutioncode": "PT1M",
            "quarterhour": ts.dt.floor("15min"),
            "qualitystatus": "NotValidated",
            "systemimbalance": 0.0,
            "ace": 0.0,
            "marginalincrementalprice": 1.0,
            "floorprice": 82.13,
            "r2priceup": 118.0,
            "upwardavailablemfrrprice": np.nan,
            "marginaldecrementalprice": 60.0,
            "cap": 60.0,
            "r2pricedown": 118.0,
            "downwardavailablemfrrprice": np.nan,
            "intertsopriceimport": np.nan,
            "intertsopriceexport": np.nan,
        }
    )


def test_normalize_minute_maps_contract_columns_and_fills_grid() -> None:
    ods133 = _ods133().drop(index=[5, 6])  # two minutes missing in ODS133 only
    frame, stats = normalize_minute(ods133, _ods126(), _ods165(), T0, T0 + pd.Timedelta(days=1))
    assert list(frame.columns) == MINUTE_COLUMNS
    assert len(frame) == 30 and frame["timestamp_utc"].is_monotonic_increasing and frame["timestamp_utc"].is_unique
    assert str(frame["timestamp_utc"].dtype) == "datetime64[ns, UTC]"
    assert (frame["qh_start_utc"] == frame["timestamp_utc"].dt.floor("15min")).all()
    assert frame["si_cum_mw"].dtype == np.float32 and frame["afrr_price_up_eur_mwh"].dtype == np.float32
    assert frame["si_cum_mw"].isna().sum() == 2 and frame["si_mw"].notna().all()
    assert stats["ods133_missing_minutes"] == 2 and stats["minutes_missing_in_all_sources"] == 0
    assert stats["ods133_quarterhour_mismatches"] == 0
    assert frame.loc[0, "floor_price_eur_mwh"] == pytest.approx(82.13, abs=1e-4)
    assert frame["quality_status"].iloc[0] == "NotValidated"


def test_normalize_minute_clips_to_window() -> None:
    frame, _ = normalize_minute(
        _ods133(), _ods126(), _ods165(), T0 + pd.Timedelta(minutes=10), T0 + pd.Timedelta(minutes=20)
    )
    assert frame["timestamp_utc"].min() == T0 + pd.Timedelta(minutes=10)
    assert frame["timestamp_utc"].max() == T0 + pd.Timedelta(minutes=19)


def test_normalize_qh_merges_three_sources() -> None:
    ts = pd.Series(pd.date_range(T0, periods=8, freq="15min", tz="UTC"))
    ods134 = pd.DataFrame(
        {
            "datetime": ts,
            "qualitystatus": "Validated",
            "ace": 1.0,
            "systemimbalance": -20.0,
            "alpha": 0.0,
            "alpha_prime": 0.0,
            "marginalincrementalprice": 96.6,
            "marginaldecrementalprice": 60.0,
            "imbalanceprice": 96.6,
        }
    )
    ods166 = pd.DataFrame({"datetime": ts, "afrrpriceup": 90.0, "mfrrsaup": np.nan, "floorprice": 82.0, "cap": 60.0})
    ods127 = pd.DataFrame(
        {"datetime": ts.iloc[:6], "systemimbalance": 5.0, "afrrvolumeup": 40.0, "igccvolumedown": 1.0}
    )
    frame, stats = normalize_qh(ods134, ods166, ods127, T0, T0 + pd.Timedelta(days=1))
    assert list(frame.columns) == QH_COLUMNS
    assert len(frame) == 8
    assert stats["ods127_missing_qh"] == 2
    assert frame["si_mw"].iloc[0] == pytest.approx(-20.0)  # ODS127's SI duplicate is dropped
    assert frame["afrr_up_mw"].isna().sum() == 2
    assert frame["imbalance_price_eur_mwh"].dtype == np.float32


def test_normalize_new_formula_minute_and_qh() -> None:
    ts = pd.Series(pd.date_range("2026-01-01T00:15:00Z", periods=20, freq="1min", tz="UTC"))
    raw = pd.DataFrame({"datetime": ts, "resolutioncode": "PT1M", "qualitystatus": "NotValidated"})
    for upstream in NEW_FORMULA_MAP:
        if upstream != "qualitystatus":
            raw[upstream] = np.arange(20, dtype=float)
    minute, stats = normalize_new_formula(raw.drop(index=[3]), minute=True)
    assert list(minute.columns) == NEW_FORMULA_MINUTE_COLUMNS
    assert len(minute) == 20 and stats["missing_intervals_inside_coverage"] == 1
    assert minute["frce_rolling_mw"].dtype == np.float32
    qh_raw = raw.iloc[[0]].copy()
    qh_raw["datetime"] = pd.Timestamp("2026-01-01T00:00:00Z")
    qh, _ = normalize_new_formula(qh_raw, minute=False)
    assert list(qh.columns) == NEW_FORMULA_QH_COLUMNS
    assert "si_rolling_mw" not in qh.columns


def test_normalize_si_forecast_converts_percent_quality_and_rounds() -> None:
    raw = pd.DataFrame(
        {
            "predictiontimeutc": pd.to_datetime(
                ["2025-08-22T06:58:01Z", "2025-08-22T06:59:00Z", "2025-08-22T06:59:00Z"]
            ),
            "resolutioncode": "PT1M",
            "quality_status": "Valid",
            "predictions_forecastedtimeutc": pd.to_datetime(["2025-08-22T06:45:00Z"] * 3),
            "predictionquality": [1.0, 72.41, 72.41],
            "predictions_silinearregressionforecast": [-15.929126, 12.3456, 12.3456],
            "predictions_categoricalsiprediction_from_minus_inf_to_minus_400": [3.4e-14, 0.0, 0.0],
            "predictions_categoricalsiprediction_from_minus_400_to_minus_200": [0.000001, 0.1, 0.1],
            "predictions_categoricalsiprediction_from_minus_200_to_0": [0.913755, 0.4, 0.4],
            "predictions_categoricalsiprediction_from_0_to_200": [0.086243, 0.4, 0.4],
            "predictions_categoricalsiprediction_from_200_to_400": [4.97e-08, 0.1, 0.1],
            "predictions_categoricalsiprediction_from_400_to_inf": [7.2e-14, 0.0, 0.0],
        }
    )
    frame, stats = normalize_si_forecast(raw)
    assert list(frame.columns) == SI_FORECAST_COLUMNS
    assert len(frame) == 2 and stats["duplicates_or_null_keys_dropped"] == 1
    assert frame["prediction_quality"].tolist() == pytest.approx([1.0, 0.7241], abs=1e-6)
    assert stats["prediction_quality_rows_converted_from_percent"] == 1
    assert frame["p_si_m200_0"].iloc[0] == pytest.approx(0.9138, abs=1e-6)
    assert frame["si_forecast_mw"].iloc[0] == pytest.approx(-15.93, abs=1e-4)
    assert frame["p_si_lt_m400"].iloc[0] == 0.0


def test_normalize_res_load_sums_wind_and_selects_belgium_pv() -> None:
    ts = pd.Series(pd.date_range(T0, periods=4, freq="15min", tz="UTC"))
    load = pd.DataFrame({"datetime": ts, "totalload": 8000.0, "dayaheadforecast": 8100.0, "mostrecentforecast": 8050.0})
    wind_rows = []
    for t in ts:
        for kind, region, conn, mw in [
            ("Offshore", "Federal", "Elia", 500.0),
            ("Onshore", "Flanders", "Elia", 80.0),
            ("Onshore", "Flanders", "Dso", 300.0),
            ("Onshore", "Wallonia", "Elia", 40.0),
            ("Onshore", "Wallonia", "Dso", 200.0),
        ]:
            wind_rows.append(
                {
                    "datetime": t,
                    "offshoreonshore": kind,
                    "region": region,
                    "gridconnectiontype": conn,
                    "measured": mw,
                    "dayahead11hforecast": mw * 0.9,
                    "monitoredcapacity": mw * 4,
                }
            )
    pv = pd.DataFrame(
        {
            "datetime": list(ts) * 2,
            "region": ["Belgium"] * 4 + ["Flanders"] * 4,
            "measured": [100.0] * 4 + [60.0] * 4,
            "dayahead11hforecast": [90.0] * 4 + [50.0] * 4,
        }
    )
    frame, stats = normalize_res_load(load, pd.DataFrame(wind_rows), pv, T0, T0 + pd.Timedelta(days=1))
    assert list(frame.columns) == RES_LOAD_COLUMNS
    assert frame["wind_mw"].iloc[0] == pytest.approx(1120.0)
    assert frame["wind_offshore_mw"].iloc[0] == pytest.approx(500.0)
    assert frame["wind_onshore_mw"].iloc[0] == pytest.approx(620.0)
    assert frame["pv_mw"].iloc[0] == pytest.approx(100.0)
    assert frame["load_da_forecast_mw"].iloc[0] == pytest.approx(8100.0)
    assert stats["wind_rows_per_qh"] == {"5": 4}


def test_normalize_legacy_qh_keeps_long_and_short_prices() -> None:
    ts = pd.Series(pd.date_range("2019-01-01T00:00:00Z", periods=4, freq="15min", tz="UTC"))
    raw = pd.DataFrame(
        {
            "datetime": ts,
            "qualitystatus": "Validated",
            "netregulationvolume": 10.0,
            "systemimbalance": -10.0,
            "alpha": 0.0,
            "marginalincrementalprice": 50.0,
            "marginaldecrementalprice": 20.0,
            "strategicreserveprice": np.nan,
            "positiveimbalanceprice": [50.0, 50.0, 40.0, 40.0],
            "negativeimbalanceprice": [50.0, 55.0, 40.0, 40.0],
        }
    )
    frame, stats = normalize_legacy(
        raw, minute=False, window_start=ts.min(), window_end=ts.max() + pd.Timedelta(hours=1)
    )
    assert stats["dual_price_intervals"] == 1
    assert (frame["imbalance_price_eur_mwh"] == frame["imbalance_price_long_eur_mwh"]).all()


# --- Energy-Charts -------------------------------------------------------------------------------------------------


def test_energy_charts_parse_and_resolution(tmp_path: Path) -> None:
    hourly = list(range(1759262400 - 4 * 3600, 1759262400, 3600))  # 4 hourly products before 2025-09-30T22:00Z
    quarter = list(range(1759262400, 1759262400 + 8 * 900, 900))
    payload = {
        "license_info": "CC BY 4.0 (creativecommons.org/licenses/by/4.0) from Bundesnetzagentur | SMARD.de",
        "unix_seconds": hourly + quarter + [quarter[-1]],
        "price": [80.0, 81.0, None, 83.0] + [70.0] * 8 + [70.0],
        "unit": "EUR / MWh",
    }
    path = tmp_path / "price.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    chunk = energy_charts.MonthChunk("BE", pd.Timestamp("2025-09-01"), path, None)  # type: ignore[arg-type]
    frame, stats = energy_charts.normalize_months([chunk])
    assert list(frame.columns) == ["timestamp_utc", "resolution_minutes", "price_eur_mwh"]
    assert stats["null_prices_dropped"] == 1 and stats["duplicates_dropped"] == 1
    assert frame["resolution_minutes"].tolist() == [60, 60, 60, 15, 15, 15, 15, 15, 15, 15, 15]
    assert str(frame["timestamp_utc"].dtype).endswith("UTC]")
    assert frame["price_eur_mwh"].dtype == np.float32


# --- regelleistung.net -----------------------------------------------------------------------------------------------


def _xlsx(rows: list[list[object]]) -> bytes:
    strings: list[str] = []

    def cell(ref: str, value: object) -> str:
        if isinstance(value, int | float):
            return f'<c r="{ref}"><v>{value}</v></c>'
        strings.append(str(value))
        return f'<c r="{ref}" t="s"><v>{len(strings) - 1}</v></c>'

    def col(i: int) -> str:
        name = ""
        i += 1
        while i:
            i, rem = divmod(i - 1, 26)
            name = chr(65 + rem) + name
        return name

    body = "".join(
        f'<row r="{r + 1}">' + "".join(cell(f"{col(c)}{r + 1}", v) for c, v in enumerate(row)) + "</row>"
        for r, row in enumerate(rows)
    )
    ns = 'xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"'
    sheet = f"<worksheet {ns}><sheetData>{body}</sheetData></worksheet>"
    shared = f"<sst {ns}>" + "".join(f"<si><t>{s}</t></si>" for s in strings) + "</sst>"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("xl/worksheets/sheet1.xml", sheet)
        archive.writestr("xl/sharedStrings.xml", shared)
    return buffer.getvalue()


def test_regelleistung_fcr_results_parse(tmp_path: Path) -> None:
    header = [
        "DATE_FROM",
        "DATE_TO",
        "PRODUCT_TYPE",
        "TENDER_NUMBER",
        "PRODUCTNAME",
        "BELGIUM_DEMAND_[MW]",
        "BELGIUM_SETTLEMENTCAPACITY_PRICE_[EUR/MW]",
        "BELGIUM_DEFICIT(-)_SURPLUS(+)_[MW]",
        "GERMANY_DEMAND_[MW]",
        "GERMANY_SETTLEMENTCAPACITY_PRICE_[EUR/MW]",
        "GERMANY_DEFICIT(-)_SURPLUS(+)_[MW]",
    ]
    rows = [
        header,
        [45292.0, 45292.0, "FCR", 1.0, "NEGPOS_00_04", 93.0, 219.04, -65.0, 564.0, 114.8, 34.0],
        [45292.0, 45292.0, "FCR", 1.0, "NEGPOS_04_08", 93.0, 219.4, -65.0, 564.0, 104.4, 33.0],
        [45292.0, 45292.0, "FCR", 2.0, "NEGPOS_00_04", 1.0, "-", "-", "-", "-", "-"],
    ]
    path = tmp_path / "fcr.xlsx"
    path.write_bytes(_xlsx(rows))
    frame = regelleistung.xlsx_frame(path)
    assert list(frame.columns) == header
    germany, countries = regelleistung.parse_fcr_results(frame)
    germany = regelleistung.finalize_germany([germany])
    assert list(germany.columns) == regelleistung.GERMANY_COLUMNS
    assert len(germany) == 2
    first = germany.iloc[0]
    assert first["delivery_start_utc"] == pd.Timestamp("2023-12-31T23:00:00Z")
    assert first["delivery_end_utc"] == pd.Timestamp("2024-01-01T03:00:00Z")
    assert first["price_eur_per_mw_h"] == pytest.approx(114.8 / 4, rel=1e-6)
    assert first["volume_mw"] == pytest.approx(564.0) and first["direction"] == "symmetric"
    assert set(countries["country"]) == {"belgium", "germany"}


def test_regelleistung_afrr_results_use_demand_file(tmp_path: Path) -> None:
    results = pd.DataFrame(
        {
            "DATE_FROM": ["46315.0", "46315.0"],
            "PRODUCT": ["POS_00_04", "NEG_00_04"],
            "GERMANY_AVERAGE_CAPACITY_PRICE_[(EUR/MW)/h]": ["1.41", "0.5"],
            "GERMANY_MARGINAL_CAPACITY_PRICE_[(EUR/MW)/h]": ["1.61", "0.7"],
            "GERMANY_ALLOCATED_VOLUME_[MW]": ["1957.0", "1800.0"],
        }
    )
    demands = pd.DataFrame({"PRODUCT": ["POS_00_04", "NEG_00_04"], "GERMANY_BLOCK_DEMAND_[MW]": ["1884.0", "-"]})
    frame = regelleistung.finalize_germany([regelleistung.parse_afrr_results(results, demands)])
    by_dir = frame.set_index("direction")
    assert by_dir.loc["up", "volume_mw"] == pytest.approx(1884.0)
    assert by_dir.loc["down", "volume_mw"] == pytest.approx(1800.0)  # falls back to allocated volume
    assert by_dir.loc["up", "marginal_price_eur_per_mw_h"] == pytest.approx(1.61, rel=1e-6)
    assert by_dir.loc["up", "delivery_start_utc"] == pd.Timestamp("2026-10-19T22:00:00Z")


# --- capacity awards and bid curves -------------------------------------------------------------------------------


def test_parse_period_variants() -> None:
    assert aggregate.parse_period("0 - 24 - G2") == (0, 24)
    assert aggregate.parse_period("20-24") == (20, 24)
    assert aggregate.parse_period("4 - 8") == (4, 8)
    assert aggregate.parse_period("nonsense") is None
    assert aggregate.parse_period(None) is None


def test_capacity_awards_expand_daily_bids_and_vwap() -> None:
    legacy = pd.DataFrame(
        {
            "capacitybiddeliveryperiod": [None, None, None, None],
            "delivery_period": ["0 - 4", "0 - 4", "0 - 24", "4 - 8"],
            "delivery_date": ["2026-03-03"] * 4,
            "afrr_offered_volume_up": [10.0, 5.0, 20.0, 8.0],
            "afrr_awarded_volume_up": [10.0, 0.0, 20.0, 8.0],
            "price_up": [10.0, 99.0, 4.0, 6.0],
            "afrr_offered_volume_down": [np.nan, 3.0, np.nan, np.nan],
            "afrr_awarded_volume_down": [np.nan, 3.0, np.nan, np.nan],
            "price_down": [np.nan, 2.0, np.nan, np.nan],
        }
    )
    bids = aggregate.afrr_bids_long(legacy, pd.DataFrame())
    frame, stats = aggregate.aggregate_capacity_awards(bids)
    assert list(frame.columns) == aggregate.CAPACITY_COLUMNS
    up = frame[frame["direction"] == "up"].set_index("delivery_start_utc")
    first = up.loc[pd.Timestamp("2026-03-02T23:00:00Z")]  # 00:00 CET
    assert first["awarded_mw"] == pytest.approx(30.0)
    assert first["vwap_eur_per_mw_h"] == pytest.approx((10 * 10 + 20 * 4) / 30, rel=1e-6)
    assert first["marginal_eur_per_mw_h"] == pytest.approx(10.0)
    assert first["n_bids"] == 2 and first["n_offered_bids"] == 3
    assert first["awarded_mw_multi_block"] == pytest.approx(20.0)
    assert len(up) == 6  # the daily bid creates rows for all six 4-hour blocks
    assert up.loc[pd.Timestamp("2026-03-03T03:00:00Z"), "awarded_mw"] == pytest.approx(28.0)
    assert stats["multi_block_bids"] == 1
    down = frame[frame["direction"] == "down"]
    assert len(down) == 1 and down["awarded_mw"].iloc[0] == pytest.approx(3.0)


def test_mfrr_awarded_only_source_nulls_offer_statistics() -> None:
    legacy = pd.DataFrame(
        {
            "capacitybiddeliveryperiod": ["0-4", "0-4"],
            "delivery_date": ["2026-03-03", "2026-03-03"],
            "regulation_direction": ["Up", "Up"],
            "mfrr_offered_volume": [1.0, 2.0],
            "mfrr_std_awarded_volume": [1.0, 2.0],
            "mfrr_std_price": [3.0, 6.0],
        }
    )
    current = pd.DataFrame(
        {
            "deliverydate": [pd.Timestamp("2026-06-10T22:00:00Z")] * 2,
            "capacitybiddeliveryperiod": ["0-4", "0-4"],
            "regulationdirection": ["Up", "Up"],
            "mfrrofferedvolume": [1.0, 1.0],
            "mfrrstdawardedvolume": [0.0, 1.0],
            "mfrrstdcapbidprice": [9.5, 7.0],
        }
    )
    frame, _ = aggregate.aggregate_capacity_awards(aggregate.mfrr_bids_long(legacy, current))
    assert len(frame) == 2
    old, new = frame.iloc[0], frame.iloc[1]
    assert pd.isna(old["offered_mw"]) and pd.isna(old["n_offered_bids"])
    assert old["vwap_eur_per_mw_h"] == pytest.approx(5.0)
    assert new["offered_mw"] == pytest.approx(2.0) and new["n_bids"] == 1
    assert new["delivery_start_utc"] == pd.Timestamp("2026-06-10T22:00:00Z")


def test_bid_curves_price_sorted_merit_order_excludes_zero_volume_bids() -> None:
    ts = pd.Timestamp("2026-03-03T00:00:00Z")
    raw = pd.DataFrame(
        {
            "datetime": [ts] * 6,
            "rank": ["1", "2", "3", "4", "5", "6"],
            "cumulativevolume": [10.0, 30.0, 30.0, 60.0, 100.0, 100.0],
            "energybidmarginalprice": [80.0, 120.0, 400.0, 100.0, 150.0, 10.0],
            "availability": ["Available", "Available", "Unavailable", "Available", "Available", "Available"],
        }
    )
    grid = np.array([0.0, 10.0, 25.0, 50.0, 100.0, 125.0])
    up, stats = aggregate.bid_curves(raw, "up", grid)
    # price-sorted: 80 (10 MW), 100 (30 MW), 120 (20 MW), 150 (40 MW); rank 6 adds no volume (exclusive bid)
    assert up["price_eur_mwh"].tolist()[:5] == pytest.approx([80.0, 80.0, 100.0, 120.0, 150.0])
    assert np.isnan(up["price_eur_mwh"].iloc[5])
    assert stats["rows_not_available"] == 1 and stats["rows_without_volume"] == 1
    raw_down = raw.assign(energybidmarginalprice=[60.0, 40.0, -999.0, 50.0, -100.0, 99.0])
    down, _ = aggregate.bid_curves(raw_down, "down", grid)
    # highest price first: 60 (10 MW), 50 (30 MW), 40 (20 MW), -100 (40 MW)
    assert down["price_eur_mwh"].tolist()[:5] == pytest.approx([60.0, 60.0, 50.0, 40.0, -100.0])
    assert set(down["direction"]) == {"down"} and str(down["timestamp_utc"].dtype) == "datetime64[ns, UTC]"
    with pytest.raises(ValueError):
        aggregate.bid_curves(raw, "sideways", grid)
