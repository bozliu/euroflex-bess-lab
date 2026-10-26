from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import requests

from euroflex_bess_lab.public_data.normalize import (
    MINUTE_COLUMNS,
    NEW_FORMULA_MINUTE_COLUMNS,
    NEW_FORMULA_QH_COLUMNS,
    QH_COLUMNS,
    SI_FORECAST_COLUMNS,
)
from euroflex_bess_lab.realtime.live.clock import ManualClock
from euroflex_bess_lab.realtime.live.feeds import (
    DEFAULT_FEEDS,
    IMBALANCE_PRICE_MINUTE,
    IMBALANCE_PRICE_QH,
    NEW_FORMULA_MINUTE,
    NEW_FORMULA_QH,
    PRICE_COMPONENTS_MINUTE,
    SI_FORECAST_CURRENT_QH,
    SI_FORECAST_NEXT_QH,
    SYSTEM_IMBALANCE_MINUTE,
    EliaLiveFeeds,
    FeedError,
    FeedSchemaError,
    HttpPolicy,
    JsonHttpClient,
    RetryLater,
    aligned_after,
    expected_local_day_rows,
    normalize_energy_charts,
    normalize_records,
    records_from_payload,
)
from euroflex_bess_lab.realtime.live.testing import FakeResponse

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "live"


def _payload(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _records(dataset: str) -> list[dict]:
    return _payload(f"{dataset}_records.json")["results"]


def test_ods161_minute_prices_map_to_the_contract() -> None:
    raw = _records("ods161")
    frame = normalize_records(IMBALANCE_PRICE_MINUTE, raw)
    assert list(frame.columns) == [
        "timestamp_utc",
        "qh_start_utc",
        "si_cum_mw",
        "ace_cum_mw",
        "alpha_eur_mwh",
        "alpha_prime_eur_mwh",
        "mip_eur_mwh",
        "mdp_eur_mwh",
        "price_cum_eur_mwh",
        "quality_status",
    ]
    assert str(frame["timestamp_utc"].dtype) == "datetime64[ns, UTC]"
    assert frame["price_cum_eur_mwh"].dtype == np.float32
    assert frame["timestamp_utc"].is_monotonic_increasing and frame["timestamp_utc"].is_unique
    assert len(frame) == len(raw)
    assert (frame["qh_start_utc"] == frame["timestamp_utc"].dt.floor("15min")).all()
    latest = raw[0]  # records come newest first
    row = frame.iloc[-1]
    assert row["timestamp_utc"] == pd.Timestamp(latest["datetime"])
    assert row["price_cum_eur_mwh"] == np.float32(latest["imbalanceprice"])
    assert row["si_cum_mw"] == np.float32(latest["systemimbalance"])
    assert row["mip_eur_mwh"] == np.float32(latest["marginalincrementalprice"])
    assert row["quality_status"] == latest["qualitystatus"]


def test_instantaneous_components_qh_and_forecast_feeds_map_to_the_contract() -> None:
    si = normalize_records(SYSTEM_IMBALANCE_MINUTE, _records("ods169"))
    raw_si = _records("ods169")[0]
    assert si.iloc[-1]["si_mw"] == np.float32(raw_si["systemimbalance"])
    assert si.iloc[-1]["afrr_up_mw"] == np.float32(raw_si["afrrvolumeup"])
    assert {"igcc_up_mw", "mfrr_sa_up_mw", "rs_import_mw", "rs_export_mw"} <= set(si.columns)

    comp = normalize_records(PRICE_COMPONENTS_MINUTE, _records("ods175"))
    raw_comp = _records("ods175")[0]
    assert comp.iloc[-1]["afrr_price_up_eur_mwh"] == np.float32(raw_comp["r2priceup"])
    assert comp.iloc[-1]["floor_price_eur_mwh"] == np.float32(raw_comp["floorprice"])
    # ods175 names these differently from its historical twin ods165; both land in the same contract columns
    assert {"mfrr_avail_price_up_eur_mwh", "inter_tso_price_import_eur_mwh", "inter_tso_price_export_eur_mwh"} <= set(
        comp.columns
    )

    qh = normalize_records(IMBALANCE_PRICE_QH, _records("ods162"))
    raw_qh = _records("ods162")[0]
    assert qh.iloc[-1]["imbalance_price_eur_mwh"] == np.float32(raw_qh["imbalanceprice"])
    assert (qh["timestamp_utc"].dt.minute % 15 == 0).all()

    for spec, dataset in ((SI_FORECAST_CURRENT_QH, "ods136"), (SI_FORECAST_NEXT_QH, "ods147")):
        fc = normalize_records(spec, _records(dataset))
        assert list(fc.columns[:3]) == ["prediction_time_utc", "target_qh_utc", "si_forecast_mw"]
        probabilities = fc[[c for c in fc.columns if c.startswith("p_si_")]].sum(axis=1)
        assert np.allclose(probabilities, 1.0, atol=0.01)
        assert (fc["target_qh_utc"].dt.minute % 15 == 0).all()
    current = normalize_records(SI_FORECAST_CURRENT_QH, _records("ods136"))
    assert (current["target_qh_utc"] == current["prediction_time_utc"].dt.floor("15min")).all()


def test_new_formula_feeds_keep_rolling_columns_only_per_minute() -> None:
    minute = normalize_records(NEW_FORMULA_MINUTE, _records("ods211"))
    qh = normalize_records(NEW_FORMULA_QH, _records("ods212"))
    assert {"frce_rolling_mw", "si_rolling_mw", "incentive_eur_mwh", "weight_afrr"} <= set(minute.columns)
    assert "si_rolling_mw" not in qh.columns and "main_up_eur_mwh" in qh.columns
    raw = _records("ods212")[0]
    assert qh.iloc[-1]["imbalance_price_eur_mwh"] == np.float32(raw["imbalanceprice"])


def test_live_feed_columns_are_the_public_data_contract() -> None:
    contract = {
        "minute": set(MINUTE_COLUMNS),
        "qh": set(QH_COLUMNS),
        "nf_minute": set(NEW_FORMULA_MINUTE_COLUMNS),
        "nf_qh": set(NEW_FORMULA_QH_COLUMNS),
        "si_forecast_current": set(SI_FORECAST_COLUMNS),
        "si_forecast_next": set(SI_FORECAST_COLUMNS),
    }
    for spec in DEFAULT_FEEDS:
        if spec.table == "day_ahead":
            continue
        assert set(spec.contract_columns) <= contract[spec.table], spec.name
        historical = spec.historical()
        if historical is not None:
            assert set(historical.columns.values()) == set(spec.columns.values()), spec.name


def test_energy_charts_dst_day_has_100_quarter_hours() -> None:
    frame = normalize_energy_charts(_payload("energy_charts_be_2026-10-25.json"))
    assert len(frame) == expected_local_day_rows(pd.Timestamp("2026-10-25")) == 100
    assert (frame["resolution_minutes"] == 15).all()
    assert frame["timestamp_utc"].iloc[0] == pd.Timestamp("2026-10-24T22:00:00Z")
    assert frame["price_eur_mwh"].dtype == np.float32
    assert expected_local_day_rows(pd.Timestamp("2026-03-29")) == 92


def test_schema_checks_reject_broken_payloads() -> None:
    broken = [{k: v for k, v in r.items() if k != "imbalanceprice"} for r in _records("ods161")]
    with pytest.raises(FeedSchemaError, match="imbalanceprice"):
        normalize_records(IMBALANCE_PRICE_MINUTE, broken)
    with pytest.raises(FeedError, match="InvalidRESTParameterError"):
        records_from_payload(IMBALANCE_PRICE_MINUTE, {"error_code": "InvalidRESTParameterError", "message": "bad"})
    with pytest.raises(FeedSchemaError):
        records_from_payload(IMBALANCE_PRICE_MINUTE, {"results": "nope"})
    with pytest.raises(FeedSchemaError):
        normalize_energy_charts({"unix_seconds": [1, 2], "price": [1.0]})
    with pytest.raises(FeedSchemaError, match="unit"):
        normalize_energy_charts({"unix_seconds": [1], "price": [1.0], "unit": "MW"})
    empty = normalize_records(IMBALANCE_PRICE_MINUTE, [])
    assert empty.empty and "price_cum_eur_mwh" in empty.columns


class _ScriptedSession:
    """Returns scripted responses / raises scripted exceptions in order."""

    def __init__(self, script: list) -> None:
        self.script = list(script)
        self.calls: list[dict] = []
        self.headers: dict[str, str] = {}

    def get(self, url, params=None, timeout=None):  # type: ignore[no-untyped-def]
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def test_http_client_retries_with_backoff_and_honours_retry_after() -> None:
    sleeps: list[float] = []
    ok = FakeResponse(200, {"results": []}, {"Date": "Mon, 26 Oct 2026 03:28:38 GMT"})
    session = _ScriptedSession([requests.Timeout("slow"), FakeResponse(503, {}), ok])
    client = JsonHttpClient(HttpPolicy(max_retries=3, backoff_s=1.0), session=session, sleep=sleeps.append)
    result = client.get_json("https://example.test/records", {"limit": "3"})
    assert result.payload == {"results": []} and result.attempts == 3
    assert sleeps == [1.0, 2.0]
    assert result.server_date == pd.Timestamp("2026-10-26T03:28:38Z")
    assert session.calls[0]["timeout"] == (4.0, 10.0)

    sleeps.clear()
    session = _ScriptedSession([FakeResponse(429, {}, {"Retry-After": "7"}), ok])
    JsonHttpClient(HttpPolicy(max_retries=2), session=session, sleep=sleeps.append).get_json("https://example.test")
    assert sleeps == [7.0]

    sleeps.clear()
    session = _ScriptedSession([FakeResponse(404, {"error_code": "NotFound"})])
    with pytest.raises(FeedError, match="HTTP 404"):
        JsonHttpClient(session=session, sleep=sleeps.append).get_json("https://example.test")
    assert sleeps == []

    session = _ScriptedSession([FakeResponse(429, {}, {"Retry-After": "11"})] * 3)
    with pytest.raises(RetryLater) as info:
        JsonHttpClient(HttpPolicy(max_retries=2, backoff_cap_s=5.0), session=session, sleep=lambda _: None).get_json(
            "https://example.test"
        )
    assert info.value.retry_after_s == 11.0

    session = _ScriptedSession([FakeResponse(200, "<html>maintenance</html>")] * 2)
    with pytest.raises(FeedSchemaError, match="invalid JSON"):
        JsonHttpClient(HttpPolicy(max_retries=1), session=session, sleep=lambda _: None).get_json(
            "https://example.test"
        )


def _minute_record(ts: str, price: float) -> dict:
    stamp = pd.Timestamp(ts)
    return {
        "datetime": stamp.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "resolutioncode": "PT1M",
        "quarterhour": stamp.floor("15min").strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "qualitystatus": "NotValidated",
        "ace": 1.0,
        "systemimbalance": -50.0,
        "alpha": 0.0,
        "alpha_prime": 0.0,
        "marginalincrementalprice": 120.0,
        "marginaldecrementalprice": 80.0,
        "imbalanceprice": price,
    }


def test_poller_reports_new_rows_revisions_gaps_latency_and_cache_hits() -> None:
    clock = ManualClock("2026-10-26T10:05:30Z")
    first = {"results": [_minute_record(f"2026-10-26T10:0{m}:00Z", 100.0 + m) for m in (4, 3, 2, 1, 0)]}
    # second poll: minute 10:05 is missing (gap), 10:06 is new, 10:03 was revised
    second_rows = [_minute_record(f"2026-10-26T10:0{m}:00Z", 100.0 + m) for m in (6, 4, 3, 2)]
    second_rows[2]["imbalanceprice"] = 999.0
    session = _ScriptedSession(
        [
            FakeResponse(200, first),
            FakeResponse(200, {"results": second_rows}),
            FakeResponse(200, {"results": second_rows}),
        ]
    )
    feeds = EliaLiveFeeds([IMBALANCE_PRICE_MINUTE], client=JsonHttpClient(session=session, now=clock.now))
    update = feeds.poll(clock.now())[0]
    assert update.ok and update.n_new == 5 and update.n_revised == 0 and update.latency_s == ()
    clock.set("2026-10-26T10:07:20Z")
    update = feeds.poll(clock.now(), force=["imbalance_price_minute"])[0]
    assert update.n_new == 1 and update.n_revised == 1
    assert set(update.rows["timestamp_utc"]) == {
        pd.Timestamp("2026-10-26T10:06:00Z"),
        pd.Timestamp("2026-10-26T10:03:00Z"),
    }
    # the 10:06 row completed at 10:07:00 and was first seen at 10:07:20
    assert update.latency_s == pytest.approx((20.0,))
    clock.set("2026-10-26T10:07:40Z")
    update = feeds.poll(clock.now(), force=["imbalance_price_minute"])[0]
    assert update.n_new == 0 and len(update.rows) == 0
    status = feeds.status(clock.now())["imbalance_price_minute"]
    assert status["unchanged_polls"] == 1 and status["rows_revised"] == 1
    assert status["gaps"]["count"] == 1 and status["gaps"]["last"] == ["2026-10-26T10:05:00Z"]
    assert status["latency_upper_s"]["n"] == 1 and status["staleness_s"] == pytest.approx(40.0)
    assert session.calls[0]["params"] == {"order_by": "datetime desc", "limit": "20"}


def test_failed_polls_back_off_and_never_raise() -> None:
    clock = ManualClock("2026-10-26T10:00:00Z")
    session = _ScriptedSession([requests.ConnectionError("down")] * 3)
    client = JsonHttpClient(HttpPolicy(max_retries=2), session=session, sleep=lambda _: None, now=clock.now)
    feeds = EliaLiveFeeds([IMBALANCE_PRICE_MINUTE], client=client)
    update = feeds.poll(clock.now())[0]
    assert not update.ok and "down" in (update.error or "")
    state = feeds.status(clock.now())["imbalance_price_minute"]
    assert state["errors"] == 1 and state["consecutive_errors"] == 1
    # exponential backoff: the next poll is scheduled later than the regular 15 s cadence
    assert pd.Timestamp(state["next_due_utc"]) - clock.now() > pd.Timedelta(seconds=15)


def test_schedule_lands_one_poll_before_each_minute_boundary() -> None:
    t = pd.Timestamp("2026-10-26T10:00:01Z")
    assert aligned_after(t, 60.0, 52.0) == pd.Timestamp("2026-10-26T10:00:52Z")
    assert aligned_after(t, 15.0, 7.0) == pd.Timestamp("2026-10-26T10:00:07Z")
    assert aligned_after(pd.Timestamp("2026-10-26T10:00:52Z"), 15.0, 7.0) == pd.Timestamp("2026-10-26T10:01:07Z")
    clock = ManualClock("2026-10-26T10:00:01Z")
    ok = FakeResponse(200, {"results": []})
    feeds = EliaLiveFeeds(
        [IMBALANCE_PRICE_MINUTE], client=JsonHttpClient(session=_ScriptedSession([ok]), now=clock.now)
    )
    assert feeds.next_due() == pd.Timestamp(0, tz="UTC")  # never polled: due now
    feeds.poll(clock.now())
    assert feeds.next_due() == pd.Timestamp("2026-10-26T10:00:07Z")  # 15 s cadence, 8 s before the minute


def test_backfill_reads_exports_and_the_historical_price_twin() -> None:
    clock = ManualClock("2026-10-26T10:00:00Z")
    qh_rows = [
        {
            "datetime": f"2026-10-26T0{h}:00:00+00:00",
            "resolutioncode": "PT15M",
            "qualitystatus": "NotValidated",
            "ace": 0.0,
            "systemimbalance": 10.0,
            "alpha": 0.0,
            "alpha_prime": 0.0,
            "marginalincrementalprice": 90.0,
            "marginaldecrementalprice": 60.0,
            "imbalanceprice": 60.0 + h,
        }
        for h in range(3)
    ]
    session = _ScriptedSession([FakeResponse(200, qh_rows[:1]), FakeResponse(200, qh_rows[1:])])
    feeds = EliaLiveFeeds([IMBALANCE_PRICE_QH], client=JsonHttpClient(session=session, now=clock.now))
    updates = feeds.backfill(
        pd.Timestamp("2026-10-26T04:00:00Z"), clock.now(), qh_history_start=pd.Timestamp("2026-10-12T00:00:00Z")
    )
    assert [c["url"].split("/")[-3] for c in session.calls] == ["ods134", "ods162"]
    assert session.calls[0]["url"].endswith("/exports/json")
    assert (
        session.calls[0]["params"]["where"]
        == 'datetime >= "2026-10-12T00:00:00Z" and datetime < "2026-10-26T10:00:00Z"'
    )
    assert updates[0].backfill and updates[0].n_new == 3
    assert list(updates[0].rows["imbalance_price_eur_mwh"]) == [60.0, 61.0, 62.0]


def test_latency_is_not_measured_for_rows_completed_before_watching() -> None:
    clock = ManualClock("2026-10-26T10:00:00Z")
    old = {"results": _records("ods212")}  # simulation rows from July 2026
    session = _ScriptedSession([FakeResponse(200, {"results": []}), FakeResponse(200, old)])
    feeds = EliaLiveFeeds([NEW_FORMULA_QH], client=JsonHttpClient(session=session, now=clock.now))
    feeds.poll(clock.now())
    clock.advance(600)
    update = feeds.poll(clock.now(), force=["new_formula_qh"])[0]
    assert update.n_new == len(old["results"]) and update.latency_s == ()
