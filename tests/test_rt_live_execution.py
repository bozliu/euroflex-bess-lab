from __future__ import annotations

import csv
import inspect
from pathlib import Path

import pandas as pd
import pytest
import requests

from euroflex_bess_lab.realtime.live import execution as execution_module
from euroflex_bess_lab.realtime.live.execution import (
    CsvSetpointExecution,
    PaperExecution,
    Setpoint,
    WebhookExecution,
    build_execution,
    redact_url,
)
from euroflex_bess_lab.realtime.live.testing import FakeResponse


def _setpoint(minute: str = "2026-10-26T10:00:00Z", power: float = -1.0) -> Setpoint:
    return Setpoint(
        run_id="live-test",
        strategy="threshold",
        minute_utc=pd.Timestamp(minute),
        power_mw=power,
        requested_mw=power,
        soc_start_mwh=1.0,
        soc_end_mwh=1.0 - power / 60 * (0.95 if power < 0 else 1 / 0.95),
        mechanism="current",
        decided_at_utc=pd.Timestamp(minute) + pd.Timedelta(seconds=0.5),
    )


def test_paper_execution_only_logs() -> None:
    adapter = PaperExecution()
    receipt = adapter.send(_setpoint())
    assert receipt.status == "logged" and adapter.sent == 1
    assert adapter.describe() == {"adapter": "paper", "market_submission": False}


def test_csv_setpoints_are_appended_for_an_ems(tmp_path: Path) -> None:
    target = tmp_path / "ems" / "setpoints.csv"
    adapter = CsvSetpointExecution(target, fsync=False)
    assert adapter.send(_setpoint("2026-10-26T10:00:00Z", -1.0)).status == "written"
    assert adapter.send(_setpoint("2026-10-26T10:01:00Z", 0.5)).status == "written"
    rows = list(csv.DictReader(target.open(encoding="utf-8")))
    assert [r["minute_utc"] for r in rows] == ["2026-10-26T10:00:00Z", "2026-10-26T10:01:00Z"]
    assert rows[0]["valid_until_utc"] == "2026-10-26T10:01:00Z"
    assert float(rows[1]["power_mw"]) == 0.5 and rows[1]["mode"] == "paper"
    assert target.read_text(encoding="utf-8").count("minute_utc") == 1  # one header
    assert adapter.describe()["file"] == "setpoints.csv"


def test_csv_adapter_reports_io_errors_instead_of_raising(tmp_path: Path) -> None:
    blocker = tmp_path / "not_a_dir"
    blocker.write_text("x", encoding="utf-8")
    receipt = CsvSetpointExecution(blocker / "setpoints.csv").send(_setpoint())
    assert receipt.status == "error"


class _RecordingSession:
    def __init__(self, response: FakeResponse | Exception) -> None:
        self.response = response
        self.posts: list[dict] = []

    def post(self, url, json=None, headers=None, timeout=None):  # type: ignore[no-untyped-def]
        self.posts.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
        if isinstance(self.response, Exception):
            raise self.response
        return self.response

    def close(self) -> None:
        return None


def test_webhook_is_disabled_unless_explicitly_enabled() -> None:
    session = _RecordingSession(FakeResponse(200, {}))
    adapter = WebhookExecution("https://ems.example.test/setpoint", session=session)
    receipt = adapter.send(_setpoint())
    assert receipt.status == "disabled" and session.posts == []
    assert WebhookExecution().send(_setpoint()).status == "disabled"


def test_enabled_webhook_posts_a_paper_setpoint_and_never_raises() -> None:
    session = _RecordingSession(FakeResponse(204, {}))
    adapter = WebhookExecution("https://ems.example.test/setpoint?token=secret", enabled=True, session=session)
    assert adapter.send(_setpoint()).status == "sent"
    payload = session.posts[0]["json"]
    assert payload["mode"] == "paper" and payload["market_submission"] is False
    assert payload["power_mw"] == -1.0 and payload["minute_utc"] == "2026-10-26T10:00:00Z"
    assert session.posts[0]["timeout"] == 2.0
    assert adapter.describe()["url"] == "https://ems.example.test/setpoint"  # query (token) redacted
    failing = WebhookExecution(
        "https://ems.example.test", enabled=True, session=_RecordingSession(FakeResponse(500, {}))
    )
    assert failing.send(_setpoint()).status == "error" and failing.errors == 1
    broken = WebhookExecution(
        "https://ems.example.test", enabled=True, session=_RecordingSession(requests.ConnectionError("refused"))
    )
    assert broken.send(_setpoint()).status == "error"
    with pytest.raises(ValueError):
        WebhookExecution("ftp://ems.example.test", enabled=True)


def test_factory_requires_explicit_webhook_opt_in(tmp_path: Path) -> None:
    assert isinstance(build_execution("paper", run_dir=tmp_path), PaperExecution)
    csv_adapter = build_execution("csv", run_dir=tmp_path)
    assert isinstance(csv_adapter, CsvSetpointExecution) and csv_adapter.path == tmp_path / "setpoints.csv"
    with pytest.raises(ValueError, match="enable-webhook"):
        build_execution("webhook", run_dir=tmp_path, webhook_url="https://ems.example.test")
    with pytest.raises(ValueError, match="webhook-url"):
        build_execution("webhook", run_dir=tmp_path, enable_webhook=True)
    with pytest.raises(ValueError, match="unknown"):
        build_execution("market", run_dir=tmp_path)
    assert redact_url("https://user:pw@host.test:8443/a/b?x=1#f") == "https://host.test:8443/a/b"


def test_there_is_no_market_submission_adapter() -> None:
    adapters = {
        obj.name
        for _, obj in inspect.getmembers(execution_module, inspect.isclass)
        if obj.__module__ == execution_module.__name__
        and hasattr(obj, "send")
        and not getattr(obj, "_is_protocol", False)
    }
    assert adapters == {"paper", "csv", "webhook"}
    payload = _setpoint().as_payload()
    assert payload["market_submission"] is False and payload["mode"] == "paper"
