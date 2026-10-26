"""Execution adapters for live paper trading: where a minute setpoint goes after the strategy decided it.

Safety statement: none of these adapters can submit an order or a bid to any market (Elia, EPEX, a TSO platform or
a broker). There is deliberately no market-submission adapter in this repository. A setpoint is a *paper*
instruction for a battery energy management system (EMS):

- :class:`PaperExecution` (default) only logs the setpoint;
- :class:`CsvSetpointExecution` appends it to a CSV file that an external EMS can read (shadow integration);
- :class:`WebhookExecution` POSTs it as JSON to a user-supplied URL and is disabled unless explicitly enabled.

Whether an EMS acts on a setpoint is the EMS operator's decision and responsibility; every payload carries
``"mode": "paper"`` and ``"market_submission": false``.
"""

from __future__ import annotations

import csv
import logging
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol, runtime_checkable
from urllib.parse import urlsplit, urlunsplit

import pandas as pd
import requests

from .feeds import USER_AGENT, iso_utc

LOGGER = logging.getLogger(__name__)

ReceiptStatus = Literal["logged", "written", "sent", "disabled", "error"]


@dataclass(frozen=True)
class Setpoint:
    """Battery power for one minute ``[minute_utc, minute_utc + 1 min)``; ``power_mw > 0`` is discharge."""

    run_id: str
    strategy: str
    minute_utc: pd.Timestamp
    power_mw: float
    requested_mw: float
    soc_start_mwh: float
    soc_end_mwh: float
    mechanism: str
    decided_at_utc: pd.Timestamp

    @property
    def valid_until_utc(self) -> pd.Timestamp:
        return self.minute_utc + pd.Timedelta(minutes=1)

    def as_payload(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "strategy": self.strategy,
            "minute_utc": iso_utc(self.minute_utc),
            "valid_until_utc": iso_utc(self.valid_until_utc),
            "power_mw": _round(self.power_mw),
            "requested_mw": _round(self.requested_mw),
            "soc_start_mwh": _round(self.soc_start_mwh),
            "soc_end_mwh": _round(self.soc_end_mwh),
            "mechanism": self.mechanism,
            "decided_at_utc": iso_utc(self.decided_at_utc, fractional=True),
            "sign_convention": "power_mw > 0 = discharge (net injection)",
            "mode": "paper",
            "market_submission": False,
        }


def _round(value: float, digits: int = 6) -> float | None:
    if value is None or math.isnan(value):
        return None
    return round(float(value), digits) + 0.0


@dataclass(frozen=True)
class ExecutionReceipt:
    adapter: str
    status: ReceiptStatus
    detail: str = ""

    def as_dict(self) -> dict[str, str]:
        return {"adapter": self.adapter, "status": self.status, "detail": self.detail}


@runtime_checkable
class ExecutionAdapter(Protocol):
    """Receives one setpoint per minute. Implementations must not raise: return an ``error`` receipt instead."""

    name: str

    def send(self, setpoint: Setpoint) -> ExecutionReceipt: ...

    def describe(self) -> dict[str, Any]: ...

    def close(self) -> None: ...


class PaperExecution:
    """Default adapter: logs the setpoint (the decision journal is the record); never contacts anything."""

    name = "paper"

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self.logger = logger or LOGGER
        self.sent = 0

    def send(self, setpoint: Setpoint) -> ExecutionReceipt:
        self.sent += 1
        self.logger.info(
            "paper setpoint %s %+.3f MW (requested %+.3f, SOC %.3f -> %.3f MWh)",
            iso_utc(setpoint.minute_utc),
            setpoint.power_mw,
            setpoint.requested_mw,
            setpoint.soc_start_mwh,
            setpoint.soc_end_mwh,
        )
        return ExecutionReceipt(self.name, "logged")

    def describe(self) -> dict[str, Any]:
        return {"adapter": self.name, "market_submission": False}

    def close(self) -> None:
        return None


SETPOINT_COLUMNS = (
    "minute_utc",
    "valid_until_utc",
    "power_mw",
    "requested_mw",
    "soc_start_mwh",
    "soc_end_mwh",
    "mechanism",
    "strategy",
    "run_id",
    "decided_at_utc",
    "mode",
)


class CsvSetpointExecution:
    """Appends one CSV row per minute for an external EMS to read (``power_mw > 0`` = discharge)."""

    name = "csv"

    def __init__(self, path: str | Path, *, fsync: bool = True) -> None:
        self.path = Path(path)
        self.fsync = fsync
        self.sent = 0

    def send(self, setpoint: Setpoint) -> ExecutionReceipt:
        payload = setpoint.as_payload()
        row = {column: payload.get(column) for column in SETPOINT_COLUMNS}
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            header = not self.path.exists() or self.path.stat().st_size == 0
            with self.path.open("a", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(SETPOINT_COLUMNS))
                if header:
                    writer.writeheader()
                writer.writerow(row)
                handle.flush()
                if self.fsync:
                    os.fsync(handle.fileno())
        except OSError as exc:
            return ExecutionReceipt(self.name, "error", f"{type(exc).__name__}: {exc.strerror or exc}")
        self.sent += 1
        return ExecutionReceipt(self.name, "written")

    def describe(self) -> dict[str, Any]:
        return {"adapter": self.name, "file": self.path.name, "market_submission": False}

    def close(self) -> None:
        return None


def redact_url(url: str) -> str:
    """Scheme, host and path only (query strings and credentials may hold secrets)."""
    parts = urlsplit(url)
    host = parts.hostname or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    return urlunsplit((parts.scheme, host, parts.path, "", ""))


class WebhookExecution:
    """POSTs each setpoint as JSON to a user-supplied URL. Disabled unless ``enabled=True`` and a URL is given.

    The endpoint is expected to be the user's own EMS gateway; the payload is a paper setpoint, never an order.
    Network errors and non-2xx responses are reported in the receipt and never interrupt the live loop.
    """

    name = "webhook"

    def __init__(
        self,
        url: str | None = None,
        *,
        enabled: bool = False,
        timeout_s: float = 2.0,
        headers: dict[str, str] | None = None,
        session: Any = None,
    ) -> None:
        if url is not None and urlsplit(url).scheme not in {"http", "https"}:
            raise ValueError("webhook URL must start with http:// or https://")
        self.url = url
        self.enabled = bool(enabled and url)
        self.timeout_s = timeout_s
        self.headers = {"Content-Type": "application/json", "User-Agent": USER_AGENT, **(headers or {})}
        self.session = session if session is not None else (requests.Session() if self.enabled else None)
        self.sent = 0
        self.errors = 0

    def send(self, setpoint: Setpoint) -> ExecutionReceipt:
        if not self.enabled or self.url is None or self.session is None:
            return ExecutionReceipt(self.name, "disabled", "webhook not enabled (pass enabled=True and a URL)")
        try:
            response = self.session.post(
                self.url, json=setpoint.as_payload(), headers=self.headers, timeout=self.timeout_s
            )
            status = int(response.status_code)
        except requests.RequestException as exc:
            self.errors += 1
            return ExecutionReceipt(self.name, "error", f"{type(exc).__name__}")
        if not 200 <= status < 300:
            self.errors += 1
            return ExecutionReceipt(self.name, "error", f"HTTP {status}")
        self.sent += 1
        return ExecutionReceipt(self.name, "sent", f"HTTP {status}")

    def describe(self) -> dict[str, Any]:
        return {
            "adapter": self.name,
            "enabled": self.enabled,
            "url": redact_url(self.url) if self.url else None,
            "market_submission": False,
        }

    def close(self) -> None:
        if self.session is not None and hasattr(self.session, "close"):
            self.session.close()


def build_execution(
    kind: str,
    *,
    run_dir: Path,
    setpoint_csv: Path | None = None,
    webhook_url: str | None = None,
    enable_webhook: bool = False,
) -> ExecutionAdapter:
    """Factory used by the CLI: ``paper`` | ``csv`` | ``webhook``."""
    if kind == "paper":
        return PaperExecution()
    if kind == "csv":
        return CsvSetpointExecution(setpoint_csv or run_dir / "setpoints.csv")
    if kind == "webhook":
        if not webhook_url:
            raise ValueError("--execution webhook needs --webhook-url")
        if not enable_webhook:
            raise ValueError("the webhook adapter is disabled by default; pass --enable-webhook to POST setpoints")
        return WebhookExecution(webhook_url, enabled=True)
    raise ValueError(f"unknown execution adapter `{kind}`; choose paper, csv or webhook")
