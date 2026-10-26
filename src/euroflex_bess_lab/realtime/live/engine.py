"""Live paper-trading (shadow) engine: the backtest ``Strategy`` objects, minute by minute, on live data.

The engine keeps a rolling :class:`~euroflex_bess_lab.realtime.MarketData` window (recent history from
``data/public`` when available, API backfill, then live rows) and takes one decision per minute with exactly the
visibility semantics of :func:`~euroflex_bess_lab.realtime.simulate`:

- rows are written into the window only once the :class:`~euroflex_bess_lab.realtime.PublicationModel` makes them
  visible (minute row ``j`` at decision ``t`` iff ``j + 1 + minute_lag <= t``; the final price of quarter-hour ``k``
  iff ``15k + 15 + qh_final_lag_minutes <= t``; day-ahead prices from 13:00 local time on D-1). A row that arrived
  early waits in a pending buffer; a row that is late is simply missing (NaN) and the decision is flagged;
- the strategy receives a real :class:`~euroflex_bess_lab.realtime.DecisionContext`;
- battery physics are :func:`~euroflex_bess_lab.realtime.apply_power` (identical arithmetic to the simulator);
- each closed quarter-hour is settled with :func:`~euroflex_bess_lab.realtime.settle_quarter_hours` on the price
  published by Elia (``current`` formula before the go-live, ``new2026`` after it once the live feed carries it).

The setpoint goes to an :class:`~euroflex_bess_lab.realtime.live.execution.ExecutionAdapter`; the default only logs.
Nothing in this package submits an order to any market. Time comes from an injected
:class:`~euroflex_bess_lab.realtime.live.clock.Clock`, so tests replay days in milliseconds without sleeping.
"""

from __future__ import annotations

import importlib
import logging
import math
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd

from ...public_data.normalize import (
    MINUTE_COLUMNS,
    NEW_FORMULA_MINUTE_COLUMNS,
    NEW_FORMULA_QH_COLUMNS,
    QH_COLUMNS,
    SI_FORECAST_COLUMNS,
    STRING_COLUMNS,
)
from ..battery import apply_power
from ..replay import DecisionContext, MarketData, day_ahead_visible_from_minute, expand_day_ahead_to_qh
from ..settlement import settle_quarter_hours
from ..strategies.base import Strategy
from ..types import MINUTES_PER_QH, BatteryParams, PublicationModel
from .clock import Clock, SystemClock
from .execution import ExecutionAdapter, ExecutionReceipt, PaperExecution, Setpoint
from .feeds import (
    LOCAL_TIMEZONE,
    MINUTE_TABLES,
    QH_TABLES,
    FeedUpdate,
    LiveSource,
    finite_or_none,
    iso_utc,
    sanitize_message,
)
from .replay_feeds import replay_spec
from .store import CONFIG, DECISIONS, EVENTS, FEED_LOG, STATE, STATUS, RunStore
from .strategies import strategy_diagnostics

LOGGER = logging.getLogger(__name__)

MINUTE_NS = 60 * 1_000_000_000
QH_NS = MINUTES_PER_QH * MINUTE_NS
#: Planned switch to the new imbalance-price formula: 24 Nov 2026 10:00 Europe/Brussels (CET), subject to CREG.
NEW_FORMULA_GO_LIVE_UTC = pd.Timestamp("2026-11-24T09:00:00Z")
SAFETY_STATEMENT = (
    "Paper trading (shadow mode) only: setpoints are logged or handed to the configured adapter; "
    "no order or bid is ever submitted to any market."
)
STATUS_SCHEMA = "euroflex-live-status/1"
MechanismSetting = Literal["auto", "current", "new2026"]
ReplanSetting = Literal["never", "minute"]
SETTLEMENT_COLUMNS = {"current": "imbalance_price_eur_mwh", "new2026": "nf_imbalance_price_eur_mwh"}
DEFAULT_SIGNALS: tuple[str, ...] = (
    "price_cum_eur_mwh",
    "si_cum_mw",
    "si_mw",
    "alpha_eur_mwh",
    "mip_eur_mwh",
    "mdp_eur_mwh",
    "afrr_price_up_eur_mwh",
    "afrr_price_down_eur_mwh",
    "elia_fc_cur_si_forecast_mw",
    "elia_fc_next_si_forecast_mw",
    "nf_imbalance_price_eur_mwh",
)


def _numeric(columns: Sequence[str]) -> list[str]:
    return [c for c in columns if not c.endswith("_utc") and c not in STRING_COLUMNS]


#: Every minute / quarter-hour field of the data contract, so that live windows expose the same field names as
#: ``MarketData.from_public`` (fields without a live feed stay NaN for live minutes).
CONTRACT_MINUTE_FIELDS: tuple[str, ...] = (
    *_numeric(MINUTE_COLUMNS),
    *(f"nf_{c}" for c in _numeric(NEW_FORMULA_MINUTE_COLUMNS)),
    *(f"elia_fc_cur_{c}" for c in _numeric(SI_FORECAST_COLUMNS)),
    *(f"elia_fc_next_{c}" for c in _numeric(SI_FORECAST_COLUMNS)),
)
CONTRACT_QH_FIELDS: tuple[str, ...] = (
    *_numeric(QH_COLUMNS),
    *(f"nf_{c}" for c in _numeric(NEW_FORMULA_QH_COLUMNS)),
)


def _utc(value: str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def _floor_minute(ts: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(ts).floor("1min")


def _floor_qh(ts: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(ts).floor("15min")


def local_midnight_after(ts: pd.Timestamp, days: int, timezone: str = LOCAL_TIMEZONE) -> pd.Timestamp:
    """UTC instant of local midnight ``days`` calendar days after the local date of ``ts`` (DST-safe)."""
    local_date = pd.Timestamp(ts).tz_convert(timezone).date()
    midnight = pd.Timestamp(local_date) + pd.Timedelta(days=days)
    return midnight.tz_localize(timezone).tz_convert("UTC")


@dataclass(frozen=True)
class LiveConfig:
    """Settings of one live run (all times UTC)."""

    mechanism: MechanismSetting = "auto"
    publication: PublicationModel = field(default_factory=PublicationModel)
    go_live_utc: pd.Timestamp = NEW_FORMULA_GO_LIVE_UTC
    new_formula_grace_minutes: float = 30.0
    history_days: float = 14.0
    minute_history_hours: float = 6.0
    horizon_days: int = 2
    window_start_utc: pd.Timestamp | None = None
    window_end_utc: pd.Timestamp | None = None
    decision_offset_s: float = 0.5
    max_late_s: float = 20.0
    settle_timeout_minutes: float = 90.0
    drain_minutes: float = 0.0
    backfill: bool = True
    fsync: bool = True
    status_every_minutes: int = 1
    signals: tuple[str, ...] = DEFAULT_SIGNALS
    timezone: str = LOCAL_TIMEZONE
    #: ``minute`` re-runs ``strategy.reset(ctx)`` on the data visible now before every decision: required for
    #: strategies that precompute their whole plan in ``reset`` (forecast MPC, PRISM). A strategy can instead
    #: implement ``refresh(ctx)``, which the engine calls before every decision.
    replan_every: ReplanSetting = "never"
    #: archive every row version seen (raw upstream JSON + normalized) under ``<run_dir>/archive``
    archive: bool = True
    #: a feed without new rows for this long is treated as stopped (e.g. the current-rule datasets after the
    #: go-live): it no longer counts for the missing-row check, and it no longer counts as the live settlement rule
    stale_after_minutes: float = 60.0

    def __post_init__(self) -> None:
        if self.mechanism not in ("auto", "current", "new2026"):
            raise ValueError("mechanism must be auto, current or new2026")
        if self.replan_every not in ("never", "minute"):
            raise ValueError("replan_every must be never or minute")
        if self.history_days < 0 or self.minute_history_hours < 0:
            raise ValueError("history lengths must be non-negative")
        if not 0.0 <= self.decision_offset_s < 30.0:
            raise ValueError("decision_offset_s must be in [0, 30)")

    def as_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["publication"] = asdict(self.publication)
        for key in ("go_live_utc", "window_start_utc", "window_end_utc"):
            out[key] = iso_utc(out[key]) if out[key] is not None else None
        out["signals"] = list(self.signals)
        return out


@dataclass
class QuarterHourBook:
    """Minute powers of one quarter-hour while it is open (``status`` "" = outside the session)."""

    start_ns: int
    power: list[float] = field(default_factory=lambda: [0.0] * MINUTES_PER_QH)
    soc: list[float | None] = field(default_factory=lambda: [None] * MINUTES_PER_QH)
    status: list[str] = field(default_factory=lambda: [""] * MINUTES_PER_QH)
    closed: bool = False

    @property
    def start(self) -> pd.Timestamp:
        return pd.Timestamp(self.start_ns, tz="UTC")

    def to_json(self) -> dict[str, Any]:
        return {
            "start_utc": iso_utc(self.start),
            "power": self.power,
            "soc": self.soc,
            "status": self.status,
            "closed": self.closed,
        }

    @classmethod
    def from_json(cls, payload: Mapping[str, Any]) -> QuarterHourBook:
        return cls(
            start_ns=int(_utc(payload["start_utc"]).value),
            power=[float(v or 0.0) for v in payload["power"]],
            soc=[None if v is None else float(v) for v in payload["soc"]],
            status=[str(v) for v in payload["status"]],
            closed=bool(payload.get("closed", False)),
        )


@dataclass
class PriceRecord:
    price: float
    quality: str | None
    source: str
    first_seen_utc: pd.Timestamp
    updated_utc: pd.Timestamp
    revisions: int = 0


def _new_counters() -> dict[str, int]:
    return {
        "decisions": 0,
        "missed": 0,
        "late": 0,
        "strategy_errors": 0,
        "execution_errors": 0,
        "gap_decisions": 0,
        "resumes": 0,
        "settlements": 0,
        "corrections": 0,
    }


@dataclass
class EngineState:
    """What must survive a restart (snapshot; the decision journal is the source of truth)."""

    run_id: str
    created_at_utc: str
    soc_mwh: float
    last_power_mw: float = 0.0
    last_minute_utc: str | None = None
    books: dict[int, QuarterHourBook] = field(default_factory=dict)
    settled: dict[int, dict[str, Any]] = field(default_factory=dict)
    counters: dict[str, int] = field(default_factory=_new_counters)

    @property
    def last_minute(self) -> pd.Timestamp | None:
        return _utc(self.last_minute_utc) if self.last_minute_utc else None

    def to_json(self) -> dict[str, Any]:
        return {
            "schema": "euroflex-live-state/1",
            "run_id": self.run_id,
            "created_at_utc": self.created_at_utc,
            "soc_mwh": self.soc_mwh,
            "last_power_mw": self.last_power_mw,
            "last_minute_utc": self.last_minute_utc,
            "books": [book.to_json() for _, book in sorted(self.books.items())],
            "settled": {iso_utc(pd.Timestamp(k, tz="UTC")): v for k, v in sorted(self.settled.items())},
            "counters": self.counters,
        }

    @classmethod
    def from_json(cls, payload: Mapping[str, Any]) -> EngineState:
        books = [QuarterHourBook.from_json(item) for item in payload.get("books", [])]
        settled = {int(_utc(k).value): dict(v) for k, v in dict(payload.get("settled", {})).items()}
        counters = {**_new_counters(), **{k: int(v) for k, v in dict(payload.get("counters", {})).items()}}
        return cls(
            run_id=str(payload["run_id"]),
            created_at_utc=str(payload["created_at_utc"]),
            soc_mwh=float(payload["soc_mwh"]),
            last_power_mw=float(payload.get("last_power_mw", 0.0)),
            last_minute_utc=payload.get("last_minute_utc"),
            books={book.start_ns: book for book in books},
            settled=settled,
            counters=counters,
        )


HistoryLoader = Callable[[pd.Timestamp, pd.Timestamp], Sequence[tuple[str, pd.DataFrame]]]


class PublicDataHistory:
    """Recent history from the committed public data set (``data/public``) when it covers the window.

    Missing files are not an error: the live engine then relies on the API backfill. Notes about what could not be
    loaded are kept in :attr:`notes` (local paths stripped).
    """

    def __init__(self, *, minute_hours: float = 6.0, include_day_ahead: bool = True) -> None:
        self.minute_hours = minute_hours
        self.include_day_ahead = include_day_ahead
        self.notes: list[str] = []

    def __call__(self, start: pd.Timestamp, end: pd.Timestamp) -> list[tuple[str, pd.DataFrame]]:
        try:
            pdata: Any = importlib.import_module("euroflex_bess_lab.public_data")
        except ImportError as exc:  # pragma: no cover - the package ships with the repo
            self.notes.append(f"public data layer unavailable: {exc}")
            return []
        minute_start = max(start, end - pd.Timedelta(hours=self.minute_hours))
        jobs: list[tuple[str, str, tuple[Any, ...]]] = [
            ("qh", "load_belgium_qh", (start, end)),
            ("minute", "load_belgium_minute", (minute_start, end)),
            ("si_forecast_current", "load_belgium_si_forecast", ("current", minute_start, end)),
            ("si_forecast_next", "load_belgium_si_forecast", ("next", minute_start, end)),
        ]
        if self.include_day_ahead:
            jobs.append(("day_ahead", "load_day_ahead_prices", ("BE", start, end + pd.Timedelta(days=2))))
        if start < pd.Timestamp("2026-08-01", tz="UTC") and end > pd.Timestamp("2026-01-01", tz="UTC"):
            jobs.append(("nf_qh", "load_belgium_new_formula_qh", (start, end)))
            jobs.append(("nf_minute", "load_belgium_new_formula_minute", (minute_start, end)))
        out: list[tuple[str, pd.DataFrame]] = []
        for table, function, args in jobs:
            loader = getattr(pdata, function, None)
            if loader is None:
                self.notes.append(f"{table}: loader {function} not available")
                continue
            try:
                frame = loader(*args)
            except (FileNotFoundError, KeyError, ValueError, OSError) as exc:
                self.notes.append(f"{table}: {sanitize_message(str(exc))[:200]}")
                continue
            if frame is not None and len(frame):
                out.append((table, frame))
        return out


class LiveEngine:
    """Run a :class:`~euroflex_bess_lab.realtime.Strategy` live in paper mode.

    Typical use (see :mod:`euroflex_bess_lab.realtime.live.cli`)::

        engine = LiveEngine(ThresholdStrategy(), BatteryParams(1.0, 2.0), source=EliaLiveFeeds(),
                            run_dir="artifacts/live/demo")
        summary = engine.run(max_minutes=30)

    ``run_dir`` holds the journal, the paper ledger and the heartbeat; running again on the same directory
    resumes (SOC, open quarter-hours and unsettled prices are restored; minutes while the engine was down are
    recorded as missed, i.e. idle).
    """

    def __init__(
        self,
        strategy: Strategy,
        battery: BatteryParams,
        *,
        source: LiveSource,
        run_dir: str | Path,
        execution: ExecutionAdapter | None = None,
        clock: Clock | None = None,
        config: LiveConfig | None = None,
        strategy_label: str | None = None,
        history: HistoryLoader | None = None,
        run_id: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        self.strategy = strategy
        self.battery = battery
        self.source = source
        self.execution: ExecutionAdapter = execution or PaperExecution()
        self.clock: Clock = clock or SystemClock()
        self.config = config or LiveConfig()
        self.publication = self.config.publication
        self.store = RunStore(run_dir, fsync=self.config.fsync)
        self.strategy_label = strategy_label or str(getattr(strategy, "name", type(strategy).__name__))
        self.history = history
        self.metadata = dict(metadata or {})
        self._requested_run_id = run_id
        self.state: EngineState | None = None
        self.market: MarketData | None = None
        self.ctx: DecisionContext | None = None
        self.next_minute: pd.Timestamp | None = None
        self._minute_fields: list[str] = list(CONTRACT_MINUTE_FIELDS)
        self._qh_fields: list[str] = list(CONTRACT_QH_FIELDS)
        self._pending: list[tuple[str, pd.DataFrame]] = []
        self._da_rows = pd.DataFrame(
            {
                "timestamp_utc": pd.Series(dtype="datetime64[ns, UTC]"),
                "resolution_minutes": pd.Series(dtype="int64"),
                "price_eur_mwh": pd.Series(dtype="float64"),
            }
        )
        self._da_full = np.zeros(0)
        self._prices: dict[str, dict[int, PriceRecord]] = {"current": {}, "new2026": {}}
        self._received: dict[str, set[int]] = {}
        self._latest_received: dict[str, int] = {}
        self._feed_tables: dict[str, str] = {}
        self._resettle: set[int] = set()
        self._nf_live = False
        self._latest_price_ns: dict[str, int] = {}
        self._go_live_observed_ns: int | None = None
        self._needs_reset = True
        self._started = False
        self._stop_requested = False
        self._started_at: pd.Timestamp | None = None
        self._ledger = pd.DataFrame()
        self._last_status_minute: pd.Timestamp | None = None
        self._notes: list[str] = []
        self._fast_nf_polling = False

    # ------------------------------------------------------------------ properties

    @property
    def run_id(self) -> str:
        return self.state.run_id if self.state is not None else (self._requested_run_id or "")

    @property
    def ledger(self) -> pd.DataFrame:
        return self._ledger

    def request_stop(self) -> None:
        self._stop_requested = True

    # ------------------------------------------------------------------ lifecycle

    def start(self, *, resume: bool = True) -> None:
        """Load or create the run state, build the window, load history and backfill from the API."""
        now = self.clock.now()
        self._load_or_create_state(resume=resume, now=now)
        first = self._first_minute(now)
        anchor = self.config.window_start_utc or _floor_qh(first - pd.Timedelta(days=self.config.history_days))
        anchor = _utc(anchor)
        if anchor > first:
            raise ValueError("window_start_utc must not be after the first decision minute")
        horizon = self.config.window_end_utc or local_midnight_after(
            first, self.config.horizon_days + 1, self.config.timezone
        )
        self._rebuild(_floor_qh(anchor), _utc(horizon))
        self._load_history(anchor, first)
        if self.config.backfill:
            self._backfill(anchor, first, now)
        if self.config.mechanism == "new2026" and first < self.config.go_live_utc:
            self._note(
                "mechanism new2026 before the go-live: Elia publishes no live new-formula price yet, so quarter-hours "
                "stay unsettled (price_missing) until `live reconcile`"
            )
        self._fill_missed_until(first, reason="engine_down")
        self.next_minute = first
        self._needs_reset = True
        self._started = True
        self._started_at = now
        self._ledger = self.store.read_ledger()
        self.store.append_jsonl(
            EVENTS,
            {
                "event": "start" if self.state is not None and self.state.counters["resumes"] == 0 else "resume",
                "at_utc": now,
                "first_minute_utc": first,
                "window_start_utc": self.market.start_utc if self.market is not None else None,
                "window_end_utc": self.market.end_utc if self.market is not None else None,
                "notes": list(self._notes),
            },
        )
        self._persist(now, label="running", force_status=True)

    def run(
        self,
        max_minutes: int | None = None,
        *,
        until: pd.Timestamp | None = None,
        finish_quarter_hour: bool = False,
        drain_minutes: float | None = None,
        resume: bool = True,
    ) -> dict[str, Any]:
        """Decide minute by minute until ``max_minutes`` decisions (or ``until``), then drain and stop."""
        if not self._started:
            self.start(resume=resume)
        reason = "completed"
        steps = 0
        try:
            while not self._stop_requested:
                minute = self.next_minute
                assert minute is not None
                if until is not None and minute >= until:
                    break
                if self.config.window_end_utc is not None and minute >= _utc(self.config.window_end_utc):
                    break
                if max_minutes is not None and steps >= max_minutes:
                    if not (finish_quarter_hour and minute.minute % MINUTES_PER_QH != 0):
                        break
                self.step()
                steps += 1
            if self._stop_requested:
                reason = "stopped"
            drain = self.config.drain_minutes if drain_minutes is None else drain_minutes
            if drain > 0 and not self._stop_requested:
                self.drain(drain)
        except KeyboardInterrupt:
            reason = "interrupted"
        return self.stop(reason)

    def step(self) -> dict[str, Any] | None:
        """Wait for the next decision minute (polling feeds meanwhile), decide, settle and persist."""
        assert self.next_minute is not None and self.state is not None
        minute = self.next_minute
        self._ensure_window(minute)
        assert self.market is not None
        if minute >= self.market.end_utc:
            raise RuntimeError(f"decision minute {iso_utc(minute)} is outside the window (fixed window_end_utc?)")
        decision_at = minute + pd.Timedelta(seconds=self.config.decision_offset_s)
        self._poll_until(decision_at)
        self._sleep_until(decision_at)
        if self._stop_requested:
            return None
        now = self.clock.now()
        late_s = (now - decision_at).total_seconds()
        if late_s > self.config.max_late_s:
            resume_at = self._first_minute(now)
            self.state.counters["late"] += 1
            self._fill_missed_until(resume_at, reason="late")
            self.next_minute = resume_at
            self._settle_ready(now)
            self._persist(now)
            return None
        record = self._decide(minute, now, late_s)
        self.next_minute = minute + pd.Timedelta(minutes=1)
        self._settle_ready(now)
        self._persist(now)
        return record

    def drain(self, minutes: float) -> None:
        """No more decisions: keep polling until closed quarter-hours are settled or ``minutes`` elapse."""
        assert self.state is not None
        end = self.clock.now() + pd.Timedelta(minutes=minutes)
        while not self._stop_requested and any(b.closed for b in self.state.books.values()):
            now = self.clock.now()
            if now >= end:
                break
            due = self.source.next_due()
            target = min(due, end) if due is not None else min(now + pd.Timedelta(seconds=30), end)
            self._sleep_until(max(target, now))
            self._poll(deadline=None)
            now = self.clock.now()
            self._settle_ready(now)
            self._persist(now, label="draining")
            if due is None or (self.source.next_due() or end) <= now:
                self.clock.sleep_until(now + pd.Timedelta(seconds=5))

    def stop(self, reason: str = "stopped") -> dict[str, Any]:
        now = self.clock.now()
        if self.state is not None:
            self._settle_ready(now)
            self._persist(now, label=reason, force_status=True)
            self.store.append_jsonl(EVENTS, {"event": "stop", "reason": reason, "at_utc": now})
        try:
            self.execution.close()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("execution adapter close failed: %s", exc)
        return self.summary(now, label=reason)

    # ------------------------------------------------------------------ state

    def _config_payload(self) -> dict[str, Any]:
        return {
            "schema": "euroflex-live-config/1",
            "strategy": self.strategy_label,
            "strategy_class": f"{type(self.strategy).__module__}.{type(self.strategy).__qualname__}",
            "battery": asdict(self.battery),
            "config": self.config.as_dict(),
            "execution": self.execution.describe(),
            "source": type(self.source).__name__,
            "feeds": [
                {"name": s.name, "dataset_id": s.dataset_id, "table": s.table, "poll_every_s": s.poll_every_s}
                for s in self.source.specs
            ],
            "metadata": self.metadata,
            "mode": "paper",
            "market_submission": False,
            "safety": SAFETY_STATEMENT,
            "attribution": [
                "Elia Transmission Belgium SA, Elia Open Data (CC BY 4.0)",
                "Bundesnetzagentur | SMARD.de via Energy-Charts (CC BY 4.0)",
            ],
        }

    def _load_or_create_state(self, *, resume: bool, now: pd.Timestamp) -> None:
        self.store.ensure()
        repaired = {k: v for k, v in self.store.repair().items() if v}
        if repaired:
            self._note(f"repaired partial trailing lines after a crash: {repaired}")
        snapshot = self.store.read_json(STATE) if resume else None
        journal_exists = self.store.exists(DECISIONS) and resume
        if snapshot is not None or journal_exists:
            previous = self.store.read_json(CONFIG) or {}
            self._check_compatible(previous)
            if snapshot is not None:
                self.state = EngineState.from_json(snapshot)
            else:
                self.state = self._fresh_state(now, run_id=str(previous.get("run_id") or "") or None)
            self._roll_forward(self.store.read_jsonl(DECISIONS))
            self._reconcile_settled_with_ledger()
            self.state.counters["resumes"] += 1
            return
        if not resume and (self.store.exists(DECISIONS) or self.store.exists(STATE)):
            raise FileExistsError("run directory already holds a run; resume it or choose another --run-dir")
        self.state = self._fresh_state(now, run_id=self._requested_run_id)
        payload = self._config_payload()
        payload["run_id"] = self.state.run_id
        payload["created_at_utc"] = self.state.created_at_utc
        self.store.write_json(CONFIG, payload)

    def _fresh_state(self, now: pd.Timestamp, *, run_id: str | None) -> EngineState:
        stamp = now.strftime("%Y%m%dT%H%M%SZ")
        return EngineState(
            run_id=run_id or f"live-{stamp}-{uuid.uuid4().hex[:6]}",
            created_at_utc=iso_utc(now) or "",
            soc_mwh=self.battery.initial_soc_mwh,
        )

    def _check_compatible(self, previous: Mapping[str, Any]) -> None:
        if not previous:
            return
        battery = previous.get("battery")
        if battery and dict(battery) != asdict(self.battery):
            raise ValueError("cannot resume: the battery differs from the one this run was started with")
        strategy = previous.get("strategy")
        if strategy and strategy != self.strategy_label:
            raise ValueError(f"cannot resume: run was started with strategy `{strategy}`, not `{self.strategy_label}`")

    def _roll_forward(self, records: Sequence[Mapping[str, Any]]) -> None:
        """Apply journal records newer than the snapshot (the journal is written before the snapshot)."""
        assert self.state is not None
        last = self.state.last_minute
        for record in records:
            if "minute_utc" not in record:
                continue
            minute = _utc(record["minute_utc"])
            if last is not None and minute <= last:
                continue
            applied = float(record.get("applied_mw") or 0.0)
            soc_end = record.get("soc_end_mwh")
            soc_end = float(soc_end) if soc_end is not None else self.state.soc_mwh
            status = str(record.get("status") or "ok")
            self._book_minute(minute, applied, soc_end, status)
            self.state.soc_mwh = soc_end
            self.state.last_power_mw = applied
            self.state.last_minute_utc = iso_utc(minute)
            key = "missed" if status == "missed" else "decisions"
            self.state.counters[key] += 1
            last = minute

    def _reconcile_settled_with_ledger(self) -> None:
        """Quarter-hours already in the ledger are settled, even if the snapshot predates that write."""
        assert self.state is not None
        ledger = self.store.read_ledger()
        if ledger.empty:
            return
        for row in ledger.to_dict("records"):
            ns = int(_utc(row["timestamp_utc"]).value)
            book = self.state.books.get(ns)
            if book is not None and book.closed:
                del self.state.books[ns]
            if ns not in self.state.settled:
                self.state.settled[ns] = self._settled_info_from_row(row)

    @staticmethod
    def _settled_info_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
        keys = (
            "energy_mwh",
            "discharged_mwh",
            "charged_mwh",
            "degradation_eur",
            "soc_end_mwh",
            "price_eur_mwh",
            "mechanism",
            "revision",
            "price_missing",
            "minutes_decided",
            "minutes_missed",
            "minutes_outside_session",
            "mechanism_fallback",
            "mechanism_basis",
        )
        info = {k: row.get(k) for k in keys}
        for k, v in list(info.items()):
            if isinstance(v, float) and math.isnan(v):
                info[k] = None
            elif isinstance(v, np.generic):
                info[k] = v.item()
        return info

    def _note(self, text: str) -> None:
        clean = sanitize_message(text)
        self._notes.append(clean)
        LOGGER.info(clean)

    # ------------------------------------------------------------------ window

    def _allocate(self, anchor: pd.Timestamp, horizon: pd.Timestamp) -> MarketData:
        minute_times = pd.date_range(anchor, horizon, freq="1min", inclusive="left")
        qh_times = pd.date_range(anchor, horizon, freq="15min", inclusive="left")
        minute = {name: np.full(len(minute_times), np.nan) for name in self._minute_fields}
        qh = {name: np.full(len(qh_times), np.nan) for name in self._qh_fields}
        settlement = {name: qh[column] for name, column in SETTLEMENT_COLUMNS.items()}
        da_visible = day_ahead_visible_from_minute(
            qh_times,
            anchor,
            timezone=self.config.timezone,
            publication_hour_local=self.publication.da_publication_hour_local,
        )
        return MarketData(
            start_utc=anchor,
            end_utc=horizon,
            minute_times=minute_times,
            qh_times=qh_times,
            minute=minute,
            qh=qh,
            settlement_prices=settlement,
            da_price=np.full(len(qh_times), np.nan),
            da_visible_from_minute=da_visible,
            timezone=self.config.timezone,
            meta={"source": "live", "run_id": self.run_id},
        )

    def _rebuild(self, anchor: pd.Timestamp, horizon: pd.Timestamp) -> None:
        """(Re)allocate the window, copying everything already visible from the previous window."""
        old = self.market
        new = self._allocate(anchor, horizon)
        if old is not None:
            shift_m = int((old.start_utc - anchor) / pd.Timedelta(minutes=1))
            shift_q = shift_m // MINUTES_PER_QH
            for target, source, shift in ((new.minute, old.minute, shift_m), (new.qh, old.qh, shift_q)):
                size = len(next(iter(target.values()))) if target else 0
                for name, values in source.items():
                    if name not in target:
                        target[name] = np.full(size, np.nan)
                    _copy_shifted(target[name], values, shift)
            _copy_shifted(new.da_price, old.da_price, shift_q)
            if anchor != old.start_utc:
                self._needs_reset = True
                self._note(f"window re-anchored at {iso_utc(anchor)}; strategy.reset() runs before the next decision")
        self.market = new
        self._da_full = (
            expand_day_ahead_to_qh(self._da_rows, new.qh_times) if len(self._da_rows) else np.full(new.n_qh, np.nan)
        )
        ctx = DecisionContext(new, self.battery, self.publication, self._mechanism_at(anchor))
        if self.state is not None:
            ctx.soc_mwh = self.state.soc_mwh
            ctx.last_power_mw = self.state.last_power_mw
        self.ctx = ctx
        self._prune(anchor)

    def _ensure_window(self, minute: pd.Timestamp) -> None:
        assert self.market is not None
        if self.config.window_end_utc is not None:
            return
        anchor, horizon = self.market.start_utc, self.market.end_utc
        new_anchor, new_horizon = anchor, horizon
        if minute + pd.Timedelta(days=1) >= horizon:
            # at least two local midnights ahead, so the window always grows past minute + 1 day
            new_horizon = local_midnight_after(minute, self.config.horizon_days + 2, self.config.timezone)
        if self.config.window_start_utc is None and minute - anchor > pd.Timedelta(days=self.config.history_days + 1):
            new_anchor = _floor_qh(minute - pd.Timedelta(days=self.config.history_days))
        if (new_anchor, new_horizon) != (anchor, horizon):
            self._rebuild(new_anchor, new_horizon)

    def _prune(self, anchor: pd.Timestamp) -> None:
        cutoff = int(anchor.value)
        for name, seen in self._received.items():
            if len(seen) > 20_000:
                self._received[name] = {v for v in seen if v >= cutoff}
        for prices in self._prices.values():
            for key in [k for k in prices if k < cutoff - 2 * 86_400 * 1_000_000_000]:
                del prices[key]
        if self.state is not None:
            old = [k for k in self.state.settled if k < cutoff - 3 * 86_400 * 1_000_000_000]
            for key in old:
                del self.state.settled[key]

    # ------------------------------------------------------------------ data in

    def _load_history(self, anchor: pd.Timestamp, first: pd.Timestamp) -> None:
        if self.history is None:
            return
        try:
            tables = list(self.history(anchor, first))
        except Exception as exc:  # noqa: BLE001 - history is optional
            self._note(f"history loader failed: {type(exc).__name__}: {exc}")
            return
        notes = getattr(self.history, "notes", None)
        if notes:
            for text in notes:
                self._note(f"data/public: {text}")
        for table, frame in tables:
            spec = replay_spec(table, frame, name=f"history_{table}")
            update = FeedUpdate(spec=spec, received_at=self.clock.now(), rows=frame, n_new=len(frame), backfill=True)
            self._ingest(update)
            self._note(f"data/public history: {table} rows={len(frame)}")

    def _backfill(self, anchor: pd.Timestamp, first: pd.Timestamp, now: pd.Timestamp) -> None:
        start = max(anchor, first - pd.Timedelta(hours=self.config.minute_history_hours))
        last = self.state.last_minute if self.state is not None else None
        if last is not None:
            # resuming after downtime: re-read everything since the last journaled minute so that the archive has
            # no hole (as far as Elia still serves it), bounded by the rolling window
            start = max(anchor, min(start, last - pd.Timedelta(hours=1)))
        updates = self.source.backfill(start, now, qh_history_start=anchor)
        for update in updates:
            self._ingest(update)
            self._archive(update)
            if update.error:
                self._note(f"backfill {update.spec.name}: {update.error}")
        if updates:
            self.store.append_jsonl_many(FEED_LOG, [u.log_record() for u in updates])

    def _sleep_until(self, target: pd.Timestamp) -> None:
        """Wait on the clock; with the real clock, wake every second so a stop request is honoured quickly."""
        if not isinstance(self.clock, SystemClock):
            self.clock.sleep_until(target)
            return
        while not self._stop_requested:
            now = self.clock.now()
            if now >= target:
                return
            self.clock.sleep_until(min(target, now + pd.Timedelta(seconds=1)))

    def _poll_until(self, decision_at: pd.Timestamp) -> None:
        while not self._stop_requested:
            due = self.source.next_due()
            if due is None or due >= decision_at:
                return
            self._sleep_until(due)
            if self._stop_requested:
                return
            if self.clock.now() >= decision_at:
                return
            updates = self._poll(deadline=decision_at - pd.Timedelta(seconds=0.25))
            if not updates:
                return

    def _poll(self, *, deadline: pd.Timestamp | None) -> list[FeedUpdate]:
        now = self.clock.now()
        updates = self.source.poll(now, deadline=deadline)
        for update in updates:
            self._ingest(update)
            self._archive(update)
        if updates:
            self.store.append_jsonl_many(FEED_LOG, [u.log_record() for u in updates])
        self._adapt_new_formula_polling(now)
        return updates

    def _adapt_new_formula_polling(self, now: pd.Timestamp) -> None:
        """Poll the new-formula feeds fast from one hour before the go-live (slowly before)."""
        if self._fast_nf_polling or now < self.config.go_live_utc - pd.Timedelta(hours=1):
            return
        for spec in self.source.specs:
            if spec.table == "nf_minute":
                self.source.set_poll_interval(spec.name, 15.0)
            elif spec.table == "nf_qh":
                self.source.set_poll_interval(spec.name, 30.0)
        self._fast_nf_polling = True

    def _archive(self, update: FeedUpdate) -> None:
        """Keep every row version exactly as published (raw upstream JSON) plus its normalized form."""
        rows = update.rows
        if not self.config.archive or rows is None or rows.empty:
            return
        spec = update.spec
        try:
            times = pd.DatetimeIndex(pd.to_datetime(rows[spec.time_column], utc=True))
            days = list(times.strftime("%Y-%m-%d"))
            kinds = list(update.kinds) if len(update.kinds) == len(rows) else ["new"] * len(rows)
            raw = list(update.raw) if len(update.raw) == len(rows) else [None] * len(rows)
            received = iso_utc(update.received_at, fractional=True)
            groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
            for i, item in enumerate(raw):
                if item is None:
                    continue
                dataset_id, record = item
                groups.setdefault((dataset_id, days[i]), []).append(
                    {
                        "received_at_utc": received,
                        "dataset_id": dataset_id,
                        "feed": spec.name,
                        "kind": kinds[i],
                        "backfill": update.backfill,
                        "record": record,
                    }
                )
            for (dataset_id, day), lines in groups.items():
                self.store.append_archive_raw(dataset_id, day, lines)
            frame = rows.reset_index(drop=True).copy()
            frame.insert(0, "received_at_utc", received)
            frame.insert(1, "kind", kinds)
            frame.insert(2, "backfill", update.backfill)
            frame.insert(3, "source_dataset_id", [item[0] if item is not None else spec.dataset_id for item in raw])
            for day, part in frame.groupby(pd.Series(days, index=frame.index), sort=True):
                self.store.append_archive_normalized(spec.dataset_id, str(day), part)
        except OSError as exc:  # the archive must never stop trading; report it
            self._note(f"archive write failed for {spec.dataset_id}: {type(exc).__name__}")

    def _ingest(self, update: FeedUpdate) -> None:
        spec = update.spec
        self._feed_tables[spec.name] = spec.table
        rows = update.rows
        if rows is None or rows.empty:
            return
        if spec.table == "day_ahead":
            self._ingest_day_ahead(rows)
            return
        times = pd.DatetimeIndex(pd.to_datetime(rows[spec.time_column], utc=True))
        if spec.table in ("si_forecast_current", "si_forecast_next"):
            times = times.floor("1min")
        ns = np.asarray(times.asi8, dtype=np.int64)
        columns = [c for c in spec.numeric_columns if c in rows.columns]
        if spec.table in QH_TABLES:
            self._record_prices(spec.table, spec.dataset_id, rows, times, update.received_at)
        if not columns:
            return
        values = rows[columns].to_numpy(dtype=float, na_value=np.nan)
        data = pd.DataFrame(np.array(values, dtype=float, copy=True), columns=[spec.prefix + c for c in columns])
        data.insert(0, "_ns", ns)
        kind = "qh" if spec.table in QH_TABLES else "minute"
        if kind == "minute":
            seen = self._received.setdefault(spec.name, set())
            seen.update(int(v) for v in ns)
            if len(ns):
                self._latest_received[spec.name] = max(self._latest_received.get(spec.name, 0), int(ns.max()))
            if spec.table == "nf_minute" and len(ns) and int(ns.max()) >= int(self.config.go_live_utc.value):
                self._nf_live = True
        self._pending.append((kind, data))

    def _record_prices(
        self, table: str, source: str, rows: pd.DataFrame, times: pd.DatetimeIndex, received_at: pd.Timestamp
    ) -> None:
        if "imbalance_price_eur_mwh" not in rows.columns:
            return
        mechanism = "current" if table == "qh" else "new2026"
        prices = rows["imbalance_price_eur_mwh"].to_numpy(dtype=float, na_value=np.nan)
        qualities = rows["quality_status"].tolist() if "quality_status" in rows.columns else [None] * len(rows)
        store = self._prices[mechanism]
        go_live_ns = int(self.config.go_live_utc.value)
        for ts_ns, price, quality in zip(times.asi8, prices, qualities, strict=True):
            if math.isnan(price):
                continue
            key = int(ts_ns)
            quality_text = (
                None if quality is None or (isinstance(quality, float) and math.isnan(quality)) else str(quality)
            )
            if quality_text in ("<NA>", "nan"):
                quality_text = None
            record = store.get(key)
            if record is None:
                store[key] = PriceRecord(float(price), quality_text, source, received_at, received_at)
                if (
                    self.state is not None
                    and key in self.state.settled
                    and self.state.settled[key].get("price_missing")
                ):
                    self._resettle.add(key)
            elif abs(record.price - float(price)) > 1e-6 or record.quality != quality_text:
                changed_price = abs(record.price - float(price)) > 1e-6
                record.price, record.quality, record.updated_utc = float(price), quality_text, received_at
                record.source = source
                if changed_price:
                    record.revisions += 1
                    if self.state is not None and key in self.state.settled:
                        self._resettle.add(key)
            if key > self._latest_price_ns.get(mechanism, -1):
                self._latest_price_ns[mechanism] = key
            if mechanism == "new2026" and key >= go_live_ns:
                self._nf_live = True
                if self._go_live_observed_ns is None or key < self._go_live_observed_ns:
                    self._go_live_observed_ns = key

    def _ingest_day_ahead(self, rows: pd.DataFrame) -> None:
        frame = rows.copy()
        frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True).astype("datetime64[ns, UTC]")
        if "resolution_minutes" not in frame.columns:
            frame["resolution_minutes"] = MINUTES_PER_QH
        frame = frame[["timestamp_utc", "resolution_minutes", "price_eur_mwh"]].astype(
            {"resolution_minutes": "int64", "price_eur_mwh": "float64"}
        )
        merged = pd.concat([self._da_rows, frame], ignore_index=True) if len(self._da_rows) else frame
        self._da_rows = (
            merged.drop_duplicates("timestamp_utc", keep="last").sort_values("timestamp_utc").reset_index(drop=True)
        )
        if self.market is not None:
            self._da_full = expand_day_ahead_to_qh(self._da_rows, self.market.qh_times)

    def _publish(self, t: int) -> None:
        """Write every buffered row that the publication model makes visible at decision minute ``t``."""
        market = self.market
        assert market is not None
        anchor_ns = int(market.start_utc.value)
        cut_minute = self.publication.visible_minute_end(t)
        cut_qh = self.publication.visible_qh_end(t)
        keep: list[tuple[str, pd.DataFrame]] = []
        slack = {"minute": 3 * 1440, "qh": 3 * 96}
        for kind, data in self._pending:
            ns = data["_ns"].to_numpy(dtype=np.int64)
            if kind == "minute":
                index = (ns - anchor_ns) // MINUTE_NS
                size, cut, arrays, fields = market.n_minutes, cut_minute, market.minute, self._minute_fields
            else:
                index = (ns - anchor_ns) // QH_NS
                size, cut, arrays, fields = market.n_qh, cut_qh, market.qh, self._qh_fields
            visible = (index >= 0) & (index < size) & (index < cut)
            if visible.any():
                rows = index[visible]
                for column in data.columns[1:]:
                    target = arrays.get(column)
                    if target is None:
                        target = np.full(size, np.nan)
                        arrays[column] = target
                        fields.append(column)
                    target[rows] = data[column].to_numpy(dtype=float)[visible]
            later = (~visible) & (index >= 0) & (index < size + slack[kind])
            if later.any():
                keep.append((kind, data.loc[later]))
        self._pending = keep
        if len(self._da_full) == market.n_qh:
            shown = t >= market.da_visible_from_minute
            market.da_price[shown] = self._da_full[shown]

    # ------------------------------------------------------------------ decisions

    def _first_minute(self, now: pd.Timestamp) -> pd.Timestamp:
        floor = _floor_minute(now)
        if (now - floor).total_seconds() <= self.config.decision_offset_s:
            return floor
        return floor + pd.Timedelta(minutes=1)

    def _minute_index(self, minute: pd.Timestamp) -> int:
        assert self.market is not None
        return int((minute - self.market.start_utc) // pd.Timedelta(minutes=1))

    def _rule_live(self, mechanism: str, now: pd.Timestamp) -> bool:
        """Elia still publishes quarter-hour prices under ``mechanism`` (latest price within the staleness window)."""
        latest = self._latest_price_ns.get(mechanism)
        if latest is None:
            return False
        age = now.value - (latest + QH_NS)
        return age <= int(self.config.stale_after_minutes * MINUTE_NS)

    def _switch_observed(self, now: pd.Timestamp) -> bool:
        """The feeds show the switch: new-formula prices are live and the current-rule prices stopped."""
        return self._rule_live("new2026", now) and not self._rule_live("current", now)

    def _mechanism_at(self, minute: pd.Timestamp) -> str:
        """Settlement rule for decisions at ``minute``: by date, confirmed by which feed is actually live."""
        setting = self.config.mechanism
        if setting != "auto":
            return setting
        if minute < self.config.go_live_utc:
            return "new2026" if self._switch_observed(minute) else "current"
        return "new2026" if self._nf_live else "current"

    def _book_minute(self, minute: pd.Timestamp, power: float, soc_end: float, status: str) -> QuarterHourBook:
        assert self.state is not None
        start = _floor_qh(minute)
        key = int(start.value)
        book = self.state.books.get(key)
        if book is None:
            book = QuarterHourBook(start_ns=key)
            self.state.books[key] = book
        j = int((minute - start) / pd.Timedelta(minutes=1))
        book.power[j] = power
        book.soc[j] = soc_end
        book.status[j] = status
        if j == MINUTES_PER_QH - 1:
            book.closed = True
        return book

    def _missing_expected_rows(self, t: int) -> list[str]:
        assert self.market is not None
        cut = self.publication.visible_minute_end(t)
        if cut <= 0:
            return []
        anchor_ns = int(self.market.start_utc.value)
        expected = anchor_ns + (cut - 1) * MINUTE_NS
        live_feeds = {spec.name for spec in self.source.specs}
        missing: list[str] = []
        for name, seen in self._received.items():
            if name not in live_feeds or self._feed_tables.get(name) not in MINUTE_TABLES:
                continue
            latest = self._latest_received.get(name, 0)
            if latest < anchor_ns:
                continue  # feed not live in this window (e.g. the new formula before the go-live)
            if expected - latest > int(self.config.stale_after_minutes * MINUTE_NS):
                continue  # feed stopped publishing (e.g. the current-rule datasets after the go-live)
            if expected not in seen:
                missing.append(name)
        return sorted(missing)

    def _signals(self, ctx: DecisionContext) -> dict[str, float | None]:
        out: dict[str, float | None] = {}
        fields = list(self.config.signals)
        extra = getattr(self.strategy, "signal_field", None)
        if isinstance(extra, str) and extra not in fields:
            fields.append(extra)
        for name in fields:
            if name in ctx.market.minute:
                out[name] = finite_or_none(ctx.latest(name))
        out["da_price_eur_mwh"] = finite_or_none(ctx.da_price(ctx.qh_index))
        last_final = ctx.settlement_window(1)
        out["last_final_price_eur_mwh"] = finite_or_none(last_final[-1]) if len(last_final) else None
        return out

    def _decide(self, minute: pd.Timestamp, now: pd.Timestamp, late_s: float) -> dict[str, Any]:
        state, market, ctx = self.state, self.market, self.ctx
        assert state is not None and market is not None and ctx is not None
        t = self._minute_index(minute)
        self._publish(t)
        mechanism = self._mechanism_at(minute)
        ctx.mechanism = mechanism
        soc_start = state.soc_mwh
        ctx.advance(t, soc_start)
        ctx.last_power_mw = state.last_power_mw
        status, error = "ok", None
        started = time.perf_counter()
        refresh = getattr(self.strategy, "refresh", None)
        try:
            if self._needs_reset:
                ctx.market = self._market_view()
                self.strategy.reset(ctx)
                self._needs_reset = False
            elif callable(refresh):
                ctx.market = self._market_view()
                refresh(ctx)
            elif self.config.replan_every == "minute":
                ctx.market = self._market_view()
                self.strategy.reset(ctx)
            request = float(self.strategy.decide(ctx))
        except Exception as exc:  # noqa: BLE001 - a strategy bug must not stop the live loop
            request, status = 0.0, "strategy_error"
            error = sanitize_message(f"{type(exc).__name__}: {exc}")[:300]
            state.counters["strategy_errors"] += 1
            LOGGER.exception("strategy %s failed at %s", self.strategy_label, iso_utc(minute))
        compute_ms = (time.perf_counter() - started) * 1000.0
        effective = request if math.isfinite(request) else 0.0
        applied, soc_end = apply_power(effective, soc_start, self.battery)
        applied += 0.0  # normalise -0.0
        setpoint = Setpoint(
            run_id=state.run_id,
            strategy=self.strategy_label,
            minute_utc=minute,
            power_mw=applied,
            requested_mw=request,
            soc_start_mwh=soc_start,
            soc_end_mwh=soc_end,
            mechanism=mechanism,
            decided_at_utc=now,
        )
        receipt = self._send(setpoint)
        missing = self._missing_expected_rows(t)
        if missing:
            state.counters["gap_decisions"] += 1
        state.counters["decisions"] += 1
        anchor = market.start_utc
        cut_minute = self.publication.visible_minute_end(t)
        cut_qh = self.publication.visible_qh_end(t)
        record: dict[str, Any] = {
            "minute_utc": iso_utc(minute),
            "status": status,
            "error": error,
            "strategy": self.strategy_label,
            "mechanism": mechanism,
            "requested_mw": request if math.isfinite(request) else None,
            "applied_mw": applied,
            "soc_start_mwh": soc_start,
            "soc_end_mwh": soc_end,
            "soc_frac": round(soc_end / self.battery.energy_mwh, 6),
            "qh_start_utc": iso_utc(_floor_qh(minute)),
            "minute_in_qh": int(ctx.minute_in_qh),
            "decided_at_utc": iso_utc(now, fractional=True),
            "late_s": round(late_s, 3),
            "window_minute_index": t,
            "visible_minute_end_utc": iso_utc(anchor + pd.Timedelta(minutes=cut_minute)),
            "visible_qh_end_utc": iso_utc(anchor + pd.Timedelta(minutes=MINUTES_PER_QH * cut_qh)),
            "missing_expected_rows": missing,
            "signals": self._signals(ctx),
            "strategy_state": strategy_diagnostics(self.strategy),
            "execution": receipt.as_dict(),
            "compute_ms": round(compute_ms, 3),
        }
        self.store.append_jsonl(DECISIONS, record)
        self._book_minute(minute, applied, soc_end, status)
        state.soc_mwh = soc_end
        state.last_power_mw = applied
        state.last_minute_utc = iso_utc(minute)
        return record

    def _market_view(self) -> MarketData:
        """A new MarketData object sharing the window's arrays.

        Strategies may memoise work per market object (e.g. a cached forecaster keyed by identity); the live window
        is updated in place, so every (re)plan gets a fresh object to see the rows published since the last one.
        """
        assert self.market is not None
        return MarketData(**self.market.__dict__)

    def _send(self, setpoint: Setpoint) -> ExecutionReceipt:
        assert self.state is not None
        try:
            receipt = self.execution.send(setpoint)
        except Exception as exc:  # noqa: BLE001 - adapters must not raise, but never trust them
            receipt = ExecutionReceipt(getattr(self.execution, "name", "unknown"), "error", type(exc).__name__)
        if receipt.status == "error":
            self.state.counters["execution_errors"] += 1
        return receipt

    def _fill_missed_until(self, until: pd.Timestamp, *, reason: str) -> None:
        """Record minutes from the one after the last journal entry up to ``until`` (exclusive) as missed (idle)."""
        assert self.state is not None
        last = self.state.last_minute
        if last is None:
            if self.next_minute is None:
                return
            start = self.next_minute
        else:
            start = last + pd.Timedelta(minutes=1)
        if start >= until:
            return
        records = []
        minute = start
        soc = self.state.soc_mwh
        while minute < until:
            records.append(
                {
                    "minute_utc": iso_utc(minute),
                    "status": "missed",
                    "reason": reason,
                    "strategy": self.strategy_label,
                    "requested_mw": None,
                    "applied_mw": 0.0,
                    "soc_start_mwh": soc,
                    "soc_end_mwh": soc,
                    "qh_start_utc": iso_utc(_floor_qh(minute)),
                }
            )
            self._book_minute(minute, 0.0, soc, "missed")
            self.state.counters["missed"] += 1
            minute = minute + pd.Timedelta(minutes=1)
        self.store.append_jsonl_many(DECISIONS, records)
        self.state.last_power_mw = 0.0
        self.state.last_minute_utc = iso_utc(until - pd.Timedelta(minutes=1))
        LOGGER.warning("recorded %d missed minute(s) (%s) before %s", len(records), reason, iso_utc(until))

    # ------------------------------------------------------------------ settlement

    def _settlement_choice(self, key: int, now: pd.Timestamp) -> tuple[str, PriceRecord | None, str] | None:
        """``(mechanism, price record or None, basis)`` for a closed quarter-hour, or ``None`` to keep waiting.

        ``basis`` is ``configured`` (forced mechanism), ``date`` (the configured go-live decides),
        ``switch_observed`` (before the configured go-live, but the current-rule feed stopped and the new-formula
        feed publishes this quarter-hour) or ``fallback_current_still_live`` (after the configured go-live, but no
        live new-formula price exists while Elia still publishes current-rule prices, i.e. the go-live slipped).
        """
        start = pd.Timestamp(key, tz="UTC")
        age_min = (now - (start + pd.Timedelta(minutes=MINUTES_PER_QH))).total_seconds() / 60.0
        current = self._prices["current"].get(key)
        new = self._prices["new2026"].get(key)
        setting = self.config.mechanism
        mechanism: str
        record: PriceRecord | None
        basis = "date"
        if setting == "current":
            mechanism, record, basis = "current", current, "configured"
        elif setting == "new2026":
            mechanism, record, basis = "new2026", new, "configured"
        elif start < self.config.go_live_utc:
            if current is None and new is not None and self._switch_observed(now):
                mechanism, record, basis = "new2026", new, "switch_observed"
            else:
                mechanism, record = "current", current
        elif new is not None:
            mechanism, record = "new2026", new
        elif not self._nf_live and current is not None and age_min >= self.config.new_formula_grace_minutes:
            mechanism, record, basis = "current", current, "fallback_current_still_live"
        else:
            mechanism, record = "new2026", None
        if record is not None:
            return mechanism, record, basis
        if age_min >= self.config.settle_timeout_minutes:
            return mechanism, None, basis
        return None

    def _book_arrays(self, book: QuarterHourBook) -> tuple[np.ndarray, np.ndarray]:
        power = np.asarray(book.power, dtype=float)
        soc_values = [v for v in book.soc if v is not None]
        fill = soc_values[0] if soc_values else (self.state.soc_mwh if self.state is not None else 0.0)
        soc = []
        for value in book.soc:
            if value is None:
                soc.append(fill)
            else:
                soc.append(value)
                fill = value
        return power, np.asarray(soc, dtype=float)

    def _ledger_extras(
        self,
        *,
        mechanism: str,
        record: PriceRecord | None,
        now: pd.Timestamp,
        revision: int,
        fallback: bool,
        minutes: Mapping[str, int],
        basis: str = "date",
    ) -> dict[str, Any]:
        return {
            "strategy": self.strategy_label,
            "mechanism": mechanism,
            "mechanism_basis": basis,
            "fleet_mw": self.battery.power_mw,
            "minutes_decided": int(minutes["decided"]),
            "minutes_missed": int(minutes["missed"]),
            "minutes_outside_session": int(minutes["outside"]),
            "price_source": record.source if record is not None else None,
            "price_quality_status": record.quality if record is not None else None,
            "price_first_seen_utc": record.first_seen_utc if record is not None else pd.NaT,
            "price_revisions_seen": record.revisions if record is not None else 0,
            "mechanism_fallback": bool(fallback),
            "provisional": True,
            "revision": int(revision),
            "settled_at_utc": now,
            "run_id": self.run_id,
        }

    def _settle_ready(self, now: pd.Timestamp) -> None:
        state = self.state
        assert state is not None
        rows: list[dict[str, Any]] = []
        for key in sorted(k for k, b in state.books.items() if b.closed):
            choice = self._settlement_choice(key, now)
            if choice is None:
                continue
            mechanism, record, basis = choice
            book = state.books.pop(key)
            power, soc = self._book_arrays(book)
            price = np.array([record.price if record is not None else np.nan])
            ledger = settle_quarter_hours(power, soc, price, pd.DatetimeIndex([book.start]), self.battery)
            row = ledger.iloc[0].to_dict()
            minutes = {
                "decided": sum(s in ("ok", "strategy_error") for s in book.status),
                "missed": sum(s == "missed" for s in book.status),
                "outside": sum(s == "" for s in book.status),
            }
            fallback = basis == "fallback_current_still_live"
            row.update(
                self._ledger_extras(
                    mechanism=mechanism,
                    record=record,
                    now=now,
                    revision=0,
                    fallback=fallback,
                    minutes=minutes,
                    basis=basis,
                )
            )
            rows.append(row)
            state.settled[key] = self._settled_info_from_row(row)
            state.counters["settlements"] += 1
        for key in sorted(self._resettle):
            info = state.settled.get(key)
            if info is None:
                continue
            mechanism = str(info.get("mechanism") or "current")
            record = self._prices.get(mechanism, {}).get(key)
            if record is None and info.get("price_missing"):
                choice = self._settlement_choice(key, now)
                if choice is None or choice[1] is None:
                    continue
                mechanism, record, _ = choice
            if record is None:
                continue
            rows.append(self._corrected_row(key, info, mechanism, record, now))
            state.settled[key] = self._settled_info_from_row(rows[-1])
            state.counters["corrections"] += 1
        self._resettle.clear()
        if rows:
            self._ledger = self.store.append_ledger(pd.DataFrame(rows))

    def _corrected_row(
        self, key: int, info: Mapping[str, Any], mechanism: str, record: PriceRecord, now: pd.Timestamp
    ) -> dict[str, Any]:
        energy = float(info.get("energy_mwh") or 0.0)
        degradation = float(info.get("degradation_eur") or 0.0)
        price = float(record.price)
        revenue = energy * price
        row: dict[str, Any] = {
            "timestamp_utc": pd.Timestamp(key, tz="UTC"),
            "energy_mwh": energy,
            "discharged_mwh": float(info.get("discharged_mwh") or 0.0),
            "charged_mwh": float(info.get("charged_mwh") or 0.0),
            "base_price_eur_mwh": price,
            "price_eur_mwh": price,
            "price_missing": False,
            "revenue_eur": revenue,
            "degradation_eur": degradation,
            "pnl_eur": revenue - degradation,
            "soc_end_mwh": float(info.get("soc_end_mwh") or 0.0),
        }
        minutes = {
            "decided": int(info.get("minutes_decided") or 0),
            "missed": int(info.get("minutes_missed") or 0),
            "outside": int(info.get("minutes_outside_session") or 0),
        }
        row.update(
            self._ledger_extras(
                mechanism=mechanism,
                record=record,
                now=now,
                revision=int(info.get("revision") or 0) + 1,
                fallback=bool(info.get("mechanism_fallback") or False),
                minutes=minutes,
                basis=str(info.get("mechanism_basis") or "date"),
            )
        )
        return row

    # ------------------------------------------------------------------ persistence / reporting

    def _persist(self, now: pd.Timestamp, *, label: str = "running", force_status: bool = False) -> None:
        assert self.state is not None
        self.store.write_json(STATE, self.state.to_json(), indent=None)
        minute = _floor_minute(now)
        every = max(1, int(self.config.status_every_minutes))
        due = (
            force_status
            or self._last_status_minute is None
            or (minute - self._last_status_minute) >= pd.Timedelta(minutes=every)
        )
        if due:
            self.store.write_json(STATUS, self.status(now, label=label))
            self._last_status_minute = minute

    def mechanism_detection(self, now: pd.Timestamp) -> dict[str, Any]:
        """Which settlement rule Elia is actually publishing (feeds), next to the configured go-live (date)."""
        latest = {m: self._latest_price_ns.get(m) for m in ("current", "new2026")}
        return {
            "configured_go_live_utc": iso_utc(self.config.go_live_utc),
            "current_rule_live": self._rule_live("current", now),
            "new_rule_live": self._rule_live("new2026", now),
            "latest_current_price_qh_utc": iso_utc(pd.Timestamp(latest["current"], tz="UTC"))
            if latest["current"]
            else None,
            "latest_new_price_qh_utc": iso_utc(pd.Timestamp(latest["new2026"], tz="UTC"))
            if latest["new2026"]
            else None,
            "first_post_go_live_new_price_qh_utc": iso_utc(pd.Timestamp(self._go_live_observed_ns, tz="UTC"))
            if self._go_live_observed_ns is not None
            else None,
            "switch_observed": self._switch_observed(now),
            "stale_after_minutes": self.config.stale_after_minutes,
        }

    def ledger_totals(self) -> dict[str, Any]:
        ledger = self._ledger
        if ledger.empty:
            return {
                "settled_qh": 0,
                "pnl_eur": 0.0,
                "revenue_eur": 0.0,
                "degradation_eur": 0.0,
                "discharged_mwh": 0.0,
                "charged_mwh": 0.0,
                "price_missing_qh": 0,
            }
        return {
            "settled_qh": int(len(ledger)),
            "pnl_eur": round(float(ledger["pnl_eur"].sum()), 4),
            "revenue_eur": round(float(ledger["revenue_eur"].sum()), 4),
            "degradation_eur": round(float(ledger["degradation_eur"].sum()), 4),
            "discharged_mwh": round(float(ledger["discharged_mwh"].sum()), 6),
            "charged_mwh": round(float(ledger["charged_mwh"].sum()), 6),
            "price_missing_qh": int(ledger["price_missing"].astype(bool).sum()),
        }

    def status(self, now: pd.Timestamp, *, label: str = "running") -> dict[str, Any]:
        state = self.state
        assert state is not None
        execution = self.execution.describe()
        for attr in ("sent", "errors"):
            if hasattr(self.execution, attr):
                execution[attr] = getattr(self.execution, attr)
        return {
            "schema": STATUS_SCHEMA,
            "run_id": state.run_id,
            "state": label,
            "mode": "paper",
            "market_submission": False,
            "safety": SAFETY_STATEMENT,
            "strategy": self.strategy_label,
            "mechanism_setting": self.config.mechanism,
            "mechanism_now": self._mechanism_at(_floor_minute(now)),
            "new_formula_live": self._nf_live,
            "mechanism_detection": self.mechanism_detection(now),
            "battery": asdict(self.battery),
            "publication": asdict(self.publication),
            "started_at_utc": iso_utc(self._started_at),
            "heartbeat_utc": iso_utc(now, fractional=True),
            "last_minute_utc": state.last_minute_utc,
            "next_minute_utc": iso_utc(self.next_minute),
            "soc_mwh": round(state.soc_mwh, 6),
            "soc_frac": round(state.soc_mwh / self.battery.energy_mwh, 6),
            "last_power_mw": round(state.last_power_mw, 6),
            "counters": dict(state.counters),
            "ledger": self.ledger_totals(),
            "open_quarter_hours": [iso_utc(b.start) for b in state.books.values() if not b.closed],
            "awaiting_price": [iso_utc(b.start) for b in state.books.values() if b.closed],
            "window": {
                "start_utc": iso_utc(self.market.start_utc) if self.market is not None else None,
                "end_utc": iso_utc(self.market.end_utc) if self.market is not None else None,
            },
            "feeds": self.source.status(now),
            "execution": execution,
            "notes": list(self._notes[-20:]),
        }

    def summary(self, now: pd.Timestamp | None = None, *, label: str = "completed") -> dict[str, Any]:
        now = now or self.clock.now()
        assert self.state is not None
        feeds = self.source.status(now)
        return {
            "run_id": self.state.run_id,
            "state": label,
            "strategy": self.strategy_label,
            "mechanism_setting": self.config.mechanism,
            "started_at_utc": iso_utc(self._started_at),
            "stopped_at_utc": iso_utc(now),
            "counters": dict(self.state.counters),
            "ledger": self.ledger_totals(),
            "soc_mwh": round(self.state.soc_mwh, 6),
            "open_quarter_hours": [iso_utc(b.start) for b in self.state.books.values() if not b.closed],
            "awaiting_price": [iso_utc(b.start) for b in self.state.books.values() if b.closed],
            "feed_latency_s": {
                name: {
                    "dataset_id": info.get("dataset_id"),
                    "latency_upper_s": info.get("latency_upper_s"),
                    "latency_mid_s": info.get("latency_mid_s"),
                    "errors": info.get("errors"),
                    "gaps": info.get("gaps", {}).get("count"),
                }
                for name, info in feeds.items()
            },
            "run_dir": self.store.run_dir.name,
            "mode": "paper",
            "market_submission": False,
        }

    def decisions(self) -> pd.DataFrame:
        """The decision journal as a frame (one row per minute, including missed minutes)."""
        records = self.store.read_jsonl(DECISIONS)
        if not records:
            return pd.DataFrame()
        frame = pd.json_normalize(records)
        frame["minute_utc"] = pd.to_datetime(frame["minute_utc"], utc=True)
        return frame.drop_duplicates("minute_utc", keep="last").sort_values("minute_utc").reset_index(drop=True)


def _copy_shifted(target: np.ndarray, source: np.ndarray, shift: int) -> None:
    """``target[i + shift] = source[i]`` for the overlapping range (shift may be negative)."""
    n_t, n_s = len(target), len(source)
    lo = max(0, -shift)
    hi = min(n_s, n_t - shift)
    if hi > lo:
        target[lo + shift : hi + shift] = source[lo:hi]
