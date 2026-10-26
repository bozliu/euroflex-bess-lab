"""Replay contract-shaped frames through the live engine as if they were being published now.

:class:`ReplayFeeds` is a :class:`~euroflex_bess_lab.realtime.live.feeds.LiveSource` without a network: every row
becomes available ``delay_s`` seconds after its completion time (day-ahead prices on D-1 at 12:45 local time). With a
:class:`~euroflex_bess_lab.realtime.live.clock.ManualClock` the live engine then runs a historical period at full
speed, which is how the tests prove that live decisions equal :func:`~euroflex_bess_lab.realtime.simulate`
decisions on the same data, and how a strategy can be dry-run through the live code path before going live.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from typing import Any, cast

import numpy as np
import pandas as pd

from .feeds import (
    LOCAL_TIMEZONE,
    QH_TABLES,
    TABLE_PREFIX,
    FeedSpec,
    FeedUpdate,
    TableKind,
    _ScheduledFeeds,
)

DEFAULT_DELAYS_S: dict[str, float] = {
    "minute": 20.0,
    "nf_minute": 20.0,
    "si_forecast_current": 30.0,
    "si_forecast_next": 30.0,
    "qh": 180.0,
    "nf_qh": 180.0,
}


def replay_spec(table: str, frame: pd.DataFrame, *, name: str | None = None, poll_every_s: float = 60.0) -> FeedSpec:
    """A pseudo feed for one contract table (``minute``, ``qh``, ``nf_minute``, ..., ``day_ahead``)."""
    if table not in TABLE_PREFIX:
        raise ValueError(f"unknown table `{table}`; expected one of {sorted(TABLE_PREFIX)}")
    forecast = table.startswith("si_forecast")
    time_column = "prediction_time_utc" if forecast else "timestamp_utc"
    if time_column not in frame.columns:
        raise KeyError(f"replay table `{table}` needs a `{time_column}` column")
    extra = {"target_qh_utc": "target_qh_utc"} if forecast and "target_qh_utc" in frame.columns else {}
    skip = {time_column, *extra, "qh_start_utc", "available_utc", "resolution_minutes"}
    columns = {c: c for c in frame.columns if c not in skip}
    if table in QH_TABLES or table == "day_ahead":
        resolution = 15
    else:
        resolution = 0 if forecast else 1
    label = name or f"replay_{table}"
    return FeedSpec(
        name=label,
        dataset_id=label,
        table=cast(TableKind, table),
        columns=columns,
        time_field=time_column,
        time_column=time_column,
        extra_time_fields=extra,
        resolution_minutes=resolution,
        poll_every_s=poll_every_s,
        limit=0,
        source="replay",
    )


class ReplayFeeds(_ScheduledFeeds):
    """Serve contract frames on a simulated publication schedule.

    ``tables`` maps a table kind (optionally ``"<table>:<label>"`` to split one table into several feeds) to a
    contract-shaped frame. An optional ``available_utc`` column overrides the computed availability per row (a row
    repeated with a later ``available_utc`` and different values is served as a revision).
    """

    def __init__(
        self,
        tables: Mapping[str, pd.DataFrame],
        *,
        delays_s: Mapping[str, float] | None = None,
        poll_every_s: float = 60.0,
        prefetch_s: float = 8.0,
        da_publication_local: tuple[int, int] = (12, 45),
        timezone: str = LOCAL_TIMEZONE,
    ) -> None:
        delays = {**DEFAULT_DELAYS_S, **dict(delays_s or {})}
        specs: list[FeedSpec] = []
        prepared: dict[str, tuple[pd.DataFrame, np.ndarray]] = {}
        for key, frame in tables.items():
            table, _, label = key.partition(":")
            spec = replay_spec(table, frame, name=f"replay_{label or table}", poll_every_s=poll_every_s)
            specs.append(spec)
            data = frame.copy()
            for column in data.columns:
                if column.endswith("_utc"):
                    data[column] = pd.to_datetime(data[column], utc=True).astype("datetime64[ns, UTC]")
            available = self._availability(
                spec, data, delays.get(key, delays.get(table, 0.0)), da_publication_local, timezone
            )
            order = np.argsort(available, kind="stable")
            data = data.drop(columns=["available_utc"], errors="ignore")
            prepared[spec.name] = (data.iloc[order].reset_index(drop=True), available[order])
        super().__init__(specs, prefetch_s=prefetch_s, min_interval_s=0.0, timezone=timezone)
        self._frames = prepared
        self._cursor = {name: 0 for name in prepared}

    @staticmethod
    def _availability(
        spec: FeedSpec,
        frame: pd.DataFrame,
        delay_s: float,
        da_publication_local: tuple[int, int],
        timezone: str,
    ) -> np.ndarray:
        times = pd.DatetimeIndex(frame[spec.time_column])
        if spec.table == "day_ahead":
            local_day = times.tz_convert(timezone).normalize()
            hour, minute = da_publication_local
            published = (local_day - pd.Timedelta(days=1) + pd.Timedelta(hours=hour, minutes=minute)).tz_convert("UTC")
            available = np.asarray(published.asi8, dtype=np.int64)
        else:
            completion = times + spec.completion_offset + pd.Timedelta(seconds=delay_s)
            available = np.asarray(completion.asi8, dtype=np.int64)
        if "available_utc" in frame.columns:
            override = pd.DatetimeIndex(pd.to_datetime(frame["available_utc"], utc=True))
            mask = ~override.isna()
            available = np.where(mask, override.asi8, available)
        return available

    def _take(self, name: str, until_ns: int) -> pd.DataFrame:
        frame, available = self._frames[name]
        start = self._cursor[name]
        stop = int(np.searchsorted(available, until_ns, side="right"))
        self._cursor[name] = max(start, stop)
        return frame.iloc[start:stop]

    def poll(
        self, now: pd.Timestamp, *, deadline: pd.Timestamp | None = None, force: Collection[str] = ()
    ) -> list[FeedUpdate]:
        updates: list[FeedUpdate] = []
        for name, state in self.state.items():
            due = state.next_due is None or state.next_due <= now
            if not due and name not in force:
                continue
            state.stats.polls += 1
            state.stats.successes += 1
            state.stats.last_poll_utc = now
            rows = self._take(name, int(now.value))
            if state.spec.table == "day_ahead":
                self._mark_complete_days(state, rows)
            new_rows, n_new, n_revised, latencies, kinds = self._diff(state, rows, now, backfill=False)
            state.last_success_at = now
            state.stats.last_success_utc = now
            self._schedule(state, now)
            updates.append(
                FeedUpdate(
                    spec=state.spec,
                    received_at=now,
                    rows=new_rows,
                    n_new=n_new,
                    n_revised=n_revised,
                    latest_row_utc=state.stats.latest_row_utc,
                    status=200,
                    latency_s=tuple(latencies),
                    kinds=tuple(kinds),
                )
            )
        return updates

    def backfill(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        *,
        deadline: pd.Timestamp | None = None,
        qh_history_start: pd.Timestamp | None = None,
    ) -> list[FeedUpdate]:
        """Rows already available at ``end`` with time ``>= start`` (``>= qh_history_start`` for prices)."""
        updates: list[FeedUpdate] = []
        for name, state in self.state.items():
            spec = state.spec
            rows = self._take(name, int(end.value))
            first = qh_history_start if (spec.table in QH_TABLES or spec.table == "day_ahead") else start
            if first is not None and not rows.empty:
                rows = rows[pd.DatetimeIndex(rows[spec.time_column]) >= first]
            if spec.table == "day_ahead":
                self._mark_complete_days(state, rows)
            new_rows, n_new, n_revised, _, kinds = self._diff(state, rows, end, backfill=True)
            state.last_success_at = end
            updates.append(
                FeedUpdate(
                    spec=spec,
                    received_at=end,
                    rows=new_rows,
                    n_new=n_new,
                    n_revised=n_revised,
                    latest_row_utc=state.stats.latest_row_utc,
                    backfill=True,
                    kinds=tuple(kinds),
                )
            )
        return updates

    def remaining(self) -> dict[str, int]:
        return {name: len(frame) - self._cursor[name] for name, (frame, _) in self._frames.items()}

    def describe(self) -> dict[str, Any]:
        return {"source": "replay", "feeds": [s.name for s in self.specs]}


def tables_from_market_frames(
    minute: pd.DataFrame | None = None,
    qh: pd.DataFrame | None = None,
    *,
    day_ahead: pd.DataFrame | None = None,
    si_forecast_current: pd.DataFrame | None = None,
    si_forecast_next: pd.DataFrame | None = None,
    nf_minute: pd.DataFrame | None = None,
    nf_qh: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame]:
    """Convenience: the ``tables`` mapping for :class:`ReplayFeeds` from contract frames (``None`` = absent)."""
    candidates: Sequence[tuple[str, pd.DataFrame | None]] = (
        ("minute", minute),
        ("qh", qh),
        ("day_ahead", day_ahead),
        ("si_forecast_current", si_forecast_current),
        ("si_forecast_next", si_forecast_next),
        ("nf_minute", nf_minute),
        ("nf_qh", nf_qh),
    )
    return {name: frame for name, frame in candidates if frame is not None and len(frame)}
