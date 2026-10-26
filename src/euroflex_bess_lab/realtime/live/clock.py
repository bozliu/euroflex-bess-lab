"""Injectable clocks: the live engine never calls ``time.sleep`` or ``datetime.now`` directly."""

from __future__ import annotations

import time
from typing import Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class Clock(Protocol):
    """Wall clock used by :class:`~euroflex_bess_lab.realtime.live.engine.LiveEngine` (tz-aware UTC)."""

    def now(self) -> pd.Timestamp: ...

    def sleep_until(self, when: pd.Timestamp) -> None: ...


class SystemClock:
    """Real time. Sleeps in short slices so that Ctrl-C stays responsive."""

    def __init__(self, max_slice_s: float = 1.0) -> None:
        self.max_slice_s = max_slice_s

    def now(self) -> pd.Timestamp:
        return pd.Timestamp.now(tz="UTC")

    def sleep_until(self, when: pd.Timestamp) -> None:
        target = pd.Timestamp(when)
        while True:
            remaining = (target - self.now()).total_seconds()
            if remaining <= 0:
                return
            time.sleep(min(remaining, self.max_slice_s))


class ManualClock:
    """Simulated time for tests and replays: ``sleep_until`` jumps forward instantly, never backwards."""

    def __init__(self, start: str | pd.Timestamp) -> None:
        ts = pd.Timestamp(start)
        self._now = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
        self.sleeps = 0

    def now(self) -> pd.Timestamp:
        return self._now

    def sleep_until(self, when: pd.Timestamp) -> None:
        self.sleeps += 1
        target = pd.Timestamp(when)
        if target > self._now:
            self._now = target

    def advance(self, seconds: float) -> pd.Timestamp:
        self._now = self._now + pd.Timedelta(seconds=seconds)
        return self._now

    def set(self, when: str | pd.Timestamp) -> None:
        ts = pd.Timestamp(when)
        self._now = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
