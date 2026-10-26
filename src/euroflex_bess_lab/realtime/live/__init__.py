"""Live paper trading (shadow mode) on Elia near-real-time Open Data.

The live loop runs the *same* :class:`~euroflex_bess_lab.realtime.Strategy` objects as the backtests, minute by
minute, with the simulator's visibility semantics; it records every decision and settles a paper ledger per
quarter-hour on Elia's published imbalance price. Execution is pluggable and paper-only: nothing here can submit an
order to any market.

- :mod:`.feeds`: polling clients (Elia records API, Energy-Charts) with retries, schema checks, gap detection;
- :mod:`.engine`: :class:`LiveEngine` (rolling window, decisions, settlement, crash-safe persistence);
- :mod:`.execution`: :class:`PaperExecution` (default), :class:`CsvSetpointExecution`, :class:`WebhookExecution`;
- :mod:`.reconcile`: compare the paper ledger with validated data later;
- :mod:`.dryrun`: replay a ``data/public`` period through the live engine and compare with the backtest;
- :mod:`.cli`: ``live_app`` (``run``, ``status``, ``reconcile``, ``replay``, ``strategies``).
"""

from __future__ import annotations

from .clock import Clock, ManualClock, SystemClock
from .dryrun import replay_check
from .engine import NEW_FORMULA_GO_LIVE_UTC, SAFETY_STATEMENT, LiveConfig, LiveEngine, PublicDataHistory
from .execution import (
    CsvSetpointExecution,
    ExecutionAdapter,
    ExecutionReceipt,
    PaperExecution,
    Setpoint,
    WebhookExecution,
)
from .feeds import (
    DEFAULT_FEEDS,
    ELIA_FEEDS,
    EliaLiveFeeds,
    FeedError,
    FeedSchemaError,
    FeedSpec,
    FeedUpdate,
    HttpPolicy,
    JsonHttpClient,
    LiveSource,
    normalize_energy_charts,
    normalize_records,
)
from .replay_feeds import ReplayFeeds, replay_spec, tables_from_market_frames
from .strategies import MechanismSwitchingStrategy, available_strategies, make_strategy

__all__ = [
    "DEFAULT_FEEDS",
    "ELIA_FEEDS",
    "NEW_FORMULA_GO_LIVE_UTC",
    "SAFETY_STATEMENT",
    "Clock",
    "CsvSetpointExecution",
    "EliaLiveFeeds",
    "ExecutionAdapter",
    "ExecutionReceipt",
    "FeedError",
    "FeedSchemaError",
    "FeedSpec",
    "FeedUpdate",
    "HttpPolicy",
    "JsonHttpClient",
    "LiveConfig",
    "LiveEngine",
    "LiveSource",
    "ManualClock",
    "MechanismSwitchingStrategy",
    "PaperExecution",
    "PublicDataHistory",
    "ReplayFeeds",
    "Setpoint",
    "SystemClock",
    "WebhookExecution",
    "available_strategies",
    "make_strategy",
    "normalize_energy_charts",
    "normalize_records",
    "replay_check",
    "replay_spec",
    "tables_from_market_frames",
]
