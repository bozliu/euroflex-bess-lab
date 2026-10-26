"""Minute-level real-time engine: Belgian implicit balancing with explicit settlement mechanisms.

Layers (see ``docs/realtime_engine.md``):

- state: :class:`MarketData` built from the public data set (minute + quarter-hour grids);
- mechanism: :mod:`.mechanisms` maps the quarter-hour state to the single imbalance price
  (``current`` = marginal +/- alpha, ``new2026`` = volume-weighted aFRR/mFRR);
- impact: :class:`ImpactModel` hooks for the fleet's own effect on the price;
- control: :class:`Strategy` policies evaluated by :func:`simulate` under a :class:`PublicationModel`;
- audit: :func:`check_no_lookahead` perturbs unpublished data and proves decisions did not use it.
"""

from __future__ import annotations

from .audit import LookaheadAudit, check_no_lookahead
from .battery import apply_power, feasible_power
from .impact import CurrentFormulaImpact, NewFormula2026Impact
from .impact.policies import ImpactAwareThresholdStrategy
from .mechanisms import (
    CurrentAlphaMechanism,
    MechanismValidation,
    NewWeightedMechanism2026,
    PublishedPrice,
    SettlementMechanism,
    mechanism_by_name,
    validate_mechanism,
)
from .metrics import daily_pnl, summarize_ledger
from .replay import DecisionContext, MarketData
from .settlement import ImpactModel, LinearImpact, NoImpact, settle_quarter_hours
from .simulator import SimulationResult, simulate
from .strategies import BaseStrategy, IdleStrategy, OracleQHStrategy, Strategy, ThresholdStrategy
from .types import MINUTES_PER_QH, BatteryParams, PublicationModel

__all__ = [
    "MINUTES_PER_QH",
    "BaseStrategy",
    "BatteryParams",
    "CurrentAlphaMechanism",
    "CurrentFormulaImpact",
    "DecisionContext",
    "IdleStrategy",
    "ImpactAwareThresholdStrategy",
    "ImpactModel",
    "LinearImpact",
    "LookaheadAudit",
    "MarketData",
    "MechanismValidation",
    "NewFormula2026Impact",
    "NewWeightedMechanism2026",
    "NoImpact",
    "OracleQHStrategy",
    "PublicationModel",
    "PublishedPrice",
    "SettlementMechanism",
    "SimulationResult",
    "Strategy",
    "ThresholdStrategy",
    "apply_power",
    "check_no_lookahead",
    "daily_pnl",
    "feasible_power",
    "mechanism_by_name",
    "settle_quarter_hours",
    "simulate",
    "summarize_ledger",
    "validate_mechanism",
]
