"""Run the reserve policy suite (fixed splits, causal forecast split, ex-post oracle) and tabulate revenue stacks."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from numpy.typing import NDArray

from ..replay import MarketData
from ..strategies.base import Strategy
from ..types import BatteryParams, PublicationModel
from .activation import ActivationModel, ActivationSeries, build_activation
from .allocation import (
    SYMMETRIC_CANDIDATES,
    AllocationDecision,
    ForecastSplitPolicy,
    SplitCandidate,
    offers_from_choice,
    oracle_decision,
)
from .capacity import AwardModel, CapacityAwards, forecast_capacity_prices
from .engine import ReserveSettings, ReserveSimulationResult, ledger_window, revenue_stack, simulate_reserve
from .plan import ReservePlan
from .products import AfrrProduct

FloatArray = NDArray[np.float64]

LEDGER_EXPORT_COLUMNS = (
    "timestamp_utc",
    "strategy",
    "mechanism",
    "fleet_mw",
    "energy_mwh",
    "discharged_mwh",
    "charged_mwh",
    "base_price_eur_mwh",
    "price_eur_mwh",
    "price_missing",
    "revenue_eur",
    "degradation_eur",
    "pnl_eur",
    "soc_end_mwh",
    "afrr_up_reserved_mw",
    "afrr_down_reserved_mw",
    "afrr_capacity_eur",
    "afrr_energy_eur",
    "implicit_revenue_eur",
    "shortfall_imbalance_eur",
    "penalty_eur",
    "afrr_up_energy_mwh",
    "afrr_down_energy_mwh",
    "shortfall_mwh",
)

STACK_COLUMNS = (
    "afrr_capacity",
    "afrr_energy",
    "implicit_balancing",
    "shortfall_imbalance",
    "penalty",
    "degradation",
    "total",
)


@dataclass(frozen=True)
class ReserveStudyConfig:
    battery: BatteryParams = field(default_factory=lambda: BatteryParams(power_mw=1.0, energy_mwh=2.0))
    product: AfrrProduct = field(default_factory=AfrrProduct)
    settings: ReserveSettings = field(default_factory=ReserveSettings)
    award: AwardModel = field(default_factory=AwardModel)
    activation: ActivationModel = field(default_factory=ActivationModel)
    candidates: tuple[SplitCandidate, ...] = SYMMETRIC_CANDIDATES
    forecast: ForecastSplitPolicy = field(default_factory=ForecastSplitPolicy)
    capacity_proxy: Literal["vwap", "marginal"] = "vwap"
    capacity_lookback_days: int = 7
    include_oracle: bool = True

    def describe(self) -> dict[str, Any]:
        return {
            "battery": asdict(self.battery),
            "product": asdict(self.product),
            "settings": asdict(self.settings),
            "award_clearing": self.award.clearing,
            "activation": self.activation.label(),
            "pay_as_bid_until_utc": str(self.activation.pay_as_bid_until_utc),
            "bid_markup_eur_mwh": self.activation.bid_markup_eur_mwh,
            "candidates": [c.label for c in self.candidates],
            "forecast_lookback_days": self.forecast.lookback_days,
            "capacity_forecast": f"{self.capacity_proxy}, mean of the same block over the last "
            f"{self.capacity_lookback_days} published auctions",
        }


@dataclass
class PolicySuite:
    mechanism: str
    results: dict[str, ReserveSimulationResult]
    decisions: dict[str, AllocationDecision]
    activation: ActivationSeries
    capacity_forecast: dict[str, FloatArray]
    awards: CapacityAwards
    config: ReserveStudyConfig


def _candidate_plan(
    name: str,
    up_mw: FloatArray,
    down_mw: FloatArray,
    awards: CapacityAwards,
    config: ReserveStudyConfig,
    bids: Mapping[str, FloatArray] | None,
) -> ReservePlan:
    return ReservePlan.from_offers(
        name,
        awards,
        config.battery,
        offered_up_mw=up_mw,
        offered_down_mw=down_mw,
        award_model=config.award,
        bid_up_eur_per_mw_h=None if bids is None else bids["up"],
        bid_down_eur_per_mw_h=None if bids is None else bids["down"],
        product=config.product,
    )


def run_policy_suite(
    market: MarketData,
    awards: CapacityAwards,
    *,
    make_inner: Callable[[], Strategy],
    mechanism: str = "current",
    config: ReserveStudyConfig | None = None,
    publication: PublicationModel | None = None,
    activation: ActivationSeries | None = None,
    policies: Sequence[str] | None = None,
    history_awards: CapacityAwards | None = None,
) -> PolicySuite:
    """Simulate every candidate split, then the forecast policy and (optionally) the ex-post oracle.

    ``awards`` must be aligned to blocks covering the market (``CapacityAwards.aligned_to``). ``history_awards``
    (optional, e.g. the full published table) lets the capacity-price forecast use auctions held before the
    simulated window. ``policies`` restricts which of ``"forecast"`` / ``"oracle"`` run after the fixed splits.
    """
    config = config or ReserveStudyConfig()
    publication = publication or PublicationModel()
    activation = activation or build_activation(market, awards, config.activation)
    capacity_forecast = forecast_capacity_prices(
        history_awards if history_awards is not None else awards,
        proxy=config.capacity_proxy,
        lookback_days=config.capacity_lookback_days,
        target_blocks=awards.blocks if history_awards is not None else None,
    )
    pay_as_bid = config.award.clearing == "pay_as_bid"
    bids = capacity_forecast if pay_as_bid else None
    wanted = set(policies) if policies is not None else {"forecast", "oracle"}
    power = config.battery.max_power_mw
    n = awards.blocks.n

    def run(plan: ReservePlan) -> ReserveSimulationResult:
        return simulate_reserve(
            market,
            make_inner(),
            config.battery,
            plan,
            activation,
            mechanism=mechanism,
            publication=publication,
            settings=config.settings,
            product=config.product,
        )

    results: dict[str, ReserveSimulationResult] = {}
    reference: dict[SplitCandidate, pd.DataFrame] = {}
    candidates = tuple(dict.fromkeys((*config.candidates, *config.forecast.candidates)))
    for candidate in candidates:
        plan = _candidate_plan(
            candidate.label,
            np.full(n, candidate.up_frac * power),
            np.full(n, candidate.down_frac * power),
            awards,
            config,
            bids,
        )
        result = run(plan)
        results[candidate.label] = result
        reference[candidate] = result.block_values()
    decisions: dict[str, AllocationDecision] = {}
    if "forecast" in wanted:
        decision = config.forecast.decide(awards, capacity_forecast, reference, config.battery)
        up, down = offers_from_choice(decision.choice, decision.candidates, config.battery)
        results[decision.policy] = run(_candidate_plan(decision.policy, up, down, awards, config, bids))
        decisions[decision.policy] = decision
    if "oracle" in wanted and config.include_oracle:
        decision = oracle_decision(config.forecast.candidates, reference)
        up, down = offers_from_choice(decision.choice, decision.candidates, config.battery)
        oracle_bids = (
            {"up": awards.price("up", "marginal"), "down": awards.price("down", "marginal")} if pay_as_bid else None
        )
        results[decision.policy] = run(_candidate_plan(decision.policy, up, down, awards, config, oracle_bids))
        decisions[decision.policy] = decision
    return PolicySuite(
        mechanism=mechanism,
        results=results,
        decisions=decisions,
        activation=activation,
        capacity_forecast=capacity_forecast,
        awards=awards,
        config=config,
    )


def stack_table(
    results: Mapping[str, ReserveSimulationResult],
    periods: Mapping[str, tuple[str | pd.Timestamp, str | pd.Timestamp]],
) -> pd.DataFrame:
    """Revenue stack (EUR/MW/year) per policy and period, plus cycles and mean reserved MW."""
    rows: list[dict[str, Any]] = []
    for period, (start, end) in periods.items():
        for policy, result in results.items():
            frame = ledger_window(result.qh, start, end)
            if frame.empty:
                continue
            stack = revenue_stack(frame, result.battery, n_minutes=len(frame) * 15)
            rows.append(
                {
                    "period": period,
                    "policy": policy,
                    "mechanism": result.mechanism,
                    "days": len(frame) / 96.0,
                    **{column: stack[column] for column in STACK_COLUMNS},
                    "cycles_per_day": stack["cycles_per_day"],
                    "mean_up_reserved_mw": stack["mean_up_reserved_mw"],
                    "mean_down_reserved_mw": stack["mean_down_reserved_mw"],
                    "shortfall_mwh_per_mw_year": stack["shortfall_mwh_per_mw_year"],
                }
            )
    return pd.DataFrame(rows)


def monthly_allocation(result: ReserveSimulationResult, timezone: str = "Europe/Brussels") -> pd.DataFrame:
    """Mean reserved MW (up/down) and P&L per local month for one policy run."""
    frame = result.qh[["timestamp_utc", "afrr_up_reserved_mw", "afrr_down_reserved_mw", "pnl_eur"]].copy()
    local = pd.to_datetime(frame["timestamp_utc"], utc=True).dt.tz_convert(timezone)
    frame["month"] = local.dt.strftime("%Y-%m")
    grouped = frame.groupby("month", sort=True)
    return pd.DataFrame(
        {
            "up_reserved_mw": grouped["afrr_up_reserved_mw"].mean(),
            "down_reserved_mw": grouped["afrr_down_reserved_mw"].mean(),
            "pnl_eur": grouped["pnl_eur"].sum(),
        }
    ).reset_index()


def save_ledger(
    result: ReserveSimulationResult,
    directory: str | Path,
    *,
    period_label: str,
    start: str | pd.Timestamp | None = None,
    end: str | pd.Timestamp | None = None,
    extra: Mapping[str, Any] | None = None,
    max_bytes: int = 5_000_000,
) -> dict[str, Path]:
    """Write ``reserve_<policy>_<mechanism>_<period>.parquet`` (+ ``.json``) for downstream valuation.

    Keeps the core ledger columns (Plan.md section 8) plus one column per revenue component
    (``LEDGER_EXPORT_COLUMNS``), stored as float32 with zstd to stay well within the 5 MB budget. The full
    float64 ledger is ``result.qh``.
    """
    out_dir = Path(directory)
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"reserve_{result.policy}_{result.mechanism}_{period_label}"
    full = result.ledger_for_export(start, end)
    full["shortfall_mwh"] = full["shortfall_up_mwh"] + full["shortfall_down_mwh"]
    frame = full[list(LEDGER_EXPORT_COLUMNS)].copy()
    floats = [c for c in frame.columns if frame[c].dtype == np.float64]
    for column in floats:
        frame[column] = frame[column].astype(np.float32)
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True).astype("datetime64[ns, UTC]")
    parquet = out_dir / f"{stem}.parquet"
    table = pa.Table.from_pandas(frame, preserve_index=False)
    pq.write_table(
        table,
        parquet,
        compression="zstd",
        compression_level=19,
        use_dictionary=[c for c in frame.columns if c not in floats and c != "timestamp_utc"],
        use_byte_stream_split=floats,
        column_encoding={"timestamp_utc": "DELTA_BINARY_PACKED"},
    )
    size = parquet.stat().st_size
    if size > max_bytes:
        raise ValueError(f"{parquet.name} is {size} bytes, above the {max_bytes} byte budget")
    stack = revenue_stack(ledger_window(result.qh, start, end), result.battery)
    sidecar = {
        "name": stem,
        "owner": "reserve",
        "policy": result.policy,
        "strategy": f"reserve_{result.policy}",
        "implicit_strategy": result.strategy,
        "mechanism": result.mechanism,
        "period": period_label,
        "start_utc": str(frame["timestamp_utc"].iloc[0]) if len(frame) else None,
        "end_utc": str(frame["timestamp_utc"].iloc[-1] + pd.Timedelta(minutes=15)) if len(frame) else None,
        "rows": int(len(frame)),
        "battery": asdict(result.battery),
        "publication": asdict(result.publication),
        "settings": asdict(result.settings),
        "product": asdict(result.product),
        "activation_model": result.activation_model,
        "capacity_clearing": result.plan.meta.get("clearing"),
        "stack_eur_per_mw_year": stack,
        "columns": {
            "revenue_eur": "total revenue = afrr_capacity_eur + afrr_energy_eur + imbalance_revenue_eur - penalty_eur",
            "pnl_eur": "revenue_eur - degradation_eur",
            "energy_mwh": "BRP perimeter (imbalance) energy = metered - requested aFRR energy",
            "price_eur_mwh": "single imbalance price of the mechanism",
            "afrr_capacity_eur": "reserved MW x capacity price paid x 0.25 h",
            "afrr_energy_eur": "requested aFRR energy x aFRR energy price (up earns, down pays)",
            "implicit_revenue_eur": "implicit energy x imbalance price",
            "shortfall_imbalance_eur": "undelivered aFRR energy settled at the imbalance price",
            "penalty_eur": "activation-control penalty on undelivered energy (0 unless configured)",
            "shortfall_mwh": "requested aFRR energy that could not be delivered (SOC or power limits)",
            "dtype": "float columns stored as float32",
        },
        "approximations": [
            "aFRR activation of the battery is approximated pro rata from Belgian activated / awarded aFRR volume",
            "capacity remuneration uses the published awards as clearing proxy (see capacity_clearing)",
            "before 2024-11-26 (PICASSO) aFRR energy was pay-as-bid; see activation_model",
        ],
        **dict(extra or {}),
    }
    sidecar_path = out_dir / f"{stem}.json"
    sidecar_path.write_text(json.dumps(sidecar, indent=2, default=str), encoding="utf-8")
    return {"parquet": parquet, "json": sidecar_path}
