from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

FORECAST_COLUMNS = [
    "market",
    "delivery_start_utc",
    "delivery_end_utc",
    "forecast_price_eur_per_mwh",
    "issue_time_utc",
    "available_from_utc",
    "provider_name",
]
SCENARIO_COLUMNS = ["scenario_id", "scenario_weight"]


class ForecastProvider(ABC):
    name: str
    auditable: bool = True
    supported_modes: tuple[str, ...] = ("point",)

    def initialize(self, *, config: dict[str, Any], run_context: dict[str, Any]) -> None:
        _ = config, run_context

    @abstractmethod
    def get_forecast(
        self,
        *,
        market: str,
        decision_time_utc: pd.Timestamp,
        delivery_frame: pd.DataFrame,
        actual_frame: pd.DataFrame,
        visible_inputs: dict[str, pd.DataFrame] | None = None,
    ) -> pd.DataFrame:
        """Return one forecast row per delivery interval visible as of decision_time_utc."""


class CustomForecastModel(ABC):
    @abstractmethod
    def initialize(self, *, config: dict[str, Any], run_context: dict[str, Any]) -> None:
        """Load weights, set up APIs, or precompute state for the run."""

    @abstractmethod
    def generate_forecast(
        self,
        *,
        market: str,
        decision_time_utc: pd.Timestamp,
        delivery_frame: pd.DataFrame,
        visible_inputs: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        """Return a forecast snapshot for the requested delivery horizon."""


def scenario_weights(snapshot: pd.DataFrame) -> pd.DataFrame:
    if "scenario_id" not in snapshot.columns or "scenario_weight" not in snapshot.columns:
        raise ValueError("Scenario forecast snapshots must include scenario_id and scenario_weight")
    weights = (
        snapshot[["scenario_id", "scenario_weight"]].drop_duplicates().sort_values("scenario_id").reset_index(drop=True)
    )
    return weights


def validate_forecast_snapshot(
    snapshot: pd.DataFrame,
    *,
    decision_time_utc: pd.Timestamp,
    expected_delivery_starts: pd.Series,
    auditable: bool,
    mode: str = "point",
) -> pd.DataFrame:
    required_columns = list(FORECAST_COLUMNS)
    if mode == "scenario_bundle":
        required_columns += SCENARIO_COLUMNS
    missing = set(required_columns).difference(snapshot.columns)
    if missing:
        raise ValueError(f"Forecast snapshot is missing required columns: {sorted(missing)}")

    frame = snapshot.copy()
    for column in ("delivery_start_utc", "delivery_end_utc", "issue_time_utc", "available_from_utc"):
        frame[column] = pd.to_datetime(frame[column], utc=True, format="mixed")

    if auditable and (frame["available_from_utc"] > decision_time_utc).any():
        raise ValueError("Forecast snapshot violates strict as-of visibility constraints")

    expected = pd.Index(pd.to_datetime(expected_delivery_starts, utc=True, format="mixed"))
    if mode == "point":
        duplicated = frame["delivery_start_utc"].duplicated()
        if duplicated.any():
            raise ValueError("Forecast snapshot must contain at most one row per delivery interval")

        actual = pd.Index(frame["delivery_start_utc"])
        missing_intervals = expected.difference(actual)
        if not missing_intervals.empty:
            raise ValueError("Forecast snapshot does not cover every requested delivery interval")
        frame["scenario_id"] = None
        frame["scenario_weight"] = 1.0
        frame = frame.sort_values("delivery_start_utc").reset_index(drop=True)
        return frame

    if frame["scenario_id"].isna().any():
        raise ValueError("Scenario forecast snapshot must provide scenario_id for every row")
    weights = scenario_weights(frame)
    if weights["scenario_weight"].isna().any():
        raise ValueError("Scenario forecast snapshot must provide scenario_weight for every scenario")
    if not weights["scenario_weight"].between(0.0, 1.0).all():
        raise ValueError("Scenario forecast weights must be between 0 and 1")
    if abs(float(weights["scenario_weight"].sum()) - 1.0) > 1e-6:
        raise ValueError("Scenario forecast weights must sum to 1.0")

    duplicated = frame.duplicated(subset=["scenario_id", "delivery_start_utc"])
    if duplicated.any():
        raise ValueError("Scenario forecast snapshot must contain at most one row per scenario and delivery interval")

    for scenario_id, scenario_frame in frame.groupby("scenario_id"):
        actual = pd.Index(scenario_frame["delivery_start_utc"])
        missing_intervals = expected.difference(actual)
        if not missing_intervals.empty:
            raise ValueError(f"Scenario `{scenario_id}` does not cover every requested delivery interval")

    frame = frame.sort_values(["scenario_id", "delivery_start_utc"]).reset_index(drop=True)
    return frame
