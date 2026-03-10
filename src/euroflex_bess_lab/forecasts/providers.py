from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from .base import CustomForecastModel, ForecastProvider, validate_forecast_snapshot

if TYPE_CHECKING:
    from ..config import BacktestConfig


class PerfectForesightForecastProvider(ForecastProvider):
    name = "perfect_foresight"
    auditable = False
    supported_modes = ("point",)

    def get_forecast(
        self,
        *,
        market: str,
        decision_time_utc: pd.Timestamp,
        delivery_frame: pd.DataFrame,
        actual_frame: pd.DataFrame,
        visible_inputs: dict[str, pd.DataFrame] | None = None,
    ) -> pd.DataFrame:
        _ = visible_inputs
        horizon = delivery_frame.sort_values("timestamp_utc").reset_index(drop=True).copy()
        resolution_minutes = int(horizon["resolution_minutes"].iloc[0])
        snapshot = pd.DataFrame(
            {
                "market": market,
                "delivery_start_utc": horizon["timestamp_utc"],
                "delivery_end_utc": horizon["timestamp_utc"] + pd.Timedelta(minutes=resolution_minutes),
                "forecast_price_eur_per_mwh": horizon["price_eur_per_mwh"],
                "issue_time_utc": decision_time_utc,
                "available_from_utc": decision_time_utc,
                "provider_name": self.name,
            }
        )
        snapshot["scenario_id"] = None
        snapshot["actual_price_eur_per_mwh"] = horizon["price_eur_per_mwh"].values
        return validate_forecast_snapshot(
            snapshot,
            decision_time_utc=decision_time_utc,
            expected_delivery_starts=horizon["timestamp_utc"],
            auditable=self.auditable,
            mode="point",
        )


class PersistenceForecastProvider(ForecastProvider):
    name = "persistence"
    auditable = True
    supported_modes = ("point",)

    def get_forecast(
        self,
        *,
        market: str,
        decision_time_utc: pd.Timestamp,
        delivery_frame: pd.DataFrame,
        actual_frame: pd.DataFrame,
        visible_inputs: dict[str, pd.DataFrame] | None = None,
    ) -> pd.DataFrame:
        _ = visible_inputs
        horizon = delivery_frame.sort_values("timestamp_utc").reset_index(drop=True).copy()
        actual = actual_frame.sort_values("timestamp_utc").reset_index(drop=True).copy()
        resolution_minutes = int(horizon["resolution_minutes"].iloc[0])
        if market in {
            "day_ahead",
            "fcr_capacity",
            "afrr_capacity_up",
            "afrr_capacity_down",
            "afrr_activation_price_up",
            "afrr_activation_price_down",
            "afrr_activation_ratio_up",
            "afrr_activation_ratio_down",
        }:
            actual["local_date"] = actual["timestamp_local"].dt.date
            actual["local_clock"] = actual["timestamp_local"].dt.strftime("%H:%M")
            horizon["local_date"] = horizon["timestamp_local"].dt.date
            horizon["local_clock"] = horizon["timestamp_local"].dt.strftime("%H:%M")
            horizon["previous_local_date"] = horizon["timestamp_local"].dt.date.map(
                lambda value: value.fromordinal(value.toordinal() - 1)
            )
            merged = horizon.merge(
                actual[["local_date", "local_clock", "price_eur_per_mwh"]].rename(
                    columns={"local_date": "previous_local_date", "price_eur_per_mwh": "forecast_price_eur_per_mwh"}
                ),
                on=["previous_local_date", "local_clock"],
                how="left",
            )
            if merged["forecast_price_eur_per_mwh"].isna().any():
                raise ValueError("PersistenceForecastProvider requires a full previous local day of day-ahead actuals")
            values = merged["forecast_price_eur_per_mwh"]
        else:
            history = actual[actual["timestamp_utc"] < decision_time_utc].copy()
            if history.empty:
                raise ValueError(
                    "PersistenceForecastProvider requires at least one realized imbalance point before decision time"
                )
            last_value = float(history.iloc[-1]["price_eur_per_mwh"])
            values = pd.Series(last_value, index=horizon.index, dtype=float)

        snapshot = pd.DataFrame(
            {
                "market": market,
                "delivery_start_utc": horizon["timestamp_utc"],
                "delivery_end_utc": horizon["timestamp_utc"] + pd.Timedelta(minutes=resolution_minutes),
                "forecast_price_eur_per_mwh": values.values,
                "issue_time_utc": decision_time_utc,
                "available_from_utc": decision_time_utc,
                "provider_name": self.name,
            }
        )
        snapshot["scenario_id"] = None
        snapshot["actual_price_eur_per_mwh"] = horizon["price_eur_per_mwh"].values
        return validate_forecast_snapshot(
            snapshot,
            decision_time_utc=decision_time_utc,
            expected_delivery_starts=horizon["timestamp_utc"],
            auditable=self.auditable,
            mode="point",
        )


class CsvForecastProvider(ForecastProvider):
    name = "csv"
    auditable = True
    supported_modes = ("point", "scenario_bundle")

    def __init__(
        self,
        *,
        mode: str = "point",
        day_ahead_path: Path,
        imbalance_path: Path | None = None,
        fcr_capacity_path: Path | None = None,
        afrr_capacity_up_path: Path | None = None,
        afrr_capacity_down_path: Path | None = None,
        afrr_activation_price_up_path: Path | None = None,
        afrr_activation_price_down_path: Path | None = None,
        afrr_activation_ratio_up_path: Path | None = None,
        afrr_activation_ratio_down_path: Path | None = None,
        scenario_id: str | None = None,
    ) -> None:
        self.mode = mode
        self.day_ahead_path = day_ahead_path
        self.imbalance_path = imbalance_path
        self.fcr_capacity_path = fcr_capacity_path
        self.afrr_capacity_up_path = afrr_capacity_up_path
        self.afrr_capacity_down_path = afrr_capacity_down_path
        self.afrr_activation_price_up_path = afrr_activation_price_up_path
        self.afrr_activation_price_down_path = afrr_activation_price_down_path
        self.afrr_activation_ratio_up_path = afrr_activation_ratio_up_path
        self.afrr_activation_ratio_down_path = afrr_activation_ratio_down_path
        self.scenario_id = scenario_id
        self._cache: dict[str, pd.DataFrame] = {}

    def _load(self, market: str) -> pd.DataFrame:
        if market in self._cache:
            return self._cache[market]
        source_path = {
            "day_ahead": self.day_ahead_path,
            "imbalance": self.imbalance_path,
            "fcr_capacity": self.fcr_capacity_path,
            "afrr_capacity_up": self.afrr_capacity_up_path,
            "afrr_capacity_down": self.afrr_capacity_down_path,
            "afrr_activation_price_up": self.afrr_activation_price_up_path,
            "afrr_activation_price_down": self.afrr_activation_price_down_path,
            "afrr_activation_ratio_up": self.afrr_activation_ratio_up_path,
            "afrr_activation_ratio_down": self.afrr_activation_ratio_down_path,
        }.get(market)
        if source_path is None:
            raise ValueError(f"CSV forecast path is required for market={market}")
        frame = pd.read_csv(source_path)
        required = {
            "market",
            "delivery_start_utc",
            "delivery_end_utc",
            "forecast_price_eur_per_mwh",
            "issue_time_utc",
            "available_from_utc",
            "provider_name",
        }
        missing = required.difference(frame.columns)
        if missing:
            raise ValueError(f"CSV forecast file {source_path} is missing columns: {sorted(missing)}")
        if self.mode == "scenario_bundle":
            scenario_required = {"scenario_id", "scenario_weight"}
            missing_scenario = scenario_required.difference(frame.columns)
            if missing_scenario:
                raise ValueError(
                    f"CSV scenario forecast file {source_path} is missing columns: {sorted(missing_scenario)}"
                )
        for column in ("delivery_start_utc", "delivery_end_utc", "issue_time_utc", "available_from_utc"):
            frame[column] = pd.to_datetime(frame[column], utc=True, format="mixed")
        if "scenario_id" not in frame.columns:
            frame["scenario_id"] = None
        if "scenario_weight" not in frame.columns:
            frame["scenario_weight"] = 1.0
        frame = frame.sort_values(["delivery_start_utc", "available_from_utc", "issue_time_utc"]).reset_index(drop=True)
        self._cache[market] = frame
        return frame

    def get_forecast(
        self,
        *,
        market: str,
        decision_time_utc: pd.Timestamp,
        delivery_frame: pd.DataFrame,
        actual_frame: pd.DataFrame,
        visible_inputs: dict[str, pd.DataFrame] | None = None,
    ) -> pd.DataFrame:
        _ = visible_inputs
        horizon = delivery_frame.sort_values("timestamp_utc").reset_index(drop=True).copy()
        source = self._load(market)
        if self.mode == "point" and self.scenario_id is not None:
            source = source[source["scenario_id"] == self.scenario_id].copy()

        relevant = source[source["delivery_start_utc"].isin(horizon["timestamp_utc"])].copy()
        if relevant.empty:
            raise ValueError(f"CSV forecast provider has no rows for market={market} and requested delivery horizon")
        hidden = relevant[relevant["available_from_utc"] > decision_time_utc]
        visible = relevant[relevant["available_from_utc"] <= decision_time_utc].copy()
        if visible.empty:
            raise ValueError(
                "CSV forecast provider would require lookahead data; no visible vintages exist at decision time"
            )

        visible = visible.sort_values(["scenario_id", "delivery_start_utc", "available_from_utc", "issue_time_utc"])
        if self.mode == "scenario_bundle":
            latest = (
                visible.groupby(["scenario_id", "delivery_start_utc"], as_index=False).tail(1).reset_index(drop=True)
            )
            expected_count = horizon["timestamp_utc"].nunique()
            for scenario_id, scenario_frame in latest.groupby("scenario_id"):
                if scenario_frame["delivery_start_utc"].nunique() != expected_count:
                    future_only = (
                        hidden[hidden["scenario_id"] == scenario_id]["delivery_start_utc"].nunique()
                        > scenario_frame["delivery_start_utc"].nunique()
                    )
                    if future_only:
                        raise ValueError(
                            "CSV forecast provider violates no-lookahead requirements for part of the scenario horizon"
                        )
                    raise ValueError(
                        f"CSV forecast provider is missing one or more delivery intervals for scenario `{scenario_id}`"
                    )
        else:
            latest = visible.groupby("delivery_start_utc", as_index=False).tail(1).reset_index(drop=True)
            if latest["delivery_start_utc"].nunique() != horizon["timestamp_utc"].nunique():
                future_only = hidden["delivery_start_utc"].nunique() > latest["delivery_start_utc"].nunique()
                if future_only:
                    raise ValueError("CSV forecast provider violates no-lookahead requirements for part of the horizon")
                raise ValueError("CSV forecast provider does not cover every requested delivery interval")

        latest["actual_price_eur_per_mwh"] = latest["delivery_start_utc"].map(
            horizon.set_index("timestamp_utc")["price_eur_per_mwh"]
        )
        return validate_forecast_snapshot(
            latest,
            decision_time_utc=decision_time_utc,
            expected_delivery_starts=horizon["timestamp_utc"],
            auditable=self.auditable,
            mode=self.mode,
        )


class CustomPythonForecastProvider(ForecastProvider):
    name = "custom_python"
    auditable = True
    supported_modes = ("point", "scenario_bundle")

    def __init__(
        self,
        *,
        mode: str,
        module_path: Path,
        class_name: str,
        init_kwargs: dict[str, Any],
        config: BacktestConfig,
    ) -> None:
        self.mode = mode
        self.module_path = module_path
        self.class_name = class_name
        self.init_kwargs = dict(init_kwargs)
        self.config = config
        self.model = self._load_model()
        self.initialize(
            config=config.model_dump(mode="json"),
            run_context={
                "market_id": config.market.id,
                "workflow": config.workflow,
                "base_workflow": config.execution_workflow,
                "run_scope": config.run_scope,
                "timezone": config.timing.timezone,
                "resolution_minutes": config.timing.resolution_minutes,
                "delivery_start_date": str(config.timing.delivery_start_date),
                "delivery_end_date": str(config.timing.delivery_end_date),
                "artifacts_root_dir": str(config.artifacts.root_dir),
                "module_path": str(module_path),
                "provider_name": self.name,
                "forecast_mode": mode,
            },
        )

    def _load_model(self) -> CustomForecastModel:
        if not self.module_path.exists():
            raise ValueError(f"custom_python module_path does not exist: {self.module_path}")
        module_name = f"euroflex_custom_provider_{hashlib.sha256(str(self.module_path).encode()).hexdigest()[:12]}"
        spec = importlib.util.spec_from_file_location(module_name, self.module_path)
        if spec is None or spec.loader is None:
            raise ValueError(f"Unable to load custom_python module from {self.module_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        candidate = getattr(module, self.class_name, None)
        if candidate is None:
            raise ValueError(f"custom_python class `{self.class_name}` was not found in {self.module_path}")
        if not callable(candidate):
            raise ValueError(f"custom_python class `{self.class_name}` is not callable")
        instance = candidate(**self.init_kwargs)
        if not hasattr(instance, "initialize") or not hasattr(instance, "generate_forecast"):
            raise ValueError(
                "custom_python forecast classes must define initialize(config=..., run_context=...) "
                "and generate_forecast(...)"
            )
        return instance

    def initialize(self, *, config: dict[str, Any], run_context: dict[str, Any]) -> None:
        self.model.initialize(config=config, run_context=run_context)

    def get_forecast(
        self,
        *,
        market: str,
        decision_time_utc: pd.Timestamp,
        delivery_frame: pd.DataFrame,
        actual_frame: pd.DataFrame,
        visible_inputs: dict[str, pd.DataFrame] | None = None,
    ) -> pd.DataFrame:
        horizon = delivery_frame.sort_values("timestamp_utc").reset_index(drop=True).copy()
        visible = (
            {name: frame.copy() for name, frame in visible_inputs.items()}
            if visible_inputs is not None
            else {
                market: actual_frame[actual_frame["timestamp_utc"] < decision_time_utc]
                .sort_values("timestamp_utc")
                .reset_index(drop=True)
            }
        )
        snapshot = self.model.generate_forecast(
            market=market,
            decision_time_utc=decision_time_utc,
            delivery_frame=horizon.copy(),
            visible_inputs=visible,
        )
        if not isinstance(snapshot, pd.DataFrame):
            raise ValueError("custom_python generate_forecast() must return a pandas DataFrame")
        resolved = snapshot.copy()
        if "market" not in resolved.columns:
            resolved["market"] = market
        if "provider_name" not in resolved.columns:
            resolved["provider_name"] = self.name
        if "actual_price_eur_per_mwh" not in resolved.columns and "delivery_start_utc" in resolved.columns:
            actual_lookup = horizon[["timestamp_utc", "price_eur_per_mwh"]].rename(
                columns={"timestamp_utc": "delivery_start_utc", "price_eur_per_mwh": "actual_price_eur_per_mwh"}
            )
            resolved = resolved.merge(actual_lookup, on="delivery_start_utc", how="left")
        return validate_forecast_snapshot(
            resolved,
            decision_time_utc=decision_time_utc,
            expected_delivery_starts=horizon["timestamp_utc"],
            auditable=self.auditable,
            mode=self.mode,
        )
