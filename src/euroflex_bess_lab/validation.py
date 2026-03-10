from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field
from importlib.util import find_spec
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import pandas as pd
from pydantic import ValidationError

from .benchmarks import BenchmarkRegistry
from .config import BacktestConfig, load_config
from .forecasts import CsvForecastProvider
from .markets import MarketRegistry
from .optimization.solver import ensure_solver_available


@dataclass
class CheckResult:
    name: str
    status: str
    detail: str
    context: dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationReport:
    report_type: str
    ok: bool
    checks: list[CheckResult]
    metadata: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "report_type": self.report_type,
            "ok": self.ok,
            "checks": [asdict(check) for check in self.checks],
            "metadata": self.metadata,
        }


def _build_report(
    report_type: str, checks: list[CheckResult], *, metadata: dict[str, Any] | None = None
) -> ValidationReport:
    return ValidationReport(
        report_type=report_type,
        ok=all(check.status != "fail" for check in checks),
        checks=checks,
        metadata=metadata or {},
    )


def _market_frame_checks(frame: pd.DataFrame, *, market_name: str, timezone: str) -> list[CheckResult]:
    checks: list[CheckResult] = []
    if frame.empty:
        return [CheckResult(name=f"{market_name}_non_empty", status="fail", detail=f"{market_name} frame is empty")]

    timestamps = pd.to_datetime(frame["timestamp_utc"], utc=True)
    checks.append(
        CheckResult(
            name=f"{market_name}_unique_timestamps",
            status="pass" if not timestamps.duplicated().any() else "fail",
            detail="UTC timestamps are unique"
            if not timestamps.duplicated().any()
            else "Duplicate UTC timestamps found",
        )
    )
    checks.append(
        CheckResult(
            name=f"{market_name}_sorted_timestamps",
            status="pass" if timestamps.is_monotonic_increasing else "fail",
            detail="UTC timestamps are sorted ascending"
            if timestamps.is_monotonic_increasing
            else "UTC timestamps are not sorted",
        )
    )

    diffs = timestamps.diff().dropna()
    cadence_ok = diffs.empty or diffs.eq(pd.Timedelta(minutes=15)).all()
    checks.append(
        CheckResult(
            name=f"{market_name}_fifteen_minute_cadence",
            status="pass" if cadence_ok else "fail",
            detail="15-minute cadence is consistent" if cadence_ok else "Found gaps or non-15-minute cadence",
        )
    )

    expected_local = timestamps.dt.tz_convert(timezone).astype(str).reset_index(drop=True)
    local_timestamps = frame["timestamp_local"].astype(str).reset_index(drop=True)
    timezone_ok = expected_local.equals(local_timestamps)
    checks.append(
        CheckResult(
            name=f"{market_name}_timezone_alignment",
            status="pass" if timezone_ok else "fail",
            detail=f"Local timestamps align to {timezone}"
            if timezone_ok
            else f"Local timestamps do not align to {timezone}",
            context={"expected_timezone": timezone},
        )
    )
    return checks


def _expected_delivery_utc_index(config: BacktestConfig) -> pd.DatetimeIndex:
    start_local = pd.Timestamp(f"{config.timing.delivery_start_date} 00:00:00", tz=config.timing.timezone)
    next_local_date = pd.Timestamp(config.timing.delivery_end_date) + pd.Timedelta(days=1)
    end_local = pd.Timestamp(f"{next_local_date.date()} 00:00:00", tz=config.timing.timezone)
    return pd.DatetimeIndex(pd.date_range(start_local, end_local, freq="15min", inclusive="left").tz_convert("UTC"))


def _evaluation_window_check(frame: pd.DataFrame, *, config: BacktestConfig, market_name: str) -> CheckResult:
    local_dates = pd.to_datetime(frame["timestamp_utc"], utc=True).dt.tz_convert(config.timing.timezone).dt.date
    filtered = frame[
        (local_dates >= config.timing.delivery_start_date) & (local_dates <= config.timing.delivery_end_date)
    ].copy()
    actual = pd.DatetimeIndex(pd.to_datetime(filtered["timestamp_utc"], utc=True))
    expected = _expected_delivery_utc_index(config)
    missing = expected.difference(actual)
    extra = actual.difference(expected)
    if missing.empty and extra.empty:
        return CheckResult(
            name=f"{market_name}_delivery_window_coverage",
            status="pass",
            detail="Delivery window coverage matches the expected 15-minute grid",
            context={"expected_intervals": len(expected), "actual_intervals": len(actual)},
        )
    return CheckResult(
        name=f"{market_name}_delivery_window_coverage",
        status="fail",
        detail="Delivery window coverage does not match the expected 15-minute grid",
        context={"missing_intervals": len(missing), "extra_intervals": len(extra)},
    )


def _reserve_feasibility_check(config: BacktestConfig) -> CheckResult:
    if config.execution_workflow not in {"da_plus_fcr", "da_plus_afrr"}:
        return CheckResult(
            name="reserve_feasibility",
            status="skip",
            detail="Reserve feasibility is only checked for reserve-aware workflows",
        )
    if config.fcr is not None:
        sustain_minutes = config.fcr.sustain_duration_minutes
    elif config.afrr is not None:
        sustain_minutes = config.afrr.sustain_duration_minutes
    else:
        return CheckResult(
            name="reserve_feasibility",
            status="fail",
            detail="Reserve workflow is missing the corresponding fcr/afrr configuration block",
        )
    sustain_hours = sustain_minutes / 60.0
    max_asset_reserve = 0.0
    for asset in config.assets:
        numerator = asset.battery.usable_energy_mwh
        denominator = sustain_hours * ((1.0 / asset.battery.discharge_efficiency) + asset.battery.charge_efficiency)
        asset_max = min(asset.battery.effective_power_limit_mw, numerator / denominator) if denominator > 0.0 else 0.0
        max_asset_reserve += max(asset_max, 0.0)
    max_site_reserve = min(max_asset_reserve, config.site.poi_import_limit_mw, config.site.poi_export_limit_mw)
    if max_site_reserve > 0.0:
        return CheckResult(
            name="reserve_feasibility",
            status="pass" if max_site_reserve > 0.0 else "fail",
            detail="Site can sustain a positive symmetric reserve commitment",
            context={"max_theoretical_site_reserve_mw": round(max_site_reserve, 6)},
        )
    return CheckResult(
        name="reserve_feasibility",
        status="fail",
        detail="Site cannot sustain any positive symmetric reserve commitment under the configured headroom rules",
    )


def _filter_delivery_window(frame: pd.DataFrame, config: BacktestConfig) -> pd.DataFrame:
    local_dates = pd.to_datetime(frame["timestamp_utc"], utc=True).dt.tz_convert(config.timing.timezone).dt.date
    return frame[
        (local_dates >= config.timing.delivery_start_date) & (local_dates <= config.timing.delivery_end_date)
    ].copy()


def _visible_inputs_at(
    decision_time_utc: pd.Timestamp,
    **frames: pd.DataFrame | None,
) -> dict[str, pd.DataFrame]:
    visible: dict[str, pd.DataFrame] = {}
    for name, frame in frames.items():
        if frame is None:
            continue
        timestamps = pd.to_datetime(frame["timestamp_utc"], utc=True)
        visible[name] = frame.loc[timestamps < decision_time_utc].copy().reset_index(drop=True)
    return visible


def _validate_csv_forecasts(
    config: BacktestConfig,
    *,
    day_ahead: pd.DataFrame,
    imbalance: pd.DataFrame | None,
    fcr: pd.DataFrame | None,
    afrr: dict[str, pd.DataFrame | None],
) -> list[CheckResult]:
    if config.forecast_provider.name != "csv":
        return [CheckResult(name="csv_forecasts", status="skip", detail="Configured forecast provider is not csv")]

    provider = BenchmarkRegistry.build_provider(config)
    if not isinstance(provider, CsvForecastProvider):
        return [
            CheckResult(name="csv_forecasts", status="fail", detail="Expected CsvForecastProvider for csv validation")
        ]

    checks: list[CheckResult] = []
    schedule = MarketRegistry.get(config.market.id).decision_schedule(config)
    delivery_dates = pd.date_range(config.timing.delivery_start_date, config.timing.delivery_end_date, freq="D")
    execution_workflow = config.execution_workflow
    try:
        for delivery_date in delivery_dates:
            day_frame = day_ahead[day_ahead["timestamp_local"].dt.date == delivery_date.date()].reset_index(drop=True)
            schedule_row = schedule[schedule["delivery_date_local"] == str(delivery_date.date())]
            if schedule_row.empty:
                raise ValueError(f"No day-ahead schedule row found for {delivery_date.date()}")
            decision_time = pd.Timestamp(schedule_row.iloc[0]["day_ahead_gate_closure_utc"])
            visible_frames = {
                "day_ahead": day_ahead,
                "imbalance": imbalance,
                "fcr_capacity": fcr,
                **afrr,
            }
            provider.get_forecast(
                market="day_ahead",
                decision_time_utc=decision_time,
                delivery_frame=day_frame,
                actual_frame=day_ahead,
                visible_inputs=_visible_inputs_at(decision_time, **visible_frames),
            )

            if execution_workflow == "da_plus_fcr":
                if fcr is None:
                    raise ValueError("FCR actuals are required for csv reserve validation")
                fcr_day = fcr[fcr["timestamp_local"].dt.date == delivery_date.date()].reset_index(drop=True)
                provider.get_forecast(
                    market="fcr_capacity",
                    decision_time_utc=decision_time,
                    delivery_frame=fcr_day,
                    actual_frame=fcr,
                    visible_inputs=_visible_inputs_at(decision_time, **visible_frames),
                )
            if execution_workflow == "da_plus_afrr":
                for market_name in (
                    "afrr_capacity_up",
                    "afrr_capacity_down",
                    "afrr_activation_price_up",
                    "afrr_activation_price_down",
                    "afrr_activation_ratio_up",
                    "afrr_activation_ratio_down",
                ):
                    actual_frame = afrr.get(market_name)
                    if actual_frame is None:
                        raise ValueError(f"{market_name} actuals are required for csv aFRR validation")
                    day_frame = actual_frame[
                        actual_frame["timestamp_local"].dt.date == delivery_date.date()
                    ].reset_index(drop=True)
                    provider.get_forecast(
                        market=market_name,
                        decision_time_utc=decision_time,
                        delivery_frame=day_frame,
                        actual_frame=actual_frame,
                        visible_inputs=_visible_inputs_at(decision_time, **visible_frames),
                    )

            if config.is_revision_workflow and config.revision is not None:
                for checkpoint_local in config.revision.revision_checkpoints_local:
                    checkpoint = pd.Timestamp(f"{delivery_date.date()} {checkpoint_local}", tz=config.timing.timezone)
                    day_remaining = day_frame[day_frame["timestamp_utc"] > checkpoint.tz_convert("UTC")].reset_index(
                        drop=True
                    )
                    if not day_remaining.empty and execution_workflow in {"da_only", "da_plus_fcr"}:
                        provider.get_forecast(
                            market="day_ahead",
                            decision_time_utc=checkpoint.tz_convert("UTC"),
                            delivery_frame=day_remaining,
                            actual_frame=day_ahead,
                            visible_inputs=_visible_inputs_at(checkpoint.tz_convert("UTC"), **visible_frames),
                        )
                    if not day_remaining.empty and execution_workflow == "da_plus_afrr":
                        provider.get_forecast(
                            market="day_ahead",
                            decision_time_utc=checkpoint.tz_convert("UTC"),
                            delivery_frame=day_remaining,
                            actual_frame=day_ahead,
                            visible_inputs=_visible_inputs_at(checkpoint.tz_convert("UTC"), **visible_frames),
                        )
                    if not day_remaining.empty and execution_workflow == "da_plus_imbalance":
                        if imbalance is None:
                            raise ValueError(
                                "Imbalance actuals are required for schedule_revision imbalance validation"
                            )
                        imb_remaining = imbalance[
                            (imbalance["timestamp_local"].dt.date == delivery_date.date())
                            & (imbalance["timestamp_utc"] > checkpoint.tz_convert("UTC"))
                        ].reset_index(drop=True)
                        if not imb_remaining.empty:
                            provider.get_forecast(
                                market="imbalance",
                                decision_time_utc=checkpoint.tz_convert("UTC"),
                                delivery_frame=imb_remaining,
                                actual_frame=imbalance,
                                visible_inputs=_visible_inputs_at(checkpoint.tz_convert("UTC"), **visible_frames),
                            )

            if execution_workflow == "da_plus_imbalance" and not config.is_revision_workflow:
                if imbalance is None:
                    raise ValueError("Imbalance actuals are required for csv imbalance validation")
                imbalance_day = imbalance[imbalance["timestamp_local"].dt.date == delivery_date.date()].reset_index(
                    drop=True
                )
                rebalance_step = max(
                    config.timing.rebalance_cadence_minutes // config.timing.resolution_minutes,
                    config.timing.execution_lock_intervals,
                )
                for idx in range(0, len(imbalance_day), rebalance_step):
                    remaining = imbalance_day.iloc[idx:].reset_index(drop=True)
                    rebalance_time = pd.Timestamp(imbalance_day.iloc[idx]["timestamp_utc"])
                    provider.get_forecast(
                        market="imbalance",
                        decision_time_utc=rebalance_time,
                        delivery_frame=remaining,
                        actual_frame=imbalance,
                        visible_inputs=_visible_inputs_at(rebalance_time, **visible_frames),
                    )
    except Exception as exc:
        return [CheckResult(name="csv_forecasts", status="fail", detail=str(exc))]

    checks.append(
        CheckResult(name="csv_forecasts", status="pass", detail="CSV forecasts satisfy visibility and coverage checks")
    )
    return checks


def validate_config_file(path: str | Path) -> ValidationReport:
    config_path = Path(path).resolve()
    checks: list[CheckResult] = []
    try:
        config = load_config(config_path)
    except (ValidationError, ValueError) as exc:
        checks.append(CheckResult(name="config_schema", status="fail", detail=str(exc)))
        return _build_report("validate-config", checks, metadata={"config_path": str(config_path)})

    adapter = MarketRegistry.get(config.market.id)
    checks.append(CheckResult(name="config_schema", status="pass", detail="Config validates against schema_version 4"))
    checks.append(
        CheckResult(
            name="asset_count",
            status="pass" if len(config.assets) > 0 else "fail",
            detail=f"Config defines {len(config.assets)} asset(s)",
        )
    )
    checks.append(
        CheckResult(
            name="run_scope",
            status="pass",
            detail=f"Run scope resolved to `{config.run_scope}`",
        )
    )
    checks.append(
        CheckResult(
            name="workflow_supported",
            status="pass" if config.execution_workflow in adapter.supported_workflows else "fail",
            detail=f"Workflow `{config.execution_workflow}` is supported by `{adapter.market_id}`"
            if config.execution_workflow in adapter.supported_workflows
            else f"Workflow `{config.execution_workflow}` is not supported by `{adapter.market_id}`",
        )
    )
    try:
        BenchmarkRegistry.resolve(
            config.market.id,
            config.execution_workflow,
            config.forecast_provider.name,
            run_scope=config.run_scope,
            benchmark_suffix="revision" if config.is_revision_workflow else None,
        )
        BenchmarkRegistry.build_provider(config)
        checks.append(
            CheckResult(name="forecast_provider_supported", status="pass", detail="Forecast provider is supported")
        )
    except Exception as exc:
        checks.append(CheckResult(name="forecast_provider_supported", status="fail", detail=str(exc)))

    try:
        adapter.validate_timing(config)
        checks.append(CheckResult(name="market_timing", status="pass", detail="Timing matches adapter requirements"))
    except Exception as exc:
        checks.append(CheckResult(name="market_timing", status="fail", detail=str(exc)))

    if config.execution_workflow == "da_plus_imbalance" and config.run_scope == "portfolio":
        checks.append(
            CheckResult(
                name="portfolio_imbalance_scope",
                status="fail",
                detail="Portfolio da_plus_imbalance is not supported",
            )
        )
    else:
        checks.append(
            CheckResult(
                name="portfolio_imbalance_scope",
                status="pass",
                detail="Workflow scope is supported for the configured asset count",
            )
        )

    reserve_product = adapter.build_reserve_product(config)
    if config.execution_workflow in {"da_plus_fcr", "da_plus_afrr"}:
        if reserve_product is None:
            checks.append(CheckResult(name="reserve_product", status="fail", detail="Failed to build reserve product"))
        else:
            checks.append(
                CheckResult(
                    name="reserve_product",
                    status="pass",
                    detail=f"Reserve product `{reserve_product.product_id}` is available",
                )
            )
    else:
        checks.append(
            CheckResult(name="reserve_product", status="skip", detail="Reserve product not required for this workflow")
        )

    checks.append(_reserve_feasibility_check(config))
    return _build_report(
        "validate-config",
        checks,
        metadata={
            "config_path": str(config_path),
            "market_id": config.market.id,
            "workflow": config.workflow,
            "base_workflow": config.execution_workflow,
            "forecast_provider": config.forecast_provider.name,
        },
    )


def validate_data_file(path: str | Path) -> ValidationReport:
    try:
        config = load_config(path)
    except (ValidationError, ValueError) as exc:
        return _build_report(
            "validate-data",
            [CheckResult(name="config_schema", status="fail", detail=str(exc))],
            metadata={"config_path": str(Path(path).resolve())},
        )
    adapter = MarketRegistry.get(config.market.id)
    checks: list[CheckResult] = []

    try:
        adapter.validate_timing(config)
        actuals = adapter.load_actuals(config)
    except Exception as exc:
        checks.append(CheckResult(name="load_actuals", status="fail", detail=str(exc)))
        return _build_report(
            "validate-data",
            checks,
            metadata={
                "config_path": str(Path(path).resolve()),
                "market_id": config.market.id,
                "workflow": config.workflow,
                "base_workflow": config.execution_workflow,
            },
        )

    day_ahead = actuals.day_ahead.data.copy()
    eval_day_ahead = _filter_delivery_window(day_ahead, config)
    checks.extend(_market_frame_checks(day_ahead, market_name="day_ahead", timezone=adapter.timezone))
    checks.append(_evaluation_window_check(day_ahead, config=config, market_name="day_ahead"))

    imbalance = actuals.imbalance.data.copy() if actuals.imbalance is not None else None
    eval_imbalance = _filter_delivery_window(imbalance, config) if imbalance is not None else None
    if imbalance is not None:
        checks.extend(_market_frame_checks(imbalance, market_name="imbalance", timezone=adapter.timezone))
        checks.append(_evaluation_window_check(imbalance, config=config, market_name="imbalance"))
    else:
        checks.append(CheckResult(name="imbalance_presence", status="skip", detail="No imbalance input configured"))

    fcr = actuals.fcr_capacity.data.copy() if actuals.fcr_capacity is not None else None
    eval_fcr = _filter_delivery_window(fcr, config) if fcr is not None else None
    if fcr is not None:
        checks.extend(_market_frame_checks(fcr, market_name="fcr_capacity", timezone=adapter.timezone))
        checks.append(_evaluation_window_check(fcr, config=config, market_name="fcr_capacity"))
    else:
        checks.append(CheckResult(name="fcr_presence", status="skip", detail="No FCR capacity input configured"))

    if eval_imbalance is not None:
        aligned = pd.Index(pd.to_datetime(eval_day_ahead["timestamp_utc"], utc=True)).equals(
            pd.Index(pd.to_datetime(eval_imbalance["timestamp_utc"], utc=True))
        )
        checks.append(
            CheckResult(
                name="day_ahead_imbalance_alignment",
                status="pass" if aligned else "fail",
                detail="Day-ahead and imbalance series align on UTC delivery intervals"
                if aligned
                else "Day-ahead and imbalance series do not align on UTC delivery intervals",
            )
        )
    if eval_fcr is not None:
        aligned = pd.Index(pd.to_datetime(eval_day_ahead["timestamp_utc"], utc=True)).equals(
            pd.Index(pd.to_datetime(eval_fcr["timestamp_utc"], utc=True))
        )
        checks.append(
            CheckResult(
                name="day_ahead_fcr_alignment",
                status="pass" if aligned else "fail",
                detail="Day-ahead and FCR series align on UTC delivery intervals"
                if aligned
                else "Day-ahead and FCR series do not align on UTC delivery intervals",
            )
        )
    afrr_frames = {
        "afrr_capacity_up": actuals.afrr_capacity_up.data.copy() if actuals.afrr_capacity_up is not None else None,
        "afrr_capacity_down": actuals.afrr_capacity_down.data.copy()
        if actuals.afrr_capacity_down is not None
        else None,
        "afrr_activation_price_up": actuals.afrr_activation_price_up.data.copy()
        if actuals.afrr_activation_price_up is not None
        else None,
        "afrr_activation_price_down": actuals.afrr_activation_price_down.data.copy()
        if actuals.afrr_activation_price_down is not None
        else None,
        "afrr_activation_ratio_up": actuals.afrr_activation_ratio_up.data.copy()
        if actuals.afrr_activation_ratio_up is not None
        else None,
        "afrr_activation_ratio_down": actuals.afrr_activation_ratio_down.data.copy()
        if actuals.afrr_activation_ratio_down is not None
        else None,
    }
    for market_name, frame in afrr_frames.items():
        if frame is not None:
            checks.extend(_market_frame_checks(frame, market_name=market_name, timezone=adapter.timezone))
            checks.append(_evaluation_window_check(frame, config=config, market_name=market_name))
            aligned = pd.Index(pd.to_datetime(eval_day_ahead["timestamp_utc"], utc=True)).equals(
                pd.Index(pd.to_datetime(_filter_delivery_window(frame, config)["timestamp_utc"], utc=True))
            )
            checks.append(
                CheckResult(
                    name=f"day_ahead_{market_name}_alignment",
                    status="pass" if aligned else "fail",
                    detail=f"Day-ahead and {market_name} series align on UTC delivery intervals"
                    if aligned
                    else f"Day-ahead and {market_name} series do not align on UTC delivery intervals",
                )
            )

    checks.extend(_validate_csv_forecasts(config, day_ahead=day_ahead, imbalance=imbalance, fcr=fcr, afrr=afrr_frames))
    checks.append(_reserve_feasibility_check(config))
    return _build_report(
        "validate-data",
        checks,
        metadata={
            "config_path": str(Path(path).resolve()),
            "market_id": config.market.id,
            "workflow": config.workflow,
            "base_workflow": config.execution_workflow,
        },
    )


def doctor(config_path: str | Path | None = None) -> ValidationReport:
    checks: list[CheckResult] = []
    metadata: dict[str, Any] = {}
    config: BacktestConfig | None = None
    if config_path is not None:
        try:
            config = load_config(config_path)
        except (ValidationError, ValueError) as exc:
            return _build_report(
                "doctor",
                [CheckResult(name="config_schema", status="fail", detail=str(exc))],
                metadata={"config_path": str(Path(config_path).resolve())},
            )
        metadata["config_path"] = str(Path(config_path).resolve())
        metadata["market_id"] = config.market.id
        metadata["workflow"] = config.workflow

    try:
        solver_name = ensure_solver_available()
        checks.append(CheckResult(name="solver", status="pass", detail=f"Solver available: {solver_name}"))
    except Exception as exc:
        checks.append(CheckResult(name="solver", status="fail", detail=str(exc)))

    for module_name in ("nbclient", "nbformat", "matplotlib", "pandas"):
        available = find_spec(module_name) is not None
        checks.append(
            CheckResult(
                name=f"dependency_{module_name}",
                status="pass" if available else "fail",
                detail=f"{module_name} is available" if available else f"{module_name} is not installed",
            )
        )

    if config is None:
        checks.append(
            CheckResult(name="credentials", status="skip", detail="No config provided; credential checks skipped")
        )
        artifact_root = Path("artifacts").resolve()
    else:
        for env_var in config.market.live_data_auth_env_var_names:
            available = os.environ.get(env_var) is not None
            checks.append(
                CheckResult(
                    name=f"credential_{env_var}",
                    status="pass" if available else "warn",
                    detail=f"{env_var} is set"
                    if available
                    else f"{env_var} is not set (only needed for live connectors)",
                )
            )
        artifact_root = config.artifacts.root_dir.resolve()

    try:
        artifact_root.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile("w", encoding="utf-8", dir=artifact_root, prefix=".doctor-", delete=True) as handle:
            handle.write("ok")
        checks.append(
            CheckResult(name="artifact_root", status="pass", detail=f"Writable artifact root: {artifact_root}")
        )
    except Exception as exc:
        checks.append(
            CheckResult(
                name="artifact_root", status="fail", detail=str(exc), context={"artifact_root": str(artifact_root)}
            )
        )

    return _build_report("doctor", checks, metadata=metadata)
