from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .analytics.reporting import load_report_summary
from .contracts import validate_export_manifest_payload
from .data.io import save_json

SCHEDULE_EXPORT_PROFILES = {"benchmark", "operator", "submission_candidate"}
BID_EXPORT_PROFILES = {"benchmark", "bid_planning", "submission_candidate"}


def _schedule_profile_metadata(profile: str) -> dict[str, Any]:
    if profile == "benchmark":
        return {
            "profile": "benchmark",
            "intended_consumer": "analytics",
            "benchmark_grade_only": True,
            "live_submission_ready": False,
        }
    if profile == "operator":
        return {
            "profile": "operator",
            "intended_consumer": "scheduler",
            "benchmark_grade_only": False,
            "live_submission_ready": False,
        }
    if profile == "submission_candidate":
        return {
            "profile": "submission_candidate",
            "intended_consumer": "execution_router_or_scheduler",
            "benchmark_grade_only": False,
            "live_submission_ready": False,
        }
    raise ValueError(f"Unsupported schedule export profile: {profile}")


def _bid_profile_metadata(profile: str) -> dict[str, Any]:
    if profile == "benchmark":
        return {
            "profile": "benchmark",
            "intended_consumer": "analytics",
            "benchmark_grade_only": True,
            "live_submission_ready": False,
        }
    if profile == "bid_planning":
        return {
            "profile": "bid_planning",
            "intended_consumer": "trader_or_scheduler",
            "benchmark_grade_only": False,
            "live_submission_ready": False,
        }
    if profile == "submission_candidate":
        return {
            "profile": "submission_candidate",
            "intended_consumer": "execution_router_or_scheduler",
            "benchmark_grade_only": False,
            "live_submission_ready": False,
        }
    raise ValueError(f"Unsupported bid export profile: {profile}")


def _load_run_bundle(
    run_dir: str | Path,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame | None,
    pd.DataFrame | None,
    pd.DataFrame | None,
]:
    target = Path(run_dir).resolve()
    summary = load_report_summary(target)
    with (target / "config_snapshot.json").open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    site_dispatch = pd.read_parquet(target / "site_dispatch.parquet")
    asset_dispatch = pd.read_parquet(target / "asset_dispatch.parquet")
    baseline_schedule = (
        pd.read_parquet(target / "baseline_schedule.parquet")
        if (target / "baseline_schedule.parquet").exists()
        else None
    )
    revision_schedule = (
        pd.read_parquet(target / "revision_schedule.parquet")
        if (target / "revision_schedule.parquet").exists()
        else None
    )
    schedule_lineage = (
        pd.read_parquet(target / "schedule_lineage.parquet") if (target / "schedule_lineage.parquet").exists() else None
    )
    return summary, config, site_dispatch, asset_dispatch, baseline_schedule, revision_schedule, schedule_lineage


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_payload(frame: pd.DataFrame, *, path_stem: Path, metadata: dict[str, Any]) -> list[dict[str, Any]]:
    csv_path = path_stem.with_suffix(".csv")
    parquet_path = path_stem.with_suffix(".parquet")
    json_path = path_stem.with_suffix(".json")
    frame.to_csv(csv_path, index=False)
    frame.to_parquet(parquet_path, index=False)
    save_json({"metadata": metadata, "records": frame.to_dict(orient="records")}, json_path)
    entries = []
    for path in (csv_path, parquet_path, json_path):
        entries.append({"path": path.name, "bytes": path.stat().st_size, "sha256": _sha256(path)})
    return entries


def _write_manifest(
    *,
    export_dir: Path,
    files: list[dict[str, Any]],
    metadata: dict[str, Any],
    source_run_dir: Path,
    schema_version: int,
) -> Path:
    payload = {
        "schema_version": schema_version,
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "source_run_dir": str(source_run_dir),
        "metadata": metadata,
        "files": files,
    }
    validate_export_manifest_payload(payload)
    save_json(payload, export_dir / "manifest.json")
    return export_dir


def export_schedule(
    run_dir: str | Path,
    output_dir: str | Path | None = None,
    *,
    profile: str = "benchmark",
) -> Path:
    source_run_dir = Path(run_dir).resolve()
    summary, config, site_dispatch, asset_dispatch, baseline_schedule, revision_schedule, _ = _load_run_bundle(
        source_run_dir
    )
    if profile not in SCHEDULE_EXPORT_PROFILES:
        raise ValueError(f"Unsupported schedule export profile: {profile}")
    target = (
        Path(output_dir).resolve() if output_dir is not None else source_run_dir / "exports" / f"schedule-{profile}"
    )
    target.mkdir(parents=True, exist_ok=True)
    generation_time = datetime.now(tz=UTC).isoformat()

    if profile == "benchmark":
        site_columns = [
            "timestamp_utc",
            "timestamp_local",
            "charge_mw",
            "discharge_mw",
            "net_export_mw",
            "soc_mwh",
            "reserved_capacity_mw",
            "afrr_up_reserved_mw",
            "afrr_down_reserved_mw",
            "reserve_headroom_up_mw",
            "reserve_headroom_down_mw",
            "reason_code",
            "decision_type",
            "schedule_version",
            "lock_state",
        ]
        asset_columns = [
            "timestamp_utc",
            "timestamp_local",
            "asset_id",
            "asset_name",
            "charge_mw",
            "discharge_mw",
            "net_export_mw",
            "soc_mwh",
            "fcr_reserved_mw",
            "afrr_up_reserved_mw",
            "afrr_down_reserved_mw",
            "availability_factor",
            "reason_code",
            "decision_type",
            "schedule_version",
            "lock_state",
        ]
    elif profile in {"operator", "submission_candidate"}:
        site_columns = [
            "timestamp_utc",
            "timestamp_local",
            "net_export_mw",
            "soc_mwh",
            "reserved_capacity_mw",
            "afrr_up_reserved_mw",
            "afrr_down_reserved_mw",
            "decision_type",
            "schedule_version",
            "lock_state",
        ]
        asset_columns = [
            "timestamp_utc",
            "timestamp_local",
            "asset_id",
            "asset_name",
            "net_export_mw",
            "soc_mwh",
            "fcr_reserved_mw",
            "afrr_up_reserved_mw",
            "afrr_down_reserved_mw",
            "availability_factor",
            "reason_code",
            "schedule_version",
            "lock_state",
        ]
    site_frame = site_dispatch[site_columns].copy()
    site_frame.insert(0, "run_id", summary["run_id"])
    site_frame.insert(1, "site_id", summary["site_id"])
    site_frame.insert(2, "market_id", summary["market_id"])
    site_frame.insert(3, "workflow", summary["workflow"])
    site_frame.insert(4, "run_scope", summary["run_scope"])
    site_frame.insert(5, "benchmark_name", summary["benchmark_name"])
    site_frame.insert(6, "market_timezone", summary["market_timezone"])
    site_frame.insert(7, "generated_at_utc", generation_time)
    site_frame.insert(8, "export_kind", "site_schedule")

    asset_frame = asset_dispatch[asset_columns].copy()
    asset_frame.insert(0, "run_id", summary["run_id"])
    asset_frame.insert(1, "site_id", summary["site_id"])
    asset_frame.insert(2, "market_id", summary["market_id"])
    asset_frame.insert(3, "workflow", summary["workflow"])
    asset_frame.insert(4, "run_scope", summary["run_scope"])
    asset_frame.insert(5, "generated_at_utc", generation_time)
    asset_frame.insert(6, "export_kind", "asset_allocation")

    metadata = {
        "export_kind": "schedule",
        "run_id": summary["run_id"],
        "source_run_id": summary["run_id"],
        "site_id": summary["site_id"],
        "market_id": summary["market_id"],
        "workflow": summary["workflow"],
        "run_scope": summary["run_scope"],
        "benchmark_name": summary["benchmark_name"],
        "market_timezone": summary["market_timezone"],
        "generation_time_utc": generation_time,
        "latest_schedule_version": "revision_latest" if revision_schedule is not None else "baseline",
        "config_run_name": config["run_name"],
        **_schedule_profile_metadata(profile),
    }
    files = []
    files.extend(
        _write_payload(site_frame, path_stem=target / "site_schedule", metadata={**metadata, "payload": "site"})
    )
    files.extend(
        _write_payload(
            asset_frame,
            path_stem=target / "asset_allocation",
            metadata={**metadata, "payload": "asset"},
        )
    )
    if baseline_schedule is not None:
        baseline_frame = baseline_schedule[
            [
                "timestamp_utc",
                "timestamp_local",
                "net_export_mw",
                "soc_mwh",
                "fcr_reserved_mw",
                "afrr_up_reserved_mw",
                "afrr_down_reserved_mw",
                "schedule_version",
                "schedule_state",
                "lock_state",
            ]
        ].copy()
        baseline_frame.insert(0, "run_id", summary["run_id"])
        baseline_frame.insert(1, "site_id", summary["site_id"])
        baseline_frame.insert(2, "market_id", summary["market_id"])
        baseline_frame.insert(3, "workflow", summary["workflow"])
        baseline_frame.insert(4, "run_scope", summary["run_scope"])
        baseline_frame.insert(5, "generated_at_utc", generation_time)
        baseline_frame.insert(6, "export_kind", "baseline_schedule")
        files.extend(
            _write_payload(
                baseline_frame,
                path_stem=target / "baseline_schedule",
                metadata={**metadata, "payload": "baseline_schedule"},
            )
        )
    if revision_schedule is not None:
        revised_frame = revision_schedule[
            [
                "timestamp_utc",
                "timestamp_local",
                "net_export_mw",
                "soc_mwh",
                "fcr_reserved_mw",
                "afrr_up_reserved_mw",
                "afrr_down_reserved_mw",
                "schedule_version",
                "schedule_state",
                "lock_state",
            ]
        ].copy()
        revised_frame.insert(0, "run_id", summary["run_id"])
        revised_frame.insert(1, "site_id", summary["site_id"])
        revised_frame.insert(2, "market_id", summary["market_id"])
        revised_frame.insert(3, "workflow", summary["workflow"])
        revised_frame.insert(4, "run_scope", summary["run_scope"])
        revised_frame.insert(5, "generated_at_utc", generation_time)
        revised_frame.insert(6, "export_kind", "latest_revised_schedule")
        files.extend(
            _write_payload(
                revised_frame,
                path_stem=target / "latest_revised_schedule",
                metadata={**metadata, "payload": "latest_revised_schedule"},
            )
        )
    return _write_manifest(
        export_dir=target,
        files=files,
        metadata=metadata,
        source_run_dir=source_run_dir,
        schema_version=int(summary["schema_version"]),
    )


def export_bids(
    run_dir: str | Path,
    output_dir: str | Path | None = None,
    *,
    profile: str = "benchmark",
) -> Path:
    source_run_dir = Path(run_dir).resolve()
    summary, config, site_dispatch, asset_dispatch, _, revision_schedule, _ = _load_run_bundle(source_run_dir)
    if profile not in BID_EXPORT_PROFILES:
        raise ValueError(f"Unsupported bid export profile: {profile}")
    target = Path(output_dir).resolve() if output_dir is not None else source_run_dir / "exports" / f"bids-{profile}"
    target.mkdir(parents=True, exist_ok=True)
    generation_time = datetime.now(tz=UTC).isoformat()
    bid_site_dispatch = revision_schedule if revision_schedule is not None else site_dispatch

    nominated_net_export = (
        bid_site_dispatch["baseline_net_export_mw"]
        if summary["workflow"] == "da_plus_imbalance"
        else bid_site_dispatch["net_export_mw"]
    )
    if profile == "benchmark":
        site_payload = {
            "day_ahead_nominated_net_export_mw": nominated_net_export,
            "reserved_capacity_mw": bid_site_dispatch["reserved_capacity_mw"],
            "afrr_up_reserved_mw": bid_site_dispatch["afrr_up_reserved_mw"],
            "afrr_down_reserved_mw": bid_site_dispatch["afrr_down_reserved_mw"],
            "reserve_product_id": summary.get("reserve_product_id"),
            "reserve_settlement_mode": summary.get("reserve_settlement_mode"),
            "reserve_activation_mode": summary.get("reserve_activation_mode"),
            "schedule_version": bid_site_dispatch.get("schedule_version"),
        }
        asset_payload = {
            "day_ahead_nominated_net_export_mw": asset_dispatch["net_export_mw"],
            "reserved_capacity_mw": asset_dispatch["reserved_capacity_mw"],
            "afrr_up_reserved_mw": asset_dispatch["afrr_up_reserved_mw"],
            "afrr_down_reserved_mw": asset_dispatch["afrr_down_reserved_mw"],
            "reserve_product_id": summary.get("reserve_product_id"),
            "schedule_version": asset_dispatch.get("schedule_version"),
        }
    elif profile in {"bid_planning", "submission_candidate"}:
        site_payload = {
            "day_ahead_nominated_net_export_mw": nominated_net_export,
            "reserved_capacity_mw": bid_site_dispatch["reserved_capacity_mw"],
            "afrr_up_reserved_mw": bid_site_dispatch["afrr_up_reserved_mw"],
            "afrr_down_reserved_mw": bid_site_dispatch["afrr_down_reserved_mw"],
            "reserve_product_id": summary.get("reserve_product_id"),
            "schedule_version": bid_site_dispatch.get("schedule_version"),
            "schedule_state": bid_site_dispatch.get("schedule_state"),
            "lock_state": bid_site_dispatch.get("lock_state"),
        }
        asset_payload = {
            "day_ahead_nominated_net_export_mw": asset_dispatch["net_export_mw"],
            "reserved_capacity_mw": asset_dispatch["reserved_capacity_mw"],
            "afrr_up_reserved_mw": asset_dispatch["afrr_up_reserved_mw"],
            "afrr_down_reserved_mw": asset_dispatch["afrr_down_reserved_mw"],
            "reserve_product_id": summary.get("reserve_product_id"),
            "schedule_version": asset_dispatch.get("schedule_version"),
        }

    site_bids = pd.DataFrame(
        {
            "run_id": summary["run_id"],
            "site_id": summary["site_id"],
            "market_id": summary["market_id"],
            "workflow": summary["workflow"],
            "run_scope": summary["run_scope"],
            "benchmark_name": summary["benchmark_name"],
            "market_timezone": summary["market_timezone"],
            "generated_at_utc": generation_time,
            "timestamp_utc": bid_site_dispatch["timestamp_utc"],
            "timestamp_local": bid_site_dispatch["timestamp_local"],
            "export_kind": "site_bids",
            **_bid_profile_metadata(profile),
            **site_payload,
        }
    )
    asset_annex = pd.DataFrame(
        {
            "run_id": summary["run_id"],
            "site_id": summary["site_id"],
            "market_id": summary["market_id"],
            "workflow": summary["workflow"],
            "run_scope": summary["run_scope"],
            "generated_at_utc": generation_time,
            "timestamp_utc": asset_dispatch["timestamp_utc"],
            "timestamp_local": asset_dispatch["timestamp_local"],
            "asset_id": asset_dispatch["asset_id"],
            "asset_name": asset_dispatch["asset_name"],
            "export_kind": "asset_reserve_allocation",
            **_bid_profile_metadata(profile),
            **asset_payload,
        }
    )

    metadata = {
        "export_kind": "bids",
        "run_id": summary["run_id"],
        "source_run_id": summary["run_id"],
        "site_id": summary["site_id"],
        "market_id": summary["market_id"],
        "workflow": summary["workflow"],
        "run_scope": summary["run_scope"],
        "benchmark_name": summary["benchmark_name"],
        "market_timezone": summary["market_timezone"],
        "generation_time_utc": generation_time,
        "latest_schedule_version": "revision_latest" if revision_schedule is not None else "baseline",
        "config_run_name": config["run_name"],
        "reserve_product_id": summary.get("reserve_product_id"),
        "reserve_settlement_mode": summary.get("reserve_settlement_mode"),
        "reserve_activation_mode": summary.get("reserve_activation_mode"),
        **_bid_profile_metadata(profile),
    }
    files = []
    files.extend(_write_payload(site_bids, path_stem=target / "site_bids", metadata={**metadata, "payload": "site"}))
    files.extend(
        _write_payload(
            asset_annex,
            path_stem=target / "asset_reserve_allocation",
            metadata={**metadata, "payload": "asset"},
        )
    )
    return _write_manifest(
        export_dir=target,
        files=files,
        metadata=metadata,
        source_run_dir=source_run_dir,
        schema_version=int(summary["schema_version"]),
    )


def export_revision(run_dir: str | Path, output_dir: str | Path | None = None) -> Path:
    source_run_dir = Path(run_dir).resolve()
    summary, config, _, asset_dispatch, baseline_schedule, revision_schedule, schedule_lineage = _load_run_bundle(
        source_run_dir
    )
    target = Path(output_dir).resolve() if output_dir is not None else source_run_dir / "exports" / "revision"
    target.mkdir(parents=True, exist_ok=True)
    generation_time = datetime.now(tz=UTC).isoformat()
    if baseline_schedule is None or revision_schedule is None:
        raise ValueError("export-revision requires a schedule_revision run with baseline and revision artifacts")

    metadata = {
        "export_kind": "revision",
        "run_id": summary["run_id"],
        "source_run_id": summary["run_id"],
        "site_id": summary["site_id"],
        "market_id": summary["market_id"],
        "workflow": summary["workflow"],
        "run_scope": summary["run_scope"],
        "benchmark_name": summary["benchmark_name"],
        "market_timezone": summary["market_timezone"],
        "generation_time_utc": generation_time,
        "latest_schedule_version": "revision_latest",
        "config_run_name": config["run_name"],
        "profile": "benchmark",
        "intended_consumer": "analytics",
        "benchmark_grade_only": True,
        "live_submission_ready": False,
    }
    files = []
    files.extend(
        _write_payload(
            baseline_schedule,
            path_stem=target / "baseline_schedule",
            metadata={**metadata, "payload": "baseline_schedule"},
        )
    )
    files.extend(
        _write_payload(
            revision_schedule,
            path_stem=target / "latest_revised_schedule",
            metadata={**metadata, "payload": "latest_revised_schedule"},
        )
    )
    if schedule_lineage is not None:
        files.extend(
            _write_payload(
                schedule_lineage,
                path_stem=target / "schedule_lineage",
                metadata={**metadata, "payload": "schedule_lineage"},
            )
        )
    asset_payload = asset_dispatch.copy()
    asset_payload.insert(0, "generated_at_utc", generation_time)
    files.extend(
        _write_payload(
            asset_payload,
            path_stem=target / "asset_revision_allocation",
            metadata={**metadata, "payload": "asset_revision_allocation"},
        )
    )
    return _write_manifest(
        export_dir=target,
        files=files,
        metadata=metadata,
        source_run_dir=source_run_dir,
        schema_version=int(summary["schema_version"]),
    )
