"""Reconcile a live paper ledger with validated data once Elia has published it.

During a live run every quarter-hour is settled on the *near-real-time* price (``ods162``, not validated). Elia
publishes the validated history (``ods134`` per quarter-hour, ``ods133`` per minute) one to two days later; the
committed ``data/public`` set is rebuilt from the same datasets. :func:`reconcile_run` recomputes every quarter-hour
the run touched from its decision journal and reports:

- price revisions (provisional vs validated price) and their P&L effect;
- quarter-hours whose price was missing live and is now available ("filled") or still pending;
- missed minutes (engine down or late) and decisions taken while an expected feed row was missing;
- minute-level revisions of the signals the strategy acted on (running price / SI as seen live vs validated).

Sources are tried in order (``public`` = ``data/public``, ``api`` = Elia historical datasets, ``nrt`` = a later
snapshot of the near-real-time dataset, labelled as not validated). A price marked ``Validated`` wins over any
earlier non-validated candidate.
"""

from __future__ import annotations

import importlib
from collections.abc import Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ..settlement import settle_quarter_hours
from ..types import MINUTES_PER_QH, BatteryParams
from .feeds import (
    IMBALANCE_PRICE_MINUTE,
    IMBALANCE_PRICE_QH,
    NEW_FORMULA_MINUTE,
    NEW_FORMULA_QH,
    EliaLiveFeeds,
    FeedError,
    FeedSpec,
    finite_or_none,
    iso_utc,
    sanitize_message,
)
from .store import CONFIG, DECISIONS, RunStore

PRICE_TOLERANCE_EUR_MWH = 0.005
SOURCES = ("public", "api", "nrt")


@dataclass
class ReconcileResult:
    summary: dict[str, Any]
    quarter_hours: pd.DataFrame
    minutes: pd.DataFrame
    output_dir: Path | None


def _price_frame(frame: pd.DataFrame, mechanism: str, source: str) -> pd.DataFrame:
    if frame is None or frame.empty or "imbalance_price_eur_mwh" not in frame.columns:
        return pd.DataFrame(columns=["timestamp_utc", "mechanism", "price_eur_mwh", "quality_status", "source"])
    out = pd.DataFrame(
        {
            "timestamp_utc": pd.to_datetime(frame["timestamp_utc"], utc=True),
            "mechanism": mechanism,
            "price_eur_mwh": pd.to_numeric(frame["imbalance_price_eur_mwh"], errors="coerce").astype(float),
            "quality_status": frame["quality_status"].astype(str) if "quality_status" in frame.columns else None,
            "source": source,
        }
    )
    return out.dropna(subset=["price_eur_mwh"])


def validated_prices(
    start: pd.Timestamp,
    end: pd.Timestamp,
    *,
    sources: Sequence[str] = SOURCES,
    feeds: EliaLiveFeeds | None = None,
    notes: list[str] | None = None,
    current_dataset: str = "ods134",
    new_formula_dataset: str = "ods212",
    nrt_dataset: str = "ods162",
) -> pd.DataFrame:
    """Candidate settlement prices for ``[start, end)`` from every requested source (one row per candidate).

    ``current_dataset`` (ODS134) holds the current-formula quarter-hour prices; ``new_formula_dataset`` (ODS212)
    the new-formula prices, which after the go-live are the settlement prices for every quarter-hour from the
    switch. Both dataset ids can be overridden should Elia publish the validated history under another id.
    """
    notes = notes if notes is not None else []
    frames: list[pd.DataFrame] = []
    for source in sources:
        try:
            if source == "public":
                pdata: Any = importlib.import_module("euroflex_bess_lab.public_data")
                try:
                    qh = pdata.load_belgium_qh(start, end, columns=["imbalance_price_eur_mwh", "quality_status"])
                    frames.append(_price_frame(qh, "current", f"data/public:{current_dataset}"))
                except (FileNotFoundError, KeyError, ValueError) as exc:
                    notes.append(f"public current: {sanitize_message(str(exc))[:200]}")
                try:
                    nf = pdata.load_belgium_new_formula_qh(
                        start, end, columns=["imbalance_price_eur_mwh", "quality_status"]
                    )
                    frames.append(_price_frame(nf, "new2026", f"data/public:{new_formula_dataset}"))
                except (FileNotFoundError, KeyError, ValueError) as exc:
                    notes.append(f"public new2026: {sanitize_message(str(exc))[:200]}")
            elif source == "api":
                client = feeds or EliaLiveFeeds()
                current_spec = replace(IMBALANCE_PRICE_QH, dataset_id=current_dataset, name="validated_current_qh")
                new_spec = replace(NEW_FORMULA_QH, dataset_id=new_formula_dataset, name="validated_new_formula_qh")
                frames.append(
                    _price_frame(client.fetch_export(current_spec, start, end), "current", f"api:{current_dataset}")
                )
                frames.append(
                    _price_frame(client.fetch_export(new_spec, start, end), "new2026", f"api:{new_formula_dataset}")
                )
            elif source == "nrt":
                client = feeds or EliaLiveFeeds()
                nrt_spec = replace(IMBALANCE_PRICE_QH, dataset_id=nrt_dataset)
                frames.append(_price_frame(client.fetch_export(nrt_spec, start, end), "current", f"nrt:{nrt_dataset}"))
            else:
                raise ValueError(f"unknown source `{source}`; choose from {SOURCES}")
        except FeedError as exc:
            notes.append(f"{source}: {sanitize_message(str(exc))[:200]}")
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=["timestamp_utc", "mechanism", "price_eur_mwh", "quality_status", "source", "rank"])
    out = pd.concat(frames, ignore_index=True)
    out["rank"] = out.groupby(["timestamp_utc", "mechanism"]).cumcount()
    return out


def _choose(candidates: pd.DataFrame) -> pd.DataFrame:
    """One price per (quarter-hour, mechanism): the first ``Validated`` candidate, else the first candidate."""
    if candidates.empty:
        return candidates
    frame = candidates.copy()
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True)
    if "rank" not in frame.columns:
        frame["rank"] = frame.groupby(["timestamp_utc", "mechanism"]).cumcount()
    frame["validated"] = frame["quality_status"].astype(str).str.lower().eq("validated")
    frame = frame.sort_values(["timestamp_utc", "mechanism", "validated", "rank"], ascending=[True, True, False, True])
    return frame.drop_duplicates(["timestamp_utc", "mechanism"], keep="first").reset_index(drop=True)


def _journal_frame(records: Sequence[dict[str, Any]]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=["minute_utc", "status", "applied_mw"])
    frame = pd.json_normalize(list(records))
    frame["minute_utc"] = pd.to_datetime(frame["minute_utc"], utc=True)
    frame = frame.drop_duplicates("minute_utc", keep="last").sort_values("minute_utc").reset_index(drop=True)
    frame["applied_mw"] = pd.to_numeric(frame.get("applied_mw"), errors="coerce").fillna(0.0)
    return frame


def _quarter_hours_from_journal(journal: pd.DataFrame, battery: BatteryParams) -> pd.DataFrame:
    """Energy per quarter-hour recomputed from the journal (undecided minutes are idle)."""
    if journal.empty:
        return pd.DataFrame()
    qh_start = journal["minute_utc"].dt.floor("15min")
    first, last = qh_start.min(), qh_start.max()
    qh_times = pd.date_range(first, last, freq="15min")
    grid = pd.date_range(first, last + pd.Timedelta(minutes=MINUTES_PER_QH), freq="1min", inclusive="left")
    power = journal.set_index("minute_utc")["applied_mw"].reindex(grid).fillna(0.0).to_numpy(dtype=float)
    soc_series = pd.to_numeric(journal.set_index("minute_utc").get("soc_end_mwh"), errors="coerce")
    soc = soc_series.reindex(grid).ffill().bfill().to_numpy(dtype=float)
    ledger = settle_quarter_hours(power, soc, np.full(len(qh_times), np.nan), qh_times, battery)
    status = journal.set_index("minute_utc")["status"].reindex(grid)
    by_qh = pd.DataFrame({"qh": grid.floor("15min"), "status": status.to_numpy()})
    counts = by_qh.groupby("qh")["status"].agg(
        minutes_decided=lambda s: int(s.isin(["ok", "strategy_error"]).sum()),
        minutes_missed=lambda s: int((s == "missed").sum()),
    )
    ledger = ledger.set_index("timestamp_utc").join(counts).reset_index()
    return ledger[
        [
            "timestamp_utc",
            "energy_mwh",
            "discharged_mwh",
            "charged_mwh",
            "degradation_eur",
            "minutes_decided",
            "minutes_missed",
        ]
    ]


#: (mechanism, journal signal, validated column, API spec, public loader) for minute-level signal revisions
_MINUTE_SIGNALS: tuple[tuple[str, str, str, FeedSpec, str], ...] = (
    ("current", "price_cum_eur_mwh", "price_cum_eur_mwh", IMBALANCE_PRICE_MINUTE, "load_belgium_minute"),
    (
        "new2026",
        "nf_imbalance_price_eur_mwh",
        "imbalance_price_eur_mwh",
        NEW_FORMULA_MINUTE,
        "load_belgium_new_formula_minute",
    ),
)


def _validated_minutes(
    mechanism: str,
    column: str,
    spec: FeedSpec,
    loader: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    *,
    sources: Sequence[str],
    feeds: EliaLiveFeeds | None,
    notes: list[str],
) -> pd.DataFrame | None:
    for source in sources:
        try:
            if source == "public":
                pdata: Any = importlib.import_module("euroflex_bess_lab.public_data")
                frame = getattr(pdata, loader)(start, end, columns=[column, "quality_status"])
                label = f"data/public:{spec.historical_dataset_id or spec.dataset_id}"
            elif source == "api":
                target = spec.historical() or spec
                frame = (feeds or EliaLiveFeeds()).fetch_export(target, start, end)
                label = f"api:{target.dataset_id}"
            else:
                continue
        except (FileNotFoundError, KeyError, ValueError, FeedError, AttributeError) as exc:
            notes.append(f"minutes {mechanism} {source}: {sanitize_message(str(exc))[:200]}")
            continue
        if frame is not None and len(frame) and column in frame.columns:
            out = pd.DataFrame(
                {
                    "row_utc": pd.to_datetime(frame["timestamp_utc"], utc=True),
                    "validated": pd.to_numeric(frame[column], errors="coerce"),
                    "source": label,
                }
            )
            return out.drop_duplicates("row_utc", keep="last")
    return None


def _minute_revisions(
    journal: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    *,
    sources: Sequence[str],
    feeds: EliaLiveFeeds | None,
    notes: list[str],
) -> pd.DataFrame:
    """The latest visible minute row at each decision, as seen live, vs the validated minute tables.

    Compares the current-formula running price (``ods161`` live vs ``ods133``) and, when the run saw it, the
    new-formula running price (``ods211`` live vs ``ods211`` later).
    """
    if journal.empty or "visible_minute_end_utc" not in journal.columns:
        return pd.DataFrame()
    decided = journal[journal["status"] != "missed"].copy()
    decided["row_utc"] = pd.to_datetime(decided["visible_minute_end_utc"], utc=True) - pd.Timedelta(minutes=1)
    out = pd.DataFrame({"minute_utc": decided["minute_utc"].to_numpy(), "row_utc": decided["row_utc"].to_numpy()})
    compared_any = False
    for mechanism, signal, column, spec, loader in _MINUTE_SIGNALS:
        live_column = f"signals.{signal}"
        if live_column not in decided.columns:
            continue
        live = pd.to_numeric(decided[live_column], errors="coerce").to_numpy()
        if not np.isfinite(live).any():
            continue
        validated = _validated_minutes(
            mechanism, column, spec, loader, start, end, sources=sources, feeds=feeds, notes=notes
        )
        if validated is None:
            continue
        merged = out[["row_utc"]].merge(validated, on="row_utc", how="left")
        prefix = "price_cum" if mechanism == "current" else "nf_price"
        out[f"live_{prefix}_eur_mwh"] = live
        out[f"validated_{prefix}_eur_mwh"] = merged["validated"].to_numpy()
        out[f"{prefix}_revision_eur_mwh"] = out[f"validated_{prefix}_eur_mwh"] - out[f"live_{prefix}_eur_mwh"]
        out[f"{prefix}_source"] = merged["source"].to_numpy()
        compared_any = True
    return out if compared_any else pd.DataFrame()


def _ranges(minutes: pd.Series) -> list[dict[str, Any]]:
    if minutes.empty:
        return []
    values = pd.to_datetime(minutes, utc=True).sort_values().reset_index(drop=True)
    breaks = values.diff().ne(pd.Timedelta(minutes=1)).cumsum()
    out = []
    for _, group in values.groupby(breaks):
        out.append({"from_utc": iso_utc(group.iloc[0]), "to_utc": iso_utc(group.iloc[-1]), "minutes": int(len(group))})
    return out


def reconcile_run(
    run_dir: str | Path,
    *,
    sources: Sequence[str] = SOURCES,
    feeds: EliaLiveFeeds | None = None,
    prices: pd.DataFrame | None = None,
    include_minutes: bool = True,
    output_dir: str | Path | None = None,
    write: bool = True,
    current_dataset: str = "ods134",
    new_formula_dataset: str = "ods212",
) -> ReconcileResult:
    """Compare the paper ledger of ``run_dir`` with validated data and write ``reconcile/`` outputs.

    ``prices`` (columns ``timestamp_utc``, ``mechanism``, ``price_eur_mwh``, ``quality_status``, ``source``) skips
    the source lookup, e.g. in tests or when the validated prices were obtained elsewhere.
    """
    store = RunStore(run_dir, fsync=False)
    config = store.read_json(CONFIG) or {}
    battery_payload = config.get("battery")
    if not battery_payload:
        raise FileNotFoundError("not a live run directory (config.json missing)")
    battery = BatteryParams(**battery_payload)
    journal = _journal_frame(store.read_jsonl(DECISIONS))
    ledger = store.read_ledger()
    notes: list[str] = []
    qh = _quarter_hours_from_journal(journal, battery)
    if qh.empty:
        raise ValueError("the run has no decisions to reconcile yet")
    start = qh["timestamp_utc"].min()
    end = qh["timestamp_utc"].max() + pd.Timedelta(minutes=MINUTES_PER_QH)
    go_live = pd.Timestamp(config.get("config", {}).get("go_live_utc") or "2026-11-24T09:00:00Z")
    go_live = go_live.tz_localize("UTC") if go_live.tzinfo is None else go_live.tz_convert("UTC")
    setting = str(config.get("config", {}).get("mechanism") or "auto")

    if not ledger.empty:
        provisional = ledger[
            ["timestamp_utc", "mechanism", "price_eur_mwh", "pnl_eur", "energy_mwh", "price_missing", "revision"]
        ].rename(
            columns={
                "price_eur_mwh": "provisional_price_eur_mwh",
                "pnl_eur": "provisional_pnl_eur",
                "energy_mwh": "ledger_energy_mwh",
                "price_missing": "provisional_price_missing",
                "revision": "ledger_revision",
            }
        )
        qh = qh.merge(provisional, on="timestamp_utc", how="left")
    else:
        for column in ("mechanism", "provisional_price_eur_mwh", "provisional_pnl_eur", "ledger_energy_mwh"):
            qh[column] = np.nan
        qh["provisional_price_missing"] = np.nan
        qh["ledger_revision"] = np.nan
    qh["in_ledger"] = qh["ledger_energy_mwh"].notna()
    default_mechanism = np.where(
        setting in ("current", "new2026"), setting, np.where(qh["timestamp_utc"] < go_live, "current", "new2026")
    )
    qh["mechanism"] = qh["mechanism"].where(qh["mechanism"].notna(), pd.Series(default_mechanism, index=qh.index))

    candidates = (
        prices
        if prices is not None
        else validated_prices(
            start,
            end,
            sources=sources,
            feeds=feeds,
            notes=notes,
            current_dataset=current_dataset,
            new_formula_dataset=new_formula_dataset,
        )
    )
    chosen = _choose(candidates)
    if not chosen.empty:
        chosen = chosen.rename(
            columns={
                "price_eur_mwh": "validated_price_eur_mwh",
                "quality_status": "validated_quality_status",
                "source": "validated_source",
            }
        )[["timestamp_utc", "mechanism", "validated_price_eur_mwh", "validated_quality_status", "validated_source"]]
        chosen["timestamp_utc"] = pd.to_datetime(chosen["timestamp_utc"], utc=True)
        qh = qh.merge(chosen, on=["timestamp_utc", "mechanism"], how="left")
    else:
        qh["validated_price_eur_mwh"] = np.nan
        qh["validated_quality_status"] = None
        qh["validated_source"] = None

    qh["energy_matches_ledger"] = np.where(
        qh["in_ledger"], np.isclose(qh["energy_mwh"], qh["ledger_energy_mwh"].fillna(0.0), atol=1e-9), np.nan
    )
    qh["validated_revenue_eur"] = qh["energy_mwh"] * qh["validated_price_eur_mwh"]
    qh["validated_pnl_eur"] = qh["validated_revenue_eur"] - qh["degradation_eur"]
    qh["price_revision_eur_mwh"] = qh["validated_price_eur_mwh"] - qh["provisional_price_eur_mwh"]
    qh["pnl_difference_eur"] = qh["validated_pnl_eur"] - qh["provisional_pnl_eur"].fillna(0.0)
    has_validated = qh["validated_price_eur_mwh"].notna()
    has_provisional = qh["provisional_price_eur_mwh"].notna()
    revised = has_validated & has_provisional & (qh["price_revision_eur_mwh"].abs() > PRICE_TOLERANCE_EUR_MWH)
    status = np.select(
        [
            ~qh["in_ledger"] & has_validated,
            ~qh["in_ledger"],
            revised,
            has_validated & has_provisional,
            has_validated & ~has_provisional,
        ],
        ["unsettled_filled", "unsettled_pending", "revised", "confirmed", "filled"],
        default="pending",
    )
    qh["status"] = status

    minutes = pd.DataFrame()
    if include_minutes:
        minutes = _minute_revisions(
            journal, start, end, sources=[s for s in sources if s != "nrt"], feeds=feeds, notes=notes
        )

    missed = journal.loc[journal["status"] == "missed", "minute_utc"] if not journal.empty else pd.Series(dtype=object)
    gap_rows: dict[str, int] = {}
    if "missing_expected_rows" in journal.columns:
        for value in journal["missing_expected_rows"].dropna():
            for feed in value if isinstance(value, list) else []:
                gap_rows[feed] = gap_rows.get(feed, 0) + 1
    with_validated = qh[has_validated]
    summary: dict[str, Any] = {
        "schema": "euroflex-live-reconcile/1",
        "run_id": config.get("run_id"),
        "strategy": config.get("strategy"),
        "period": {"start_utc": iso_utc(start), "end_utc": iso_utc(end)},
        "sources": list(sources) if prices is None else ["given"],
        "quarter_hours": int(len(qh)),
        "status_counts": {str(k): int(v) for k, v in qh["status"].value_counts().items()},
        "in_ledger": int(qh["in_ledger"].sum()),
        "validated_available": int(has_validated.sum()),
        "energy_mismatches": int((qh["energy_matches_ledger"] == 0).sum()),
        "price_revisions": {
            "count": int(revised.sum()),
            "max_abs_eur_mwh": finite_or_none(qh.loc[revised, "price_revision_eur_mwh"].abs().max())
            if revised.any()
            else 0.0,
            "mean_abs_eur_mwh": finite_or_none(qh.loc[revised, "price_revision_eur_mwh"].abs().mean())
            if revised.any()
            else 0.0,
        },
        "pnl": {
            "provisional_eur": finite_or_none(qh["provisional_pnl_eur"].fillna(0.0).sum()),
            "provisional_on_validated_subset_eur": finite_or_none(
                with_validated["provisional_pnl_eur"].fillna(0.0).sum()
            ),
            "validated_eur": finite_or_none(with_validated["validated_pnl_eur"].sum()),
            "difference_eur": finite_or_none(with_validated["pnl_difference_eur"].sum()),
        },
        "minutes": {
            "journal_minutes": int(len(journal)),
            "decisions": int((journal["status"] != "missed").sum()) if not journal.empty else 0,
            "missed": int(len(missed)),
            "missed_ranges": _ranges(missed),
            "decisions_with_missing_feed_rows": int(
                journal["missing_expected_rows"].map(lambda v: bool(v) if isinstance(v, list) else False).sum()
            )
            if "missing_expected_rows" in journal.columns
            else 0,
            "missing_feed_rows_by_feed": gap_rows,
        },
        "notes": notes,
    }
    if not minutes.empty:
        revisions: dict[str, Any] = {}
        for prefix in ("price_cum", "nf_price"):
            if f"validated_{prefix}_eur_mwh" not in minutes.columns:
                continue
            compared = minutes[f"validated_{prefix}_eur_mwh"].notna() & minutes[f"live_{prefix}_eur_mwh"].notna()
            delta = minutes.loc[compared, f"{prefix}_revision_eur_mwh"].abs()
            sources_seen = minutes[f"{prefix}_source"].dropna()
            revisions[prefix] = {
                "source": str(sources_seen.iloc[0]) if len(sources_seen) else None,
                "compared": int(compared.sum()),
                "revised": int((delta > PRICE_TOLERANCE_EUR_MWH).sum()),
                "max_abs_eur_mwh": finite_or_none(delta.max()) if len(delta) else None,
            }
        summary["minute_signal_revisions"] = revisions
    target: Path | None = None
    if write:
        target = Path(output_dir) if output_dir is not None else store.run_dir / "reconcile"
        target.mkdir(parents=True, exist_ok=True)
        qh_out = qh.copy()
        qh_out.to_parquet(target / "qh_reconciliation.parquet", index=False)
        qh_out.assign(timestamp_utc=qh_out["timestamp_utc"].map(iso_utc)).to_csv(
            target / "qh_reconciliation.csv", index=False
        )
        if not minutes.empty:
            minutes.assign(
                minute_utc=minutes["minute_utc"].map(iso_utc), row_utc=minutes["row_utc"].map(iso_utc)
            ).to_csv(target / "minute_reconciliation.csv", index=False)
        RunStore(target, fsync=False).write_json("summary.json", summary)
    return ReconcileResult(summary=summary, quarter_hours=qh, minutes=minutes, output_dir=target)
