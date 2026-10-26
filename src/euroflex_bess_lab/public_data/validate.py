"""Data-quality checks for the normalized public tables."""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pandas as pd


def minute_grid_report(timestamps: pd.Series, freq: str = "1min") -> dict[str, object]:
    """Uniqueness / ordering / gap report for a regular UTC grid."""

    ts = pd.Series(pd.to_datetime(timestamps, utc=True)).reset_index(drop=True)
    step = pd.Timedelta(freq)
    if ts.empty:
        return {"rows": 0, "unique": True, "sorted": True, "missing_intervals": 0, "gaps": []}
    expected = int((ts.max() - ts.min()) / step) + 1
    diffs = ts.diff().dropna()
    gaps = diffs[diffs > step]
    gap_list: list[tuple[int, str]] = [
        (int(gap / step) - 1, f"{ts.iloc[int(i) - 1]:%Y-%m-%dT%H:%M:%SZ}") for i, gap in gaps.items()
    ]
    largest = [{"after_utc": after, "missing_intervals": n} for n, after in sorted(gap_list, key=lambda g: -g[0])[:10]]
    return {
        "rows": int(len(ts)),
        "unique": bool(ts.is_unique),
        "sorted": bool(ts.is_monotonic_increasing),
        "expected_rows": expected,
        "missing_intervals": int(expected - ts.nunique()),
        "largest_gaps": largest,
    }


def null_minutes_report(
    frame: pd.DataFrame, columns: Sequence[str], time_column: str = "timestamp_utc"
) -> dict[str, int]:
    """Count grid rows where *all* of ``columns`` are null (i.e. the source had no row for that interval)."""

    present = [c for c in columns if c in frame.columns]
    if not present:
        return {"rows_all_null": int(len(frame))}
    mask = frame[present].isna().all(axis=1)
    return {"rows_all_null": int(mask.sum())}


def last_minute_vs_qh(
    minute: pd.DataFrame,
    qh: pd.DataFrame,
    *,
    pairs: Sequence[tuple[str, str]] = (
        ("price_cum_eur_mwh", "imbalance_price_eur_mwh"),
        ("si_cum_mw", "si_mw"),
        ("alpha_eur_mwh", "alpha_eur_mwh"),
        ("mip_eur_mwh", "mip_eur_mwh"),
        ("mdp_eur_mwh", "mdp_eur_mwh"),
    ),
    tolerance: float = 0.01,
) -> dict[str, object]:
    """Compare the last minute of each quarter-hour (minute 14, cumulative QH-to-date values) with the final QH row.

    ``minute`` must contain ``timestamp_utc`` and ``qh_start_utc`` plus the left columns of ``pairs``; ``qh`` must
    contain ``timestamp_utc`` plus the right columns. Returns match rates within ``tolerance`` (absolute) and
    within 1 EUR/MWh (or 1 MW), plus difference quantiles.
    """

    minute_ts = pd.to_datetime(minute["timestamp_utc"], utc=True)
    qh_start = pd.to_datetime(minute["qh_start_utc"], utc=True)
    last_mask = (minute_ts - qh_start) == pd.Timedelta(minutes=14)
    left_cols = [a for a, _ in pairs if a in minute.columns]
    last = minute.loc[last_mask, ["qh_start_utc", *left_cols]].copy()
    last["qh_start_utc"] = pd.to_datetime(last["qh_start_utc"], utc=True)
    right_cols = [b for _, b in pairs if b in qh.columns]
    final = qh[["timestamp_utc", *right_cols]].copy()
    final["timestamp_utc"] = pd.to_datetime(final["timestamp_utc"], utc=True)
    merged = last.merge(final, left_on="qh_start_utc", right_on="timestamp_utc", how="inner", suffixes=("_min", "_qh"))
    report: dict[str, object] = {"qh_compared": int(len(merged)), "tolerance": tolerance}
    for left, right in pairs:
        if left not in minute.columns or right not in qh.columns:
            continue
        lname = f"{left}_min" if left == right else left
        rname = f"{right}_qh" if left == right else right
        a = merged[lname].astype("float64")
        b = merged[rname].astype("float64")
        both = a.notna() & b.notna()
        diff = (a[both] - b[both]).abs()
        key = left if left == right else f"{left}_vs_{right}"
        if both.sum() == 0:
            report[key] = {"compared": 0}
            continue
        report[key] = {
            "compared": int(both.sum()),
            "match_rate": float((diff <= tolerance).mean()),
            "match_rate_within_1": float((diff <= 1.0).mean()),
            "sign_match_rate": float((np.sign(a[both]) == np.sign(b[both])).mean()),
            "median_abs_diff": float(diff.median()),
            "p99_abs_diff": float(np.quantile(diff, 0.99)),
            "max_abs_diff": float(diff.max()),
        }
    return report


def curve_vs_published_prices(curves: pd.DataFrame, qh: pd.DataFrame, si_threshold: float = 100.0) -> dict[str, object]:
    """Price read off each quarter-hour's bid curve at the volume Elia actually activated vs. the published MIP/MDP.

    Upward: activated aFRR + mFRR SA + mFRR DA up volume vs ``mip_eur_mwh`` on quarter-hours with SI below
    ``-si_threshold``; downward analogously vs ``mdp_eur_mwh`` with SI above ``si_threshold``.
    """

    if curves.empty or qh.empty:
        return {"compared": 0}
    q = qh.copy()
    q["timestamp_utc"] = pd.to_datetime(q["timestamp_utc"], utc=True)
    q = q.set_index("timestamp_utc")
    report: dict[str, object] = {"si_threshold_mw": si_threshold}
    for direction, volume_cols, target, mask in (
        ("up", ["afrr_up_mw", "mfrr_sa_up_mw", "mfrr_da_up_mw"], "mip_eur_mwh", q["si_mw"] < -si_threshold),
        ("down", ["afrr_down_mw", "mfrr_sa_down_mw", "mfrr_da_down_mw"], "mdp_eur_mwh", q["si_mw"] > si_threshold),
    ):
        part = curves[curves["direction"] == direction]
        if part.empty:
            continue
        matrix = part.pivot_table(index="timestamp_utc", columns="cum_mw", values="price_eur_mwh", aggfunc="first")
        matrix.index = pd.to_datetime(matrix.index, utc=True)
        grid = matrix.columns.to_numpy(dtype=float)
        volume = q[[c for c in volume_cols if c in q.columns]].fillna(0.0).sum(axis=1).reindex(matrix.index)
        k = np.clip(np.searchsorted(grid, volume.to_numpy(dtype=float), side="left"), 0, len(grid) - 1)
        implied = pd.Series(matrix.to_numpy()[np.arange(len(matrix)), k], index=matrix.index)
        frame = pd.DataFrame({"published": q[target].reindex(matrix.index), "implied": implied})
        frame = frame[mask.reindex(matrix.index).fillna(False).astype(bool)].dropna()
        if len(frame) < 3:
            report[direction] = {"compared": int(len(frame))}
            continue
        report[direction] = {
            "compared": int(len(frame)),
            "pearson": float(frame["published"].corr(frame["implied"])),
            "spearman": float(frame["published"].corr(frame["implied"], method="spearman")),
            "median_abs_diff_eur_mwh": float((frame["published"] - frame["implied"]).abs().median()),
        }
    return report
