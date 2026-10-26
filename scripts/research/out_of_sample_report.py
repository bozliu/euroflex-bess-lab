"""Report writer for ``out_of_sample.py``: statistics, ledgers, figures and ``reports/out_of_sample.md``.

Called by the main script's ``report`` stage with the script module itself (constants, evaluations, cache layout);
it only reads cached results, so the report can be rebuilt without re-running simulations.
"""

from __future__ import annotations

import json
import pickle
from pathlib import Path
from types import ModuleType
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

from euroflex_bess_lab.realtime import daily_pnl
from euroflex_bess_lab.realtime.forecasting import holdout

TZ = holdout.DEFAULT_TIMEZONE
# dataviz reference palette, light mode: categorical slots 1-3 (validated all-pairs), chart chrome and ink
SLOTS = ("#2a78d6", "#eb6834", "#1baf7a")
INK, INK2, MUTED, GRID, AXIS, SURFACE = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7", "#fcfcfb"
LINE_PT, MARKER_PT = 1.2, 5.0
LEDGER_MAX_BYTES = 5 * 1024 * 1024
#: frozen arms whose quarter-hour ledgers are committed (the evidence for the headline)
EXPORT_ARMS = {
    "current": ("prism_risk_frozen", "forecast_mpc_risk_frozen"),
    "new2026": ("prism_zero_risk_frozen", "prism_zero_live_risk_frozen"),
}
#: (frozen arm, short label) shown in the public tables, primary first
PUBLIC_ARMS = {
    "current": (("prism_risk_frozen", "SCALES-L"), ("forecast_mpc_risk_frozen", "Forecast MPC")),
    "new2026": (
        ("prism_zero_risk_frozen", "SCALES-L zero-history"),
        ("prism_zero_live_risk_frozen", "SCALES-L zero-history + live running price"),
    ),
}
HORIZON_BUCKETS = ((1, 3), (4, 6), (7, 10), (11, 16))
RULE_PHRASES = {
    "threshold_price": "the threshold rule on the running price",
    "threshold_si": "the threshold rule on the running system imbalance",
}


# ------------------------------------------------------------------------------------------------------------------
# formatting


def _fmt(value: Any, digits: int = 0) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, bool | np.bool_):
        return "yes" if value else "NO"
    if isinstance(value, float | int | np.floating | np.integer):
        if not np.isfinite(float(value)):
            return "n/a"
        return f"{float(value):,.{digits}f}"
    return str(value)


def _pct(value: float | None, digits: int = 0, signed: bool = False) -> str:
    if value is None or not np.isfinite(value):
        return "n/a"
    return f"{value:+.{digits}f} %" if signed else f"{value:.{digits}f} %"


def _ci(lo: float | None, hi: float | None, digits: int = 0, unit: str = " %", signed: bool = True) -> str:
    if lo is None or hi is None or not (np.isfinite(lo) and np.isfinite(hi)):
        return "n/a"
    spec = f"+,.{digits}f" if signed else f",.{digits}f"
    return f"[{lo:{spec}}, {hi:{spec}}]{unit}"


def _table(rows: list[dict[str, Any]], columns: list[tuple[str, str, int]]) -> str:
    head = "| " + " | ".join(c[1] for c in columns) + " |"
    sep = "| " + " | ".join("---" if i == 0 else "---:" for i in range(len(columns))) + " |"
    body = ["| " + " | ".join(_fmt(r.get(c[0]), c[2]) for c in columns) + " |" for r in rows]
    return "\n".join([head, sep, *body])


def _long_date(day: str) -> str:
    return pd.Timestamp(day).strftime("%-d %B %Y")


def _months(n: int) -> str:
    return f"{n} month" if n == 1 else f"{n} months"


def _short_date(day: str) -> str:
    return pd.Timestamp(day).strftime("%-d %b %Y")


def _test_days_local(split: dict[str, Any]) -> tuple[str, str]:
    first = pd.Timestamp(split["test_start_utc"]).tz_convert(TZ).strftime("%Y-%m-%d")
    last = (pd.Timestamp(split["test_end_utc"]) - pd.Timedelta(minutes=1)).tz_convert(TZ).strftime("%Y-%m-%d")
    return first, last


# ------------------------------------------------------------------------------------------------------------------
# assembling the summary


def _frozen_logs(s: Any, oos: ModuleType) -> list[dict[str, Any]]:
    """Training audits of the frozen models used by the current design (one per evaluation and model kind)."""
    logs = []
    for ev in oos.evaluations(s):
        for kind in ev.frozen_kinds:
            path = oos.frozen_model_path(s, kind, ev.split.label).with_suffix(".json")
            logs.append(json.loads(path.read_text()))
    return logs


def _walk_forward_info(s: Any, oos: ModuleType) -> dict[str, Any]:
    info: dict[str, Any] = {}
    for name in oos.WF_KINDS:
        if not any(name in ev.wf_models for ev in oos.evaluations(s)):
            continue
        path = oos.wf_model_path(s, name)
        with open(path, "rb") as fh:
            forecaster = pickle.load(fh)
        start, end = oos.needed_window(s, name)
        if name == "prism_zero_wf":
            start = max(start, oos.utc("2026-01-01T00:00:00Z"))
        coverage = oos.fold_coverage(forecaster, start, end)
        info[name] = {
            "source": "realtime_backtest.py cache (same specification)"
            if path.parent == s.wf_dir
            else "fitted by out_of_sample.py (same specification as realtime_backtest.py)",
            "file": path.name,
            "window_utc": [start.isoformat(), end.isoformat()],
            "train_months": oos.rb.TRAIN_MONTHS,
            **coverage,
        }
        del forecaster
    return info


def _audit_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    tradable = [r for r in rows if r["tradable"]]
    oracle = [r for r in rows if not r["tradable"]]
    arms = sorted({(r["evaluation"], r["arm"]) for r in tradable})
    white = [r for r in tradable if r.get("white_box_passed") is not None]
    return {
        "rows": rows,
        "cuts": sorted({int(r["cut_minute"]) for r in rows}),
        "slices": sorted({r["slice"] for r in rows}),
        "tradable_audits": len(tradable),
        "tradable_passed": int(sum(bool(r["passed"]) for r in tradable)),
        "tradable_arms_audited": [f"{e}:{a}" for e, a in arms],
        "all_tradable_passed": bool(tradable) and all(bool(r["passed"]) for r in tradable),
        "white_box_audits": len(white),
        "white_box_passed": int(sum(bool(r["white_box_passed"]) for r in white)),
        "oracle_audits": len(oracle),
        "oracle_failed": int(sum(not bool(r["passed"]) for r in oracle)),
    }


def headline_sentence(
    block: dict[str, Any], arm: str, *, zero_history: bool = False, live: str = "prism_zero_live_risk_frozen"
) -> str:
    split = block["split"]
    summary = block["arms"][arm]
    wf = block["comparisons"][arm]["vs_walk_forward"]
    th = block["comparisons"][arm]["vs_best_threshold"]
    first, last = _test_days_local(split)
    months = len(split["months"])
    uplift, (lo, hi) = th["uplift_pct"], th["uplift_ci_pct"]
    relation = "above" if uplift >= 0 else "below"
    ci = f"95 % CI {lo:+.0f} % to {hi:+.0f} %"
    if zero_history:
        cutoff = _long_date(pd.Timestamp(split["cutoff_utc"]).tz_convert(TZ).strftime("%Y-%m-%d"))
        sentence = (
            f"A zero-history SCALES-L model frozen at {cutoff}, which never saw a new-formula price, earns "
            f"{summary['eur_per_mw_year']:,.0f} EUR/MW/yr at the new-formula price over the {_months(months)} "
            f"{_short_date(first)} to {_short_date(last)}, {100 * wf['ratio']:.0f} % of its monthly-retrained version "
            f"and {abs(uplift):.0f} % {relation} the best reactive rule ({ci})"
        )
        if live in block["arms"]:
            th_live = block["comparisons"][live]["vs_best_threshold"]
            lo_l, hi_l = th_live["uplift_ci_pct"]
            relation_l = "above" if th_live["uplift_pct"] >= 0 else "below"
            sentence += (
                f"; with the live running new-formula price as an extra input it earns "
                f"{block['arms'][live]['eur_per_mw_year']:,.0f} EUR/MW/yr, {abs(th_live['uplift_pct']):.0f} % "
                f"{relation_l} the rule (95 % CI {lo_l:+.0f} % to {hi_l:+.0f} %)"
            )
        return sentence + "."
    return (
        f"Models trained only on data up to {_long_date(split['label'])} and never updated earn "
        f"{summary['eur_per_mw_year']:,.0f} EUR/MW/yr over the {_months(months)} that followed "
        f"({_short_date(first)} to {_short_date(last)}), {100 * wf['ratio']:.0f} % of the monthly-retrained model "
        f"and {abs(uplift):.0f} % {relation} the best reactive rule ({ci})."
    )


def pooled_ageing(blocks: list[dict[str, Any]], arm: str, wf: str) -> list[dict[str, Any]]:
    """Ratio of summed frozen to summed walk-forward P&L per months-since-cutoff bucket, pooled over cutoffs."""
    out = []
    for lo, hi in HORIZON_BUCKETS:
        num = den = 0.0
        months = cutoffs = 0
        labels = set()
        for block in blocks:
            for row in block["monthly"]:
                if lo <= row["months_since_cutoff"] <= hi and arm in row["pnl_eur"] and wf in row["pnl_eur"]:
                    num += row["pnl_eur"][arm]
                    den += row["pnl_eur"][wf]
                    months += 1
                    labels.add(block["split"]["label"])
        cutoffs = len(labels)
        if months:
            out.append(
                {
                    "bucket": f"{lo}-{hi}",
                    "months": months,
                    "cutoffs": cutoffs,
                    "ratio": num / den if den > 0 else None,
                    "frozen_eur": num,
                    "walk_forward_eur": den,
                }
            )
    return out


def evaluate(
    s: Any, oos: ModuleType, ev: Any, ledgers: dict[str, pd.DataFrame], arms: tuple[str, ...]
) -> dict[str, Any]:
    return holdout.evaluate_holdout(
        {name: ledgers[name] for name in arms},
        ev.split,
        battery=oos.battery_params(),
        mechanism=ev.mechanism,
        frozen_arms=[a for a in arms if a.endswith(("_frozen", "_frozen_plan"))],
        walk_forward=oos.walk_forward_pairs(ev.mechanism, arms),
        thresholds=oos.THRESHOLDS,
        oracle="oracle_qh",
        block_days=oos.BLOCK_DAYS,
        n_boot=s.n_boot,
        seed=oos.SEED,
        confidence=oos.CONFIDENCE,
    )


def write_ablation(s: Any, oos: ModuleType) -> None:
    """Frozen-price-profile ablation: local cache output only (``ablation_summary.json``), never the report."""
    out: dict[str, Any] = {}
    for ev in oos.evaluations(s):
        ledgers = oos.load_ledgers(s, ev)
        arms = tuple(a for a in ev.arms if a in ledgers)
        block = holdout.to_jsonable(evaluate(s, oos, ev, ledgers, arms))
        out[ev.key] = {
            arm: {
                "eur_per_mw_year": block["arms"][arm]["eur_per_mw_year"],
                "vs_walk_forward_ratio": block["comparisons"][arm].get("vs_walk_forward", {}).get("ratio"),
                "vs_walk_forward_ratio_ci": block["comparisons"][arm].get("vs_walk_forward", {}).get("ratio_ci"),
                "vs_best_threshold_uplift_pct": block["comparisons"][arm]["vs_best_threshold"]["uplift_pct"],
                "vs_best_threshold_uplift_ci_pct": block["comparisons"][arm]["vs_best_threshold"]["uplift_ci_pct"],
                "months_won": block["comparisons"][arm]["vs_best_threshold"]["months_won"],
                "months": block["comparisons"][arm]["vs_best_threshold"]["months"],
            }
            for arm in block["frozen_arms"]
        }
    path = s.cache / "ablation_summary.json"
    path.write_text(json.dumps(out, indent=1))
    print(f"[report] ablation summary (local only): {path}", flush=True)


def build_summary(s: Any, oos: ModuleType, timings: dict[str, float]) -> tuple[dict[str, Any], dict[str, Any]]:
    battery = oos.battery_params()
    blocks: list[dict[str, Any]] = []
    ledgers_by_ev: dict[str, dict[str, pd.DataFrame]] = {}
    for ev in oos.evaluations(s):
        ledgers = oos.load_ledgers(s, ev)
        public = oos.ARMS[ev.mechanism]
        missing = [a for a in public if a not in ledgers]
        if missing:
            raise FileNotFoundError(f"{ev.key}: missing simulated arms {missing}; run the simulate stage")
        block = evaluate(s, oos, ev, ledgers, public)
        skill_path = s.cache / "sim" / ev.key / "skill.json"
        block["skill"] = json.loads(skill_path.read_text()) if skill_path.exists() else {}
        timing_path = s.cache / "sim" / ev.key / "timings.json"
        timings_ev = json.loads(timing_path.read_text()) if timing_path.exists() else {}
        block["timings_seconds"] = {k: v for k, v in timings_ev.items() if k in public or k in ("load_market", "skill")}
        blocks.append(holdout.to_jsonable(block))
        ledgers_by_ev[ev.key] = {name: holdout.trim_to_test(ledgers[name], ev.split) for name in public}
    audit_path = s.cache / "audit.json"
    audit_rows = json.loads(audit_path.read_text()) if audit_path.exists() else []
    audit_rows = [r for r in audit_rows if r["arm"] in oos.ARMS[r["mechanism"]]]
    current = [b for b in blocks if b["mechanism"] == "current"]
    headline = [headline_sentence(b, oos.PRIMARY["current"]) for b in current]
    headline += [
        headline_sentence(b, oos.PRIMARY["new2026"], zero_history=True) for b in blocks if b["mechanism"] == "new2026"
    ]
    summary = {
        "schema_version": holdout.SUMMARY_SCHEMA_VERSION,
        "generated_by": "scripts/research/out_of_sample.py",
        "data_source": "data/public (Elia Open Data, CC BY 4.0, attribution 'Elia Transmission Belgium SA'; "
        "day-ahead prices from Energy-Charts, CC BY 4.0)",
        "battery": {
            "power_mw": battery.power_mw,
            "energy_mwh": battery.energy_mwh,
            "charge_efficiency": battery.charge_efficiency,
            "discharge_efficiency": battery.discharge_efficiency,
            "soc_min_frac": battery.soc_min_frac,
            "soc_max_frac": battery.soc_max_frac,
            "initial_soc_frac": battery.initial_soc_frac,
            "degradation_eur_per_mwh": battery.degradation_eur_per_mwh,
        },
        "publication": {"minute_lag": oos.MINUTE_LAG, "qh_final_lag_minutes": oos.QH_FINAL_LAG},
        "settings": {
            "cutoffs": list(s.cutoffs),
            "end_day": s.end_day,
            "new_formula_cutoff": s.nf_cutoff,
            "new_formula_end_day": s.nf_end_day,
            "timezone": TZ,
            "train_months": s.train_months,
            "quick_train_days": s.quick_train_days,
            "warmup_days": oos.rb.WARMUP_DAYS,
            "risk_quantile": oos.rb.RISK_QUANTILE,
            "gbm": s.gbm or "GBMParams() defaults (200 trees, learning rate 0.08, 31 leaves, min 80 per leaf)",
            "price_profile_days": s.profile_days,
            "block_days": oos.BLOCK_DAYS,
            "n_boot": s.n_boot,
            "seed": oos.SEED,
            "confidence": oos.CONFIDENCE,
            "quick": s.quick,
            "price_taker": True,
        },
        "primary_arms": dict(oos.PRIMARY),
        "labels": {k: v for k, v in oos.LABELS.items() if any(k in arms for arms in oos.ARMS.values())},
        "evaluations": blocks,
        "pooled_ageing": {
            f"{arm}/{wf}": pooled_ageing(current, arm, wf)
            for arm, wf in oos.walk_forward_pairs("current", oos.ARMS["current"]).items()
        },
        "frozen_training_audit": _frozen_logs(s, oos),
        "walk_forward_models": _walk_forward_info(s, oos),
        "lookahead_audit": _audit_summary(audit_rows),
        "headline": headline,
        "runtime_seconds": {**timings},
    }
    summary = holdout.to_jsonable(summary)
    errors = holdout.validate_summary(summary)
    if errors:
        raise ValueError("summary does not match the schema:\n" + "\n".join(errors))
    return summary, ledgers_by_ev


# ------------------------------------------------------------------------------------------------------------------
# ledgers


def export_ledgers(
    s: Any, oos: ModuleType, summary: dict[str, Any], ledgers: dict[str, dict[str, pd.DataFrame]]
) -> list[str]:
    s.ledgers.mkdir(parents=True, exist_ok=True)
    written: list[str] = []
    blocks = {b["id"]: b for b in summary["evaluations"]}
    daily_rows = []
    for ev in oos.evaluations(s):
        block = blocks[ev.key]
        for name, qh in ledgers[ev.key].items():
            daily = daily_pnl(qh, timezone=TZ)
            daily.insert(0, "arm", name)
            daily.insert(0, "evaluation", ev.key)
            daily_rows.append(daily)
        for name in EXPORT_ARMS[ev.mechanism]:
            frame = ledgers[ev.key][name].copy()
            for column in frame.columns:
                if frame[column].dtype == np.float64:
                    frame[column] = frame[column].astype(np.float32)
            frame["strategy"] = name
            frame["mechanism"] = ev.mechanism
            frame["fleet_mw"] = np.float32(summary["battery"]["power_mw"])
            stem = f"oos_{name}_{ev.mechanism}_{ev.split.label}"
            path = s.ledgers / f"{stem}.parquet"
            frame.to_parquet(path, index=False, compression="zstd")
            if path.stat().st_size > LEDGER_MAX_BYTES:
                raise RuntimeError(f"{path.name} exceeds the 5 MB ledger budget")
            arm = block["arms"][name]
            sidecar = {
                "strategy": name,
                "label": oos.LABELS[name],
                "mechanism": ev.mechanism,
                "evaluation": ev.key,
                "start_utc": block["split"]["test_start_utc"],
                "end_utc": block["split"]["test_end_utc"],
                "cutoff_utc": block["split"]["cutoff_utc"],
                "train_start_utc": block["split"]["train_start_utc"],
                "frozen": True,
                "battery": summary["battery"],
                "publication": summary["publication"],
                "price_taker": True,
                "fleet_mw": summary["battery"]["power_mw"],
                "source": "scripts/research/out_of_sample.py on data/public (Elia Open Data, CC BY 4.0)",
                "summary": {
                    k: arm.get(k)
                    for k in (
                        "eur_per_mw_year",
                        "pnl_eur",
                        "cycles_per_day",
                        "capture_vs_oracle",
                        "daily_pnl_cvar05_eur",
                    )
                },
            }
            (s.ledgers / f"{stem}.json").write_text(json.dumps(sidecar, indent=1))
            written.append(stem)
    daily_all = pd.concat(daily_rows, ignore_index=True)
    daily_all["date"] = daily_all["date"].astype(str)
    for column in ("revenue_eur", "degradation_eur", "pnl_eur", "discharged_mwh", "charged_mwh"):
        daily_all[column] = daily_all[column].astype(np.float64)
    daily_all.to_parquet(s.ledgers / "oos_daily_pnl.parquet", index=False, compression="zstd")
    (s.ledgers / "oos_daily_pnl.json").write_text(
        json.dumps(
            {
                "description": "net P&L per local delivery day (Europe/Brussels) for every arm of every evaluation; "
                "input of the paired block-bootstrap statistics in oos_summary.json",
                "columns": list(daily_all.columns),
                "evaluations": sorted(daily_all["evaluation"].unique().tolist()),
                "arms": sorted(daily_all["arm"].unique().tolist()),
                "battery": summary["battery"],
                "publication": summary["publication"],
                "source": "scripts/research/out_of_sample.py on data/public (Elia Open Data, CC BY 4.0)",
            },
            indent=1,
        )
    )
    written.append("oos_daily_pnl")
    return written


# ------------------------------------------------------------------------------------------------------------------
# figures


def _style(ax: Any) -> None:
    ax.set_facecolor(SURFACE)
    ax.grid(True, color=GRID, linewidth=0.5, linestyle="-")
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(AXIS)
        ax.spines[side].set_linewidth(0.6)
    ax.tick_params(colors=INK2, labelsize=8, length=0)
    ax.xaxis.label.set_color(INK2)
    ax.yaxis.label.set_color(INK2)
    ax.title.set_color(INK)


def _cutoff_colors(blocks: list[dict[str, Any]]) -> dict[str, str]:
    labels = sorted({b["split"]["label"] for b in blocks if b["mechanism"] == "current"})
    return {label: SLOTS[i % len(SLOTS)] for i, label in enumerate(labels)}


def fig_ageing(blocks: list[dict[str, Any]], primary: dict[str, str], path: Path) -> None:
    colors = _cutoff_colors(blocks)
    panels = [
        ("current", "prism_risk_frozen", "SCALES-L, risk filter (current formula)"),
        ("current", "forecast_mpc_risk_frozen", "Forecast MPC, risk filter (current formula)"),
        ("new2026", primary["new2026"], "SCALES-L zero-history, risk filter (new formula)"),
    ]
    fig, axes = plt.subplots(1, 3, figsize=(11, 3.6), sharey=True)
    fig.patch.set_facecolor(SURFACE)
    handles: dict[str, Any] = {}
    for ax, (mechanism, arm, title) in zip(axes, panels, strict=True):
        for block in blocks:
            if block["mechanism"] != mechanism:
                continue
            rows = [r for r in block["ageing"] if r["arm"] == arm and r["ratio"] is not None]
            if not rows:
                continue
            label = block["split"]["label"]
            x = [r["months_since_cutoff"] for r in rows]
            y = [100.0 * r["ratio"] for r in rows]
            (line,) = ax.plot(
                x,
                y,
                color=colors.get(label, SLOTS[1]),
                linewidth=LINE_PT,
                marker="o",
                markersize=MARKER_PT,
                markeredgecolor=SURFACE,
                markeredgewidth=1.0,
                solid_capstyle="round",
                solid_joinstyle="round",
                label=f"Frozen at {_short_date(label)}",
            )
            handles.setdefault(label, line)
        ax.axhline(100.0, color=AXIS, linewidth=0.9)
        _style(ax)
        ax.set_title(title, fontsize=9, loc="left")
        ax.set_xlabel("Months since the cutoff T", fontsize=8)
        months = [r["months_since_cutoff"] for b in blocks for r in b["ageing"] if b["mechanism"] == mechanism]
        if months:
            ax.set_xlim(min(months) - 0.5, max(months) + 0.5)
        ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True, steps=[1, 2, 5, 10]))
    axes[0].set_ylabel("Frozen / monthly-retrained P&L, %", fontsize=8)
    ordered = [handles[k] for k in sorted(handles)]
    fig.legend(
        ordered,
        [h.get_label() for h in ordered],
        loc="lower center",
        ncol=len(ordered),
        frameon=False,
        fontsize=8,
        labelcolor=INK2,
    )
    fig.suptitle(
        "Model ageing: monthly P&L of the frozen model relative to the same model retrained every month",
        fontsize=10,
        color=INK,
        x=0.01,
        ha="left",
    )
    fig.tight_layout(rect=(0, 0.08, 1, 0.94))
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


def fig_cumulative(
    blocks: list[dict[str, Any]],
    ledgers: dict[str, dict[str, pd.DataFrame]],
    primary: dict[str, str],
    wf: dict[str, dict[str, str]],
    path: Path,
) -> None:
    fig, axes = plt.subplots(1, len(blocks), figsize=(3.3 * len(blocks), 3.4))
    fig.patch.set_facecolor(SURFACE)
    names = ("Frozen at T", "Monthly retrain", "Best reactive rule")
    for ax, block in zip(np.atleast_1d(axes), blocks, strict=True):
        arm = primary[block["mechanism"]]
        series = (arm, wf[block["mechanism"]][arm], block["best_threshold"])
        for color, name, label in zip(SLOTS, series, names, strict=True):
            daily = holdout.daily_pnl_series(ledgers[block["id"]][name], TZ)
            x = pd.to_datetime(daily.index)
            ax.plot(x, daily.cumsum() / 1000.0, color=color, linewidth=LINE_PT, label=label, solid_capstyle="round")
        _style(ax)
        formula = "current formula" if block["mechanism"] == "current" else "new formula"
        ax.set_title(f"T = {_short_date(block['split']['label'])}, {formula}", fontsize=8.5, loc="left")
        ax.set_ylabel("Cumulative P&L, kEUR per MW", fontsize=8)
        ax.tick_params(axis="x", labelrotation=30)
        ax.xaxis.set_major_locator(mdates.AutoDateLocator(maxticks=5))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %y"))
    handles, labels = np.atleast_1d(axes)[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=3, frameon=False, fontsize=8, labelcolor=INK2)
    fig.suptitle(
        "Cumulative P&L after the cutoff, SCALES-L with risk filter, 1 MW / 2 MWh (oracle omitted for scale)",
        fontsize=10,
        color=INK,
        x=0.01,
        ha="left",
    )
    fig.tight_layout(rect=(0, 0.08, 1, 0.93))
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


def fig_uplift(blocks: list[dict[str, Any]], path: Path) -> None:
    rows = []
    for block in blocks:
        for arm, short in PUBLIC_ARMS[block["mechanism"]]:
            comp = block["comparisons"][arm]
            formula = "" if block["mechanism"] == "current" else " (new formula)"
            rows.append((f"{short}{formula}, frozen {_short_date(block['split']['label'])}", comp))
    rows = rows[::-1]
    fig, axes = plt.subplots(1, 2, figsize=(10, 0.42 * len(rows) + 1.3), sharey=True)
    fig.patch.set_facecolor(SURFACE)
    y = np.arange(len(rows))
    panels = (
        ("vs_best_threshold", "uplift_pct", "uplift_ci_pct", 0.0, "Frozen vs best reactive rule, %"),
        ("vs_walk_forward", None, None, 100.0, "Frozen / monthly-retrained P&L, %"),
    )
    for ax, (key, point, ci, ref, xlabel) in zip(axes, panels, strict=True):
        if point is None:
            values = np.array([100.0 * r[1][key]["ratio"] for r in rows])
            lo = np.array([100.0 * r[1][key]["ratio_ci"][0] for r in rows])
            hi = np.array([100.0 * r[1][key]["ratio_ci"][1] for r in rows])
        else:
            values = np.array([r[1][key][point] for r in rows])
            lo = np.array([r[1][key][ci][0] for r in rows])
            hi = np.array([r[1][key][ci][1] for r in rows])
        ax.axvline(ref, color=AXIS, linewidth=0.9)
        ax.hlines(y, lo, hi, color=SLOTS[0], linewidth=LINE_PT)
        ax.plot(values, y, "o", color=SLOTS[0], markersize=MARKER_PT + 1, markeredgecolor=SURFACE, markeredgewidth=1.0)
        _style(ax)
        ax.set_xlabel(xlabel, fontsize=8)
    axes[0].set_yticks(y)
    axes[0].set_yticklabels([r[0] for r in rows], fontsize=8, color=INK2)
    fig.suptitle(
        "Frozen models after the cutoff: point estimate and 95 % CI (paired days, 7-day circular block bootstrap)",
        fontsize=10,
        color=INK,
        x=0.01,
        ha="left",
    )
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


# ------------------------------------------------------------------------------------------------------------------
# markdown


def _comparison_row(block: dict[str, Any], arm: str, short: str) -> dict[str, Any]:
    summary = block["arms"][arm]
    comp = block["comparisons"][arm]
    wf, th = comp["vs_walk_forward"], comp["vs_best_threshold"]
    wf_name = block["walk_forward"][arm]
    return {
        "cutoff": block["split"]["label"],
        "model": short,
        "window": "{} to {}".format(*_test_days_local(block["split"])),
        "months": len(block["split"]["months"]),
        "frozen": summary["eur_per_mw_year"],
        "wf": block["arms"][wf_name]["eur_per_mw_year"],
        "ratio": f"{100 * wf['ratio']:.0f} % {_ci(100 * wf['ratio_ci'][0], 100 * wf['ratio_ci'][1], signed=False)}",
        "threshold": block["arms"][block["best_threshold"]]["eur_per_mw_year"],
        "uplift": f"{th['uplift_pct']:+.0f} % {_ci(th['uplift_ci_pct'][0], th['uplift_ci_pct'][1])}",
        "months_won": f"{th['months_won']}/{th['months']}",
        "capture": summary.get("capture_vs_oracle"),
        "oracle": block["arms"]["oracle_qh"]["eur_per_mw_year"],
    }


COMPARISON_COLUMNS = [
    ("cutoff", "Cutoff T (end of day)", 0),
    ("model", "Model", 0),
    ("window", "Test window", 0),
    ("months", "Months", 0),
    ("frozen", "Frozen EUR/MW/yr", 0),
    ("wf", "Monthly retrain EUR/MW/yr", 0),
    ("ratio", "Frozen / retrain [95 % CI]", 0),
    ("threshold", "Best reactive rule EUR/MW/yr", 0),
    ("uplift", "Frozen vs best rule [95 % CI]", 0),
    ("months_won", "Months frozen > rule", 0),
    ("capture", "Capture vs oracle", 3),
    ("oracle", "Oracle EUR/MW/yr", 0),
]


def _stats_rows(blocks: list[dict[str, Any]], labels: dict[str, str]) -> list[dict[str, Any]]:
    """Paired comparisons of the public frozen arms; the one-sided p-value is shown for the test against the best
    reactive rule (H0: the frozen model earns no more than the rule)."""
    rows = []
    for block in blocks:
        for arm, _ in PUBLIC_ARMS[block["mechanism"]]:
            for key in ("vs_best_threshold", "vs_walk_forward"):
                comp = block["comparisons"][arm][key]
                diff_lo, diff_hi = comp["diff_ci_eur_per_mw_year"]
                ratio_lo, ratio_hi = comp["ratio_ci"]
                rows.append(
                    {
                        "evaluation": f"{block['mechanism']}, T = {block['split']['label']}",
                        "arm": labels[arm],
                        "reference": labels.get(comp["reference"], comp["reference"]),
                        "days": comp["days"],
                        "diff": comp["diff_eur_per_mw_year"],
                        "diff_ci": _ci(diff_lo, diff_hi, unit="", signed=True),
                        "ratio": f"{comp['ratio']:.3f}",
                        "ratio_ci": _ci(ratio_lo, ratio_hi, digits=3, unit="", signed=False),
                        "p": f"{comp['p_value']:.4f}" if key == "vs_best_threshold" else "-",
                        "months_won": f"{comp['months_won']}/{comp['months']}",
                    }
                )
    return rows


def _monthly_rows(block: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    mech = block["mechanism"]
    arms = [a for a, _ in PUBLIC_ARMS[mech]]
    wf = block["walk_forward"]
    for row in block["monthly"]:
        eur = row["eur_per_mw_year"]
        out = {
            "month": row["month"] + (" (partial)" if row["partial"] else ""),
            "m": row["months_since_cutoff"],
            "best": eur.get(block["best_threshold"]),
            "oracle": eur.get("oracle_qh"),
        }
        for i, arm in enumerate(arms):
            out[f"f{i}"] = eur.get(arm)
            out[f"w{i}"] = eur.get(wf[arm])
            a, b = row["pnl_eur"].get(arm), row["pnl_eur"].get(wf[arm])
            out[f"r{i}"] = f"{100 * a / b:.0f} %" if a is not None and b else "n/a"
        out["capture"] = row["capture_vs_oracle"].get(arms[0])
        rows.append(out)
    return rows


def _weighted_skill(rows: list[dict[str, Any]], key: str, part: str = "all") -> float | None:
    """Row-weighted mean of a monthly skill metric over the first / last three months or all months."""
    ordered = sorted(rows, key=lambda r: r["months_since_cutoff"])
    sel = ordered[:3] if part == "first" else ordered[-3:] if part == "last" else ordered
    n = sum(r["n"] for r in sel if r.get(key) is not None)
    if not n:
        return None
    return float(sum(r[key] * r["n"] for r in sel if r.get(key) is not None) / n)


def _skill_table(blocks: list[dict[str, Any]]) -> str:
    pairs = {
        "current": (
            ("frozen_prism_current", "wf_prism_current", "SCALES-L"),
            ("frozen_price_current", "wf_price_current", "Forecast MPC (price model)"),
        ),
        "new2026": (("frozen_prism_zero", "wf_prism_zero_wf", "SCALES-L zero-history"),),
    }
    out = []
    for block in blocks:
        skill = block.get("skill") or {}
        for frozen, wf, label in pairs[block["mechanism"]]:
            if frozen not in skill or wf not in skill:
                continue
            row = {
                "cutoff": f"{block['split']['label']} ({block['mechanism']})",
                "model": label,
                "mae_f_first": _weighted_skill(skill[frozen], "mae_mean", "first"),
                "mae_w_first": _weighted_skill(skill[wf], "mae_mean", "first"),
                "mae_f_last": _weighted_skill(skill[frozen], "mae_mean", "last"),
                "mae_w_last": _weighted_skill(skill[wf], "mae_mean", "last"),
                "brier_f": _weighted_skill(skill[frozen], "brier"),
                "brier_w": _weighted_skill(skill[wf], "brier"),
            }
            out.append(row)
    return _table(
        out,
        [
            ("cutoff", "Cutoff (formula)", 0),
            ("model", "Model", 0),
            ("mae_f_first", "MAE first 3 months, frozen", 1),
            ("mae_w_first", "MAE first 3 months, retrain", 1),
            ("mae_f_last", "MAE last 3 months, frozen", 1),
            ("mae_w_last", "MAE last 3 months, retrain", 1),
            ("brier_f", "Brier P(SI<0), frozen", 3),
            ("brier_w", "Brier P(SI<0), retrain", 3),
        ],
    )


def _audit_table(audit: dict[str, Any], labels: dict[str, str]) -> str:
    frame = pd.DataFrame(audit["rows"])
    order = {name: i for i, name in enumerate(labels)}
    rows = []
    keys = sorted(
        {(e, a) for e, a in zip(frame["evaluation"], frame["arm"], strict=True)},
        key=lambda k: (k[0], order.get(k[1], 99)),
    )
    for evaluation, arm in keys:
        group = frame[(frame["evaluation"] == evaluation) & (frame["arm"] == arm)]
        white = group["white_box_passed"].dropna()
        rows.append(
            {
                "arm": labels.get(arm, arm),
                "evaluation": evaluation,
                "slices": ", ".join(sorted({s.split(":", 1)[1] for s in group["slice"]})),
                "audits": len(group),
                "passed": int(group["passed"].sum()),
                "white": f"{int(white.astype(bool).sum())}/{len(white)}" if len(white) else "n/a",
            }
        )
    return _table(
        rows,
        [
            ("arm", "Strategy", 0),
            ("evaluation", "Evaluation", 0),
            ("slices", "Real-data slices (start, local)", 0),
            ("audits", "Audits", 0),
            ("passed", "Passed", 0),
            ("white", "White-box passed", 0),
        ],
    )


def _training_table(logs: list[dict[str, Any]]) -> str:
    rows = []
    for log in logs:
        h0: dict[str, Any] = next((r for r in log["training_rows"] if r["horizon"] == 0), {})
        h1: dict[str, Any] = next((r for r in log["training_rows"] if r["horizon"] == 1), {})
        fold = log["fit_log"][0] if log.get("fit_log") else {}
        rows.append(
            {
                "model": log["model"],
                "cutoff": str(log["cutoff_utc"])[:16],
                "train": f"{str(fold.get('train_start', ''))[:16]} to {str(fold.get('train_end', ''))[:16]}",
                "rows": f"{h0.get('n_rows', 0):,} / {h1.get('n_rows', 0):,}",
                "last_row": str(h0.get("last_row_utc", ""))[:16],
                "last_target": str(
                    max(h0.get("last_target_published_utc", ""), h1.get("last_target_published_utc", ""))
                )[:16],
                "data_end": str(log["fit_market_end_utc"])[:16],
                "passed": bool(log["passed"]) and bool(log["fit_market_ends_by_cutoff"]),
            }
        )
    return _table(
        rows,
        [
            ("model", "Frozen model", 0),
            ("cutoff", "Cutoff T (UTC)", 0),
            ("train", "Training window (UTC)", 0),
            ("rows", "Training rows (current / next QH)", 0),
            ("last_row", "Last training decision (UTC)", 0),
            ("last_target", "Last target published (UTC)", 0),
            ("data_end", "Data loaded until (UTC)", 0),
            ("passed", "Frozen check", 0),
        ],
    )


def write_markdown(s: Any, oos: ModuleType, summary: dict[str, Any], written: list[str]) -> Path:  # noqa: C901
    blocks = summary["evaluations"]
    labels = summary["labels"]
    current = [b for b in blocks if b["mechanism"] == "current"]
    nf = [b for b in blocks if b["mechanism"] == "new2026"]
    audit = summary["lookahead_audit"]
    battery = summary["battery"]
    lines: list[str] = []
    add = lines.append
    add("# Out-of-sample evidence: models frozen at a cutoff keep earning")
    add("")
    add("> Generated by `scripts/research/out_of_sample.py` from `data/public` (Elia Open Data, CC BY 4.0,")
    add('> attribution "Elia Transmission Belgium SA"; day-ahead prices from Energy-Charts, CC BY 4.0). Every number')
    add("> below is produced by that script; do not edit by hand. Simulated money, not trading advice.")
    add("")
    add("## Headline")
    add("")
    for sentence in summary["headline"]:
        add(f"- {sentence}")
    add("")
    mpc = [b["arms"]["forecast_mpc_risk_frozen"] for b in current]
    mpc_ratio = [b["comparisons"]["forecast_mpc_risk_frozen"]["vs_walk_forward"]["ratio"] for b in current]
    add(
        "- The second strategy of the stack, forecast MPC with the risk filter, frozen at the same cutoffs earns "
        + ", ".join(
            f"{a['eur_per_mw_year']:,.0f} EUR/MW/yr ({100 * r:.0f} % of its monthly-retrained version, T = {b['split']['label']})"
            for a, r, b in zip(mpc, mpc_ratio, current, strict=True)
        )
        + "."
    )
    add(
        f"- Every tradable arm passed every look-ahead audit on real Elia data ({audit['tradable_passed']} of "
        f"{audit['tradable_audits']} audits over {len(audit['tradable_arms_audited'])} strategy/cutoff combinations; "
        f"white-box plan comparison {audit['white_box_passed']} of {audit['white_box_audits']}); the "
        f"perfect-foresight oracle, the positive control, failed {audit['oracle_failed']} of {audit['oracle_audits']}."
    )
    add("")
    add(
        f"SCALES-L with the risk filter is the primary arm, fixed before the run. Battery {battery['power_mw']:.0f} MW / "
        f"{battery['energy_mwh']:.0f} MWh, efficiencies {battery['charge_efficiency']:.2f} / "
        f"{battery['discharge_efficiency']:.2f}, SOC band 5-95 %, degradation "
        f"{battery['degradation_eur_per_mwh']:.0f} EUR per MWh discharged; minute data usable "
        f"{summary['publication']['minute_lag']} minute after the minute ends, final quarter-hour prices "
        f"{summary['publication']['qh_final_lag_minutes']} minutes after the quarter-hour; price taker; settlement at "
        "Elia's published imbalance price (current formula: ODS134; new formula: Elia's ODS212 simulation)."
    )
    add("")
    add("![Model ageing](figures/oos_ageing.png)")
    add("")
    add("## Results")
    add("")
    add("### Current formula: frozen vs monthly-retrained vs reactive rules")
    add("")
    rows = [_comparison_row(b, arm, short) for b in current for arm, short in PUBLIC_ARMS["current"]]
    add(_table(rows, COMPARISON_COLUMNS))
    add("")
    add(
        "EUR/MW/yr annualises the net P&L (revenue minus degradation) over the test window. *Monthly retrain* is the same "
        "model specification refitted at every month start on the previous six months (the walk-forward arms of "
        "`reports/realtime_belgium.md`, re-run on exactly the same days). *Best reactive rule* is the better of the two "
        "threshold rules (running price, running system imbalance) on that window, chosen after the fact. Brackets are "
        "95 % circular block-bootstrap intervals (7-day blocks) of paired daily P&L."
    )
    add("")
    add("![Cumulative P&L](figures/oos_cumulative.png)")
    add("")
    add("### New formula: zero-history SCALES-L frozen at 1 January 2026")
    add("")
    for b in nf:
        rows = [_comparison_row(b, arm, short) for arm, short in PUBLIC_ARMS["new2026"]]
        add(_table(rows, COMPARISON_COLUMNS))
        add("")
        add(
            "The zero-history model is trained on the six months before the cutoff on current-design data only: its "
            "training targets are new-formula prices recomputed from the current-design state (ODS166 product prices, "
            "ODS127 volumes), and no new-formula price (ODS211/ODS212) is loaded while it is fitted. It is then settled "
            "at Elia's published new-formula price for "
            f"{_short_date(_test_days_local(b['split'])[0])} to {_short_date(_test_days_local(b['split'])[1])}. The "
            "live-running-price variant also reads the running new-formula price minute by minute at decision time "
            "(precision-weighted fusion whose error profile was estimated on current-formula data before the cutoff). "
            "The best reactive rule here acts on that same live running new-formula price, an input the plain "
            "zero-history model does not use."
            + (
                f" Elia's simulated series starts at 00:00 UTC on 1 January 2026, so the first "
                f"{b['arms']['oracle_qh']['price_missing_qh']} quarter-hours of the local day have no price and earn "
                "nothing for every arm."
                if b["arms"]["oracle_qh"].get("price_missing_qh")
                else ""
            )
        )
        add("")
    add("![Paired comparison](figures/oos_uplift_ci.png)")
    add("")
    add("### Model ageing")
    add("")
    add(
        "Monthly P&L of each frozen model divided by the P&L of the same model retrained every month, pooled over the "
        "three cutoffs by number of months since the cutoff (sum of frozen P&L / sum of retrained P&L in the bucket):"
    )
    add("")
    pooled_rows = []
    for key, buckets in summary["pooled_ageing"].items():
        arm = key.split("/")[0]
        for row in buckets:
            pooled_rows.append(
                {
                    "model": labels[arm].split(" - ")[0],
                    "bucket": row["bucket"],
                    "months": row["months"],
                    "cutoffs": row["cutoffs"],
                    "ratio": None if row["ratio"] is None else f"{100 * row['ratio']:.0f} %",
                }
            )
    add(
        _table(
            pooled_rows,
            [
                ("model", "Model", 0),
                ("bucket", "Months since T", 0),
                ("months", "Cutoff-months pooled", 0),
                ("cutoffs", "Cutoffs", 0),
                ("ratio", "Frozen / retrain", 0),
            ],
        )
    )
    add("")
    add(
        "Forecast skill of the frozen and the monthly-retrained forecasters (current quarter-hour, all minutes; MAE "
        "of the mean forecast in EUR/MWh against the published final price, Brier score of P(SI < 0)):"
    )
    add("")
    add(_skill_table(blocks))
    add("")
    longest = max(current, key=lambda b: len(b["split"]["months"]))
    skill = longest.get("skill") or {}
    if "frozen_prism_current" in skill and "wf_prism_current" in skill:
        ratios = [100 * r["ratio"] for r in longest["ageing"] if r["arm"] == oos.PRIMARY["current"] and r["ratio"]]
        add(
            f"The frozen forecasters do age: after the {_long_date(longest['split']['label'])} cutoff the error of "
            "SCALES-L's mean forecast goes from "
            f"{_weighted_skill(skill['frozen_prism_current'], 'mae_mean', 'first'):.1f} EUR/MWh in the first three "
            f"months to {_weighted_skill(skill['frozen_prism_current'], 'mae_mean', 'last'):.1f} EUR/MWh in the last "
            f"three (monthly retrain: {_weighted_skill(skill['wf_prism_current'], 'mae_mean', 'first'):.1f} and "
            f"{_weighted_skill(skill['wf_prism_current'], 'mae_mean', 'last'):.1f}), while its monthly P&L stays "
            f"between {min(ratios):.0f} % and {max(ratios):.0f} % of the retrained model's."
        )
        add("")
    add("### Month by month")
    add("")
    for b in blocks:
        mech = b["mechanism"]
        arms = PUBLIC_ARMS[mech]
        formula = "current formula" if mech == "current" else "new formula"
        add(f"**Frozen at {_long_date(b['split']['label'])}, {formula}** (EUR/MW/yr per month):")
        add("")
        cols = [("month", "Month", 0), ("m", "Months since T", 0)]
        for i, (_, short) in enumerate(arms):
            cols += [
                (f"f{i}", f"{short} frozen", 0),
                (f"w{i}", f"{short} retrain", 0),
                (f"r{i}", "Frozen / retrain", 0),
            ]
        cols += [
            ("best", "Best reactive rule", 0),
            ("oracle", "Oracle", 0),
            ("capture", f"{arms[0][1]} frozen capture", 3),
        ]
        add(_table(_monthly_rows(b), cols))
        add("")
        won = b["comparisons"][arms[0][0]]["vs_best_threshold"]
        rule = RULE_PHRASES.get(b["best_threshold"], b["best_threshold"])
        add(
            f"{arms[0][1]} frozen beats the best reactive rule, {rule}, in {won['months_won']} of "
            f"{won['months']} months."
        )
        add("")
    add("## Statistics")
    add("")
    add(
        "For each arm the net P&L is summed per local delivery day (Europe/Brussels). The frozen arm and its reference "
        "trade the same days, so the comparison is paired: the statistic is the mean daily difference (annualised per "
        "MW) and the ratio of total P&L. Days are serially dependent, so confidence intervals come from a circular "
        f"block bootstrap (Politis and Romano, 1992) with {summary['settings']['block_days']}-day blocks and "
        f"{summary['settings']['n_boot']:,} replicates (fixed seed), resampling both series jointly; intervals are "
        "percentile intervals at 95 %. The p-value is one-sided for a positive mean difference, from replicates "
        "re-centred on the null."
    )
    add("")
    add(
        _table(
            _stats_rows(blocks, labels),
            [
                ("evaluation", "Evaluation", 0),
                ("arm", "Arm", 0),
                ("reference", "Reference", 0),
                ("days", "Days", 0),
                ("diff", "Mean difference EUR/MW/yr", 0),
                ("diff_ci", "95 % CI", 0),
                ("ratio", "P&L ratio", 0),
                ("ratio_ci", "95 % CI", 0),
                ("p", "p (one-sided)", 0),
                ("months_won", "Months won", 0),
            ],
        )
    )
    add("")
    add("## No look-ahead")
    add("")
    add(
        "**Frozen fits.** Each frozen forecaster is fitted on a market that ends at the cutoff - no minute or "
        "quarter-hour at or after T is loaded - with the walk-forward embargo: a training row is used only if its target "
        "(the final quarter-hour value, published 5 minutes after the quarter-hour) was published by T. The fit records "
        "the last training decision and the last target publication time:"
    )
    add("")
    add(_training_table(summary["frozen_training_audit"]))
    add("")
    add(
        "Unit tests (`tests/test_oos_holdout.py`) check on synthetic and real fixture data that the fitted model is "
        "bit-identical when every value not yet published at T is replaced by noise. Loading nothing stamped at or "
        "after T is stricter than the publication model requires: it also withholds day-ahead prices for delivery "
        "after T that were published before T (they would reach the features of the last training hour)."
    )
    add("")
    add(
        "**Trading decisions.** `check_no_lookahead` replaces every value not yet published at a cut minute (minute "
        "rows, quarter-hour values, settlement prices, unpublished day-ahead prices) by noise and re-runs the strategy; "
        "any change of a decision before the earliest legitimate use of that data proves look-ahead. Forecast "
        "strategies are also audited white-box (their precomputed plan must be identical before the cut). Cuts "
        f"{', '.join(str(c) for c in audit['cuts'])} minutes from the start of each {oos.AUDIT_DAYS}-day real-data "
        "slice; every frozen arm is audited on a slice in the first week after its cutoff and on a late slice."
    )
    add("")
    add(_audit_table(audit, labels))
    add("")
    add("## Method")
    add("")
    settings = summary["settings"]
    add(
        f"- **Cutoffs.** T = end of {', '.join(_long_date(c) for c in settings['cutoffs'])} (local time, "
        f"Europe/Brussels); test window (T, end of {_long_date(settings['end_day'])}] for the current formula. New "
        f"formula: T = end of {_long_date(settings['new_formula_cutoff'])}, test window to the end of "
        f"{_long_date(settings['new_formula_end_day'])} (the period for which Elia published the simulated new price)."
    )
    add(
        f"- **Training.** The forecasters of `realtime_backtest.py` with their fixed a-priori hyper-parameters "
        f"(scikit-learn histogram gradient boosting: quantiles 10/25/50/75/90 %, mean and P(SI < 0) heads for the "
        f"current and the next quarter-hour), fitted once on the {settings['train_months']} local calendar months "
        "before T (every third minute) - the same window length as the walk-forward folds, so in the first month after "
        "T the frozen and the retrained model are trained on almost the same data and the comparison isolates ageing."
    )
    add(
        "- **Frozen** means that no learned parameter changes after T: the gradient-boosting models, the MPC settings and "
        "the fusion error profile are fixed at T. What a live system recomputes from data published before each "
        "decision keeps updating, as it would in operation: the model inputs (running quarter-hour values, recent final "
        "prices, Elia's SI forecasts, day-ahead prices), the MPC's 28-day same-time-of-day price profile for the 4-hour "
        "look-ahead and its 7-day trailing terminal price, and the threshold rules' 14-day bands."
    )
    add(
        "- **Strategies.** Forecast MPC and SCALES-L as in `reports/realtime_belgium.md`: rolling stochastic MPC on the "
        "forecast distributions, re-planned at minutes 0, 5 and 10 of each quarter-hour, risk filter = discharge on the "
        "30 % quantile and charge on the 70 % quantile; SCALES-L forecasts P(SI < 0) and the regime components and "
        "prices them with the settlement operator. Threshold rules act on the running quarter-hour price (bands from "
        "the quantiles of the last 14 days of published prices) or on the running system imbalance (+/-150 MW)."
    )
    add(
        f"- **Simulation.** `realtime.simulate`, minute decisions, quarter-hour settlement; every arm starts "
        f"{settings['warmup_days']} days before T (the frozen models idle until T because they have no forecast "
        "before their test window), ledgers are trimmed to the test window, and the oracle is the perfect-foresight "
        "LP on final prices (upper bound, not tradable)."
    )
    add(
        "- **Walk-forward models** are the cached models of `scripts/research/realtime_backtest.py` (fold windows in "
        "`reports/ledgers/realtime_forecaster_folds.json`; refitted by this script with the same specification when "
        "the cache is absent)."
    )
    add("")
    add("## Scope")
    add("")
    add(
        "Simulated money on public data, not trading results: a single 1 MW battery as a price taker (no own price "
        "impact, see `reports/impact.md` for fleets), full delivery of every minute-level power change, no outages, no "
        "intraday or reserve revenues. The new-formula price is Elia's simulation of the 2026 design on historical days "
        "(market parties were reacting to the current formula)."
    )
    add("")
    add("## Reproduce")
    add("")
    add("```bash")
    add("python scripts/research/out_of_sample.py                    # train, simulate, audit, report")
    add("python scripts/research/out_of_sample.py --stages report    # rebuild this report from cached runs")
    add("```")
    add("")
    runtime = summary["runtime_seconds"]
    total = sum(v for k, v in runtime.items() if k in ("train", "simulate", "audit"))
    add(
        f"Runtime of the last full run: train {runtime.get('train', 0) / 60:.1f} min, simulate "
        f"{runtime.get('simulate', 0) / 60:.1f} min, audit {runtime.get('audit', 0) / 60:.1f} min "
        f"(total {total / 60:.0f} min on 3 worker processes). Outputs: this report, `reports/figures/oos_*.png`, the "
        "quarter-hour ledgers of the frozen arms `reports/ledgers/oos_<arm>_<formula>_<cutoff>.parquet` (+ JSON "
        "sidecars), daily P&L of every arm `reports/ledgers/oos_daily_pnl.parquet`, and the machine-readable summary "
        "`reports/ledgers/oos_summary.json` (every number above, the audits and the fold windows). How to run: "
        "`docs/out_of_sample.md`."
    )
    add("")
    path = s.out / "out_of_sample.md"
    path.write_text("\n".join(lines))
    return path


def write_outputs(s: Any, timings: dict[str, float], oos: ModuleType) -> None:
    if s.ablation:
        write_ablation(s, oos)
    summary, ledgers = build_summary(s, oos, timings)
    written = export_ledgers(s, oos, summary, ledgers)
    s.figures.mkdir(parents=True, exist_ok=True)
    blocks = summary["evaluations"]
    fig_ageing(blocks, oos.PRIMARY, s.figures / "oos_ageing.png")
    fig_cumulative(blocks, ledgers, oos.PRIMARY, oos.WALK_FORWARD, s.figures / "oos_cumulative.png")
    fig_uplift(blocks, s.figures / "oos_uplift_ci.png")
    summary["ledgers_written"] = written
    (s.ledgers / "oos_summary.json").write_text(json.dumps(summary, indent=1))
    path = write_markdown(s, oos, summary, written)
    print(f"[report] {path} ({len(written)} ledgers)", flush=True)
