#!/usr/bin/env python
"""Health check of a live paper-trading run: reads the engine heartbeat ``<run-dir>/status.json``.

The live engine (``euroflex live run``) rewrites ``status.json`` atomically after every decision minute. This check
needs only the standard library, so it runs in any image and from any probe (Docker ``HEALTHCHECK``, Kubernetes
exec probes, ECS container health checks, systemd timers, cron).

Exit codes (container runtimes treat every non-zero code as unhealthy):

- ``0`` healthy: heartbeat younger than ``--max-age-s``, state ``running`` or ``draining``, paper mode;
- ``1`` unhealthy: stale heartbeat, the engine has stopped (``completed`` / ``stopped`` / ``interrupted``), or a
  ``--min-decisions`` / ``--max-strategy-errors`` limit is not met;
- ``3`` not started: no ``status.json`` yet (the engine backfills history first; give probes a start period);
- ``4`` invalid: unreadable or foreign ``status.json``, or safety flags other than paper-only
  (``"mode": "paper"``, ``"market_submission": false``).

Usage::

    python scripts/ops/healthcheck.py --run-dir artifacts/live/<run>        # or set EUROFLEX_RUN_DIR
    python scripts/ops/healthcheck.py --min-decisions 1 --json              # readiness, machine-readable
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

HEALTHY = 0
UNHEALTHY = 1
NOT_STARTED = 3
INVALID = 4
STATUS_FILE = "status.json"
STATUS_SCHEMA_PREFIX = "euroflex-live-status/"
HEALTHY_STATES = frozenset({"running", "draining"})
DEFAULT_MAX_AGE_S = 180.0
RUN_DIR_ENV = "EUROFLEX_RUN_DIR"


def parse_utc(value: Any) -> datetime | None:
    """ISO-8601 timestamp (``...Z`` or with offset) as an aware UTC ``datetime``; ``None`` if unparseable."""
    if not isinstance(value, str) or not value:
        return None
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=UTC)
    return stamp.astimezone(UTC)


def evaluate(
    status: Mapping[str, Any] | None,
    *,
    now: datetime,
    max_age_s: float = DEFAULT_MAX_AGE_S,
    min_decisions: int = 0,
    max_strategy_errors: int | None = None,
    healthy_states: frozenset[str] = HEALTHY_STATES,
) -> tuple[int, dict[str, Any]]:
    """Return ``(exit code, report)`` for a parsed ``status.json`` (``None`` = file absent)."""
    if status is None:
        return NOT_STARTED, {"healthy": False, "reason": f"no {STATUS_FILE} yet (engine starting or not running)"}
    schema = str(status.get("schema", ""))
    if not schema.startswith(STATUS_SCHEMA_PREFIX):
        return INVALID, {"healthy": False, "reason": f"not a live engine status (schema {schema!r})"}
    if status.get("mode") != "paper" or status.get("market_submission") is not False:
        return INVALID, {
            "healthy": False,
            "reason": "safety flags are not paper-only (expected mode=paper, market_submission=false)",
        }
    heartbeat = parse_utc(status.get("heartbeat_utc"))
    if heartbeat is None:
        return INVALID, {"healthy": False, "reason": "heartbeat_utc missing or unparseable"}
    raw_counters, raw_ledger = status.get("counters"), status.get("ledger")
    counters: Mapping[str, Any] = raw_counters if isinstance(raw_counters, Mapping) else {}
    ledger: Mapping[str, Any] = raw_ledger if isinstance(raw_ledger, Mapping) else {}
    age_s = (now - heartbeat).total_seconds()
    state = str(status.get("state", ""))
    report: dict[str, Any] = {
        "run_id": status.get("run_id"),
        "strategy": status.get("strategy"),
        "state": state,
        "mode": "paper",
        "heartbeat_utc": status.get("heartbeat_utc"),
        "heartbeat_age_s": round(age_s, 1),
        "last_minute_utc": status.get("last_minute_utc"),
        "mechanism_now": status.get("mechanism_now"),
        "decisions": int(counters.get("decisions", 0) or 0),
        "missed": int(counters.get("missed", 0) or 0),
        "strategy_errors": int(counters.get("strategy_errors", 0) or 0),
        "settled_qh": int(ledger.get("settled_qh", 0) or 0),
        "paper_pnl_eur": ledger.get("pnl_eur"),
    }
    problems: list[str] = []
    if state not in healthy_states:
        problems.append(f"engine state is {state!r} (healthy: {', '.join(sorted(healthy_states))})")
    if age_s > max_age_s:
        problems.append(f"heartbeat is {age_s:.0f} s old (limit {max_age_s:.0f} s)")
    if age_s < -max_age_s:
        problems.append(f"heartbeat is {-age_s:.0f} s in the future (clock skew?)")
    if report["decisions"] < min_decisions:
        problems.append(f"{report['decisions']} decisions so far (need {min_decisions})")
    if max_strategy_errors is not None and report["strategy_errors"] > max_strategy_errors:
        problems.append(f"{report['strategy_errors']} strategy errors (limit {max_strategy_errors})")
    report["healthy"] = not problems
    report["reason"] = "; ".join(problems) if problems else "ok"
    return (UNHEALTHY if problems else HEALTHY), report


def read_status(run_dir: Path) -> tuple[Mapping[str, Any] | None, str | None]:
    """``(status, error)``: ``(None, None)`` when absent, ``(None, message)`` when unreadable."""
    path = run_dir / STATUS_FILE
    if not path.exists():
        return None, None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return None, f"{STATUS_FILE} unreadable: {type(exc).__name__}"
    if not isinstance(payload, dict):
        return None, f"{STATUS_FILE} is not a JSON object"
    return payload, None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-dir", type=Path, default=None, help=f"live run directory (default ${RUN_DIR_ENV})")
    parser.add_argument("--max-age-s", type=float, default=float(os.environ.get("EUROFLEX_HEALTH_MAX_AGE_S", 180)))
    parser.add_argument("--min-decisions", type=int, default=0, help="readiness: decisions taken so far")
    parser.add_argument("--max-strategy-errors", type=int, default=None)
    parser.add_argument("--now", default=None, help="evaluate at this UTC instant (tests, post-mortems)")
    parser.add_argument("--json", action="store_true", help="print the report as JSON")
    parser.add_argument("--quiet", action="store_true", help="print nothing; exit code only")
    args = parser.parse_args(argv)
    run_dir = args.run_dir or (Path(os.environ[RUN_DIR_ENV]) if os.environ.get(RUN_DIR_ENV) else None)
    if run_dir is None:
        print(f"no run directory: pass --run-dir or set {RUN_DIR_ENV}", file=sys.stderr)
        return INVALID
    now = parse_utc(args.now) if args.now else datetime.now(UTC)
    if now is None:
        print("--now is not an ISO-8601 timestamp", file=sys.stderr)
        return INVALID
    status, error = read_status(run_dir)
    if error is not None:
        code, report = INVALID, {"healthy": False, "reason": error}
    else:
        code, report = evaluate(
            status,
            now=now,
            max_age_s=args.max_age_s,
            min_decisions=args.min_decisions,
            max_strategy_errors=args.max_strategy_errors,
        )
    report["exit_code"] = code
    if args.json:
        print(json.dumps(report, sort_keys=True))
    elif not args.quiet:
        label = "healthy" if code == HEALTHY else "unhealthy"
        detail = f"{report.get('strategy')} {report.get('state')} age {report.get('heartbeat_age_s')} s"
        print(f"{label}: {report['reason']}" + (f" ({detail})" if "heartbeat_age_s" in report else ""))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
