"""Deployment artefacts for live (paper) trading: where fitted forecasters live and how to load them.

``scripts/research/realtime_backtest.py --stages deploy`` fits every forecaster once more on the latest six months
of ``data/public`` with a single fold whose test window starts at the last available day and extends
``DEPLOY_HORIZON_DAYS`` into the future, so the live window is covered. The fitted objects are pickled into the
models directory (``$EUROFLEX_MODELS_DIR``, default ``~/.cache/euroflex_bess_lab/realtime_backtest/models``);
pickles are only ever read from that local directory, which the user's own script wrote. The small RL policy is
also committed as ``reports/ledgers/realtime_rl_policy.npz`` so the RL baseline can be deployed without the cache.
"""

from __future__ import annotations

import json
import os
import pickle
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from .splits import Fold

MODELS_ENV = "EUROFLEX_MODELS_DIR"
DEPLOY_HORIZON_DAYS = 400
DEPLOY_TRAIN_MONTHS = 6
DEPLOY_FILES: dict[str, str] = {
    "price_current": "deploy_price_current.pkl",
    "prism_current": "deploy_prism_current.pkl",
    "prism_new2026": "deploy_prism_zero.pkl",
}
FUSION_FILE = "deploy_fusion_variance.json"
RL_POLICY_FILE = "rl_policy.npz"
COMMITTED_RL_POLICY = Path("reports") / "ledgers" / "realtime_rl_policy.npz"
BUILD_HINT = "python scripts/research/realtime_backtest.py --stages deploy"


class DeploymentModelsMissing(FileNotFoundError, ValueError):
    """Deployment artefacts are absent. A ``ValueError`` too, so the live CLI reports it as a usage error."""


def models_dir(path: str | Path | None = None) -> Path:
    if path is not None:
        return Path(path).expanduser()
    env = os.environ.get(MODELS_ENV)
    if env:
        return Path(env).expanduser()
    return Path.home() / ".cache" / "euroflex_bess_lab" / "realtime_backtest" / "models"


def deployment_fold(data_end: str | pd.Timestamp, *, data_start: str | pd.Timestamp | None = None) -> Fold:
    """Train on the ``DEPLOY_TRAIN_MONTHS`` before ``data_end``; test window ``[data_end, data_end + horizon)``."""
    end = pd.Timestamp(data_end)
    end = end.tz_localize("UTC") if end.tzinfo is None else end.tz_convert("UTC")
    start = end - pd.DateOffset(months=DEPLOY_TRAIN_MONTHS)
    if data_start is not None:
        first = pd.Timestamp(data_start)
        first = first.tz_localize("UTC") if first.tzinfo is None else first.tz_convert("UTC")
        start = max(start, first)
    return Fold(pd.Timestamp(start), end, end, end + pd.Timedelta(days=DEPLOY_HORIZON_DAYS))


def load_forecaster(key: str, directory: str | Path | None = None) -> Any:
    """Load a deployment forecaster (``price_current``, ``prism_current`` or ``prism_new2026``)."""
    if key not in DEPLOY_FILES:
        raise KeyError(f"unknown deployment forecaster `{key}`; known: {sorted(DEPLOY_FILES)}")
    path = models_dir(directory) / DEPLOY_FILES[key]
    if not path.exists():
        raise DeploymentModelsMissing(
            f"deployment model {path.name} not found in {path.parent} (set ${MODELS_ENV}); build it with `{BUILD_HINT}`"
        )
    with open(path, "rb") as fh:
        return pickle.load(fh)


def load_fusion_variance(directory: str | Path | None = None) -> NDArray[np.float64]:
    path = models_dir(directory) / FUSION_FILE
    if not path.exists():
        raise DeploymentModelsMissing(f"{FUSION_FILE} not found in {path.parent}; build it with `{BUILD_HINT}`")
    return np.asarray(json.loads(path.read_text(encoding="utf-8"))["variance"], dtype=float)


def rl_policy_path(directory: str | Path | None = None) -> Path:
    """The deployment RL policy: models directory first, then the committed copy under ``reports/ledgers``."""
    candidate = models_dir(directory) / RL_POLICY_FILE
    if candidate.exists():
        return candidate
    for parent in (Path.cwd(), *Path(__file__).resolve().parents):
        committed = parent / COMMITTED_RL_POLICY
        if committed.exists():
            return committed
    raise DeploymentModelsMissing(f"no RL policy found; build it with `{BUILD_HINT}`")
