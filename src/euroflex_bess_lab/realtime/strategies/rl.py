"""Deep Q-learning baseline for minute-level implicit balancing (discretised power).

In the spirit of the reinforcement-learning literature on Belgian imbalance arbitrage: the agent observes causal
market features (running quarter-hour estimates, instantaneous SI and activations, Elia's SI forecast, published
prices, calendar) plus its own state of charge and the energy already delivered in the quarter-hour, and picks one
of a few power levels every minute. Training uses the realised final quarter-hour price as the (delayed) reward on
historical days; at test time the policy only sees the decision context, so it is audited like every strategy.

Implementation: double DQN with a target network, Huber loss, epsilon-greedy exploration, a vectorised
environment (many days stepped in parallel) and a replay buffer of (row, SOC, action) tuples whose features are
gathered on the device. torch is an optional extra (``pip install 'euroflex-bess-lab[rl]'``) imported lazily; the
trained policy is exported to numpy so inference needs no torch. Training stops at a wall-clock cap.
"""

from __future__ import annotations

import json
import math
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from ..forecasting.features import CURRENT_GROUPS, FeatureConfig, build_features
from ..replay import DecisionContext, MarketData
from ..types import MINUTE_HOURS, MINUTES_PER_QH, BatteryParams, PublicationModel
from .base import BaseStrategy

FloatArray = NDArray[np.float64]
F32 = NDArray[np.float32]

#: Market features observed by the agent (a compact subset of :func:`build_features` output).
RL_FEATURES: tuple[str, ...] = (
    "minute_in_qh",
    "n_visible_minutes",
    "hour_sin",
    "hour_cos",
    "si_cum",
    "si_inst",
    "si_inst_d3",
    "si_projected",
    "si_prev_qh",
    "afrr_up_inst",
    "afrr_down_inst",
    "mfrr_up_inst",
    "mfrr_down_inst",
    "cur_price_cum",
    "cur_price_prev_qh",
    "cur_price_projected",
    "cur_up_cum",
    "cur_down_cum",
    "elia_si_fc",
    "elia_p_short",
    "elia_next_si_fc",
    "da_cur",
    "hist_price_last",
    "hist_price_mean4",
    "hist_price_mean96",
    "hist_price_std16",
)
#: Price-like features are expressed relative to the trailing one-day mean published price.
_RELATIVE_PRICE_FEATURES = {
    "cur_price_cum",
    "cur_price_prev_qh",
    "cur_price_projected",
    "cur_up_cum",
    "cur_down_cum",
    "da_cur",
    "hist_price_last",
    "hist_price_mean4",
}


def require_torch() -> Any:
    try:
        import torch
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise ImportError(
            "the RL baseline needs torch; install the optional extra: pip install 'euroflex-bess-lab[rl]'"
        ) from exc
    return torch


def select_device(preference: str = "auto") -> str:
    torch = require_torch()
    if preference != "auto":
        return preference
    if torch.backends.mps.is_available():
        return "mps"
    if torch.cuda.is_available():
        return "cuda"
    return "cpu"


@dataclass(frozen=True)
class DQNConfig:
    """Hyper-parameters (fixed a priori, not tuned on any test window)."""

    actions: tuple[float, ...] = (-1.0, -0.5, 0.0, 0.5, 1.0)
    hidden: tuple[int, ...] = (128, 128)
    gamma: float = 0.997
    lr: float = 5e-4
    batch_size: int = 1024
    n_envs: int = 256
    buffer_size: int = 1_000_000
    learning_starts: int = 20_000
    updates_per_step: int = 1
    target_update_every: int = 2_000
    eps_start: float = 1.0
    eps_end: float = 0.05
    eps_decay_updates: int = 100_000
    reward_scale: float = 0.05
    max_minutes: float = 25.0
    max_updates: int = 400_000
    validate_every: int = 10_000
    seed: int = 0
    device: str = "auto"
    feature_groups: tuple[str, ...] = CURRENT_GROUPS
    features: tuple[str, ...] = RL_FEATURES


@dataclass
class FeatureScaler:
    center: F32
    scale: F32

    @classmethod
    def fit(cls, x: F32) -> FeatureScaler:
        center = np.nanmedian(x, axis=0)
        q75, q25 = np.nanpercentile(x, [75, 25], axis=0)
        scale = np.where(np.isfinite(q75 - q25) & ((q75 - q25) > 1e-6), q75 - q25, 1.0)
        return cls(np.nan_to_num(center).astype(np.float32), scale.astype(np.float32))

    def transform(self, x: F32) -> F32:
        z = (x - self.center) / self.scale
        return np.clip(np.nan_to_num(z, nan=0.0, posinf=5.0, neginf=-5.0), -5.0, 5.0).astype(np.float32)


def market_observations(
    market: MarketData,
    publication: PublicationModel,
    config: DQNConfig,
    scaler: FeatureScaler | None = None,
) -> tuple[F32, FeatureScaler]:
    """Causal market part of the observation for every decision minute (scaled)."""
    matrix = build_features(market, publication, FeatureConfig(groups=config.feature_groups))
    raw = np.column_stack(
        [
            matrix.column(name) if name in matrix.names else np.full(market.n_minutes, np.nan, dtype=np.float32)
            for name in config.features
        ]
    ).astype(np.float32)
    if "hist_price_mean96" in config.features:
        ref = raw[:, config.features.index("hist_price_mean96")]
        for j, name in enumerate(config.features):
            if name in _RELATIVE_PRICE_FEATURES:
                raw[:, j] = raw[:, j] - np.nan_to_num(ref)
    scaler = scaler or FeatureScaler.fit(raw)
    return scaler.transform(raw), scaler


@dataclass
class DQNPolicy:
    """Greedy policy exported to numpy (no torch needed at inference)."""

    weights: list[F32]
    biases: list[F32]
    scaler: FeatureScaler
    config: DQNConfig
    log: dict[str, Any] = field(default_factory=dict)

    def q_values(self, obs: F32) -> F32:
        h = obs
        for i, (w, b) in enumerate(zip(self.weights, self.biases, strict=True)):
            h = h @ w + b
            if i < len(self.weights) - 1:
                h = np.maximum(h, 0.0)
        return h

    def save(self, path: str | Path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        arrays: dict[str, Any] = {"center": self.scaler.center, "scale": self.scaler.scale}
        arrays.update({f"w{i}": w for i, w in enumerate(self.weights)})
        arrays.update({f"b{i}": b for i, b in enumerate(self.biases)})
        np.savez(path, **arrays)
        meta = {"config": asdict(self.config), "log": self.log, "layers": len(self.weights)}
        path.with_suffix(".json").write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")
        return path

    @classmethod
    def load(cls, path: str | Path) -> DQNPolicy:
        path = Path(path)
        data = np.load(path)
        meta = json.loads(path.with_suffix(".json").read_text(encoding="utf-8"))
        cfg = meta["config"]
        for key in ("actions", "hidden", "feature_groups", "features"):
            cfg[key] = tuple(cfg[key])
        n = int(meta["layers"])
        return cls(
            weights=[data[f"w{i}"] for i in range(n)],
            biases=[data[f"b{i}"] for i in range(n)],
            scaler=FeatureScaler(data["center"], data["scale"]),
            config=DQNConfig(**cfg),
            log=meta.get("log", {}),
        )


def _step_battery(soc: FloatArray, request_mw: FloatArray, battery: BatteryParams) -> tuple[FloatArray, FloatArray]:
    """Vectorised version of the simulator's per-minute clipping and SOC update."""
    p_max = battery.max_power_mw
    p = np.clip(request_mw, -p_max, p_max)
    room_d = np.maximum((soc - battery.soc_min_mwh) * battery.discharge_efficiency / MINUTE_HOURS, 0.0)
    room_c = np.maximum((battery.soc_max_mwh - soc) / (battery.charge_efficiency * MINUTE_HOURS), 0.0)
    p = np.where(p > 0.0, np.minimum(p, room_d), np.maximum(p, -room_c))
    new_soc = np.where(
        p > 0.0,
        soc - p * MINUTE_HOURS / battery.discharge_efficiency,
        soc - p * MINUTE_HOURS * battery.charge_efficiency,
    )
    return p, np.clip(new_soc, battery.soc_min_mwh, battery.soc_max_mwh)


def _dynamic_state(soc: FloatArray, qh_energy: FloatArray, battery: BatteryParams) -> F32:
    soc_frac = (soc - battery.soc_min_mwh) / battery.usable_energy_mwh
    energy = qh_energy / (battery.max_power_mw * 0.25)
    return np.column_stack([soc_frac * 2.0 - 1.0, energy]).astype(np.float32)


def _minute_rows(market: MarketData, start: str | pd.Timestamp, end: str | pd.Timestamp) -> tuple[int, int]:
    s = pd.Timestamp(start)
    e = pd.Timestamp(end)
    s = s.tz_localize("UTC") if s.tzinfo is None else s.tz_convert("UTC")
    e = e.tz_localize("UTC") if e.tzinfo is None else e.tz_convert("UTC")
    m0 = max(0, int((s - market.start_utc) / pd.Timedelta(minutes=1)))
    m1 = min(market.n_minutes, int((e - market.start_utc) / pd.Timedelta(minutes=1)))
    return m0, m1


def evaluate_policy_numpy(
    policy: DQNPolicy,
    obs_market: F32,
    prices_qh: FloatArray,
    day_starts: NDArray[np.int64],
    battery: BatteryParams,
) -> float:
    """Mean daily P&L (EUR) of the greedy policy over the given days, all stepped in parallel."""
    if len(day_starts) == 0:
        return float("nan")
    actions = np.asarray(policy.config.actions) * battery.max_power_mw
    soc = np.full(len(day_starts), battery.initial_soc_mwh)
    qh_energy = np.zeros(len(day_starts))
    pnl = np.zeros(len(day_starts))
    for t in range(1440):
        rows = day_starts + t
        if t % MINUTES_PER_QH == 0:
            qh_energy[:] = 0.0
        obs = np.hstack([obs_market[rows], _dynamic_state(soc, qh_energy, battery)])
        a = np.argmax(policy.q_values(obs), axis=1)
        p, soc = _step_battery(soc, actions[a], battery)
        qh_energy += p * MINUTE_HOURS
        price = prices_qh[rows // MINUTES_PER_QH]
        pnl += np.where(np.isfinite(price), p * MINUTE_HOURS * np.nan_to_num(price), 0.0)
        pnl -= np.maximum(p, 0.0) * MINUTE_HOURS * battery.degradation_eur_per_mwh
    return float(pnl.mean())


def train_dqn(
    market: MarketData,
    battery: BatteryParams,
    *,
    train: tuple[str | pd.Timestamp, str | pd.Timestamp],
    validation: tuple[str | pd.Timestamp, str | pd.Timestamp] | None = None,
    mechanism: str = "current",
    publication: PublicationModel | None = None,
    config: DQNConfig | None = None,
) -> DQNPolicy:
    """Train a double-DQN policy on ``train`` days (and keep the checkpoint that is best on ``validation`` days)."""
    torch = require_torch()
    config = config or DQNConfig()
    publication = publication or PublicationModel()
    device = select_device(config.device)
    rng = np.random.default_rng(config.seed)
    torch.manual_seed(config.seed)

    m0, m1 = _minute_rows(market, *train)
    fit_market = market.slice(market.minute_times[m0], market.minute_times[m1 - 1] + pd.Timedelta(minutes=1))
    _, scaler = market_observations(fit_market, publication, config)
    obs_market, _ = market_observations(market, publication, config, scaler)
    prices = np.asarray(market.settlement(mechanism), dtype=float)
    n_obs = obs_market.shape[1] + 2
    n_act = len(config.actions)
    actions_mw = np.asarray(config.actions) * battery.max_power_mw

    day_starts = np.arange(m0, m1 - 1440 - 1, 1440, dtype=np.int64)
    if len(day_starts) == 0:
        raise ValueError("training window shorter than one day")
    if validation is not None:
        v0, v1 = _minute_rows(market, *validation)
        valid_days = np.arange(v0, v1 - 1440 + 1, 1440, dtype=np.int64)
    else:
        valid_days = np.zeros(0, dtype=np.int64)

    def mlp() -> Any:
        layers: list[Any] = []
        width = n_obs
        for hidden in config.hidden:
            layers += [torch.nn.Linear(width, hidden), torch.nn.ReLU()]
            width = hidden
        layers.append(torch.nn.Linear(width, n_act))
        return torch.nn.Sequential(*layers).to(device)

    online, target = mlp(), mlp()
    target.load_state_dict(online.state_dict())
    optimiser = torch.optim.Adam(online.parameters(), lr=config.lr)
    x_dev = torch.as_tensor(obs_market, device=device)

    size = config.buffer_size
    buf_row = np.zeros(size, dtype=np.int64)
    buf_dyn = np.zeros((size, 2), dtype=np.float32)
    buf_dyn_next = np.zeros((size, 2), dtype=np.float32)
    buf_act = np.zeros(size, dtype=np.int64)
    buf_rew = np.zeros(size, dtype=np.float32)
    ptr = filled = 0

    def export() -> DQNPolicy:
        weights, biases = [], []
        for module in online:
            if isinstance(module, torch.nn.Linear):
                weights.append(module.weight.detach().cpu().numpy().T.astype(np.float32))
                biases.append(module.bias.detach().cpu().numpy().astype(np.float32))
        return DQNPolicy(weights=weights, biases=biases, scaler=scaler, config=config)

    n_env = config.n_envs
    env_day = rng.choice(day_starts, size=n_env)
    env_t = rng.integers(0, 1440, size=n_env)
    soc = rng.uniform(battery.soc_min_mwh, battery.soc_max_mwh, size=n_env)
    qh_energy = np.zeros(n_env)
    started = time.perf_counter()
    updates = env_steps = 0
    best_score, best_policy = -math.inf, export()
    history: list[dict[str, float]] = []
    losses: list[float] = []
    while True:
        elapsed = (time.perf_counter() - started) / 60.0
        if elapsed >= config.max_minutes or updates >= config.max_updates:
            break
        rows = env_day + env_t
        dyn = _dynamic_state(soc, qh_energy, battery)
        eps = max(
            config.eps_end, config.eps_start - (config.eps_start - config.eps_end) * updates / config.eps_decay_updates
        )
        with torch.no_grad():
            obs = torch.cat([x_dev[torch.as_tensor(rows, device=device)], torch.as_tensor(dyn, device=device)], 1)
            greedy = online(obs).argmax(1).cpu().numpy()
        explore = rng.random(n_env) < eps
        act = np.where(explore, rng.integers(0, n_act, size=n_env), greedy)
        p, soc_next = _step_battery(soc, actions_mw[act], battery)
        price = prices[rows // MINUTES_PER_QH]
        reward = np.where(np.isfinite(price), p * MINUTE_HOURS * np.nan_to_num(price), 0.0)
        reward -= np.maximum(p, 0.0) * MINUTE_HOURS * battery.degradation_eur_per_mwh
        qh_next = np.where((env_t + 1) % MINUTES_PER_QH == 0, 0.0, qh_energy + p * MINUTE_HOURS)
        dyn_next = _dynamic_state(soc_next, qh_next, battery)
        idx = (ptr + np.arange(n_env)) % size
        buf_row[idx], buf_dyn[idx], buf_dyn_next[idx] = rows, dyn, dyn_next
        buf_act[idx], buf_rew[idx] = act, reward * config.reward_scale
        ptr = (ptr + n_env) % size
        filled = min(size, filled + n_env)
        env_steps += 1
        soc, qh_energy = soc_next, qh_next
        env_t += 1
        done = env_t >= 1440
        if done.any():
            k = int(done.sum())
            env_day[done] = rng.choice(day_starts, size=k)
            env_t[done] = 0
            soc[done] = rng.uniform(battery.soc_min_mwh, battery.soc_max_mwh, size=k)
            qh_energy[done] = 0.0
        if filled < config.learning_starts:
            continue
        for _ in range(config.updates_per_step):
            sample = rng.integers(0, filled, size=config.batch_size)
            r_idx = torch.as_tensor(buf_row[sample], device=device)
            s = torch.cat([x_dev[r_idx], torch.as_tensor(buf_dyn[sample], device=device)], 1)
            s2 = torch.cat([x_dev[r_idx + 1], torch.as_tensor(buf_dyn_next[sample], device=device)], 1)
            a_t = torch.as_tensor(buf_act[sample], device=device)
            r_t = torch.as_tensor(buf_rew[sample], device=device)
            with torch.no_grad():
                best_next = online(s2).argmax(1, keepdim=True)
                q_next = target(s2).gather(1, best_next).squeeze(1)
                y = r_t + config.gamma * q_next
            q = online(s).gather(1, a_t[:, None]).squeeze(1)
            loss = torch.nn.functional.smooth_l1_loss(q, y)
            optimiser.zero_grad(set_to_none=True)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(online.parameters(), 10.0)
            optimiser.step()
            updates += 1
            if updates % 200 == 0:
                losses.append(float(loss.detach().cpu()))
            if updates % config.target_update_every == 0:
                target.load_state_dict(online.state_dict())
            if len(valid_days) and updates % config.validate_every == 0:
                candidate = export()
                score = evaluate_policy_numpy(candidate, obs_market, prices, valid_days, battery)
                history.append(
                    {"updates": updates, "minutes": (time.perf_counter() - started) / 60.0, "valid_daily_pnl": score}
                )
                if score > best_score:
                    best_score, best_policy = score, candidate
    final = export()
    if len(valid_days):
        score = evaluate_policy_numpy(final, obs_market, prices, valid_days, battery)
        history.append(
            {"updates": updates, "minutes": (time.perf_counter() - started) / 60.0, "valid_daily_pnl": score}
        )
        if score > best_score:
            best_score, best_policy = score, final
    else:
        best_policy = final
    best_policy.log = {
        "device": device,
        "wall_clock_minutes": (time.perf_counter() - started) / 60.0,
        "updates": updates,
        "env_steps": env_steps,
        "transitions": env_steps * n_env,
        "best_valid_daily_pnl_eur": best_score if len(valid_days) else None,
        "validation_history": history,
        "loss_tail": losses[-20:],
        "train": [str(train[0]), str(train[1])],
        "validation": [str(validation[0]), str(validation[1])] if validation else None,
        "mechanism": mechanism,
    }
    return best_policy


class RLStrategy(BaseStrategy):
    """Greedy DQN policy; observations are rebuilt from the decision context's market in :meth:`reset`."""

    def __init__(self, policy: DQNPolicy, name: str = "rl_dqn") -> None:
        self.policy = policy
        self.name = name
        self._obs: F32 = np.zeros((0, 0), dtype=np.float32)
        self._qh_energy = 0.0
        self._last_qh = -1

    def reset(self, ctx: DecisionContext) -> None:
        self._obs, _ = market_observations(ctx.market, ctx.publication, self.policy.config, self.policy.scaler)
        self._qh_energy = 0.0
        self._last_qh = -1
        self._actions = np.asarray(self.policy.config.actions) * ctx.battery.max_power_mw

    def refresh(self, ctx: DecisionContext) -> None:
        """Live re-planning: rebuild the market observations from the data visible now.

        Unlike :meth:`reset` this keeps the energy already delivered in the current quarter-hour (agent state).
        """
        if len(self._obs) == 0:
            self.reset(ctx)
            return
        self._obs, _ = market_observations(ctx.market, ctx.publication, self.policy.config, self.policy.scaler)

    def decide(self, ctx: DecisionContext) -> float:
        if ctx.qh_index != self._last_qh:
            self._qh_energy = 0.0
            self._last_qh = ctx.qh_index
        elif ctx.t > 0:
            self._qh_energy += ctx.last_power_mw * MINUTE_HOURS
        battery = ctx.battery
        dyn = _dynamic_state(np.array([ctx.soc_mwh]), np.array([self._qh_energy]), battery)
        obs = np.concatenate([self._obs[ctx.t], dyn[0]])[None, :]
        action = int(np.argmax(self.policy.q_values(obs)[0]))
        return float(self._actions[action])


def live_rl_dqn(models_dir: str | None = None, policy: str | None = None) -> RLStrategy:
    """Live factory: the DQN policy saved by the backtest (models directory, else the committed
    ``reports/ledgers/realtime_rl_policy.npz``). Inference is numpy-only."""
    from ..forecasting.deploy import rl_policy_path

    path = Path(policy).expanduser() if policy else rl_policy_path(models_dir)
    return RLStrategy(DQNPolicy.load(path), name="rl_dqn")


#: Factories for ``euroflex live run --strategy <name>`` (discovered by ``realtime.live.strategies``).
LIVE_STRATEGIES = {"rl-dqn": live_rl_dqn}
