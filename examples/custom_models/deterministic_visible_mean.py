from __future__ import annotations

from typing import Any

import pandas as pd


class DeterministicVisibleMeanForecaster:
    """Example custom provider that uses only as-of-visible data.

    For each requested market, the forecast is a deterministic blend of the
    latest visible value and the visible mean. This keeps the example simple,
    auditable, and stable across tests while demonstrating the BYO-ML contract.
    """

    def __init__(self, blend_weight: float = 0.7) -> None:
        self.blend_weight = float(blend_weight)
        self.config: dict[str, Any] = {}
        self.run_context: dict[str, Any] = {}

    def initialize(self, *, config: dict[str, Any], run_context: dict[str, Any]) -> None:
        self.config = config
        self.run_context = run_context

    def generate_forecast(
        self,
        *,
        market: str,
        decision_time_utc: pd.Timestamp,
        delivery_frame: pd.DataFrame,
        visible_inputs: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        horizon = delivery_frame.sort_values("timestamp_utc").reset_index(drop=True).copy()
        visible = visible_inputs.get(market)
        if visible is None or visible.empty:
            anchor = horizon["price_eur_per_mwh"].astype(float)
        else:
            visible_prices = visible["price_eur_per_mwh"].astype(float)
            latest = float(visible_prices.iloc[-1])
            average = float(visible_prices.mean())
            anchor_value = self.blend_weight * latest + (1.0 - self.blend_weight) * average
            anchor = pd.Series([anchor_value] * len(horizon))
        resolution_minutes = int(horizon["resolution_minutes"].iloc[0])
        return pd.DataFrame(
            {
                "market": market,
                "delivery_start_utc": horizon["timestamp_utc"],
                "delivery_end_utc": horizon["timestamp_utc"] + pd.Timedelta(minutes=resolution_minutes),
                "forecast_price_eur_per_mwh": anchor,
                "issue_time_utc": decision_time_utc,
                "available_from_utc": decision_time_utc,
                "provider_name": "custom_python",
            }
        )
