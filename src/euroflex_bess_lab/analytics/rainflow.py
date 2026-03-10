from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class RainflowSummary:
    cycle_count: float
    equivalent_full_cycles: float
    mean_cycle_depth: float
    max_cycle_depth: float

    def as_dict(self) -> dict[str, float]:
        return {
            "cycle_count": float(self.cycle_count),
            "equivalent_full_cycles": float(self.equivalent_full_cycles),
            "mean_cycle_depth": float(self.mean_cycle_depth),
            "max_cycle_depth": float(self.max_cycle_depth),
        }


def _turning_points(values: np.ndarray) -> list[float]:
    if len(values) < 3:
        return values.tolist()
    points = [float(values[0])]
    for idx in range(1, len(values) - 1):
        prev_value, value, next_value = values[idx - 1], values[idx], values[idx + 1]
        if (value >= prev_value and value > next_value) or (value <= prev_value and value < next_value):
            points.append(float(value))
    points.append(float(values[-1]))
    return points


def summarize_rainflow(soc_series: pd.Series, battery_energy_mwh: float) -> RainflowSummary:
    values = soc_series.astype(float).to_numpy()
    points = _turning_points(values)
    stack: list[float] = []
    cycle_ranges: list[float] = []
    cycle_counts: list[float] = []

    for point in points:
        stack.append(point)
        while len(stack) >= 3:
            x, y, z = stack[-3], stack[-2], stack[-1]
            range_one = abs(y - x)
            range_two = abs(z - y)
            if range_two < range_one:
                break
            if len(stack) == 3:
                cycle_ranges.append(range_one)
                cycle_counts.append(0.5)
                stack.pop(-2)
            else:
                cycle_ranges.append(range_one)
                cycle_counts.append(1.0)
                last = stack.pop()
                stack.pop()
                stack.pop()
                stack.extend([y, last])

    while len(stack) >= 2:
        cycle_ranges.append(abs(stack[-1] - stack[-2]))
        cycle_counts.append(0.5)
        stack.pop()

    if not cycle_ranges or battery_energy_mwh <= 0:
        return RainflowSummary(cycle_count=0.0, equivalent_full_cycles=0.0, mean_cycle_depth=0.0, max_cycle_depth=0.0)

    ranges = np.asarray(cycle_ranges, dtype=float)
    counts = np.asarray(cycle_counts, dtype=float)
    normalized_depth = ranges / battery_energy_mwh
    equivalent_full_cycles = float(np.sum(normalized_depth * counts) / 2.0)
    mean_cycle_depth = float(np.average(normalized_depth, weights=counts))
    max_cycle_depth = float(np.max(normalized_depth))
    return RainflowSummary(
        cycle_count=float(np.sum(counts)),
        equivalent_full_cycles=equivalent_full_cycles,
        mean_cycle_depth=mean_cycle_depth,
        max_cycle_depth=max_cycle_depth,
    )
