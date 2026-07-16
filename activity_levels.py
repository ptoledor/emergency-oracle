"""Operational activity bands shown in the forecast dashboard."""

from __future__ import annotations

import math


OPERATIONAL_ACTIVITY_THRESHOLDS = (4.0, 6.0, 8.0)


def operational_activity_level(expected_calls):
    """Map expected daily calls to stable, model-independent activity bands."""
    value = float(expected_calls)
    if not math.isfinite(value):
        raise ValueError("expected_calls must be finite")
    low_max, normal_max, high_max = OPERATIONAL_ACTIVITY_THRESHOLDS
    if value < low_max:
        return "ACTIVIDAD BAJA", "activity-low"
    if value < normal_max:
        return "ACTIVIDAD NORMAL", "activity-normal"
    if value < high_max:
        return "ACTIVIDAD ALTA", "activity-high"
    return "ACTIVIDAD MUY ALTA", "activity-alert"
