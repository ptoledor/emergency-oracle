"""Serving-time event history policies shared by forecast surfaces."""

from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd


def neutral_event_history(history_frame, start_date, days=30):
    """Expected history when recent observations are unavailable."""
    history = history_frame.copy()
    history["FECHA_DT_NEUTRAL"] = pd.to_datetime(history["FECHA_DIA"])
    history["EVENTOS_NEUTRAL"] = pd.to_numeric(
        history["EVENTOS"], errors="coerce"
    )
    history = history.dropna(subset=["EVENTOS_NEUTRAL"])
    global_mean = float(history["EVENTOS_NEUTRAL"].mean())
    values = []
    for offset in range(days, 0, -1):
        target = start_date - dt.timedelta(days=offset)
        same_weekday = history[
            history["FECHA_DT_NEUTRAL"].dt.dayofweek == target.weekday()
        ]
        same_month_weekday = same_weekday[
            same_weekday["FECHA_DT_NEUTRAL"].dt.month == target.month
        ]
        if len(same_month_weekday) >= 8:
            seasonal = float(same_month_weekday["EVENTOS_NEUTRAL"].mean())
            weekday = float(same_weekday["EVENTOS_NEUTRAL"].mean())
            values.append(0.75 * seasonal + 0.25 * weekday)
        elif not same_weekday.empty:
            values.append(float(same_weekday["EVENTOS_NEUTRAL"].mean()))
        else:
            values.append(global_mean)
    return values


def prepare_event_history(history_frame, start_date, days=30, max_age_days=1):
    """Return honest serving history plus provenance metadata."""
    history = history_frame.copy().sort_values("FECHA_DIA")
    history["EVENTOS"] = pd.to_numeric(history["EVENTOS"], errors="coerce")
    history = history.dropna(subset=["EVENTOS"])
    last_observed_date = None
    lag_age_days = None
    if not history.empty:
        last_observed_date = pd.to_datetime(history["FECHA_DIA"].iloc[-1]).date()
        lag_age_days = max((start_date - last_observed_date).days, 0)

    if len(history) >= days and lag_age_days is not None and lag_age_days <= max_age_days:
        values = history["EVENTOS"].tail(days).astype(float).tolist()
        mode = "observed"
    elif len(history) >= days:
        values = neutral_event_history(history, start_date, days=days)
        mode = "historical_baseline"
    else:
        mean = float(history["EVENTOS"].mean()) if not history.empty else 1.5
        if not np.isfinite(mean):
            mean = 1.5
        values = [mean] * days
        mode = "historical_baseline"
    return {
        "values": values,
        "mode": mode,
        "age_days": lag_age_days,
        "last_observed_date": last_observed_date,
    }
