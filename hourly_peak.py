"""Historical probability of at least one call per local hour."""

from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd


PROJECT_TIMEZONE = "America/Santiago"


def historical_hourly_distribution(
    timestamps: pd.Series,
    valid_dates: set[dt.date] | None = None,
) -> pd.DataFrame:
    """Return the empirical daily occurrence probability by local hour."""
    local_time = pd.to_datetime(timestamps, errors="coerce", utc=True).dt.tz_convert(
        PROJECT_TIMEZONE
    )
    local_time = local_time.dropna()
    if valid_dates is not None:
        local_time = local_time[local_time.dt.date.isin(valid_dates)]

    event_hours = pd.DataFrame({
        "date": local_time.dt.date,
        "hour": local_time.dt.hour.astype(int),
    })
    counts = event_hours["hour"].value_counts().reindex(range(24), fill_value=0)
    days_with_event = (
        event_hours.drop_duplicates(["date", "hour"])["hour"]
        .value_counts()
        .reindex(range(24), fill_value=0)
    )
    total_days = (
        len(valid_dates)
        if valid_dates is not None
        else event_hours["date"].nunique()
    )
    probability = (
        days_with_event.astype(float) / total_days
        if total_days
        else pd.Series(np.zeros(24), index=range(24), dtype=float)
    )
    return pd.DataFrame({
        "hour": np.arange(24, dtype=int),
        "count": counts.to_numpy(dtype=int),
        "days_with_event": days_with_event.to_numpy(dtype=int),
        "total_days": np.full(24, total_days, dtype=int),
        "probability": probability.to_numpy(dtype=float),
    })
