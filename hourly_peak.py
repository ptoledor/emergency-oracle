"""Shared features and inference helpers for hourly emergency peaks."""

from __future__ import annotations

import datetime as dt

import holidays
import numpy as np
import pandas as pd


PROJECT_TIMEZONE = "America/Santiago"

CALENDAR_FEATURES = [
    "hour",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "month_sin",
    "month_cos",
    "is_weekend",
    "is_holiday",
]

WEATHER_FEATURES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_gusts_10m",
    "weather_code",
    "cape",
]

ALL_FEATURES = CALENDAR_FEATURES + WEATHER_FEATURES


def build_hourly_features(hourly: pd.DataFrame) -> pd.DataFrame:
    """Create serving-compatible calendar and hourly weather features."""
    frame = hourly.copy()
    frame["time"] = pd.to_datetime(frame["time"], errors="raise")
    frame["date"] = frame["time"].dt.date
    frame["hour"] = frame["time"].dt.hour.astype(int)
    frame["dow"] = frame["time"].dt.dayofweek.astype(int)
    frame["month"] = frame["time"].dt.month.astype(int)
    frame["hour_sin"] = np.sin(2 * np.pi * frame["hour"] / 24)
    frame["hour_cos"] = np.cos(2 * np.pi * frame["hour"] / 24)
    frame["dow_sin"] = np.sin(2 * np.pi * frame["dow"] / 7)
    frame["dow_cos"] = np.cos(2 * np.pi * frame["dow"] / 7)
    frame["month_sin"] = np.sin(2 * np.pi * (frame["month"] - 1) / 12)
    frame["month_cos"] = np.cos(2 * np.pi * (frame["month"] - 1) / 12)
    frame["is_weekend"] = frame["dow"].isin([5, 6]).astype(int)

    years = sorted(frame["time"].dt.year.unique().tolist())
    chile_holidays = holidays.Chile(years=years)
    frame["is_holiday"] = frame["date"].map(
        lambda value: int(value.strftime("%Y-%m-%d") in chile_holidays)
    )

    fallback_values = {
        "temperature_2m": 15.0,
        "relative_humidity_2m": 75.0,
        "precipitation": 0.0,
        "wind_speed_10m": 0.0,
        "wind_gusts_10m": 0.0,
        "weather_code": 0.0,
        "cape": 0.0,
    }
    for column, fallback in fallback_values.items():
        if column not in frame:
            frame[column] = fallback
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame[column] = frame.groupby("date")[column].transform(
            lambda values: values.interpolate(limit_direction="both")
        ).fillna(fallback)
    return frame


def _normalise_rates(rates: np.ndarray) -> np.ndarray:
    rates = np.clip(np.asarray(rates, dtype=float), 1e-9, None)
    total = float(rates.sum())
    if not np.isfinite(total) or total <= 0:
        return np.full(len(rates), 1.0 / max(len(rates), 1))
    return rates / total


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


def predict_hourly_distribution(
    artifact: dict,
    hourly: pd.DataFrame,
    target_date: dt.date,
    weather_is_reliable: bool = True,
) -> pd.DataFrame:
    """Predict a calibrated 24-hour distribution for one forecast day."""
    frame = build_hourly_features(hourly)
    day = frame[frame["date"] == target_date].copy()
    if day.empty:
        raise ValueError(f"No hourly weather is available for {target_date}")

    # Open-Meteo normally provides every hour. Reindex defensively so the
    # dashboard always receives one probability for each local clock hour.
    day = day.sort_values("hour").drop_duplicates("hour", keep="last")
    day = day.set_index("hour").reindex(range(24))
    day["date"] = target_date
    day["time"] = pd.to_datetime(
        [f"{target_date:%Y-%m-%d}T{hour:02d}:00" for hour in range(24)]
    )
    day = build_hourly_features(day.reset_index(drop=True))

    calendar_model = artifact["calendar_model"]
    calendar_probability = _normalise_rates(
        calendar_model.predict(day[artifact["calendar_features"]])
    )

    weather_probability = calendar_probability
    weather_weight = 0.0
    if weather_is_reliable and artifact.get("weather_model") is not None:
        weather_probability = _normalise_rates(
            artifact["weather_model"].predict(day[artifact["weather_features"]])
        )
        weather_weight = float(artifact.get("weather_weight", 0.0))

    probability = _normalise_rates(
        weather_weight * weather_probability
        + (1.0 - weather_weight) * calendar_probability
    )
    return pd.DataFrame({
        "hour": np.arange(24, dtype=int),
        "probability": probability,
    })


def select_peak_hours(
    distribution: pd.DataFrame,
    expected_count: float,
    min_separation: int = 3,
) -> list[dict]:
    """Return only the most probable local hour."""
    del expected_count, min_separation
    probabilities = distribution.set_index("hour")["probability"].reindex(
        range(24), fill_value=0.0
    )
    selected = [int(probabilities.idxmax())]
    return [
        {
            "hour": hour,
            "label": f"{hour:02d}:00",
            "probability": float(probabilities.loc[hour]),
        }
        for hour in selected
    ]
