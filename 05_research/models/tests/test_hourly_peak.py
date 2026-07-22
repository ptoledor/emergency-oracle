import datetime as dt

import numpy as np
import pandas as pd

from hourly_peak import (
    ALL_FEATURES,
    CALENDAR_FEATURES,
    historical_hourly_distribution,
    predict_hourly_distribution,
    select_peak_hours,
)


class HourRateModel:
    def __init__(self, peaks):
        self.peaks = set(peaks)

    def predict(self, frame):
        return np.array([
            5.0 if int(hour) in self.peaks else 1.0
            for hour in frame["hour"]
        ])


def hourly_weather(target_date):
    hours = np.arange(24)
    return pd.DataFrame({
        "time": [f"{target_date:%Y-%m-%d}T{hour:02d}:00" for hour in hours],
        "temperature_2m": 12 + 4 * np.sin(2 * np.pi * hours / 24),
        "relative_humidity_2m": 80.0,
        "precipitation": 0.0,
        "wind_speed_10m": 10.0,
        "wind_gusts_10m": 20.0,
        "weather_code": 0.0,
        "cape": 0.0,
    })


def test_hourly_distribution_reconciles_to_one():
    target_date = dt.date(2026, 7, 19)
    artifact = {
        "calendar_features": CALENDAR_FEATURES,
        "weather_features": ALL_FEATURES,
        "calendar_model": HourRateModel([8, 14, 20]),
        "weather_model": HourRateModel([9, 15, 21]),
        "weather_weight": 0.25,
    }
    result = predict_hourly_distribution(
        artifact,
        hourly_weather(target_date),
        target_date,
        weather_is_reliable=True,
    )
    assert result["hour"].tolist() == list(range(24))
    assert np.isclose(result["probability"].sum(), 1.0)
    assert (result["probability"] > 0).all()


def test_historical_hourly_distribution_uses_santiago_time_and_valid_dates():
    timestamps = pd.Series([
        "2026-07-19T02:30:00Z",  # 22:30 del día anterior en Santiago
        "2026-07-19T15:00:00Z",  # 11:00 en Santiago
        "2026-07-20T15:00:00Z",  # fuera de las fechas válidas
    ])
    result = historical_hourly_distribution(
        timestamps,
        valid_dates={dt.date(2026, 7, 18), dt.date(2026, 7, 19)},
    )
    assert result["hour"].tolist() == list(range(24))
    assert result["count"].sum() == 2
    assert result.loc[result["hour"] == 22, "count"].item() == 1
    assert result.loc[result["hour"] == 11, "count"].item() == 1
    assert result.loc[result["hour"] == 22, "days_with_event"].item() == 1
    assert result.loc[result["hour"] == 11, "probability"].item() == 0.5
    assert result["total_days"].nunique() == 1
    assert result["total_days"].iloc[0] == 2


def test_peak_policy_returns_only_the_most_probable_hour():
    probability = np.full(24, 0.01)
    probability[[2, 8, 14, 20]] = [0.12, 0.15, 0.18, 0.20]
    probability /= probability.sum()
    distribution = pd.DataFrame({
        "hour": range(24),
        "probability": probability,
    })
    peaks = select_peak_hours(distribution, expected_count=5.5)
    hours = [peak["hour"] for peak in peaks]
    assert hours == [20]
    assert peaks[0]["label"] == "20:00"


def test_peak_count_does_not_scale_with_daily_activity():
    distribution = pd.DataFrame({
        "hour": range(24),
        "probability": np.linspace(1, 24, 24) / np.linspace(1, 24, 24).sum(),
    })
    assert len(select_peak_hours(distribution, expected_count=3.0)) == 1
    assert len(select_peak_hours(distribution, expected_count=5.0)) == 1
    assert len(select_peak_hours(distribution, expected_count=9.0)) == 1
