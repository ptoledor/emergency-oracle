import datetime as dt

import pandas as pd

from hourly_peak import historical_hourly_distribution


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
