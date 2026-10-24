import datetime as dt

import numpy as np
import pandas as pd

from features import event_history_features, prepare_event_history


def history_frame(periods=120):
    dates = pd.date_range("2025-01-01", periods=periods, freq="D")
    return pd.DataFrame({
        "FECHA_DIA": dates.strftime("%Y-%m-%d"),
        "EVENTOS": 4 + dates.dayofweek.to_numpy() / 2,
    })


def test_recent_history_uses_observed_values():
    frame = history_frame()
    start_date = pd.to_datetime(frame["FECHA_DIA"].iloc[-1]).date() + dt.timedelta(days=1)
    result = prepare_event_history(frame, start_date)
    assert result["mode"] == "observed"
    assert result["age_days"] == 1
    assert result["values"] == frame["EVENTOS"].tail(30).astype(float).tolist()


def test_history_is_calendar_aligned_with_missing_days():
    frame = history_frame().drop(index=[110, 111]).reset_index(drop=True)
    start_date = dt.date(2025, 5, 1)  # day after the last of 120 days
    result = prepare_event_history(frame, start_date)
    assert len(result["values"]) == 30
    assert np.isnan(result["values"][-10]) and np.isnan(result["values"][-9])
    assert np.isfinite(result["values"][-1])


def test_stale_history_is_missing_not_imputed():
    frame = history_frame(periods=420)
    last_date = pd.to_datetime(frame["FECHA_DIA"].iloc[-1]).date()
    result = prepare_event_history(frame, last_date + dt.timedelta(days=45))
    assert result["mode"] == "stale"
    assert result["age_days"] == 45
    assert np.isnan(result["values"]).all()


def test_history_features_skip_unknown_days():
    values = [5.0] * 27 + [np.nan, 2.0, 4.0]
    features = event_history_features(values)
    assert features["EVENTOS_lag_1"] == 4.0
    assert np.isnan(features["EVENTOS_lag_3"])
    assert features["EVENTOS_rolling_mean_3d"] == 3.0
    assert features["EVENTOS_rolling_max_3d"] == 4.0
    all_missing = event_history_features([np.nan] * 30)
    assert np.isnan(all_missing["EVENTOS_rolling_mean_30d"])
    assert all_missing["EVENTOS_rolling_std_7d"] == 0.0
