import datetime as dt

import numpy as np
import pandas as pd

from serving_history import prepare_event_history


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


def test_stale_history_uses_neutral_calendar_baseline():
    frame = history_frame(periods=420)
    last_date = pd.to_datetime(frame["FECHA_DIA"].iloc[-1]).date()
    result = prepare_event_history(frame, last_date + dt.timedelta(days=45))
    assert result["mode"] == "historical_baseline"
    assert result["age_days"] == 45
    assert len(result["values"]) == 30
    assert np.isfinite(result["values"]).all()
    assert result["values"] != frame["EVENTOS"].tail(30).astype(float).tolist()
