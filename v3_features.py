"""Serving-safe feature families evaluated for the hydro v3 candidate.

Every column depends only on the target date, counts observed up to the
previous day and weather already available for the target date.
"""

from __future__ import annotations

import holidays
import numpy as np
import pandas as pd


def _chile_holidays(years):
    return holidays.country_holidays("CL", years=sorted(set(years)))


def calendar_features(dates: pd.Series) -> pd.DataFrame:
    days = pd.to_datetime(dates)
    years = list(range(days.dt.year.min() - 1, days.dt.year.max() + 2))
    holiday_days = {pd.Timestamp(day) for day in _chile_holidays(years)}

    def is_free(day):
        return day.weekday() >= 5 or day in holiday_days

    out = pd.DataFrame(index=dates.index)
    free = days.map(is_free)
    out["CAL_DIA_LIBRE"] = free.astype(int)
    out["CAL_VISPERA_LIBRE"] = days.map(lambda d: int(not is_free(d) and is_free(d + pd.Timedelta(days=1)))).astype(int)
    out["CAL_POST_LIBRE"] = days.map(lambda d: int(not is_free(d) and is_free(d - pd.Timedelta(days=1)))).astype(int)

    def run_length(day):
        if not is_free(day):
            return 0
        start = day
        while is_free(start - pd.Timedelta(days=1)):
            start -= pd.Timedelta(days=1)
        end = day
        while is_free(end + pd.Timedelta(days=1)):
            end += pd.Timedelta(days=1)
        return (end - start).days + 1

    length = days.map(run_length)
    out["CAL_FIN_SEMANA_LARGO"] = (length >= 3).astype(int)
    month_day = days.dt.month * 100 + days.dt.day
    out["CAL_FIESTAS_PATRIAS"] = month_day.between(916, 921).astype(int)
    out["CAL_FIESTAS_FIN_ANO"] = ((month_day >= 1224) | (month_day <= 101)).astype(int)
    out["CAL_VACACIONES_VERANO"] = ((month_day >= 1220) | (month_day <= 228)).astype(int)
    out["CAL_VACACIONES_INVIERNO"] = month_day.between(701, 721).astype(int)
    last_day = days + pd.offsets.MonthEnd(0)
    out["CAL_DIAS_A_FIN_MES"] = (last_day - days).dt.days
    out["CAL_DIA_PAGO"] = ((out["CAL_DIAS_A_FIN_MES"] <= 2) | (days.dt.day <= 2)).astype(int)
    return out


def level_features(counts: pd.Series) -> pd.DataFrame:
    """Slow-moving activity level built from counts up to the previous day."""
    previous = counts.astype(float).shift(1)
    out = pd.DataFrame(index=counts.index)
    for window in (60, 90, 180, 365):
        out[f"LVL_MEAN_{window}D"] = previous.rolling(window, min_periods=window // 2).mean()
    for span in (7, 28):
        out[f"LVL_EWM_{span}"] = previous.ewm(span=span, min_periods=span // 2).mean()
    same_dow = pd.concat([counts.astype(float).shift(7 * step) for step in range(1, 9)], axis=1)
    out["LVL_SAME_DOW_8W"] = same_dow.mean(axis=1)
    out["LVL_TREND_7_90"] = out["LVL_EWM_7"] - out["LVL_MEAN_90D"]
    return out


def anomaly_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Weather departures from the preceding 30 days (no climatology leakage)."""
    out = pd.DataFrame(index=frame.index)
    for column in ("TEMP_MAX", "TEMP_MEDIA", "HUM_MEDIA", "VIENTO_MAX", "WX_GUST_MAX"):
        previous = frame[column].shift(1).rolling(30, min_periods=10)
        mean, std = previous.mean(), previous.std()
        out[f"ANOM_{column}"] = (frame[column] - mean) / std.replace(0, np.nan)
    rain = frame["LLUVIA"].fillna(0)
    out["ANOM_PRIMERA_LLUVIA"] = ((rain > 1.0) & (frame["DIAS_SECOS_14D_PREV"] >= 12)).astype(int)
    out["ANOM_CALOR_SECO"] = ((frame["TEMP_MAX"] >= 28) & (frame["HUM_MIN"] <= 35)).astype(int)
    return out
