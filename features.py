"""Feature definitions shared by training, evaluation, the dashboard and the CLI.

Every feature the models consume is built here, so training and serving cannot
drift apart:

- `aggregate_weather_daily`: hourly Open-Meteo data -> daily weather features.
- `weather_derived_features`: rain memory, weather lags and indices.
- `calendar_features`: holidays, weekday and seasonal encodings.
- `event_history_features` / `prepare_event_history`: recent call history.
"""

from __future__ import annotations

import datetime as dt

import holidays
import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view

OPEN_METEO_HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "precipitation",
    "pressure_msl",
    "visibility",
    "wind_speed_10m",
    "wind_gusts_10m",
    "vapour_pressure_deficit",
    "cape",
    "soil_moisture_0_to_1cm",
    "et0_fao_evapotranspiration",
    "rain",
    "showers",
    "weather_code",
    "freezing_level_height",
    "wet_bulb_temperature_2m",
]
OPEN_METEO_HOURLY_QUERY = ",".join(OPEN_METEO_HOURLY_VARIABLES)

CATEGORY_LAG_FEATURES = [
    "N_INCENDIO_ESTR_lag_1",
    "N_INCENDIO_FOREST_lag_1",
    "N_RESCATE_VEH_lag_1",
    "N_RESCATE_PERS_lag_1",
    "N_EMERGENCIAS_CLIMATICAS_lag_1",
    "N_GASES_lag_1",
]


def _numeric(frame, column, fallback):
    if not np.isscalar(fallback):
        fallback = pd.Series(fallback, index=frame.index, dtype=float)
    if column in frame:
        values = pd.to_numeric(frame[column], errors="coerce")
        return values.fillna(fallback)
    if np.isscalar(fallback):
        return pd.Series(float(fallback), index=frame.index, dtype=float)
    return pd.Series(fallback, index=frame.index, dtype=float)


def ensure_advanced_hourly_columns(hourly):
    """Fill advanced fields deterministically when local fallback is in use."""
    frame = hourly.copy()
    temperature = _numeric(frame, "temperature_2m", 15.0)
    humidity = _numeric(frame, "relative_humidity_2m", 75.0).clip(1.0, 100.0)
    wind = _numeric(frame, "wind_speed_10m", 0.0).clip(lower=0.0)
    precipitation = _numeric(frame, "precipitation", 0.0).clip(lower=0.0)

    gamma = np.log(humidity / 100.0) + (17.625 * temperature) / (243.04 + temperature)
    derived_dewpoint = 243.04 * gamma / (17.625 - gamma)
    saturation = 0.6108 * np.exp((17.27 * temperature) / (temperature + 237.3))
    derived_vpd = saturation * (1.0 - humidity / 100.0)

    frame["temperature_2m"] = temperature
    frame["relative_humidity_2m"] = humidity
    frame["wind_speed_10m"] = wind
    frame["precipitation"] = precipitation
    frame["dew_point_2m"] = _numeric(frame, "dew_point_2m", derived_dewpoint)
    frame["pressure_msl"] = _numeric(frame, "pressure_msl", 1013.25)
    frame["visibility"] = _numeric(frame, "visibility", 10000.0)
    frame["wind_gusts_10m"] = _numeric(frame, "wind_gusts_10m", wind * 1.5)
    frame["vapour_pressure_deficit"] = _numeric(
        frame,
        "vapour_pressure_deficit",
        derived_vpd,
    ).clip(lower=0.0)
    frame["cape"] = _numeric(frame, "cape", 0.0).clip(lower=0.0)
    frame["soil_moisture_0_to_1cm"] = _numeric(
        frame,
        "soil_moisture_0_to_1cm",
        0.5,
    ).clip(0.0, 1.0)
    frame["et0_fao_evapotranspiration"] = _numeric(
        frame,
        "et0_fao_evapotranspiration",
        0.0,
    ).clip(lower=0.0)
    frame["rain"] = _numeric(frame, "rain", precipitation).clip(lower=0.0)
    frame["showers"] = _numeric(frame, "showers", 0.0).clip(lower=0.0)
    frame["weather_code"] = _numeric(
        frame,
        "weather_code",
        np.where(precipitation > 0.1, 61.0, 0.0),
    ).clip(lower=0.0)
    frame["freezing_level_height"] = _numeric(
        frame,
        "freezing_level_height",
        np.clip(1500.0 + 120.0 * temperature, 0.0, None),
    ).clip(lower=0.0)
    wet_bulb = (
        temperature * np.arctan(0.151977 * np.sqrt(humidity + 8.313659))
        + np.arctan(temperature + humidity)
        - np.arctan(humidity - 1.676331)
        + 0.00391838 * humidity ** 1.5 * np.arctan(0.023101 * humidity)
        - 4.686035
    )
    frame["wet_bulb_temperature_2m"] = _numeric(
        frame,
        "wet_bulb_temperature_2m",
        wet_bulb,
    )
    return frame


def _skew(values):
    values = np.asarray(values, dtype=float)
    std = np.std(values)
    return float(np.mean((values - np.mean(values)) ** 3) / max(std, 0.1) ** 3)


def _kurtosis(values):
    values = np.asarray(values, dtype=float)
    std = np.std(values)
    return float(np.mean((values - np.mean(values)) ** 4) / max(std, 0.1) ** 4 - 3.0)


def aggregate_weather_daily(hourly):
    """Aggregate the exact base and WX_* features used by the candidate."""
    frame = ensure_advanced_hourly_columns(hourly)
    frame["time"] = pd.to_datetime(frame["time"], errors="raise")
    frame["FECHA_DIA"] = frame["time"].dt.date
    frame["_precip_hour"] = (frame["precipitation"] > 0.1).astype(float)
    frame["_heavy_rain_hour"] = (frame["precipitation"] >= 5.0).astype(float)
    frame["_low_humidity_hour"] = (
        frame["relative_humidity_2m"] < 30.0
    ).astype(float)
    frame["_wx2_shower_hour"] = (frame["showers"] > 0.1).astype(float)
    frame["_wx2_storm_hour"] = (frame["weather_code"] >= 80).astype(float)
    frame["_wx2_thunder_hour"] = (frame["weather_code"] >= 95).astype(float)

    daily = frame.groupby("FECHA_DIA", sort=True).agg(
        TEMP_MAX=("temperature_2m", "max"),
        TEMP_MIN=("temperature_2m", "min"),
        TEMP_MEDIA=("temperature_2m", "mean"),
        TEMP_SKEW=("temperature_2m", _skew),
        TEMP_KURT=("temperature_2m", _kurtosis),
        HUM_MAX=("relative_humidity_2m", "max"),
        HUM_MIN=("relative_humidity_2m", "min"),
        HUM_MEDIA=("relative_humidity_2m", "mean"),
        HUM_SKEW=("relative_humidity_2m", _skew),
        HUM_KURT=("relative_humidity_2m", _kurtosis),
        VIENTO_MAX=("wind_speed_10m", "max"),
        VIENTO_MEDIO=("wind_speed_10m", "mean"),
        VIENTO_SKEW=("wind_speed_10m", _skew),
        VIENTO_KURT=("wind_speed_10m", _kurtosis),
        LLUVIA=("precipitation", "sum"),
        WX_GUST_MAX=("wind_gusts_10m", "max"),
        WX_GUST_MEAN=("wind_gusts_10m", "mean"),
        WX_PRECIP_MAX_HOURLY=("precipitation", "max"),
        WX_PRECIP_HOURS=("_precip_hour", "sum"),
        WX_HEAVY_RAIN_HOURS=("_heavy_rain_hour", "sum"),
        WX_PRESSURE_MEAN=("pressure_msl", "mean"),
        WX_PRESSURE_MAX=("pressure_msl", "max"),
        WX_PRESSURE_MIN=("pressure_msl", "min"),
        WX_DEWPOINT_MEAN=("dew_point_2m", "mean"),
        WX_VISIBILITY_MIN=("visibility", "min"),
        WX_CAPE_MAX=("cape", "max"),
        WX_CAPE_MEAN=("cape", "mean"),
        WX_VPD_MAX=("vapour_pressure_deficit", "max"),
        WX_VPD_MEAN=("vapour_pressure_deficit", "mean"),
        WX_ET0_SUM=("et0_fao_evapotranspiration", "sum"),
        WX_LOW_HUMIDITY_HOURS=("_low_humidity_hour", "sum"),
        WX_TEMP_MAX=("temperature_2m", "max"),
        WX_HUMIDITY_MIN=("relative_humidity_2m", "min"),
        WX_WIND_MAX=("wind_speed_10m", "max"),
        _WX_SOIL=("soil_moisture_0_to_1cm", "mean"),
        WX2_RAIN_SUM=("rain", "sum"),
        WX2_SHOWERS_SUM=("showers", "sum"),
        WX2_SHOWERS_MAX=("showers", "max"),
        WX2_SHOWER_HOURS=("_wx2_shower_hour", "sum"),
        WX2_STORM_HOURS=("_wx2_storm_hour", "sum"),
        WX2_THUNDER_HOURS=("_wx2_thunder_hour", "sum"),
        WX2_FREEZING_LEVEL_MEAN=("freezing_level_height", "mean"),
        WX2_FREEZING_LEVEL_MIN=("freezing_level_height", "min"),
        WX2_WET_BULB_MEAN=("wet_bulb_temperature_2m", "mean"),
        WX2_WET_BULB_MAX=("wet_bulb_temperature_2m", "max"),
    )
    daily["WX_PRESSURE_RANGE"] = daily["WX_PRESSURE_MAX"] - daily["WX_PRESSURE_MIN"]
    daily["WX_WIND_RAIN_INDEX"] = daily["WX_GUST_MAX"] * np.log1p(
        daily["WX_PRECIP_MAX_HOURLY"]
    )
    dry_humidity = np.clip((30.0 - daily["WX_HUMIDITY_MIN"]) / 30.0, 0.0, None)
    dry_soil = np.clip(1.0 - daily["_WX_SOIL"].fillna(0.5), 0.0, 1.0)
    daily["WX_FIRE_WEATHER_INDEX"] = (
        daily["WX_GUST_MAX"]
        * (1.0 + daily["WX_VPD_MAX"])
        * (1.0 + dry_humidity)
        * (1.0 + dry_soil)
    )
    return daily.drop(
        columns=["WX_PRESSURE_MAX", "WX_PRESSURE_MIN", "_WX_SOIL"],
        errors="ignore",
    )


def event_history_features(history):
    values = np.asarray(list(history), dtype=float)
    if len(values) < 30:
        raise ValueError("At least 30 historical event counts are required")
    result = {
        "EVENTOS_lag_1": float(values[-1]),
        "EVENTOS_lag_2": float(values[-2]),
        "EVENTOS_lag_3": float(values[-3]),
        "EVENTOS_lag_7": float(values[-7]),
        "EVENTOS_rolling_mean_14d": float(np.mean(values[-14:])),
        "EVENTOS_rolling_mean_30d": float(np.mean(values[-30:])),
    }
    for window in [3, 7]:
        recent = values[-window:]
        result[f"EVENTOS_rolling_mean_{window}d"] = float(np.mean(recent))
        result[f"EVENTOS_rolling_std_{window}d"] = float(np.std(recent, ddof=1))
        result[f"EVENTOS_rolling_max_{window}d"] = float(np.max(recent))
    return result


CATEGORY_SOURCES = {column: column[: -len("_lag_1")] for column in CATEGORY_LAG_FEATURES}
RAIN_LAGS = [1, 2, 3, 5, 7, 10, 14]
RAIN_WINDOWS = [3, 7, 14, 30]
WEEKDAY_COLUMNS = [
    "DIA_LUNES", "DIA_MARTES", "DIA_MIERCOLES", "DIA_JUEVES",
    "DIA_VIERNES", "DIA_SABADO", "DIA_DOMINGO",
]
IRRENUNCIABLE_DATES = {(1, 1), (5, 1), (9, 18), (9, 19), (12, 25)}
# Column order of the training dataset.
WEATHER_DERIVED_COLUMNS = (
    [f"LLUVIA_LAG_{lag}D" for lag in RAIN_LAGS]
    + [
        f"{prefix}_{window}D_PREV"
        for window in RAIN_WINDOWS
        for prefix in ["LLUVIA_PROMEDIO", "LLUVIA_TOTAL", "LLUVIA_DESV", "LLUVIA_MAX", "DIAS_SECOS"]
    ]
    + [
        "DIAS_DESDE_ULTIMA_LLUVIA", "VIENTO_MEDIO_lag_1", "HUM_MEDIA_lag_1",
        "TEMP_HUM_INDEX", "VIENTO_LLUVIA_INDEX", "STORM_COMPOUND_INDEX",
        "FIRE_DRY_INDEX_7D", "VPD", "VPD_MAX",
    ]
)
CALENDAR_COLUMNS = [
    "MES", "DIA_SEMANA", "ES_FIN_SEMANA", "ES_FERIADO", "ES_FERIADO_IRRENUNCIABLE",
    "ES_PRE_FERIADO", "MES_SIN", "MES_COS", "DIA_SIN", "DIA_COS", "DANO_SIN", "DANO_COS",
]


def days_since_rain(rain, start_index=0, start_value=np.nan):
    """Days since the last day with rain > 0.1 mm, counting up to the day before.

    Rows before `start_index` are NaN; `start_value` seeds the count at
    `start_index` (NaN: no previous count, as at the start of training data).
    A missing rain value yields NaN and leaves the count unchanged.
    """
    rain = np.asarray(rain, dtype=float)
    values = np.full(len(rain), np.nan)
    if start_index >= len(rain):
        return values
    values[start_index] = start_value
    count = 0.0 if np.isnan(start_value) else float(start_value)
    for i in range(start_index + 1, len(rain)):
        previous = rain[i - 1]
        if np.isnan(previous):
            continue
        count = 0.0 if previous > 0.1 else count + 1.0
        values[i] = count
    return values


def _previous_windows(values, window):
    """Rows of the `window` values before each day (NaN rows when incomplete)."""
    padded = np.concatenate([np.full(window, np.nan), values])
    return sliding_window_view(padded, window)[: len(values)]


def weather_derived_features(daily, days_since_rain_start=None):
    """Rain memory, weather lags and indices for a consecutive daily frame.

    `daily` holds one row per calendar day with the base weather columns
    (TEMP_*, HUM_*, VIENTO_*, LLUVIA). Features that need more history than the
    frame provides are NaN. `days_since_rain_start` is an optional
    (row position, value) seed for DIAS_DESDE_ULTIMA_LLUVIA.
    """
    index = pd.DatetimeIndex(pd.to_datetime(daily.index))
    if len(index) > 1 and not (np.diff(index.asi8) == 86_400 * 10**9).all():
        raise ValueError("weather_derived_features requiere dias consecutivos")
    rain = daily["LLUVIA"].to_numpy(dtype=float)
    result = pd.DataFrame(index=daily.index)

    result["VIENTO_MEDIO_lag_1"] = daily["VIENTO_MEDIO"].shift(1)
    result["HUM_MEDIA_lag_1"] = daily["HUM_MEDIA"].shift(1)
    for lag in RAIN_LAGS:
        result[f"LLUVIA_LAG_{lag}D"] = daily["LLUVIA"].shift(lag)
    for window in RAIN_WINDOWS:
        windows = _previous_windows(rain, window)
        with np.errstate(invalid="ignore"):
            result[f"LLUVIA_PROMEDIO_{window}D_PREV"] = np.mean(windows, axis=1)
            result[f"LLUVIA_TOTAL_{window}D_PREV"] = np.sum(windows, axis=1)
            result[f"LLUVIA_DESV_{window}D_PREV"] = np.std(windows, axis=1, ddof=1)
            result[f"LLUVIA_MAX_{window}D_PREV"] = np.max(windows, axis=1)
            dry = np.sum(windows <= 0.1, axis=1).astype(float)
        dry[np.isnan(windows).any(axis=1)] = np.nan
        result[f"DIAS_SECOS_{window}D_PREV"] = dry

    if days_since_rain_start is None:
        result["DIAS_DESDE_ULTIMA_LLUVIA"] = days_since_rain(rain)
    else:
        start_index, start_value = days_since_rain_start
        result["DIAS_DESDE_ULTIMA_LLUVIA"] = days_since_rain(rain, start_index, start_value)

    temp_media, hum_media = daily["TEMP_MEDIA"], daily["HUM_MEDIA"]
    temp_max = daily["TEMP_MAX"]
    result["TEMP_HUM_INDEX"] = temp_media * hum_media / 100
    result["VIENTO_LLUVIA_INDEX"] = daily["VIENTO_MEDIO"] * daily["LLUVIA"]
    result["STORM_COMPOUND_INDEX"] = daily["VIENTO_MAX"] * (1 + daily["LLUVIA"])
    result["FIRE_DRY_INDEX_7D"] = (
        temp_max * result["DIAS_SECOS_7D_PREV"] / (1 + result["LLUVIA_TOTAL_7D_PREV"])
    )
    es_media = 0.6108 * np.exp((17.27 * temp_media) / (temp_media + 237.3))
    result["VPD"] = es_media * (1 - hum_media / 100)
    es_max = 0.6108 * np.exp((17.27 * temp_max) / (temp_max + 237.3))
    result["VPD_MAX"] = es_max * (1 - daily["HUM_MIN"] / 100)
    return result[WEATHER_DERIVED_COLUMNS]


def calendar_features(dates):
    """Holiday, weekday and seasonal features for each date."""
    dates = pd.DatetimeIndex(pd.to_datetime(dates))
    years = range(dates.year.min(), dates.year.max() + 2)
    chile_holidays = holidays.Chile(years=years)
    day_strings = dates.strftime("%Y-%m-%d")
    next_strings = (dates + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    month, weekday, day_of_year = dates.month, dates.dayofweek, dates.dayofyear

    def irrenunciable(date, day_string):
        if (date.month, date.day) in IRRENUNCIABLE_DATES:
            return 1
        name = chile_holidays.get(day_string)
        return int(bool(name and ("elecciones" in name.lower() or "plebiscito" in name.lower())))

    result = pd.DataFrame(index=dates)
    result["MES"] = month
    result["DIA_SEMANA"] = weekday
    result["ES_FIN_SEMANA"] = np.isin(weekday, [5, 6]).astype(int)
    result["ES_FERIADO"] = [int(day in chile_holidays) for day in day_strings]
    result["ES_FERIADO_IRRENUNCIABLE"] = [
        irrenunciable(date, day) for date, day in zip(dates, day_strings)
    ]
    result["ES_PRE_FERIADO"] = [int(day in chile_holidays) for day in next_strings]
    result["MES_SIN"] = np.sin(2 * np.pi * month / 12)
    result["MES_COS"] = np.cos(2 * np.pi * month / 12)
    result["DIA_SIN"] = np.sin(2 * np.pi * weekday / 7)
    result["DIA_COS"] = np.cos(2 * np.pi * weekday / 7)
    result["DANO_SIN"] = np.sin(2 * np.pi * day_of_year / 365)
    result["DANO_COS"] = np.cos(2 * np.pi * day_of_year / 365)
    for position, column in enumerate(WEEKDAY_COLUMNS):
        result[column] = (weekday == position).astype(int)
    return result
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
