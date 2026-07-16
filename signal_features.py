"""Shared serving features for the high-resolution signal model."""

from __future__ import annotations

import numpy as np
import pandas as pd


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
