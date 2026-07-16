"""Fetch and aggregate serving-compatible Open-Meteo signal features v2."""

from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests


LATITUDE = -36.731106
LONGITUDE = -73.11023
VARIABLES = [
    "apparent_temperature", "rain", "showers", "weather_code",
    "surface_pressure", "cloud_cover", "cloud_cover_low", "cloud_cover_mid",
    "cloud_cover_high", "wind_direction_10m", "shortwave_radiation",
    "direct_radiation", "sunshine_duration", "freezing_level_height",
    "wet_bulb_temperature_2m", "boundary_layer_height",
]


def fetch_chunk(start, end):
    session = requests.Session()
    session.trust_env = False
    params = {
        "latitude": LATITUDE, "longitude": LONGITUDE,
        "start_date": str(start), "end_date": str(end),
        "hourly": ",".join(VARIABLES), "timezone": "America/Santiago",
    }
    last_error = None
    for attempt in range(4):
        try:
            response = session.get(
                "https://historical-forecast-api.open-meteo.com/v1/forecast",
                params=params, timeout=120,
            )
            response.raise_for_status()
            return pd.DataFrame(response.json()["hourly"])
        except Exception as exc:
            last_error = exc
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Open-Meteo failed for {start}..{end}: {last_error}")


def aggregate(hourly):
    frame = hourly.copy()
    frame["time"] = pd.to_datetime(frame["time"])
    frame["FECHA_DIA"] = frame["time"].dt.date
    for column in VARIABLES:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    direction = np.deg2rad(frame["wind_direction_10m"])
    frame["_wind_dir_sin"] = np.sin(direction)
    frame["_wind_dir_cos"] = np.cos(direction)
    frame["_storm_hour"] = (frame["weather_code"] >= 80).astype(float)
    frame["_thunder_hour"] = (frame["weather_code"] >= 95).astype(float)
    frame["_shower_hour"] = (frame["showers"] > 0.1).astype(float)
    frame["_cloudless_hour"] = (frame["cloud_cover"] < 20).astype(float)
    daily = frame.groupby("FECHA_DIA", sort=True).agg(
        WX2_APPARENT_TEMP_MAX=("apparent_temperature", "max"),
        WX2_APPARENT_TEMP_MEAN=("apparent_temperature", "mean"),
        WX2_APPARENT_TEMP_MIN=("apparent_temperature", "min"),
        WX2_RAIN_SUM=("rain", "sum"),
        WX2_SHOWERS_SUM=("showers", "sum"),
        WX2_SHOWERS_MAX=("showers", "max"),
        WX2_SHOWER_HOURS=("_shower_hour", "sum"),
        WX2_WEATHER_CODE_MAX=("weather_code", "max"),
        WX2_STORM_HOURS=("_storm_hour", "sum"),
        WX2_THUNDER_HOURS=("_thunder_hour", "sum"),
        WX2_SURFACE_PRESSURE_MEAN=("surface_pressure", "mean"),
        WX2_SURFACE_PRESSURE_MAX=("surface_pressure", "max"),
        WX2_SURFACE_PRESSURE_MIN=("surface_pressure", "min"),
        WX2_CLOUD_MEAN=("cloud_cover", "mean"),
        WX2_CLOUD_MAX=("cloud_cover", "max"),
        WX2_CLOUD_LOW_MEAN=("cloud_cover_low", "mean"),
        WX2_CLOUD_MID_MEAN=("cloud_cover_mid", "mean"),
        WX2_CLOUD_HIGH_MEAN=("cloud_cover_high", "mean"),
        WX2_CLOUDLESS_HOURS=("_cloudless_hour", "sum"),
        WX2_WIND_DIR_SIN=("_wind_dir_sin", "mean"),
        WX2_WIND_DIR_COS=("_wind_dir_cos", "mean"),
        WX2_SHORTWAVE_SUM=("shortwave_radiation", "sum"),
        WX2_SHORTWAVE_MAX=("shortwave_radiation", "max"),
        WX2_DIRECT_RADIATION_SUM=("direct_radiation", "sum"),
        WX2_SUNSHINE_SECONDS=("sunshine_duration", "sum"),
        WX2_FREEZING_LEVEL_MEAN=("freezing_level_height", "mean"),
        WX2_FREEZING_LEVEL_MIN=("freezing_level_height", "min"),
        WX2_WET_BULB_MEAN=("wet_bulb_temperature_2m", "mean"),
        WX2_WET_BULB_MAX=("wet_bulb_temperature_2m", "max"),
        WX2_BOUNDARY_LAYER_MEAN=("boundary_layer_height", "mean"),
        WX2_BOUNDARY_LAYER_MAX=("boundary_layer_height", "max"),
    )
    daily["WX2_APPARENT_TEMP_RANGE"] = (
        daily.pop("WX2_APPARENT_TEMP_MAX") - daily.pop("WX2_APPARENT_TEMP_MIN")
    )
    daily["WX2_SURFACE_PRESSURE_RANGE"] = (
        daily.pop("WX2_SURFACE_PRESSURE_MAX") - daily.pop("WX2_SURFACE_PRESSURE_MIN")
    )
    daily["WX2_WIND_DIR_CONCENTRATION"] = np.sqrt(
        daily["WX2_WIND_DIR_SIN"] ** 2 + daily["WX2_WIND_DIR_COS"] ** 2
    )
    return daily.reset_index()


def main():
    root = Path(__file__).resolve().parent.parent
    source = pd.read_csv(root / "02_data" / "augmented_emergency_data.csv", sep=";")
    start = pd.to_datetime(source["FECHA_DIA"]).min().date()
    end = pd.to_datetime(source["FECHA_DIA"]).max().date()
    chunks = []
    current = start
    while current <= end:
        chunk_end = min((pd.Timestamp(current) + pd.DateOffset(years=1) - pd.Timedelta(days=1)).date(), end)
        print(f"fetch {current}..{chunk_end}", flush=True)
        chunks.append(fetch_chunk(current, chunk_end))
        current = chunk_end + pd.Timedelta(days=1)
    result = aggregate(pd.concat(chunks, ignore_index=True))
    output = root / "05_research" / "data" / "historical_forecast_features_v2.csv"
    result.to_csv(output, sep=";", index=False)
    print(f"saved={output} rows={len(result)} features={len(result.columns) - 1}")


if __name__ == "__main__":
    main()
