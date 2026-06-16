from __future__ import annotations

import argparse
from pathlib import Path
import time

import numpy as np
import pandas as pd
import requests


LATITUDE = -36.731106
LONGITUDE = -73.11023
TIMEZONE = "America/Santiago"
HISTORICAL_FORECAST_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "precipitation",
    "precipitation_probability",
    "pressure_msl",
    "visibility",
    "wind_speed_10m",
    "wind_gusts_10m",
    "vapour_pressure_deficit",
    "cape",
    "soil_moisture_0_to_1cm",
    "soil_moisture_9_to_27cm",
    "et0_fao_evapotranspiration",
]

FEATURE_GROUPS = {
    "precipitation_wind": [
        "WX_GUST_MAX",
        "WX_GUST_MEAN",
        "WX_PRECIP_PROB_MAX",
        "WX_PRECIP_MAX_HOURLY",
        "WX_PRECIP_HOURS",
        "WX_HEAVY_RAIN_HOURS",
        "WX_WIND_RAIN_INDEX",
    ],
    "atmospheric": [
        "WX_PRESSURE_MEAN",
        "WX_PRESSURE_RANGE",
        "WX_DEWPOINT_MEAN",
        "WX_VISIBILITY_MIN",
        "WX_CAPE_MAX",
        "WX_CAPE_MEAN",
    ],
    "soil_fire": [
        "WX_VPD_MAX",
        "WX_VPD_MEAN",
        "WX_SOIL_MOISTURE_SURFACE_MEAN",
        "WX_SOIL_MOISTURE_DEEP_MEAN",
        "WX_ET0_SUM",
        "WX_LOW_HUMIDITY_HOURS",
        "WX_FIRE_WEATHER_INDEX",
    ],
}


def fetch_hourly(
    start_date: str,
    end_date: str,
    timeout: int = 90,
) -> pd.DataFrame:
    response = requests.get(
        HISTORICAL_FORECAST_URL,
        params={
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ",".join(HOURLY_VARIABLES),
            "timezone": TIMEZONE,
        },
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    hourly = pd.DataFrame(payload["hourly"])
    hourly["time"] = pd.to_datetime(hourly["time"], errors="raise")
    return hourly


def fetch_hourly_chunked(
    start_date: str,
    end_date: str,
    chunk_days: int = 180,
    retries: int = 3,
) -> pd.DataFrame:
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    parts = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + pd.Timedelta(days=chunk_days - 1), end)
        for attempt in range(1, retries + 1):
            try:
                part = fetch_hourly(
                    cursor.strftime("%Y-%m-%d"),
                    chunk_end.strftime("%Y-%m-%d"),
                    timeout=120,
                )
                parts.append(part)
                print(
                    f"downloaded={cursor:%Y-%m-%d}:{chunk_end:%Y-%m-%d}",
                    flush=True,
                )
                break
            except requests.RequestException:
                if attempt == retries:
                    raise
                time.sleep(attempt * 2)
        cursor = chunk_end + pd.Timedelta(days=1)
    return (
        pd.concat(parts, ignore_index=True)
        .drop_duplicates(subset=["time"])
        .sort_values("time")
        .reset_index(drop=True)
    )


def _column(frame: pd.DataFrame, name: str) -> pd.Series:
    if name not in frame:
        return pd.Series(np.nan, index=frame.index, dtype=float)
    return pd.to_numeric(frame[name], errors="coerce")


def aggregate_daily(hourly: pd.DataFrame) -> pd.DataFrame:
    frame = hourly.copy()
    frame["FECHA_DIA"] = pd.to_datetime(frame["time"]).dt.strftime("%Y-%m-%d")
    numeric_columns = [column for column in HOURLY_VARIABLES if column in frame]
    frame[numeric_columns] = frame[numeric_columns].apply(
        pd.to_numeric, errors="coerce"
    )

    frame["_precip_hour"] = (_column(frame, "precipitation") > 0.1).astype(float)
    frame["_heavy_rain_hour"] = (_column(frame, "precipitation") >= 5.0).astype(float)
    frame["_low_humidity_hour"] = (
        _column(frame, "relative_humidity_2m") < 30.0
    ).astype(float)

    daily = frame.groupby("FECHA_DIA", sort=True).agg(
        WX_GUST_MAX=("wind_gusts_10m", "max"),
        WX_GUST_MEAN=("wind_gusts_10m", "mean"),
        WX_PRECIP_PROB_MAX=("precipitation_probability", "max"),
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
        WX_SOIL_MOISTURE_SURFACE_MEAN=("soil_moisture_0_to_1cm", "mean"),
        WX_SOIL_MOISTURE_DEEP_MEAN=("soil_moisture_9_to_27cm", "mean"),
        WX_ET0_SUM=("et0_fao_evapotranspiration", "sum"),
        WX_LOW_HUMIDITY_HOURS=("_low_humidity_hour", "sum"),
        WX_TEMP_MAX=("temperature_2m", "max"),
        WX_HUMIDITY_MIN=("relative_humidity_2m", "min"),
        WX_WIND_MAX=("wind_speed_10m", "max"),
    ).reset_index()

    daily["WX_PRESSURE_RANGE"] = daily["WX_PRESSURE_MAX"] - daily["WX_PRESSURE_MIN"]
    daily["WX_WIND_RAIN_INDEX"] = (
        daily["WX_GUST_MAX"].fillna(0)
        * np.log1p(daily["WX_PRECIP_MAX_HOURLY"].fillna(0))
    )
    dry_humidity = np.clip((30.0 - daily["WX_HUMIDITY_MIN"]) / 30.0, 0.0, None)
    dry_soil = np.clip(
        1.0 - daily["WX_SOIL_MOISTURE_SURFACE_MEAN"].fillna(0.5),
        0.0,
        1.0,
    )
    daily["WX_FIRE_WEATHER_INDEX"] = (
        daily["WX_GUST_MAX"].fillna(0)
        * (1.0 + daily["WX_VPD_MAX"].fillna(0))
        * (1.0 + dry_humidity)
        * (1.0 + dry_soil)
    )
    return daily.drop(
        columns=["WX_PRESSURE_MAX", "WX_PRESSURE_MIN"],
        errors="ignore",
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download and aggregate historical forecast weather signals."
    )
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("05_research/data/historical_forecast_features.csv"),
    )
    args = parser.parse_args()

    hourly = fetch_hourly_chunked(args.start_date, args.end_date)
    daily = aggregate_daily(hourly)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    daily.to_csv(args.output, sep=";", index=False)
    print(f"rows={len(daily)}")
    print(f"features={len(daily.columns) - 1}")
    print(f"output={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
