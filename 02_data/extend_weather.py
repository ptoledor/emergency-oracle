"""Extiende los tres caches meteorologicos (Historical Forecast API) hasta una fecha.

Solo agrega fechas posteriores al ultimo dia de cada archivo; las filas
existentes no se modifican. Antes de agregar, descarga un traslape y reporta la
diferencia maxima contra lo ya guardado para verificar consistencia.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "05_research"))

import fetch_weather_signal_v2 as wx2  # noqa: E402
import weather_features as wx  # noqa: E402

ARCHIVE_VARIABLES = ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "precipitation"]


def _skew(series):
    vals = series.values
    std = np.std(vals) or 0.1
    return np.mean((vals - np.mean(vals)) ** 3) / std ** 3


def _kurt(series):
    vals = series.values
    std = np.std(vals) or 0.1
    return np.mean((vals - np.mean(vals)) ** 4) / std ** 4 - 3


def archive_daily(start, end):
    """Misma agregacion que clean_and_augment.py para weather_archive_talcahuano.csv."""
    hourly = wx.fetch_hourly_chunked(start, end)
    hourly = hourly[["time", *ARCHIVE_VARIABLES]].copy()
    hourly["FECHA_DIA"] = pd.to_datetime(hourly["time"]).dt.strftime("%Y-%m-%d")
    return hourly.groupby("FECHA_DIA").agg(
        TEMP_MAX=("temperature_2m", "max"), TEMP_MIN=("temperature_2m", "min"),
        TEMP_MEDIA=("temperature_2m", "mean"), TEMP_SKEW=("temperature_2m", _skew),
        TEMP_KURT=("temperature_2m", _kurt),
        HUM_MAX=("relative_humidity_2m", "max"), HUM_MIN=("relative_humidity_2m", "min"),
        HUM_MEDIA=("relative_humidity_2m", "mean"), HUM_SKEW=("relative_humidity_2m", _skew),
        HUM_KURT=("relative_humidity_2m", _kurt),
        VIENTO_MAX=("wind_speed_10m", "max"), VIENTO_MEDIO=("wind_speed_10m", "mean"),
        VIENTO_SKEW=("wind_speed_10m", _skew), VIENTO_KURT=("wind_speed_10m", _kurt),
        LLUVIA=("precipitation", "sum"),
    ).reset_index()


def wx_daily(start, end):
    return wx.aggregate_daily(wx.fetch_hourly_chunked(start, end))


def wx2_daily(start, end):
    daily = wx2.aggregate(wx2.fetch_chunk(start, end))
    daily["FECHA_DIA"] = pd.to_datetime(daily["FECHA_DIA"]).dt.strftime("%Y-%m-%d")
    return daily


def extend(path, sep, builder, end, overlap_days):
    current = pd.read_csv(path, sep=sep)
    current["FECHA_DIA"] = current["FECHA_DIA"].astype(str)
    last = pd.Timestamp(current["FECHA_DIA"].max())
    if last >= pd.Timestamp(end):
        print(f"{path.name}: ya cubre hasta {last.date()}")
        return
    start = (last - pd.Timedelta(days=overlap_days - 1)).strftime("%Y-%m-%d")
    fresh = builder(start, end)
    fresh = fresh[[c for c in current.columns if c in fresh.columns]]
    shared = fresh.merge(current, on="FECHA_DIA", suffixes=("", "_old"))
    numeric = [c for c in fresh.columns if c != "FECHA_DIA" and f"{c}_old" in shared]
    drift = max(
        (float(np.nanmax(np.abs(shared[c] - shared[f"{c}_old"]))) for c in numeric),
        default=0.0,
    )
    new_rows = fresh[fresh["FECHA_DIA"] > current["FECHA_DIA"].max()]
    combined = pd.concat([current, new_rows], ignore_index=True)
    combined.to_csv(path, sep=sep, index=False)
    print(
        f"{path.name}: traslape {len(shared)} dias, desvio max {drift:.4f}; "
        f"+{len(new_rows)} dias -> {combined['FECHA_DIA'].max()}"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--overlap-days", type=int, default=14)
    args = parser.parse_args()
    extend(ROOT / "02_data" / "weather_archive_talcahuano.csv", ",", archive_daily,
           args.end_date, args.overlap_days)
    extend(ROOT / "05_research" / "data" / "historical_forecast_features.csv", ";", wx_daily,
           args.end_date, args.overlap_days)
    extend(ROOT / "05_research" / "data" / "historical_forecast_features_v2.csv", ";", wx2_daily,
           args.end_date, args.overlap_days)


if __name__ == "__main__":
    main()
