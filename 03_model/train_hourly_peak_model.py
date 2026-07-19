"""Train and validate the secondary hourly peak model.

The daily forecast remains untouched. This model only allocates the daily
expectation across local clock hours and announces separated probability peaks.
"""

from __future__ import annotations

import argparse
import json
import pickle
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from xgboost import XGBRegressor


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "02_data"))

from dedup import mark_duplicates  # noqa: E402
from hourly_peak import (  # noqa: E402
    ALL_FEATURES,
    CALENDAR_FEATURES,
    PROJECT_TIMEZONE,
    WEATHER_FEATURES,
    build_hourly_features,
)


LATITUDE = -36.731106
LONGITUDE = -73.11023
WEATHER_QUERY = ",".join(WEATHER_FEATURES)


def load_hourly_target() -> pd.DataFrame:
    raw = pd.read_csv(
        PROJECT_ROOT / "02_data" / "compiled_scraped_data.csv",
        sep=";",
    )
    daily = pd.read_csv(
        PROJECT_ROOT / "02_data" / "augmented_emergency_data.csv",
        sep=";",
        usecols=["FECHA_DIA", "EVENTOS"],
    )
    daily["FECHA_DIA"] = pd.to_datetime(daily["FECHA_DIA"]).dt.date

    _, incident_flags = mark_duplicates(raw, "Fecha", "Texto")
    incidents = raw[
        raw.index.map(lambda index: incident_flags.get(index, True))
    ].copy()
    incidents["LOCAL_DT"] = pd.to_datetime(
        incidents["Fecha"], utc=True
    ).dt.tz_convert(PROJECT_TIMEZONE)
    incidents["date"] = incidents["LOCAL_DT"].dt.date
    incidents["hour"] = incidents["LOCAL_DT"].dt.hour.astype(int)
    valid_dates = set(daily["FECHA_DIA"])
    incidents = incidents[incidents["date"].isin(valid_dates)]

    counts = incidents.groupby(["date", "hour"]).size().rename("y")
    dates = sorted(valid_dates)
    target = pd.MultiIndex.from_product(
        [dates, range(24)], names=["date", "hour"]
    ).to_frame(index=False)
    target = target.merge(counts.reset_index(), on=["date", "hour"], how="left")
    target["y"] = target["y"].fillna(0.0).astype(float)

    target_total = float(daily["EVENTOS"].sum())
    if not np.isclose(target["y"].sum(), target_total):
        raise RuntimeError(
            "Hourly target does not reconcile with the active daily counting policy: "
            f"hourly={target['y'].sum():.0f}, daily={target_total:.0f}"
        )
    target["time"] = pd.to_datetime([
        f"{date:%Y-%m-%d}T{hour:02d}:00"
        for date, hour in zip(target["date"], target["hour"])
    ])
    return target


def fetch_weather(start_date, end_date, cache_path: Path, refresh: bool) -> pd.DataFrame:
    if cache_path.exists() and not refresh:
        cached = pd.read_csv(cache_path)
        cached["time"] = pd.to_datetime(cached["time"])
        if (
            cached["time"].min().date() <= start_date
            and cached["time"].max().date() >= end_date
        ):
            return cached

    rows = []
    for year in range(start_date.year, end_date.year + 1):
        chunk_start = max(start_date, pd.Timestamp(year=year, month=1, day=1).date())
        chunk_end = min(end_date, pd.Timestamp(year=year, month=12, day=31).date())
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "start_date": chunk_start.strftime("%Y-%m-%d"),
            "end_date": chunk_end.strftime("%Y-%m-%d"),
            "hourly": WEATHER_QUERY,
            "timezone": PROJECT_TIMEZONE,
            "format": "json",
        }
        response = requests.get(url, params=params, timeout=120)
        response.raise_for_status()
        rows.append(pd.DataFrame(response.json()["hourly"]))
        time.sleep(0.5)

    weather = pd.concat(rows, ignore_index=True)
    weather["time"] = pd.to_datetime(weather["time"])
    weather = weather.drop_duplicates("time", keep="last").sort_values("time")
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    weather.to_csv(cache_path, index=False)
    return weather


def make_model(seed: int, weather: bool) -> XGBRegressor:
    return XGBRegressor(
        objective="count:poisson",
        n_estimators=420 if weather else 320,
        max_depth=3,
        learning_rate=0.035,
        subsample=0.85,
        colsample_bytree=0.9,
        min_child_weight=10,
        reg_lambda=12,
        n_jobs=4,
        random_state=seed,
    )


def normalise_by_day(frame: pd.DataFrame, rates: np.ndarray) -> pd.Series:
    rates = np.clip(np.asarray(rates, dtype=float), 1e-9, None)
    result = pd.Series(rates, index=frame.index)
    return result / result.groupby(frame["date"]).transform("sum")


def separated_peaks(probability: np.ndarray, count: int = 3, separation: int = 3):
    selected = []
    for hour in np.argsort(-probability):
        if all(
            min((hour - other) % 24, (other - hour) % 24) >= separation
            for other in selected
        ):
            selected.append(int(hour))
        if len(selected) == count:
            break
    return selected


def score_predictions(frame: pd.DataFrame, probability: pd.Series) -> dict:
    scored = frame[["date", "hour", "y"]].copy()
    scored["probability"] = probability
    total = float(scored["y"].sum())
    log_loss = float(
        -(scored["y"] * np.log(scored["probability"].clip(1e-12, 1))).sum()
        / total
    )
    exact_hits = 0.0
    window_hits = 0.0
    for _, day in scored.groupby("date"):
        peaks = separated_peaks(day["probability"].to_numpy())
        exact_hits += float(day[day["hour"].isin(peaks)]["y"].sum())
        window = {(hour + delta) % 24 for hour in peaks for delta in (-1, 0, 1)}
        window_hits += float(day[day["hour"].isin(window)]["y"].sum())
    return {
        "event_log_loss": log_loss,
        "top3_exact_hit_rate": exact_hits / total,
        "top3_within_1h_hit_rate": window_hits / total,
    }


def temporal_validation(frame: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    dates = np.array(sorted(frame["date"].unique()))
    n_dates = len(dates)
    folds = [
        (int(n_dates * 0.55), int(n_dates * 0.70)),
        (int(n_dates * 0.70), int(n_dates * 0.85)),
        (int(n_dates * 0.85), n_dates),
    ]
    weights = [0.0, 0.25, 0.5, 0.75, 1.0]
    records = []
    for fold, (train_end, test_end) in enumerate(folds):
        train_dates = set(dates[:train_end])
        test_dates = set(dates[train_end:test_end])
        train = frame[frame["date"].isin(train_dates)]
        test = frame[frame["date"].isin(test_dates)]

        calendar_model = make_model(17 + fold, weather=False).fit(
            train[CALENDAR_FEATURES], train["y"]
        )
        weather_model = make_model(117 + fold, weather=True).fit(
            train[ALL_FEATURES], train["y"]
        )
        calendar_probability = normalise_by_day(
            test, calendar_model.predict(test[CALENDAR_FEATURES])
        )
        weather_probability = normalise_by_day(
            test, weather_model.predict(test[ALL_FEATURES])
        )
        for weight in weights:
            probability = (
                weight * weather_probability
                + (1.0 - weight) * calendar_probability
            )
            metrics = score_predictions(test, probability)
            records.append({
                "fold": fold,
                "weather_weight": weight,
                **metrics,
            })

    metrics = pd.DataFrame(records)
    summary = metrics.groupby("weather_weight", as_index=False).agg({
        "event_log_loss": "mean",
        "top3_exact_hit_rate": "mean",
        "top3_within_1h_hit_rate": "mean",
    })
    # Operational peaks need to be useful, but probability calibration remains
    # the dominant component of the locked selection rule.
    summary["selection_score"] = (
        summary["event_log_loss"]
        - 0.10 * summary["top3_within_1h_hit_rate"]
    )
    selected_weight = float(
        summary.sort_values(["selection_score", "event_log_loss"]).iloc[0][
            "weather_weight"
        ]
    )
    return summary, selected_weight


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--refresh-weather", action="store_true")
    parser.add_argument(
        "--cache",
        type=Path,
        default=PROJECT_ROOT / ".tmp" / "hourly_weather_talcahuano.csv",
    )
    args = parser.parse_args()

    target = load_hourly_target()
    weather = fetch_weather(
        target["date"].min(),
        target["date"].max(),
        args.cache,
        args.refresh_weather,
    )
    frame = target.merge(weather, on="time", how="left", validate="one_to_one")
    frame = build_hourly_features(frame)
    missing_weather = frame[WEATHER_FEATURES].isna().mean().max()
    if missing_weather > 0.01:
        raise RuntimeError(f"Hourly weather coverage is incomplete: {missing_weather:.2%}")

    summary, weather_weight = temporal_validation(frame)
    calendar_model = make_model(417, weather=False).fit(
        frame[CALENDAR_FEATURES], frame["y"]
    )
    weather_model = make_model(517, weather=True).fit(
        frame[ALL_FEATURES], frame["y"]
    )
    selected = summary[summary["weather_weight"] == weather_weight].iloc[0]
    artifact = {
        "version": "hourly_peaks_v1",
        "role": "secondary_hourly_allocation",
        "timezone": PROJECT_TIMEZONE,
        "calendar_features": CALENDAR_FEATURES,
        "weather_features": ALL_FEATURES,
        "weather_weight": weather_weight,
        "calendar_model": calendar_model,
        "weather_model": weather_model,
        "trained_through": target["date"].max().strftime("%Y-%m-%d"),
        "training_days": int(target["date"].nunique()),
        "training_events": int(target["y"].sum()),
        "selection_metrics": selected.to_dict(),
        "validation_summary": summary.to_dict(orient="records"),
        "peak_policy": {"low": 2, "normal": 3, "high": 4, "min_separation_hours": 3},
    }
    output = PROJECT_ROOT / "03_model" / "saved_models" / "hourly_peak_model_v1.pkl"
    with open(output, "wb") as stream:
        pickle.dump(artifact, stream)

    metrics_dir = PROJECT_ROOT / "05_research" / "results" / "hourly_peaks_v1"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    summary.to_csv(metrics_dir / "temporal_metrics.csv", sep=";", index=False)
    with open(metrics_dir / "metadata.json", "w", encoding="utf-8") as stream:
        json.dump(
            {key: value for key, value in artifact.items() if not key.endswith("model")},
            stream,
            ensure_ascii=False,
            indent=2,
            default=float,
        )

    print(summary.to_string(index=False, float_format="%.4f"))
    print(f"Selected weather weight: {weather_weight:.2f}")
    print(f"Saved: {output}")


if __name__ == "__main__":
    main()
