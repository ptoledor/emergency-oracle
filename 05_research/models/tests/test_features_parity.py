"""Shared feature builders must reproduce the training dataset exactly."""

from pathlib import Path

import numpy as np
import pandas as pd

from features import CALENDAR_COLUMNS, calendar_features, weather_derived_features

ROOT = Path(__file__).resolve().parents[3]


def load_frames():
    dataset = pd.read_csv(ROOT / "02_data" / "augmented_emergency_data.csv", sep=";")
    weather = pd.read_csv(ROOT / "02_data" / "weather_archive_talcahuano.csv")
    weather.index = pd.to_datetime(weather["FECHA_DIA"])
    calendar = pd.date_range(weather.index.min(), weather.index.max(), freq="D")
    weather = weather.drop(columns="FECHA_DIA").reindex(calendar).ffill()
    return dataset.set_index(pd.to_datetime(dataset["FECHA_DIA"])), weather


def test_weather_derived_features_match_training_dataset():
    dataset, weather = load_frames()
    derived = weather_derived_features(weather).loc[dataset.index]
    for column in derived.columns:
        np.testing.assert_allclose(
            derived[column].to_numpy(dtype=float),
            dataset[column].to_numpy(dtype=float),
            rtol=0, atol=1e-9, err_msg=column,
        )


def test_calendar_features_match_training_dataset():
    dataset, _ = load_frames()
    calendar = calendar_features(dataset.index)
    for column in CALENDAR_COLUMNS:
        np.testing.assert_allclose(
            calendar[column].to_numpy(dtype=float),
            dataset[column].to_numpy(dtype=float),
            rtol=0, atol=1e-12, err_msg=column,
        )


def test_serving_history_matches_training_history():
    from features import CATEGORY_SOURCES, EVENT_HISTORY_COLUMNS, event_history_features, event_history_table

    dataset, _ = load_frames()
    calendar = pd.date_range(dataset.index.min(), dataset.index.max(), freq="D")
    counts = dataset[["EVENTOS", *CATEGORY_SOURCES.values()]].reindex(calendar)
    training = event_history_table(counts)
    # Rows whose 30-day window starts before the dataset cannot be rebuilt from it.
    comparable = dataset.index[dataset.index >= calendar[0] + pd.Timedelta(days=30)]
    events = counts["EVENTOS"]
    for date in comparable:
        window = events.loc[date - pd.Timedelta(days=30): date - pd.Timedelta(days=1)]
        serving = event_history_features(window.to_numpy())
        for column in EVENT_HISTORY_COLUMNS:
            if column in serving:
                expected = training.loc[date, column]
                assert np.isclose(serving[column], expected, atol=1e-9, equal_nan=True), (date, column)
                assert np.isclose(dataset.loc[date, column], expected, atol=1e-9, equal_nan=True), (date, column)
