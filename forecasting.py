"""Six-day forecast shared by the dashboard and the command-line predictor.

Weather comes in as hourly Open-Meteo data (or the local fallback), history
from the daily training dataset. Days after the first feed their predicted
count back as event history, as validated in evaluate_forecast_horizons.py.
"""

from __future__ import annotations

import datetime as dt
import pickle
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests

import model_components
from features import (
    CALENDAR_COLUMNS,
    CATEGORY_LAG_FEATURES,
    CATEGORY_SOURCES,
    OPEN_METEO_HOURLY_QUERY,
    WEEKDAY_COLUMNS,
    aggregate_weather_daily,
    calendar_features,
    event_history_features,
    prepare_event_history,
    weather_derived_features,
)


PROJECT_ROOT = Path(__file__).resolve().parent
MODELS_DIR = PROJECT_ROOT / "03_model" / "saved_models"
WEATHER_ARCHIVE_PATH = PROJECT_ROOT / "02_data" / "weather_archive_talcahuano.csv"
LATITUDE, LONGITUDE = -36.731106, -73.11023
FORECAST_DAYS = 6
HISTORY_DAYS = 30
DIAS_ES = {
    "Monday": "Lunes", "Tuesday": "Martes", "Wednesday": "Miércoles",
    "Thursday": "Jueves", "Friday": "Viernes", "Saturday": "Sábado", "Sunday": "Domingo",
}


def force_single_thread_model(model):
    if hasattr(model, "n_jobs"):
        model.n_jobs = 1
    return model


def _load_pickle(path):
    with Path(path).open("rb") as stream:
        return pickle.load(stream)


def load_forecast_models(models_dir=MODELS_DIR, prefix="_climatic_augmented"):
    """Active count model, its classifier and the optional category artifacts."""
    models_dir = Path(models_dir)
    reg_path, clf_path, meta_path = model_components.resolve_model_path(models_dir, prefix)
    models = {
        "regressor": force_single_thread_model(_load_pickle(reg_path)),
        "classifier": force_single_thread_model(_load_pickle(clf_path)),
        "metadata": _load_pickle(meta_path),
        "category_risk": None,
        "category_composition": None,
        "fifth_company": None,
    }
    risk_path = models_dir / "category_risk_models.pkl"
    if risk_path.exists():
        risk = _load_pickle(risk_path)
        for details in risk.get("models", {}).values():
            details["model"] = force_single_thread_model(details["model"])
        models["category_risk"] = risk
    composition_path = models_dir / "category_composition_models.pkl"
    if composition_path.exists():
        composition = _load_pickle(composition_path)
        for details in composition.get("groups", {}).values():
            details["baseline_model"] = force_single_thread_model(details["baseline_model"])
            details["expected_model"] = force_single_thread_model(details["expected_model"])
        models["category_composition"] = composition
    fifth_path = models_dir / "fifth_company_models.pkl"
    if fifth_path.exists():
        fifth = _load_pickle(fifth_path)
        fifth["regressor"] = force_single_thread_model(fifth["regressor"])
        fifth["classifier"] = force_single_thread_model(fifth["classifier"])
        models["fifth_company"] = fifth
    return models


def weather_query_range(start_date, is_historical):
    """Hourly weather needed: 30 days of rain memory plus the forecast days."""
    end_offset = FORECAST_DAYS - 1 if is_historical else 9
    return (
        start_date - dt.timedelta(days=HISTORY_DAYS),
        start_date + dt.timedelta(days=end_offset),
    )


def local_weather_fallback(start_date, end_date, archive_path=WEATHER_ARCHIVE_PATH):
    """Synthetic hourly weather from the local daily archive (exact day or climatology)."""
    archive = pd.read_csv(archive_path)
    archive["FECHA_DIA"] = pd.to_datetime(archive["FECHA_DIA"]).dt.date
    archive["MONTH_DAY"] = archive["FECHA_DIA"].apply(lambda value: (value.month, value.day))
    archive["MONTH"] = archive["FECHA_DIA"].apply(lambda value: value.month)
    numeric_cols = [
        "TEMP_MAX", "TEMP_MIN", "TEMP_MEDIA",
        "HUM_MAX", "HUM_MIN", "HUM_MEDIA",
        "VIENTO_MAX", "VIENTO_MEDIO", "LLUVIA",
    ]
    global_medians = archive[numeric_cols].median(numeric_only=True)

    rows = []
    for offset in range((end_date - start_date).days + 1):
        target_date = start_date + dt.timedelta(days=offset)
        exact = archive[archive["FECHA_DIA"] == target_date]
        if not exact.empty:
            source_row = exact.iloc[0]
        else:
            same_day = archive[archive["MONTH_DAY"] == (target_date.month, target_date.day)]
            if not same_day.empty:
                source_row = same_day[numeric_cols].median(numeric_only=True)
            else:
                same_month = archive[archive["MONTH"] == target_date.month]
                source_row = (
                    same_month[numeric_cols].median(numeric_only=True)
                    if not same_month.empty
                    else global_medians
                )

        temp_mean = float(source_row.get("TEMP_MEDIA", global_medians["TEMP_MEDIA"]))
        temp_amp = max(
            0.1,
            (float(source_row.get("TEMP_MAX", temp_mean)) - float(source_row.get("TEMP_MIN", temp_mean))) / 2,
        )
        hum_mean = float(source_row.get("HUM_MEDIA", global_medians["HUM_MEDIA"]))
        wind_mean = float(source_row.get("VIENTO_MEDIO", global_medians["VIENTO_MEDIO"]))
        rain_daily = max(0.0, float(source_row.get("LLUVIA", global_medians["LLUVIA"])))

        for hour in range(24):
            angle = 2 * np.pi * (hour - 15) / 24
            rows.append({
                "time": f"{target_date.strftime('%Y-%m-%d')}T{hour:02d}:00",
                "temperature_2m": temp_mean + temp_amp * np.cos(angle),
                "relative_humidity_2m": float(np.clip(hum_mean - 8 * np.cos(angle), 0, 100)),
                "wind_speed_10m": wind_mean,
                "precipitation": rain_daily / 24,
            })

    hourly = {key: [row[key] for row in rows] for key in [
        "time", "temperature_2m", "relative_humidity_2m", "wind_speed_10m", "precipitation",
    ]}
    hourly["_source"] = ["local_fallback"] * len(rows)
    return hourly


def fetch_weather(start_date, is_historical, retries=3):
    """Hourly Open-Meteo weather (archive or live forecast), local fallback on failure."""
    query_start, query_end = weather_query_range(start_date, is_historical)
    if is_historical:
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {"start_date": f"{query_start:%Y-%m-%d}", "end_date": f"{query_end:%Y-%m-%d}"}
    else:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {"past_days": HISTORY_DAYS, "forecast_days": 10}
    params.update({
        "latitude": LATITUDE, "longitude": LONGITUDE, "hourly": OPEN_METEO_HOURLY_QUERY,
        "timezone": "America/Santiago", "format": "json",
    })
    for attempt in range(1, retries + 1):
        try:
            session = requests.Session()
            session.trust_env = False
            response = session.get(url, params=params, timeout=30)
            response.raise_for_status()
            hourly = response.json()["hourly"]
            hourly["_source"] = ["open_meteo"] * len(hourly.get("time", []))
            return hourly
        except Exception:
            if attempt < retries:
                time.sleep(2 * attempt)
    return local_weather_fallback(query_start, query_end)


def predict_category_composition(artifact, feature_frame, total_prediction):
    """Expected calls per emergency group, reconciled with the official total."""
    if not artifact:
        return None
    baseline_values = {}
    expected_values = {}
    for group_name in artifact.get("group_order", []):
        details = artifact["groups"][group_name]
        baseline_values[group_name] = float(np.clip(
            details["baseline_model"].predict(feature_frame[details["baseline_feature_cols"]])[0],
            0, None,
        ))
        expected_values[group_name] = float(np.clip(
            details["expected_model"].predict(feature_frame[details["expected_feature_cols"]])[0],
            0, None,
        ))

    baseline_total = float(sum(baseline_values.values()))
    expected_total = float(sum(expected_values.values()))
    official_total = max(float(total_prediction), 0.0)
    if expected_total <= 1e-9:
        expected_values = dict(baseline_values)
        expected_total = max(baseline_total, 1e-9)
    scale = official_total / expected_total
    reconciled = {name: value * scale for name, value in expected_values.items()}
    deltas = {name: reconciled[name] - baseline_values[name] for name in reconciled}
    positive_delta_total = float(sum(max(value, 0.0) for value in deltas.values()))
    groups = {}
    for group_name in artifact.get("group_order", []):
        count = reconciled[group_name]
        groups[group_name] = {
            "label": artifact["groups"][group_name]["label"],
            "count": count,
            "share_total": count / official_total if official_total > 1e-9 else 0.0,
            "baseline": baseline_values[group_name],
            "delta": deltas[group_name],
            "share_positive_delta": (
                max(deltas[group_name], 0.0) / positive_delta_total
                if positive_delta_total > 1e-9 and official_total > baseline_total
                else 0.0
            ),
        }
    return {
        "baseline_total": baseline_total,
        "forecast_total": official_total,
        "delta_total": official_total - baseline_total,
        "groups": groups,
    }


def _initial_state(history_frame, start_date):
    """Event history, category lags and shares at the forecast origin."""
    history = pd.DataFrame()
    try:
        history = history_frame[
            history_frame["FECHA_DIA"] < start_date.strftime("%Y-%m-%d")
        ].sort_values("FECHA_DIA")
        details = prepare_event_history(history, start_date, days=HISTORY_DAYS)
    except Exception:
        details = {
            "values": [1.5] * HISTORY_DAYS, "mode": "historical_baseline",
            "age_days": None, "last_observed_date": None,
        }

    category_lags = {column: 0.0 for column in CATEGORY_LAG_FEATURES}
    category_shares = {column: 0.0 for column in CATEGORY_LAG_FEATURES}
    if not history.empty:
        latest = history.iloc[-1]
        recent = history.tail(365)
        recent_total = float(recent["EVENTOS"].sum())
        for lag_column, source_column in CATEGORY_SOURCES.items():
            if details["mode"] == "observed":
                category_lags[lag_column] = float(latest.get(source_column, 0.0))
            else:
                category_mean = pd.to_numeric(
                    recent.get(source_column, pd.Series(dtype=float)), errors="coerce"
                ).mean()
                category_lags[lag_column] = float(0.0 if pd.isna(category_mean) else category_mean)
            if recent_total > 0:
                category_shares[lag_column] = float(recent[source_column].sum() / recent_total)

    days_since_rain = 10.0
    previous = history_frame[
        history_frame["FECHA_DIA"] == (start_date - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    ]
    if not previous.empty and not pd.isna(previous["DIAS_DESDE_ULTIMA_LLUVIA"].values[0]):
        days_since_rain = float(previous["DIAS_DESDE_ULTIMA_LLUVIA"].values[0])
    return details, category_lags, category_shares, days_since_rain


def _daily_features(weather_hourly, start_date, days_since_rain_seed):
    """Daily weather, derived and calendar features for every forecast day."""
    weather = aggregate_weather_daily(pd.DataFrame(weather_hourly))
    weather.index = pd.to_datetime(weather.index)
    first, last = weather.index.min(), weather.index.max()
    daily = weather.reindex(pd.date_range(first, last, freq="D"))
    seed_date = pd.Timestamp(start_date - dt.timedelta(days=1))
    seed = (
        (daily.index.get_loc(seed_date), days_since_rain_seed)
        if seed_date in daily.index else None
    )
    derived = weather_derived_features(daily, days_since_rain_start=seed)
    calendar = calendar_features(daily.index)[CALENDAR_COLUMNS + WEEKDAY_COLUMNS]
    return pd.concat([daily, derived, calendar], axis=1), set(weather.index)


def forecast_days(start_date, weather_hourly, history_frame, models, days=FORECAST_DAYS):
    """Forecast `days` consecutive days from `start_date`.

    Returns (predictions, classification_threshold, high_activity_threshold);
    `predictions` holds one dict per day with counts, probabilities and the
    weather shown in the dashboard cards.
    """
    regressor, classifier = models["regressor"], models["classifier"]
    metadata = models["metadata"]
    history, category_lags, category_shares, days_since_rain_seed = _initial_state(
        history_frame, start_date
    )
    events = list(history["values"])
    features_by_day, weather_days = _daily_features(weather_hourly, start_date, days_since_rain_seed)

    direct_horizon_model = hasattr(regressor, "predict_horizon")
    origin_events = list(events)
    predictions = []
    for step in range(days):
        day = start_date + dt.timedelta(days=step)
        timestamp = pd.Timestamp(day)
        if timestamp not in weather_days:
            raise ValueError(f"La fecha {day:%Y-%m-%d} no tiene datos de clima disponibles.")
        features = features_by_day.loc[timestamp].to_dict()
        features.update(event_history_features(origin_events if direct_horizon_model else events))
        features.update(category_lags)
        X_all = pd.DataFrame([features])
        X_pred = X_all[metadata["feature_cols"]]

        if direct_horizon_model:
            count = float(np.clip(regressor.predict_horizon(X_pred, step + 1)[0], 0, None))
        elif hasattr(regressor, "category_models"):
            count = float(np.clip(regressor.predict(X_all)[0], 0, None))
        else:
            count = float(np.clip(regressor.predict(X_pred)[0], 0, None))
        if hasattr(classifier, "predict_proba_horizon"):
            probability = float(classifier.predict_proba_horizon(X_pred, step + 1)[0, 1])
        else:
            probability = float(classifier.predict_proba(X_pred)[0, 1])

        category_probabilities = {}
        if models.get("category_risk"):
            for group_name, details in models["category_risk"].get("models", {}).items():
                group_features = details.get("feature_cols", metadata["feature_cols"])
                category_probabilities[group_name] = float(
                    details["model"].predict_proba(X_all[group_features])[0, 1]
                )

        composition = None
        try:
            composition = predict_category_composition(
                models.get("category_composition"), X_all, count
            )
        except Exception as exc:
            print(f"No se pudo calcular la composicion por categoria: {exc}")

        fifth_count = fifth_probability = np.nan
        if models.get("fifth_company"):
            fifth = models["fifth_company"]
            fifth_X = X_all[fifth["metadata"]["feature_cols"]]
            fifth_count = float(np.clip(fifth["regressor"].predict(fifth_X)[0], 0, None))
            fifth_probability = float(fifth["classifier"].predict_proba(fifth_X)[0, 1])

        if not direct_horizon_model:
            events = events[1:] + [count]
            category_lags = {
                column: float(count * category_shares[column]) for column in CATEGORY_LAG_FEATURES
            }

        rescue = category_probabilities.get(
            "rescate_vehicular", category_probabilities.get("rescate", np.nan)
        )
        predictions.append({
            "Fecha": day,
            "FechaStr": f"{day:%Y-%m-%d}",
            "Dia": DIAS_ES[day.strftime("%A")],
            "Prediccion": count,
            "Lag_Mode": history["mode"],
            "Lag_Age_Days": history["age_days"],
            "Last_Observed_Date": history["last_observed_date"],
            "Prob_Alta": probability,
            "Prob_RVehicular_Alto": rescue,
            "Prob_Rescate_Alto": rescue,
            "Prob_Incendio_Alto": category_probabilities.get("incendio", np.nan),
            "Prob_Climaticas_Alto": category_probabilities.get("climaticas", np.nan),
            "Category_Composition": composition,
            "Pred_5ta_Cia": fifth_count,
            "Prob_5ta_Cia_Alta": fifth_probability,
            "Temp_Max": float(features["TEMP_MAX"]),
            "Temp_Media": float(features["TEMP_MEDIA"]),
            "Hum_Media": float(features["HUM_MEDIA"]),
            "Viento_Medio": float(features["VIENTO_MEDIO"]),
            "Rafaga_Media": float(features.get("WX_GUST_MEAN", features["VIENTO_MEDIO"])),
            "Lluvia": float(features["LLUVIA"]),
        })

    return (
        predictions,
        float(metadata.get("classification_threshold", 0.25)),
        float(metadata.get("umbral_alta_actividad", 7.0)),
    )
