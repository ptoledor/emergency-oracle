"""Run the six-day forecast with fixed inputs for golden tests.

Weather is supplied explicitly (a recorded Open-Meteo window or the local
fallback), so the result never depends on the network or on the current date.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import numpy as np
import pandas as pd

from forecasting import (
    forecast_days, load_forecast_models, local_weather_fallback, weather_query_range,
)


ROOT = Path(__file__).resolve().parents[3]
FIXTURES = Path(__file__).resolve().parent / "fixtures"
GOLDEN_PATH = FIXTURES / "golden_forecast.json"
# Origins: observed history with real weather, observed history with the
# local fallback, and a stale dataset (neutral history) with the fallback.
GOLDEN_CASES = [
    {"name": "invierno_clima_real", "origin": "2025-06-10", "weather": "weather_2025-06-10.csv"},
    {"name": "verano_fallback", "origin": "2026-01-15", "weather": None},
    {"name": "datos_vencidos_fallback", "origin": "2026-10-23", "weather": None},
]


def load_forecast_inputs():
    history = pd.read_csv(ROOT / "02_data" / "augmented_emergency_data.csv", sep=";")
    history = history.sort_values("FECHA_DIA").reset_index(drop=True)
    return {"models": load_forecast_models(), "history": history}


def case_weather(case):
    origin = dt.date.fromisoformat(case["origin"])
    if case["weather"] is None:
        return local_weather_fallback(*weather_query_range(origin, is_historical=True))
    hourly = pd.read_csv(FIXTURES / case["weather"], sep=";")
    return {column: hourly[column].tolist() for column in hourly.columns}


def _plain(value):
    if isinstance(value, dict):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    if isinstance(value, (dt.date, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, (np.floating, float)):
        return None if np.isnan(value) else float(value)
    if isinstance(value, (np.integer, int)) and not isinstance(value, bool):
        return int(value)
    return value


def run_case(inputs, case):
    origin = dt.date.fromisoformat(case["origin"])
    predictions, threshold, high_activity = forecast_days(
        origin, case_weather(case), inputs["history"], inputs["models"]
    )
    return {
        "classification_threshold": float(threshold),
        "umbral_alta_actividad": float(high_activity),
        "days": _plain(predictions),
    }


def write_golden():
    """Regenerate the golden file; only when a model or forecast change is intended."""
    inputs = load_forecast_inputs()
    golden = {case["name"]: run_case(inputs, case) for case in GOLDEN_CASES}
    GOLDEN_PATH.write_text(json.dumps(golden, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"Golden escrito en {GOLDEN_PATH}")


if __name__ == "__main__":
    write_golden()
