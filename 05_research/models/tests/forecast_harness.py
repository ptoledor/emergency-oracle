"""Run the dashboard's six-day forecast outside Streamlit for golden tests.

dashboard.py executes the whole app on import, so the forecast functions are
extracted from its source and executed with the same artifacts the dashboard
loads. Weather is supplied explicitly so the result never depends on the
network or on the current date.
"""

from __future__ import annotations

import ast
import datetime as dt
import pickle
import types
from pathlib import Path

import holidays
import numpy as np
import pandas as pd

import model_components
from serving_history import prepare_event_history
from signal_features import (
    CATEGORY_LAG_FEATURES,
    aggregate_weather_daily,
    event_history_features,
)


ROOT = Path(__file__).resolve().parents[3]
FIXTURES = Path(__file__).resolve().parent / "fixtures"
GOLDEN_PATH = FIXTURES / "golden_forecast.json"
DASHBOARD_FUNCTIONS = [
    "force_single_thread_model",
    "build_local_weather_fallback",
    "predict_category_composition",
    "predict_6_days",
]
# Origins: observed history with real weather, observed history with the
# local fallback, and a stale dataset (neutral history) with the fallback.
GOLDEN_CASES = [
    {"name": "invierno_clima_real", "origin": "2025-06-10", "weather": "weather_2025-06-10.csv"},
    {"name": "verano_fallback", "origin": "2026-01-15", "weather": None},
    {"name": "datos_vencidos_fallback", "origin": "2026-10-23", "weather": None},
]


class _StreamlitStub(types.SimpleNamespace):
    def error(self, message):
        raise RuntimeError(message)


def _load_pickle(path):
    if not path.exists():
        return None
    with path.open("rb") as stream:
        return pickle.load(stream)


def load_dashboard_forecast():
    """Return the dashboard's predict_6_days bound to the dashboard artifacts."""
    source = (ROOT / "dashboard.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    nodes = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in DASHBOARD_FUNCTIONS:
            node.decorator_list = []
            nodes.append(node)
    found = {node.name for node in nodes}
    missing = set(DASHBOARD_FUNCTIONS) - found
    if missing:
        raise KeyError(f"dashboard.py no define: {sorted(missing)}")

    models_dir = ROOT / "03_model" / "saved_models"
    df = pd.read_csv(ROOT / "02_data" / "augmented_emergency_data.csv", sep=";")
    df = df.sort_values("FECHA_DIA").reset_index(drop=True)
    namespace = {
        "np": np, "pd": pd, "pickle": pickle, "datetime": dt, "holidays": holidays,
        "st": _StreamlitStub(), "model_components": model_components,
        "base_dir": ROOT, "models_dir": models_dir, "df": df,
        "aggregate_weather_daily": aggregate_weather_daily,
        "prepare_event_history": prepare_event_history,
        "event_history_features": event_history_features,
        "CATEGORY_LAG_FEATURES": CATEGORY_LAG_FEATURES,
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "dashboard.py", "exec"), namespace)

    force = namespace["force_single_thread_model"]
    category_risk = _load_pickle(models_dir / "category_risk_models.pkl")
    if category_risk:
        for details in category_risk.get("models", {}).values():
            details["model"] = force(details["model"])
    composition = _load_pickle(models_dir / "category_composition_models.pkl")
    if composition:
        for details in composition.get("groups", {}).values():
            details["baseline_model"] = force(details["baseline_model"])
            details["expected_model"] = force(details["expected_model"])
    fifth = _load_pickle(models_dir / "fifth_company_models.pkl")
    if fifth:
        fifth["regressor"] = force(fifth["regressor"])
        fifth["classifier"] = force(fifth["classifier"])
    namespace.update({
        "category_risk_artifact": category_risk,
        "category_composition_artifact": composition,
        "fifth_company_artifact": fifth,
    })
    return namespace


def case_weather(namespace, case):
    origin = dt.date.fromisoformat(case["origin"])
    if case["weather"] is None:
        return namespace["build_local_weather_fallback"](origin, True)
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


def run_case(namespace, case):
    origin = dt.date.fromisoformat(case["origin"])
    predictions, threshold, high_activity = namespace["predict_6_days"](
        origin, True, prefix="_climatic_augmented",
        weather_data=case_weather(namespace, case),
    )
    return {
        "classification_threshold": float(threshold),
        "umbral_alta_actividad": float(high_activity),
        "days": _plain(predictions),
    }


def write_golden():
    """Regenerate the golden file; only when a model or forecast change is intended."""
    import json

    namespace = load_dashboard_forecast()
    golden = {case["name"]: run_case(namespace, case) for case in GOLDEN_CASES}
    GOLDEN_PATH.write_text(json.dumps(golden, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"Golden escrito en {GOLDEN_PATH}")


if __name__ == "__main__":
    write_golden()
