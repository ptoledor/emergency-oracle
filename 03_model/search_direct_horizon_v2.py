"""Fair H1-H6 benchmark: recursive active model versus direct XGBoost models.

Target-day weather/calendar is allowed because it is forecast at serving time.
Every event/category history feature is rebuilt strictly as of the forecast
origin, preventing intervening actual counts from leaking into H2-H6.
"""

from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")

import numpy as np
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit
from xgboost import XGBRegressor

from search_signal_features_v2 import (
    ACTIVE_PARAMS,
    D2_PARAMS,
    add_calendar_features,
    add_history_features,
)
from search_signal_models import N_SPLITS, TEST_SIZE, load_data, metrics


HORIZONS = range(1, 7)
CATEGORY_SOURCES = {
    "N_INCENDIO_ESTR_lag_1": "N_INCENDIO_ESTR",
    "N_INCENDIO_FOREST_lag_1": "N_INCENDIO_FOREST",
    "N_RESCATE_VEH_lag_1": "N_RESCATE_VEH",
    "N_RESCATE_PERS_lag_1": "N_RESCATE_PERS",
    "N_EMERGENCIAS_CLIMATICAS_lag_1": "N_EMERGENCIAS_CLIMATICAS",
    "N_GASES_lag_1": "N_GASES",
}


def history_values(values):
    values = np.asarray(values, dtype=float)
    result = {}
    for lag in [1, 2, 3, 7, 14, 21, 28, 35, 42, 49, 56]:
        result[f"EVENTOS_lag_{lag}"] = float(values[-lag])
    for window in [3, 7]:
        recent = values[-window:]
        result[f"EVENTOS_rolling_mean_{window}d"] = float(np.mean(recent))
        result[f"EVENTOS_rolling_std_{window}d"] = float(np.std(recent, ddof=1))
        result[f"EVENTOS_rolling_max_{window}d"] = float(np.max(recent))
    for window in [14, 30]:
        result[f"EVENTOS_rolling_mean_{window}d"] = float(np.mean(values[-window:]))
    for weeks in [4, 8]:
        same_dow = values[-7 * np.arange(1, weeks + 1)]
        result[f"EVENTOS_same_dow_mean_{weeks}w"] = float(np.mean(same_dow))
        result[f"EVENTOS_same_dow_max_{weeks}w"] = float(np.max(same_dow))
    series = pd.Series(values)
    for span in [7, 14, 28]:
        result[f"EVENTOS_ewm_{span}d"] = float(
            series.ewm(span=span, adjust=False).mean().iloc[-1]
        )
    for window in [7, 14, 30, 56]:
        result[f"EVENTOS_rolling_median_{window}d"] = float(
            np.median(values[-window:])
        )
        result[f"EVENTOS_high_rate_{window}d"] = float(
            np.mean(values[-window:] > 7)
        )
    result["EVENTOS_trend_7_30"] = float(
        np.mean(values[-7:]) - np.mean(values[-30:])
    )
    high_positions = np.flatnonzero(values > 7)
    result["EVENTOS_days_since_high"] = float(
        len(values) - 1 - high_positions[-1] if len(high_positions) else len(values)
    )
    return result


def category_values(frame, origin):
    return {
        feature: float(frame.iloc[origin][source])
        for feature, source in CATEGORY_SOURCES.items()
    }


def overwrite_history(row, history, categories, feature_columns):
    values = {**history_values(history), **categories}
    for column, value in values.items():
        if column in feature_columns:
            row[column] = value
    return row


def direct_matrix(frame, features, horizon):
    target_indices = np.arange(56 + horizon, len(frame), dtype=int)
    rows = []
    y = frame["EVENTOS"].to_numpy(dtype=float)
    for target in target_indices:
        origin = target - horizon
        row = frame.loc[target, features].copy()
        rows.append(overwrite_history(
            row, y[:origin + 1], category_values(frame, origin), features
        ))
    return pd.DataFrame(rows).reset_index(drop=True), y[target_indices], target_indices


def recursive_prediction(model, frame, features, origin, horizon):
    actual_history = frame["EVENTOS"].iloc[:origin + 1].to_numpy(dtype=float).tolist()
    recent = frame.iloc[max(0, origin - 29): origin + 1]
    total = max(float(recent["EVENTOS"].sum()), 1.0)
    shares = {
        feature: float(recent[source].sum() / total)
        for feature, source in CATEGORY_SOURCES.items()
    }
    categories = category_values(frame, origin)
    prediction = None
    for target in range(origin + 1, origin + horizon + 1):
        row = frame.loc[target, features].copy()
        row = overwrite_history(row, actual_history, categories, features)
        prediction = float(np.clip(
            model.predict(pd.DataFrame([row], columns=features))[0], 0, None
        ))
        actual_history.append(prediction)
        categories = {
            feature: prediction * shares[feature] for feature in CATEGORY_SOURCES
        }
    return prediction


def main():
    base_dir = Path(__file__).resolve().parent.parent
    output = base_dir / "05_research" / "results" / "direct_horizon_v2"
    output.mkdir(parents=True, exist_ok=True)
    frame, feature_sets, _ = load_data(base_dir)
    base_features = feature_sets["all_operational"]
    frame, weekly, regime = add_history_features(frame)
    frame, calendar = add_calendar_features(frame)
    extended_features = base_features + weekly + regime + calendar
    y = frame["EVENTOS"].astype(float)
    splits = list(TimeSeriesSplit(n_splits=N_SPLITS, test_size=TEST_SIZE).split(frame))
    matrices = {
        name: {
            horizon: direct_matrix(frame, features, horizon)
            for horizon in HORIZONS
        }
        for name, features in {
            "base": base_features,
            "extended": extended_features,
        }.items()
    }
    candidates = {
        "direct_d2_base": (D2_PARAMS, base_features, "base"),
        "direct_d2_extended": (D2_PARAMS, extended_features, "extended"),
        "direct_d3_base": (ACTIVE_PARAMS, base_features, "base"),
        "direct_d3_extended": (ACTIVE_PARAMS, extended_features, "extended"),
    }
    prediction_rows = []
    for fold, (train_idx, test_idx) in enumerate(splits, start=1):
        test_start, test_end = test_idx[0], test_idx[-1]
        print(f"fold={fold}/{N_SPLITS}", flush=True)
        active = XGBRegressor(**ACTIVE_PARAMS).fit(
            frame[base_features].iloc[train_idx], y.iloc[train_idx]
        )
        direct_models = {}
        for name, (params, _, matrix_name) in candidates.items():
            direct_models[name] = {}
            for horizon in HORIZONS:
                X, target, target_indices = matrices[matrix_name][horizon]
                mask = target_indices < test_start
                direct_models[name][horizon] = XGBRegressor(**params).fit(
                    X.loc[mask], target[mask]
                )
        for target in range(test_start, test_end + 1):
            for horizon in HORIZONS:
                origin = target - horizon
                actual = float(y.iloc[target])
                row = {
                    "fold": fold, "FECHA_DIA": frame.iloc[target]["FECHA_DIA"],
                    "target_index": target, "origin_index": origin,
                    "horizon": horizon, "EVENTOS": actual,
                    "active_recursive": recursive_prediction(
                        active, frame, base_features, origin, horizon
                    ),
                }
                for name, (_, features, matrix_name) in candidates.items():
                    X, _, indices = matrices[matrix_name][horizon]
                    position = int(np.searchsorted(indices, target))
                    row[name] = float(np.clip(
                        direct_models[name][horizon].predict(X.iloc[[position]])[0],
                        0, None,
                    ))
                prediction_rows.append(row)
    predictions = pd.DataFrame(prediction_rows)
    predictions.to_csv(output / "predictions.csv", sep=";", index=False)
    result_rows = []
    model_names = ["active_recursive", *candidates]
    for name in model_names:
        overall = metrics(predictions["EVENTOS"], predictions[name])
        confirm = predictions["fold"] >= 5
        result_rows.append({
            "candidate": name, **overall,
            **{f"confirm_{key}": value for key, value in metrics(
                predictions.loc[confirm, "EVENTOS"], predictions.loc[confirm, name]
            ).items()},
        })
        for horizon, subset in predictions.groupby("horizon"):
            result_rows.append({
                "candidate": name, "horizon": horizon,
                **metrics(subset["EVENTOS"], subset[name]),
            })
    results = pd.DataFrame(result_rows)
    results.to_csv(output / "metrics.csv", sep=";", index=False)
    print(results[results["horizon"].isna()][[
        "candidate", "mae", "rmse", "r2", "high_auc", "std_ratio",
        "boring_4_5_share", "confirm_rmse",
    ]].sort_values("rmse").to_string(index=False, float_format="%.4f"))
    print("\nBy horizon:")
    print(results[results["horizon"].notna()][[
        "candidate", "horizon", "mae", "rmse", "r2", "std_ratio",
    ]].sort_values(["horizon", "rmse"]).to_string(index=False, float_format="%.4f"))


if __name__ == "__main__":
    main()
