"""Focused temporal tuning after the broad signal-model search."""

from __future__ import annotations

import json
import os
from pathlib import Path

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import TimeSeriesSplit
from xgboost import XGBRegressor

from search_signal_models import (
    N_SPLITS,
    TEST_SIZE,
    build_model,
    load_data,
    metrics,
)


CONFIGS = [
    {"name": "d2_long", "objective": "reg:squarederror", "n_estimators": 450, "max_depth": 2, "learning_rate": 0.03, "min_child_weight": 5, "reg_lambda": 3.0},
    {"name": "d3_reference", "objective": "reg:squarederror", "n_estimators": 250, "max_depth": 3, "learning_rate": 0.03, "min_child_weight": 5, "reg_lambda": 3.0},
    {"name": "d3_long", "objective": "reg:squarederror", "n_estimators": 450, "max_depth": 3, "learning_rate": 0.02, "min_child_weight": 5, "reg_lambda": 3.0},
    {"name": "d3_fast", "objective": "reg:squarederror", "n_estimators": 180, "max_depth": 3, "learning_rate": 0.05, "min_child_weight": 5, "reg_lambda": 3.0},
    {"name": "d3_flexible", "objective": "reg:squarederror", "n_estimators": 300, "max_depth": 3, "learning_rate": 0.03, "min_child_weight": 2, "reg_lambda": 1.0},
    {"name": "d3_regularized", "objective": "reg:squarederror", "n_estimators": 350, "max_depth": 3, "learning_rate": 0.025, "min_child_weight": 10, "reg_lambda": 8.0, "reg_alpha": 0.2},
    {"name": "d4_short", "objective": "reg:squarederror", "n_estimators": 180, "max_depth": 4, "learning_rate": 0.03, "min_child_weight": 5, "reg_lambda": 3.0},
    {"name": "d4_regularized", "objective": "reg:squarederror", "n_estimators": 280, "max_depth": 4, "learning_rate": 0.02, "min_child_weight": 10, "reg_lambda": 8.0, "reg_alpha": 0.2},
    {"name": "full_sampling", "objective": "reg:squarederror", "n_estimators": 250, "max_depth": 3, "learning_rate": 0.03, "min_child_weight": 5, "reg_lambda": 3.0, "subsample": 1.0, "colsample_bytree": 1.0},
    {"name": "more_sampling", "objective": "reg:squarederror", "n_estimators": 320, "max_depth": 3, "learning_rate": 0.025, "min_child_weight": 5, "reg_lambda": 3.0, "subsample": 0.70, "colsample_bytree": 0.70},
    {"name": "poisson_d3", "objective": "count:poisson", "n_estimators": 350, "max_depth": 3, "learning_rate": 0.03, "min_child_weight": 5, "reg_lambda": 3.0},
    {"name": "absolute_d3", "objective": "reg:absoluteerror", "n_estimators": 350, "max_depth": 3, "learning_rate": 0.03, "min_child_weight": 5, "reg_lambda": 3.0},
]


def fitted_model(config):
    params = {
        "subsample": 0.85,
        "colsample_bytree": 0.85,
        "n_jobs": 1,
        "random_state": 42,
        **config,
    }
    params.pop("name")
    return XGBRegressor(**params)


def main():
    base_dir = Path(__file__).resolve().parent.parent
    output_dir = base_dir / "05_research" / "results" / "signal_search_xgb_tuning"
    output_dir.mkdir(parents=True, exist_ok=True)
    df, feature_sets, _ = load_data(base_dir)
    features = feature_sets["all_operational"]
    y = df["EVENTOS"].astype(float)
    splits = list(TimeSeriesSplit(n_splits=N_SPLITS, test_size=TEST_SIZE).split(df))

    configurations = [{"name": "official_temporal", "official": True}, *CONFIGS]
    result_rows = []
    fold_rows = []
    prediction_table = None
    for number, config in enumerate(configurations, start=1):
        name = config["name"]
        print(f"[{number:02d}/{len(configurations):02d}] {name}", flush=True)
        predictions = []
        actuals = []
        rows = []
        for fold, (train_idx, test_idx) in enumerate(splits, start=1):
            if config.get("official"):
                model = build_model("official_xgb")
                fold_features = feature_sets["official_50"]
            else:
                model = fitted_model(config)
                fold_features = features
            model.fit(df[fold_features].iloc[train_idx], y.iloc[train_idx])
            prediction = np.clip(model.predict(df[fold_features].iloc[test_idx]), 0, None)
            actual = y.iloc[test_idx].to_numpy()
            predictions.extend(prediction)
            actuals.extend(actual)
            rows.extend(test_idx)
            fold_rows.append({
                "candidate": name,
                "fold": fold,
                "start_date": df["FECHA_DIA"].iloc[test_idx[0]],
                "end_date": df["FECHA_DIA"].iloc[test_idx[-1]],
                "mae": float(mean_absolute_error(actual, prediction)),
                **{f"metric_{key}": value for key, value in metrics(actual, prediction).items()},
            })
        candidate_metrics = metrics(actuals, predictions)
        result_rows.append({"candidate": name, **candidate_metrics})
        if prediction_table is None:
            prediction_table = pd.DataFrame({
                "row": rows,
                "FECHA_DIA": df["FECHA_DIA"].iloc[rows].to_numpy(),
                "EVENTOS": actuals,
            })
        prediction_table[name] = predictions

    results = pd.DataFrame(result_rows)
    folds = pd.DataFrame(fold_rows)
    official = results[results["candidate"] == "official_temporal"].iloc[0]
    official_fold_mae = folds[folds["candidate"] == "official_temporal"].set_index("fold")["mae"]
    results["mae_vs_official_pct"] = 100 * (results["mae"] / official["mae"] - 1)
    results["rmse_vs_official_pct"] = 100 * (results["rmse"] / official["rmse"] - 1)
    results["folds_better_mae"] = results["candidate"].map(
        lambda name: int((folds[folds["candidate"] == name].set_index("fold")["mae"] < official_fold_mae).sum())
    )
    results["passes_accuracy_gate"] = (
        (results["mae"] <= official["mae"] * 1.01)
        & (results["rmse"] <= official["rmse"] * 1.01)
        & (results["high_auc"] >= official["high_auc"] - 0.01)
        & (results["folds_better_mae"] >= 3)
    )
    results["resolution_score"] = (
        0.45 * results["std_ratio"]
        + 0.35 * results["width_ratio"]
        + 0.20 * (1.0 - results["boring_4_5_share"])
    )
    results = results.sort_values(
        ["passes_accuracy_gate", "resolution_score", "mae"],
        ascending=[False, False, True],
    ).reset_index(drop=True)
    winner = results[results["passes_accuracy_gate"]].iloc[0]
    results.to_csv(output_dir / "tuning_results.csv", sep=";", index=False)
    folds.to_csv(output_dir / "fold_metrics.csv", sep=";", index=False)
    prediction_table.to_csv(output_dir / "oof_predictions.csv", sep=";", index=False)
    with (output_dir / "summary.json").open("w", encoding="utf-8") as file:
        json.dump({
            "winner": winner.to_dict(),
            "winner_params": next((config for config in CONFIGS if config["name"] == winner["candidate"]), None),
            "official": official.to_dict(),
        }, file, indent=2, default=str)
    print(results[[
        "candidate", "mae", "rmse", "r2", "high_auc", "folds_better_mae",
        "std_ratio", "p05_p95_width", "boring_4_5_share", "passes_accuracy_gate",
    ]].to_string(index=False, float_format="%.3f"))
    print(f"\nSelected: {winner['candidate']}")


if __name__ == "__main__":
    main()
