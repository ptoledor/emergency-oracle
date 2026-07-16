"""Leakage-safe feature-count ablation for the selected XGBoost setup."""

from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")

import numpy as np
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit
from xgboost import XGBRegressor

from search_signal_models import N_SPLITS, TEST_SIZE, build_model, load_data, metrics


FEATURE_COUNTS = [20, 30, 40, 60, 80, 108]
PARAMS = {
    "objective": "reg:squarederror",
    "n_estimators": 300,
    "max_depth": 3,
    "learning_rate": 0.03,
    "min_child_weight": 2,
    "reg_lambda": 1.0,
    "subsample": 0.85,
    "colsample_bytree": 0.85,
    "n_jobs": 1,
    "random_state": 42,
}


def main():
    base_dir = Path(__file__).resolve().parent.parent
    output = base_dir / "05_research" / "results" / "signal_search_feature_tuning"
    output.mkdir(parents=True, exist_ok=True)
    df, feature_sets, _ = load_data(base_dir)
    all_features = feature_sets["all_operational"]
    y = df["EVENTOS"].astype(float)
    splits = list(TimeSeriesSplit(n_splits=N_SPLITS, test_size=TEST_SIZE).split(df))
    predictions = {"official": []}
    actual = []
    dates = []
    fold_rows = []
    selection_rows = []
    for count in FEATURE_COUNTS:
        predictions[f"top_{count}"] = []

    for fold, (train_idx, test_idx) in enumerate(splits, start=1):
        print(f"fold={fold}/{N_SPLITS}", flush=True)
        official = build_model("official_xgb")
        official.fit(df[feature_sets["official_50"]].iloc[train_idx], y.iloc[train_idx])
        predictions["official"].extend(
            np.clip(official.predict(df[feature_sets["official_50"]].iloc[test_idx]), 0, None)
        )
        actual.extend(y.iloc[test_idx].to_numpy())
        dates.extend(df["FECHA_DIA"].iloc[test_idx].to_numpy())

        ranker = XGBRegressor(**PARAMS).fit(df[all_features].iloc[train_idx], y.iloc[train_idx])
        ranked = [
            feature for feature, _ in sorted(
                zip(all_features, ranker.feature_importances_),
                key=lambda item: item[1],
                reverse=True,
            )
        ]
        for rank, feature in enumerate(ranked, start=1):
            selection_rows.append({"fold": fold, "rank": rank, "feature": feature})
        for count in FEATURE_COUNTS:
            selected = ranked[:count]
            model = XGBRegressor(**PARAMS).fit(df[selected].iloc[train_idx], y.iloc[train_idx])
            fold_prediction = np.clip(model.predict(df[selected].iloc[test_idx]), 0, None)
            predictions[f"top_{count}"].extend(fold_prediction)
            fold_rows.append({
                "candidate": f"top_{count}",
                "fold": fold,
                **metrics(y.iloc[test_idx], fold_prediction),
            })

    rows = []
    for name, values in predictions.items():
        rows.append({"candidate": name, **metrics(actual, values)})
    results = pd.DataFrame(rows)
    official = results[results["candidate"] == "official"].iloc[0]
    results["mae_vs_official_pct"] = 100 * (results["mae"] / official["mae"] - 1)
    results["rmse_vs_official_pct"] = 100 * (results["rmse"] / official["rmse"] - 1)
    results = results.sort_values(["rmse", "mae"])
    results.to_csv(output / "feature_count_results.csv", sep=";", index=False)
    pd.DataFrame(fold_rows).to_csv(output / "fold_metrics.csv", sep=";", index=False)
    pd.DataFrame(selection_rows).to_csv(output / "feature_rankings.csv", sep=";", index=False)
    pd.DataFrame({"FECHA_DIA": dates, "EVENTOS": actual, **predictions}).to_csv(
        output / "oof_predictions.csv", sep=";", index=False
    )
    print(results[[
        "candidate", "mae", "rmse", "r2", "high_auc", "std_ratio",
        "p05_p95_width", "boring_4_5_share",
    ]].to_string(index=False, float_format="%.3f"))


if __name__ == "__main__":
    main()
