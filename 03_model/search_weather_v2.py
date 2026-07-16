"""Temporal ablation of the second serving-compatible weather block."""

from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")

import numpy as np
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit
from xgboost import XGBRegressor

from search_signal_features_v2 import ACTIVE_PARAMS, D2_PARAMS, add_history_features
from search_signal_models import N_SPLITS, TEST_SIZE, load_data, metrics


def main():
    root = Path(__file__).resolve().parent.parent
    output = root / "05_research" / "results" / "weather_signal_v2"
    output.mkdir(parents=True, exist_ok=True)
    frame, feature_sets, _ = load_data(root)
    base = feature_sets["all_operational"]
    weather = pd.read_csv(
        root / "05_research" / "data" / "historical_forecast_features_v2.csv",
        sep=";",
    )
    weather["FECHA_DIA"] = pd.to_datetime(weather["FECHA_DIA"]).dt.strftime("%Y-%m-%d")
    frame = frame.merge(weather, on="FECHA_DIA", how="left", validate="one_to_one")
    frame, weekly, regime = add_history_features(frame)
    complete = [
        column for column in weather.columns
        if column != "FECHA_DIA" and frame[column].notna().mean() >= 0.99
    ]
    hydro = [column for column in complete if any(token in column for token in (
        "RAIN", "SHOWER", "WET_BULB", "FREEZING", "STORM", "THUNDER",
    ))]
    cloud_radiation = [column for column in complete if any(token in column for token in (
        "CLOUD", "RADIATION", "SHORTWAVE", "SUNSHINE",
    ))]
    wind_pressure = [column for column in complete if any(token in column for token in (
        "WIND_DIR", "SURFACE_PRESSURE",
    ))]
    top_weather = [
        "WX2_RAIN_SUM", "WX2_SHOWERS_SUM", "WX2_SHOWER_HOURS",
        "WX2_WET_BULB_MEAN", "WX2_WET_BULB_MAX",
        "WX2_WIND_DIR_CONCENTRATION", "WX2_FREEZING_LEVEL_MEAN",
        "WX2_FREEZING_LEVEL_MIN", "WX2_SHOWERS_MAX",
    ]
    groups = {
        "base": base,
        "wx2_top": base + top_weather,
        "wx2_hydro": base + hydro,
        "wx2_cloud": base + cloud_radiation,
        "wx2_wind_pressure": base + wind_pressure,
        "wx2_all": base + complete,
        "history_wx2_top": base + weekly + regime + top_weather,
        "history_wx2_all": base + weekly + regime + complete,
    }
    y = frame["EVENTOS"].astype(float)
    splits = list(TimeSeriesSplit(n_splits=N_SPLITS, test_size=TEST_SIZE).split(frame))
    predictions = None
    rows = []
    fold_rows = []
    for model_name, params in [("d3", ACTIVE_PARAMS), ("d2", D2_PARAMS)]:
        for group_name, features in groups.items():
            candidate = f"{model_name}_{group_name}"
            print(candidate, flush=True)
            values, indices, folds = [], [], []
            for fold, (train_idx, test_idx) in enumerate(splits, start=1):
                model = XGBRegressor(**params).fit(
                    frame[features].iloc[train_idx], y.iloc[train_idx]
                )
                prediction = np.clip(model.predict(frame[features].iloc[test_idx]), 0, None)
                values.extend(prediction)
                indices.extend(test_idx)
                folds.extend([fold] * len(test_idx))
                fold_rows.append({
                    "candidate": candidate, "fold": fold,
                    **metrics(y.iloc[test_idx], prediction),
                })
            if predictions is None:
                predictions = pd.DataFrame({
                    "row": indices, "FECHA_DIA": frame["FECHA_DIA"].iloc[indices].to_numpy(),
                    "EVENTOS": y.iloc[indices].to_numpy(), "fold": folds,
                })
            predictions[candidate] = values
            overall = metrics(predictions["EVENTOS"], values)
            confirm = predictions["fold"] >= 5
            confirmation = metrics(
                predictions.loc[confirm, "EVENTOS"], np.asarray(values)[confirm]
            )
            rows.append({
                "candidate": candidate, "feature_count": len(features),
                **overall,
                **{f"confirm_{key}": value for key, value in confirmation.items()},
            })
            pd.DataFrame(rows).to_csv(output / "leaderboard_partial.csv", sep=";", index=False)
    result = pd.DataFrame(rows).sort_values(["rmse", "mae"])
    result.to_csv(output / "leaderboard.csv", sep=";", index=False)
    predictions.to_csv(output / "oof_predictions.csv", sep=";", index=False)
    pd.DataFrame(fold_rows).to_csv(output / "fold_metrics.csv", sep=";", index=False)
    print(result[[
        "candidate", "feature_count", "mae", "rmse", "r2", "high_auc",
        "std_ratio", "boring_4_5_share", "confirm_rmse",
    ]].to_string(index=False, float_format="%.4f"))


if __name__ == "__main__":
    main()
