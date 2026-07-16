"""Focused objective search on the hydrometeorological v2 feature block."""

from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")

import numpy as np
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit
from xgboost import XGBRegressor

from search_signal_features_v2 import ACTIVE_PARAMS, D2_PARAMS
from search_signal_models import N_SPLITS, TEST_SIZE, load_data, metrics


CONFIGS = {
    "d2_squared": D2_PARAMS,
    "d2_poisson": {**D2_PARAMS, "objective": "count:poisson"},
    "d2_absolute": {**D2_PARAMS, "objective": "reg:absoluteerror"},
    "d3_absolute": {**ACTIVE_PARAMS, "objective": "reg:absoluteerror", "n_estimators": 450},
    "d2_tweedie_11": {**D2_PARAMS, "objective": "reg:tweedie", "tweedie_variance_power": 1.1},
    "d2_tweedie_13": {**D2_PARAMS, "objective": "reg:tweedie", "tweedie_variance_power": 1.3},
    "d2_tweedie_15": {**D2_PARAMS, "objective": "reg:tweedie", "tweedie_variance_power": 1.5},
    "d2_quantile_50": {**D2_PARAMS, "objective": "reg:quantileerror", "quantile_alpha": 0.5},
    "d2_quantile_60": {**D2_PARAMS, "objective": "reg:quantileerror", "quantile_alpha": 0.6},
}


def main():
    root = Path(__file__).resolve().parent.parent
    output = root / "05_research" / "results" / "weather_objectives_v2"
    output.mkdir(parents=True, exist_ok=True)
    frame, feature_sets, _ = load_data(root)
    weather = pd.read_csv(
        root / "05_research" / "data" / "historical_forecast_features_v2.csv", sep=";"
    )
    weather["FECHA_DIA"] = pd.to_datetime(weather["FECHA_DIA"]).dt.strftime("%Y-%m-%d")
    frame = frame.merge(weather, on="FECHA_DIA", how="left", validate="one_to_one")
    hydro = [column for column in weather.columns if column != "FECHA_DIA" and any(
        token in column for token in ("RAIN", "SHOWER", "WET_BULB", "FREEZING", "STORM", "THUNDER")
    ) and frame[column].notna().mean() >= 0.99]
    features = feature_sets["all_operational"] + hydro
    y = frame["EVENTOS"].astype(float)
    splits = list(TimeSeriesSplit(n_splits=N_SPLITS, test_size=TEST_SIZE).split(frame))
    table = None
    rows = []
    folds_out = []
    for name, params in CONFIGS.items():
        print(name, flush=True)
        values, indices, fold_ids = [], [], []
        for fold, (train_idx, test_idx) in enumerate(splits, start=1):
            model = XGBRegressor(**params).fit(frame[features].iloc[train_idx], y.iloc[train_idx])
            prediction = np.clip(model.predict(frame[features].iloc[test_idx]), 0, None)
            values.extend(prediction); indices.extend(test_idx); fold_ids.extend([fold] * len(test_idx))
            folds_out.append({"candidate": name, "fold": fold, **metrics(y.iloc[test_idx], prediction)})
        if table is None:
            table = pd.DataFrame({
                "row": indices, "FECHA_DIA": frame["FECHA_DIA"].iloc[indices].to_numpy(),
                "EVENTOS": y.iloc[indices].to_numpy(), "fold": fold_ids,
            })
        table[name] = values
        overall = metrics(table["EVENTOS"], values)
        confirmation = table["fold"] >= 5
        rows.append({"candidate": name, **overall, **{
            f"confirm_{key}": value for key, value in metrics(
                table.loc[confirmation, "EVENTOS"], np.asarray(values)[confirmation]
            ).items()
        }})
    result = pd.DataFrame(rows).sort_values(["rmse", "mae"])
    result.to_csv(output / "leaderboard.csv", sep=";", index=False)
    table.to_csv(output / "oof_predictions.csv", sep=";", index=False)
    pd.DataFrame(folds_out).to_csv(output / "fold_metrics.csv", sep=";", index=False)
    print(result[[
        "candidate", "mae", "rmse", "r2", "high_auc", "std_ratio",
        "boring_4_5_share", "confirm_rmse",
    ]].to_string(index=False, float_format="%.4f"))


if __name__ == "__main__":
    main()
