"""Second leakage-safe search cycle for a higher-signal count model.

This search keeps the six expanding 120-day folds used to select the active
model.  It explores temporal weighting, rolling training windows, residual
targets and CatBoost without changing the serving-safe 108-feature contract.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")

import numpy as np
import pandas as pd
from catboost import CatBoostRegressor
from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.model_selection import TimeSeriesSplit
from xgboost import XGBRegressor

from search_signal_models import N_SPLITS, TEST_SIZE, load_data, metrics


@dataclass(frozen=True)
class Config:
    name: str
    family: str
    params: dict = field(default_factory=dict)
    half_life_days: int | None = None
    train_window_days: int | None = None
    target_baseline: str | None = None
    high_day_weight: float = 0.0


XGB_BASE = {
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


CONFIGS = [
    Config("active_d3_flexible", "xgb"),
    Config("xgb_half_life_180", "xgb", half_life_days=180),
    Config("xgb_half_life_365", "xgb", half_life_days=365),
    Config("xgb_half_life_540", "xgb", half_life_days=540),
    Config("xgb_half_life_730", "xgb", half_life_days=730),
    Config("xgb_half_life_1095", "xgb", half_life_days=1095),
    Config("xgb_window_730", "xgb", train_window_days=730),
    Config("xgb_window_1095", "xgb", train_window_days=1095),
    Config("xgb_d2_long_flexible", "xgb", {
        "n_estimators": 450, "max_depth": 2, "learning_rate": 0.03,
    }),
    Config("xgb_d3_slow", "xgb", {
        "n_estimators": 450, "learning_rate": 0.02,
    }),
    Config("xgb_high_weight_025", "xgb", high_day_weight=0.25),
    Config("xgb_high_weight_050", "xgb", high_day_weight=0.50),
    Config("xgb_residual_mean7", "xgb", target_baseline="EVENTOS_rolling_mean_7d"),
    Config("xgb_residual_mean14", "xgb", target_baseline="EVENTOS_rolling_mean_14d"),
    Config("xgb_residual_mean30", "xgb", target_baseline="EVENTOS_rolling_mean_30d"),
    Config("xgb_pseudohuber", "xgb", {"objective": "reg:pseudohubererror"}),
    Config("cat_rmse_d4", "cat", {"depth": 4, "l2_leaf_reg": 3.0}),
    Config("cat_rmse_d5", "cat", {"depth": 5, "l2_leaf_reg": 5.0}),
    Config("cat_rmse_d6", "cat", {"depth": 6, "l2_leaf_reg": 8.0}),
    Config("cat_rmse_d4_hl365", "cat", {"depth": 4, "l2_leaf_reg": 3.0}, half_life_days=365),
    Config("cat_rmse_d5_hl730", "cat", {"depth": 5, "l2_leaf_reg": 5.0}, half_life_days=730),
    Config("cat_poisson_d4", "cat", {"loss_function": "Poisson", "depth": 4, "l2_leaf_reg": 3.0}),
    Config("cat_poisson_d5", "cat", {"loss_function": "Poisson", "depth": 5, "l2_leaf_reg": 5.0}),
    Config("hist_squared_leaf7", "hist", {"max_leaf_nodes": 7, "min_samples_leaf": 20}),
    Config("hist_squared_leaf15", "hist", {"max_leaf_nodes": 15, "min_samples_leaf": 20}),
    Config("rf_poisson", "rf", {}),
]


def build_model(config: Config):
    if config.family == "xgb":
        return XGBRegressor(**{**XGB_BASE, **config.params})
    if config.family == "cat":
        return CatBoostRegressor(**{
            "loss_function": "RMSE",
            "iterations": 500,
            "learning_rate": 0.03,
            "random_seed": 42,
            "random_strength": 0.5,
            "bootstrap_type": "Bernoulli",
            "subsample": 0.85,
            "verbose": False,
            "allow_writing_files": False,
            "thread_count": 1,
            **config.params,
        })
    if config.family == "hist":
        return HistGradientBoostingRegressor(**{
            "loss": "squared_error",
            "max_iter": 300,
            "learning_rate": 0.035,
            "l2_regularization": 3.0,
            "random_state": 42,
            **config.params,
        })
    if config.family == "rf":
        return RandomForestRegressor(
            criterion="poisson",
            n_estimators=500,
            max_depth=12,
            min_samples_leaf=5,
            max_features=0.8,
            n_jobs=1,
            random_state=42,
        )
    raise ValueError(config.family)


def training_rows(df, train_idx, config: Config):
    selected = np.asarray(train_idx)
    if config.train_window_days:
        dates = pd.to_datetime(df["FECHA_DIA"])
        cutoff = dates.iloc[selected[-1]] - pd.Timedelta(days=config.train_window_days)
        selected = selected[dates.iloc[selected].to_numpy() >= cutoff.to_datetime64()]
    return selected


def training_weights(df, y, train_idx, config: Config):
    weights = np.ones(len(train_idx), dtype=float)
    if config.half_life_days:
        dates = pd.to_datetime(df["FECHA_DIA"].iloc[train_idx])
        age = (dates.iloc[-1] - dates).dt.days.to_numpy()
        weights *= np.power(0.5, age / config.half_life_days)
    if config.high_day_weight:
        weights *= 1.0 + config.high_day_weight * (y.iloc[train_idx].to_numpy() > 7)
    return weights


def evaluate(config, df, features, y, splits):
    predictions = []
    actuals = []
    rows = []
    folds = []
    for fold, (train_idx, test_idx) in enumerate(splits, start=1):
        train_idx = training_rows(df, train_idx, config)
        model = build_model(config)
        target = y.iloc[train_idx].to_numpy(dtype=float)
        baseline_train = None
        baseline_test = None
        if config.target_baseline:
            baseline_train = df[config.target_baseline].iloc[train_idx].to_numpy(dtype=float)
            baseline_test = df[config.target_baseline].iloc[test_idx].to_numpy(dtype=float)
            target = target - baseline_train
        weights = training_weights(df, y, train_idx, config)
        model.fit(df[features].iloc[train_idx], target, sample_weight=weights)
        prediction = model.predict(df[features].iloc[test_idx])
        if baseline_test is not None:
            prediction = baseline_test + prediction
        prediction = np.clip(prediction, 0, None)
        actual = y.iloc[test_idx].to_numpy(dtype=float)
        predictions.extend(prediction)
        actuals.extend(actual)
        rows.extend(test_idx)
        folds.extend([fold] * len(test_idx))
    return pd.DataFrame({
        "row": rows,
        "FECHA_DIA": df["FECHA_DIA"].iloc[rows].to_numpy(),
        "EVENTOS": actuals,
        "prediction": predictions,
        "fold": folds,
    })


def main():
    base_dir = Path(__file__).resolve().parent.parent
    output = base_dir / "05_research" / "results" / "signal_v2_search"
    output.mkdir(parents=True, exist_ok=True)
    df, feature_sets, _ = load_data(base_dir)
    features = feature_sets["all_operational"]
    y = df["EVENTOS"].astype(float)
    splits = list(TimeSeriesSplit(n_splits=N_SPLITS, test_size=TEST_SIZE).split(df))
    requested = {
        name.strip() for name in os.getenv("SIGNAL_V2_NAMES", "").split(",")
        if name.strip()
    }
    configs = [config for config in CONFIGS if not requested or config.name in requested]
    if requested - {config.name for config in configs}:
        raise ValueError(f"Unknown configs: {sorted(requested - {config.name for config in configs})}")

    result_rows = []
    fold_rows = []
    prediction_table = None
    for number, config in enumerate(configs, start=1):
        print(f"[{number:02d}/{len(configs):02d}] {config.name}", flush=True)
        frame = evaluate(config, df, features, y, splits)
        overall = metrics(frame["EVENTOS"], frame["prediction"])
        confirmation = metrics(
            frame.loc[frame["fold"] >= 5, "EVENTOS"],
            frame.loc[frame["fold"] >= 5, "prediction"],
        )
        result_rows.append({
            "candidate": config.name,
            "family": config.family,
            **overall,
            **{f"confirm_{key}": value for key, value in confirmation.items()},
        })
        for fold, subset in frame.groupby("fold"):
            fold_rows.append({"candidate": config.name, "fold": fold, **metrics(subset["EVENTOS"], subset["prediction"])})
        if prediction_table is None:
            prediction_table = frame[["row", "FECHA_DIA", "EVENTOS", "fold"]].copy()
        prediction_table[config.name] = frame["prediction"].to_numpy()
        pd.DataFrame(result_rows).to_csv(output / "leaderboard_partial.csv", sep=";", index=False)
        prediction_table.to_csv(output / "oof_predictions_partial.csv", sep=";", index=False)

    results = pd.DataFrame(result_rows)
    active = results[results["candidate"] == "active_d3_flexible"].iloc[0] if "active_d3_flexible" in set(results["candidate"]) else None
    if active is not None:
        results["rmse_vs_active_pct"] = 100 * (results["rmse"] / active["rmse"] - 1)
        results["mae_vs_active_pct"] = 100 * (results["mae"] / active["mae"] - 1)
        results["passes_active_gate"] = (
            (results["rmse"] < active["rmse"])
            & (results["mae"] <= active["mae"] * 1.005)
            & (results["high_auc"] >= active["high_auc"] - 0.005)
            & (results["std_ratio"] >= active["std_ratio"] * 0.95)
            & (results["confirm_rmse"] < active["confirm_rmse"])
        )
    results = results.sort_values(["rmse", "mae"]).reset_index(drop=True)
    results.to_csv(output / "leaderboard.csv", sep=";", index=False)
    pd.DataFrame(fold_rows).to_csv(output / "fold_metrics.csv", sep=";", index=False)
    prediction_table.to_csv(output / "oof_predictions.csv", sep=";", index=False)
    summary = {
        "protocol": f"{N_SPLITS} expanding folds x {TEST_SIZE} days",
        "candidate_count": len(configs),
        "active": active.to_dict() if active is not None else None,
        "winner": results.iloc[0].to_dict(),
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(results[[
        "candidate", "mae", "rmse", "r2", "high_auc", "std_ratio",
        "boring_4_5_share", "confirm_rmse",
    ]].to_string(index=False, float_format="%.4f"))


if __name__ == "__main__":
    main()
