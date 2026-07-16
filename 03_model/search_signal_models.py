"""Temporal search for count models that are accurate *and* informative.

The production model is deliberately left untouched.  This script compares
pre-declared, serving-safe feature sets and model families on identical
expanding-window folds.  Besides conventional error metrics it measures how
much useful day-to-day resolution survives in the point forecast.
"""

from __future__ import annotations

import json
import os
import pickle
from dataclasses import dataclass
from pathlib import Path

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")

import numpy as np
import pandas as pd
from sklearn.ensemble import (
    ExtraTreesRegressor,
    GradientBoostingRegressor,
    HistGradientBoostingRegressor,
    RandomForestRegressor,
)
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LinearRegression, PoissonRegressor
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    roc_auc_score,
)
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBRegressor


RANDOM_STATE = 42
N_SPLITS = 6
TEST_SIZE = 120
HIGH_ACTIVITY_THRESHOLD = 7.0


@dataclass(frozen=True)
class Candidate:
    name: str
    family: str
    feature_set: str


def load_data(base_dir: Path):
    models_dir = base_dir / "03_model" / "saved_models"
    df = pd.read_csv(
        base_dir / "02_data" / "augmented_emergency_data.csv",
        sep=";",
    ).sort_values("FECHA_DIA").reset_index(drop=True)
    weekday_names = [
        "DIA_LUNES",
        "DIA_MARTES",
        "DIA_MIERCOLES",
        "DIA_JUEVES",
        "DIA_VIERNES",
        "DIA_SABADO",
        "DIA_DOMINGO",
    ]
    for weekday, column in enumerate(weekday_names):
        df[column] = (df["DIA_SEMANA"] == weekday).astype(int)
    forecast_path = base_dir / "05_research" / "data" / "historical_forecast_features.csv"
    forecast_features = []
    if forecast_path.exists():
        forecast = pd.read_csv(forecast_path, sep=";")
        forecast["FECHA_DIA"] = pd.to_datetime(forecast["FECHA_DIA"]).dt.strftime("%Y-%m-%d")
        coverage = forecast.drop(columns="FECHA_DIA").notna().mean()
        forecast_features = coverage[coverage >= 0.99].index.tolist()
        df = df.merge(
            forecast[["FECHA_DIA", *forecast_features]],
            on="FECHA_DIA",
            how="left",
            validate="one_to_one",
        )
        incomplete = [column for column in forecast_features if df[column].isna().any()]
        if incomplete:
            raise ValueError(f"Forecast features are incomplete after merge: {incomplete}")
    with (models_dir / "metadata_climatic_augmented.pkl").open("rb") as file:
        metadata = pickle.load(file)

    contemporaneous_targets = {
        "FECHA_DIA",
        "EVENTOS",
        "N_5TA_CIA",
        "N_INCENDIO_ESTR",
        "N_INCENDIO_FOREST",
        "N_RESCATE_VEH",
        "N_RESCATE_PERS",
        "N_EMERGENCIAS_CLIMATICAS",
        "N_GASES",
        "N_OTROS",
    }
    all_operational = [
        column
        for column in df.columns
        if column not in contemporaneous_targets
        and pd.api.types.is_numeric_dtype(df[column])
    ]
    history_columns = [
        column
        for column in all_operational
        if column.startswith("EVENTOS_")
        or (column.startswith("N_") and column.endswith("_lag_1"))
    ]
    calendar_columns = [
        "MES",
        "DIA_SEMANA",
        "ES_FIN_SEMANA",
        "ES_FERIADO",
        "ES_FERIADO_IRRENUNCIABLE",
        "ES_PRE_FERIADO",
        "MES_SIN",
        "MES_COS",
        "DIA_SIN",
        "DIA_COS",
        "DANO_SIN",
        "DANO_COS",
    ]
    feature_sets = {
        "official_50": list(metadata["feature_cols"]),
        "official_plus_forecast": list(metadata["feature_cols"]) + forecast_features,
        "official_plus_history": list(dict.fromkeys(list(metadata["feature_cols"]) + history_columns)),
        "all_base_operational": [
            column for column in all_operational if column not in forecast_features
        ],
        "all_operational": all_operational,
        "history_calendar": list(dict.fromkeys(history_columns + calendar_columns)),
        "weather_calendar": [
            column for column in all_operational if column not in history_columns
        ],
    }
    for name, columns in feature_sets.items():
        missing = [column for column in columns if column not in df.columns]
        if missing:
            raise KeyError(f"{name} has missing columns: {missing}")
    return df, feature_sets, metadata


def build_model(family: str):
    common_xgb = dict(
        n_estimators=250,
        max_depth=3,
        learning_rate=0.03,
        min_child_weight=5,
        subsample=0.85,
        colsample_bytree=0.85,
        reg_lambda=3.0,
        n_jobs=1,
        random_state=RANDOM_STATE,
    )
    if family == "official_xgb":
        return XGBRegressor(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.03,
            subsample=0.8,
            colsample_bytree=0.8,
            n_jobs=1,
            random_state=RANDOM_STATE,
        )
    if family == "xgb_squared":
        return XGBRegressor(objective="reg:squarederror", **common_xgb)
    if family == "xgb_absolute":
        return XGBRegressor(objective="reg:absoluteerror", **common_xgb)
    if family == "xgb_poisson":
        return XGBRegressor(objective="count:poisson", **common_xgb)
    if family == "xgb_tweedie":
        return XGBRegressor(
            objective="reg:tweedie",
            tweedie_variance_power=1.35,
            **common_xgb,
        )
    if family == "hist_poisson":
        return HistGradientBoostingRegressor(
            loss="poisson",
            max_iter=300,
            learning_rate=0.04,
            max_leaf_nodes=15,
            min_samples_leaf=15,
            l2_regularization=2.0,
            random_state=RANDOM_STATE,
        )
    if family == "hist_absolute":
        return HistGradientBoostingRegressor(
            loss="absolute_error",
            max_iter=300,
            learning_rate=0.04,
            max_leaf_nodes=15,
            min_samples_leaf=15,
            l2_regularization=2.0,
            random_state=RANDOM_STATE,
        )
    if family == "gradient_huber":
        return GradientBoostingRegressor(
            loss="huber",
            n_estimators=200,
            max_depth=3,
            min_samples_leaf=8,
            learning_rate=0.03,
            subsample=0.85,
            random_state=RANDOM_STATE,
        )
    if family == "extra_trees":
        return ExtraTreesRegressor(
            n_estimators=350,
            max_depth=12,
            min_samples_leaf=4,
            max_features=0.8,
            n_jobs=1,
            random_state=RANDOM_STATE,
        )
    if family == "rf_absolute":
        return RandomForestRegressor(
            criterion="absolute_error",
            n_estimators=300,
            max_depth=12,
            min_samples_leaf=5,
            max_features=0.8,
            n_jobs=1,
            random_state=RANDOM_STATE,
        )
    if family == "poisson_glm":
        return make_pipeline(
            StandardScaler(),
            PoissonRegressor(alpha=1.0, max_iter=500),
        )
    raise ValueError(f"Unknown family: {family}")


def candidates():
    result = [Candidate("official_temporal", "official_xgb", "official_50")]
    broad_families = [
        "xgb_squared",
        "xgb_absolute",
        "xgb_poisson",
        "xgb_tweedie",
        "hist_poisson",
        "hist_absolute",
        "gradient_huber",
        "extra_trees",
        "poisson_glm",
    ]
    for feature_set in [
        "official_50",
        "official_plus_forecast",
        "official_plus_history",
        "all_base_operational",
        "all_operational",
        "history_calendar",
        "weather_calendar",
    ]:
        for family in broad_families:
            result.append(Candidate(f"{family}__{feature_set}", family, feature_set))
    return result


def safe_auc(y, prediction):
    target = np.asarray(y) > HIGH_ACTIVITY_THRESHOLD
    return float(roc_auc_score(target, prediction)) if np.unique(target).size == 2 else np.nan


def metrics(y, prediction):
    y = np.asarray(y, dtype=float)
    prediction = np.clip(np.asarray(prediction, dtype=float), 0, None)
    actual_std = np.std(y, ddof=1)
    prediction_std = np.std(prediction, ddof=1)
    actual_width = np.quantile(y, 0.95) - np.quantile(y, 0.05)
    prediction_width = np.quantile(prediction, 0.95) - np.quantile(prediction, 0.05)
    return {
        "mae": float(mean_absolute_error(y, prediction)),
        "rmse": float(mean_squared_error(y, prediction) ** 0.5),
        "r2": float(r2_score(y, prediction)),
        "high_auc": safe_auc(y, prediction),
        "actual_mean": float(np.mean(y)),
        "prediction_mean": float(np.mean(prediction)),
        "actual_std": float(actual_std),
        "prediction_std": float(prediction_std),
        "std_ratio": float(prediction_std / actual_std),
        "p05": float(np.quantile(prediction, 0.05)),
        "p95": float(np.quantile(prediction, 0.95)),
        "p05_p95_width": float(prediction_width),
        "width_ratio": float(prediction_width / actual_width),
        "boring_4_5_share": float(np.mean((prediction >= 4.0) & (prediction <= 5.999999))),
        "min_prediction": float(np.min(prediction)),
        "max_prediction": float(np.max(prediction)),
    }


def sequential_calibrations(frame):
    """Calibrate each fold using only predictions from earlier test folds."""
    variants = {name: np.full(len(frame), np.nan) for name in ["round", "affine", "isotonic", "spread"]}
    for fold in sorted(frame["fold"].unique()):
        current = frame["fold"].to_numpy() == fold
        previous = frame["fold"].to_numpy() < fold
        raw_current = frame.loc[current, "prediction"].to_numpy()
        variants["round"][current] = np.floor(raw_current + 0.5)
        if previous.sum() < TEST_SIZE:
            for name in ["affine", "isotonic", "spread"]:
                variants[name][current] = raw_current
            continue

        prior_prediction = frame.loc[previous, "prediction"].to_numpy()
        prior_y = frame.loc[previous, "EVENTOS"].to_numpy()
        affine = LinearRegression().fit(prior_prediction.reshape(-1, 1), prior_y)
        variants["affine"][current] = affine.predict(raw_current.reshape(-1, 1))
        isotonic = IsotonicRegression(out_of_bounds="clip").fit(prior_prediction, prior_y)
        variants["isotonic"][current] = isotonic.predict(raw_current)

        center_prediction = float(np.mean(prior_prediction))
        center_y = float(np.mean(prior_y))
        rows = []
        for gamma in np.arange(0.80, 2.01, 0.05):
            adjusted = np.clip(
                center_y + gamma * (prior_prediction - center_prediction),
                0,
                None,
            )
            rows.append((gamma, mean_absolute_error(prior_y, adjusted), mean_squared_error(prior_y, adjusted) ** 0.5))
        best_mae = min(row[1] for row in rows)
        best_rmse = min(row[2] for row in rows)
        eligible = [row for row in rows if row[1] <= best_mae * 1.01 and row[2] <= best_rmse * 1.01]
        gamma = max(eligible, key=lambda row: row[0])[0]
        variants["spread"][current] = center_y + gamma * (raw_current - center_prediction)
    return {name: np.clip(values, 0, None) for name, values in variants.items()}


def main():
    base_dir = Path(__file__).resolve().parent.parent
    models_dir = base_dir / "03_model" / "saved_models"
    output_name = os.getenv("SIGNAL_SEARCH_OUTPUT", "signal_search")
    results_dir = base_dir / "05_research" / "results" / output_name
    results_dir.mkdir(parents=True, exist_ok=True)
    df, feature_sets, metadata = load_data(base_dir)
    y = df["EVENTOS"].astype(float).reset_index(drop=True)
    splitter = TimeSeriesSplit(n_splits=N_SPLITS, test_size=TEST_SIZE)
    splits = list(splitter.split(df))
    all_rows = []
    prediction_frames = {}

    search_candidates = candidates()
    requested = os.getenv("SIGNAL_SEARCH_CANDIDATES", "").strip()
    if requested:
        requested_names = {name.strip() for name in requested.split(",") if name.strip()}
        search_candidates = [
            candidate for candidate in search_candidates if candidate.name in requested_names
        ]
        missing = requested_names - {candidate.name for candidate in search_candidates}
        if missing:
            raise ValueError(f"Unknown requested candidates: {sorted(missing)}")

    for number, candidate in enumerate(search_candidates, start=1):
        print(f"[{number:02d}/{len(search_candidates):02d}] {candidate.name}", flush=True)
        features = feature_sets[candidate.feature_set]
        fold_frames = []
        for fold, (train_idx, test_idx) in enumerate(splits, start=1):
            model = build_model(candidate.family)
            model.fit(df[features].iloc[train_idx], y.iloc[train_idx])
            prediction = np.clip(model.predict(df[features].iloc[test_idx]), 0, None)
            fold_frames.append(pd.DataFrame({
                "row": test_idx,
                "FECHA_DIA": df["FECHA_DIA"].iloc[test_idx].to_numpy(),
                "EVENTOS": y.iloc[test_idx].to_numpy(),
                "prediction": prediction,
                "fold": fold,
            }))
        predictions = pd.concat(fold_frames, ignore_index=True)
        prediction_frames[candidate.name] = predictions
        base_metrics = metrics(predictions["EVENTOS"], predictions["prediction"])
        all_rows.append({
            "candidate": candidate.name,
            "family": candidate.family,
            "feature_set": candidate.feature_set,
            "calibration": "raw",
            "feature_count": len(features),
            **base_metrics,
        })
        for calibration, values in sequential_calibrations(predictions).items():
            all_rows.append({
                "candidate": candidate.name,
                "family": candidate.family,
                "feature_set": candidate.feature_set,
                "calibration": calibration,
                "feature_count": len(features),
                **metrics(predictions["EVENTOS"], values),
            })
        pd.DataFrame(all_rows).to_csv(
            results_dir / "model_search_partial.csv",
            sep=";",
            index=False,
        )

    results = pd.DataFrame(all_rows)
    official = results[
        (results["candidate"] == "official_temporal")
        & (results["calibration"] == "raw")
    ].iloc[0]
    results["mae_vs_official_pct"] = 100 * (results["mae"] / official["mae"] - 1)
    results["rmse_vs_official_pct"] = 100 * (results["rmse"] / official["rmse"] - 1)
    results["passes_accuracy_gate"] = (
        (results["mae"] <= official["mae"] * 1.01)
        & (results["rmse"] <= official["rmse"] * 1.01)
        & (results["high_auc"] >= official["high_auc"] - 0.01)
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
    results.to_csv(results_dir / "model_search.csv", sep=";", index=False)

    winner = results[results["passes_accuracy_gate"]].iloc[0]
    winning_predictions = prediction_frames[winner["candidate"]].copy()
    if winner["calibration"] != "raw":
        winning_predictions["candidate_prediction"] = sequential_calibrations(
            winning_predictions
        )[winner["calibration"]]
    else:
        winning_predictions["candidate_prediction"] = winning_predictions["prediction"]
    winning_predictions.to_csv(results_dir / "winner_oof_predictions.csv", sep=";", index=False)
    summary = {
        "protocol": f"{N_SPLITS} expanding folds x {TEST_SIZE} days",
        "evaluation_start": str(winning_predictions["FECHA_DIA"].iloc[0]),
        "evaluation_end": str(winning_predictions["FECHA_DIA"].iloc[-1]),
        "official": official.to_dict(),
        "winner": winner.to_dict(),
        "accuracy_gate": {
            "max_mae_degradation": 0.01,
            "max_rmse_degradation": 0.01,
            "max_high_auc_degradation": 0.01,
        },
        "official_random_cv_r2": metadata.get("r2"),
    }
    with (results_dir / "summary.json").open("w", encoding="utf-8") as file:
        json.dump(summary, file, ensure_ascii=False, indent=2, default=str)
    print("\nTop accuracy-gated candidates:")
    print(results[results["passes_accuracy_gate"]].head(15)[[
        "candidate", "calibration", "mae", "rmse", "r2", "high_auc",
        "std_ratio", "p05_p95_width", "boring_4_5_share", "resolution_score",
    ]].to_string(index=False, float_format="%.3f"))
    print(f"\nSelected: {winner['candidate']} / {winner['calibration']}")


if __name__ == "__main__":
    main()
