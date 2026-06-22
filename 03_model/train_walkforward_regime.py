"""Train a leakage-aware walk-forward, two-regime, direct-horizon candidate."""

from __future__ import annotations

import json
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    precision_score,
    r2_score,
    recall_score,
    roc_auc_score,
)

from train_repeated_kfold import add_weekday_columns, load_feature_cols

import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from model_components import WalkForwardRegimeClassifier, WalkForwardRegimeRegressor


RANDOM_STATE = 42
HORIZONS = tuple(range(1, 7))
CRITICAL_THRESHOLD = 7.0
MIN_TRAIN_DAYS = 365
TEST_BLOCK_DAYS = 28
COUNT_PARAMS = {
    "objective": "reg:squarederror",
    "n_estimators": 140,
    "max_depth": 4,
    "learning_rate": 0.03,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 5,
    "reg_lambda": 1.0,
    "random_state": RANDOM_STATE,
    "n_jobs": 1,
}
CLASSIFIER_PARAMS = {
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "n_estimators": 160,
    "max_depth": 4,
    "learning_rate": 0.03,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 5,
    "reg_lambda": 1.0,
    "random_state": RANDOM_STATE,
    "n_jobs": 1,
}


def dump_pickle(value, path: Path) -> None:
    with path.open("wb") as stream:
        pickle.dump(value, stream)


def feature_columns(models_dir: Path, frame: pd.DataFrame) -> list[str]:
    columns, _ = load_feature_cols(models_dir, frame)
    additions = [
        "MES_SIN", "MES_COS", "DANO_SIN", "DANO_COS", "ES_FIN_SEMANA",
        "ES_FERIADO", "DIA_LUNES", "DIA_MARTES", "DIA_MIERCOLES",
        "DIA_JUEVES", "DIA_VIERNES", "DIA_SABADO", "DIA_DOMINGO",
        "ES_PRE_FERIADO", "DIAS_DESDE_ULTIMA_LLUVIA", "VPD", "VPD_MAX",
        "EVENTOS_rolling_mean_14d", "EVENTOS_rolling_mean_30d",
    ]
    for column in additions:
        if column in frame.columns and column not in columns:
            columns.append(column)
    return columns


def direct_horizon_matrix(
    frame: pd.DataFrame, columns: list[str], horizon: int
) -> tuple[pd.DataFrame, pd.Series, np.ndarray]:
    target_indices = np.arange(horizon, len(frame), dtype=int)
    origin_indices = target_indices - horizon
    X = frame.iloc[target_indices][columns].reset_index(drop=True).copy()
    target = pd.to_numeric(frame.iloc[target_indices]["EVENTOS"], errors="coerce").reset_index(drop=True)
    history = pd.to_numeric(frame["EVENTOS"], errors="coerce").fillna(0.0).to_numpy()
    if "EVENTOS_rolling_mean_14d" in X:
        X["EVENTOS_rolling_mean_14d"] = [
            float(np.mean(history[max(0, origin - 13): origin + 1]))
            for origin in origin_indices
        ]
    if "EVENTOS_rolling_mean_30d" in X:
        X["EVENTOS_rolling_mean_30d"] = [
            float(np.mean(history[max(0, origin - 29): origin + 1]))
            for origin in origin_indices
        ]
    return X, target, target_indices


def fit_regime_models(X: pd.DataFrame, y: pd.Series) -> dict[str, object]:
    high_target = (y > CRITICAL_THRESHOLD).astype(int)
    positives = max(int(high_target.sum()), 1)
    negatives = max(int((1 - high_target).sum()), 1)
    weights = np.where(
        high_target.to_numpy() == 1,
        len(y) / (2.0 * positives),
        len(y) / (2.0 * negatives),
    )
    classifier = xgb.XGBClassifier(**CLASSIFIER_PARAMS)
    classifier.fit(X, high_target, sample_weight=weights)

    normal_mask = high_target.eq(0)
    high_mask = ~normal_mask
    if int(high_mask.sum()) < 20:
        raise ValueError("Not enough high-activity observations to fit regime model")
    normal = xgb.XGBRegressor(**COUNT_PARAMS)
    high = xgb.XGBRegressor(**COUNT_PARAMS)
    normal.fit(X.loc[normal_mask], y.loc[normal_mask])
    high.fit(X.loc[high_mask], y.loc[high_mask])
    return {"classifier": classifier, "normal": normal, "high": high}


def predict_regime(details: dict[str, object], X: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    probability = details["classifier"].predict_proba(X)[:, 1]
    normal = np.clip(details["normal"].predict(X), 0, None)
    high = np.clip(details["high"].predict(X), 0, None)
    count = (1.0 - probability) * normal + probability * high
    return count, probability


def metrics(y: np.ndarray, count: np.ndarray, probability: np.ndarray) -> dict[str, float]:
    target = (y > CRITICAL_THRESHOLD).astype(int)
    labels = probability >= 0.30
    mse = float(mean_squared_error(y, count))
    return {
        "mae": float(mean_absolute_error(y, count)),
        "mse": mse,
        "rmse": float(mse**0.5),
        "r2": float(r2_score(y, count)),
        "roc_auc": float(roc_auc_score(target, probability)),
        "brier": float(brier_score_loss(target, probability)),
        "accuracy": float(accuracy_score(target, labels)),
        "precision": float(precision_score(target, labels, zero_division=0)),
        "recall": float(recall_score(target, labels, zero_division=0)),
        "f1": float(f1_score(target, labels, zero_division=0)),
        "prediction_std": float(np.std(count, ddof=1)),
        "target_std": float(np.std(y, ddof=1)),
        "variability_ratio": float(np.std(count, ddof=1) / np.std(y, ddof=1)),
    }


def main() -> int:
    models_dir = PROJECT_ROOT / "03_model" / "saved_models"
    frame = pd.read_csv(PROJECT_ROOT / "02_data" / "augmented_emergency_data.csv", sep=";")
    frame = add_weekday_columns(frame.sort_values("FECHA_DIA").reset_index(drop=True))
    frame["EVENTOS"] = pd.to_numeric(frame["EVENTOS"], errors="coerce")
    frame = frame.loc[frame["EVENTOS"].notna()].reset_index(drop=True)
    columns = feature_columns(models_dir, frame)
    matrices = {
        horizon: direct_horizon_matrix(frame, columns, horizon)
        for horizon in HORIZONS
    }

    prediction_rows: list[dict[str, object]] = []
    fold_rows: list[dict[str, object]] = []
    fold = 0
    test_start = MIN_TRAIN_DAYS
    while test_start < len(frame):
        test_end = min(test_start + TEST_BLOCK_DAYS, len(frame))
        fold += 1
        models = {}
        for horizon in HORIZONS:
            X_all, y_all, target_indices = matrices[horizon]
            train_mask = target_indices < test_start
            models[horizon] = fit_regime_models(X_all.loc[train_mask], y_all.loc[train_mask])

        fold_y, fold_count, fold_probability = [], [], []
        for offset, target_index in enumerate(range(test_start, test_end)):
            horizon = (offset % len(HORIZONS)) + 1
            X_all, y_all, target_indices = matrices[horizon]
            row_position = int(np.flatnonzero(target_indices == target_index)[0])
            X_test = X_all.iloc[[row_position]]
            count, probability = predict_regime(models[horizon], X_test)
            actual = float(y_all.iloc[row_position])
            fold_y.append(actual)
            fold_count.append(float(count[0]))
            fold_probability.append(float(probability[0]))
            prediction_rows.append({
                "fold": fold,
                "FECHA_DIA": frame.loc[target_index, "FECHA_DIA"],
                "horizon": horizon,
                "EVENTOS": actual,
                "PRED_EVENTOS_WALKFORWARD": float(count[0]),
                "PROB_ALTA_WALKFORWARD": float(probability[0]),
            })
        fold_rows.append({
            "fold": fold,
            "train_end": frame.loc[test_start - 1, "FECHA_DIA"],
            "test_start": frame.loc[test_start, "FECHA_DIA"],
            "test_end": frame.loc[test_end - 1, "FECHA_DIA"],
            "n_train": test_start,
            "n_test": test_end - test_start,
            **metrics(np.asarray(fold_y), np.asarray(fold_count), np.asarray(fold_probability)),
        })
        print(f"Completed walk-forward fold {fold}: {test_start}-{test_end - 1}", flush=True)
        test_start = test_end

    predictions = pd.DataFrame(prediction_rows)
    aggregate = metrics(
        predictions["EVENTOS"].to_numpy(),
        predictions["PRED_EVENTOS_WALKFORWARD"].to_numpy(),
        predictions["PROB_ALTA_WALKFORWARD"].to_numpy(),
    )

    final_models = {}
    classifier_models = {}
    for horizon in HORIZONS:
        X_all, y_all, _ = matrices[horizon]
        details = fit_regime_models(X_all, y_all)
        final_models[horizon] = details
        classifier_models[horizon] = details["classifier"]
    regressor = WalkForwardRegimeRegressor(final_models, columns)
    classifier = WalkForwardRegimeClassifier(classifier_models, columns)
    importances = {
        column: float(value)
        for column, value in zip(columns, regressor.feature_importances_)
    }
    metadata = {
        "model_role": "walk_forward_two_regime_direct_horizon",
        "validation_protocol": "Walk-Forward 28D · Two-Regime · Direct H1-H6",
        "operational_use": False,
        "is_primary": False,
        "feature_cols": columns,
        "feature_importances": importances,
        "umbral_alta_actividad": CRITICAL_THRESHOLD,
        "classification_threshold": 0.30,
        "regressor_type": "WalkForwardRegimeXGBRegressor",
        "classifier_type": "WalkForwardRegimeXGBClassifier",
        "forecast_horizons": list(HORIZONS),
        "outer_min_train_size": MIN_TRAIN_DAYS,
        "outer_test_size": TEST_BLOCK_DAYS,
        "cv_n_folds": fold,
        "train_samples": int(len(frame)),
        "test_samples": int(len(predictions)),
        "train_start_date": str(frame.iloc[0]["FECHA_DIA"]),
        "train_end_date": str(frame.iloc[-1]["FECHA_DIA"]),
        **aggregate,
    }

    stem = "walkforward_regime_direct6_xgboost"
    dump_pickle(regressor, models_dir / f"regressor_{stem}.pkl")
    dump_pickle(classifier, models_dir / f"classifier_{stem}.pkl")
    dump_pickle(metadata, models_dir / f"metadata_{stem}.pkl")
    predictions.to_csv(models_dir / f"{stem}_oof_predictions.csv", sep=";", index=False)
    pd.DataFrame(fold_rows).to_csv(models_dir / f"{stem}_fold_metrics.csv", sep=";", index=False)
    with (models_dir / f"{stem}_summary.json").open("w", encoding="utf-8") as stream:
        json.dump(metadata, stream, indent=2, ensure_ascii=False)

    print(json.dumps(aggregate, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
