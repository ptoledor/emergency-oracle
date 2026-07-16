"""Train a leakage-aware walk-forward, two-regime, direct-horizon candidate."""

from __future__ import annotations

import json
import pickle
import sys
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

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from model_components import WalkForwardRegimeClassifier, WalkForwardRegimeRegressor
from temporal_gate import build_origin_horizon_pairs, evaluate_promotion_gate


RANDOM_STATE = 42
HORIZONS = tuple(range(1, 7))
CRITICAL_THRESHOLD = 7.0
MIN_TRAIN_DAYS = 365
REFIT_BLOCK_DAYS = 28
COUNT_BASELINE_WINDOW = 28
RISK_BASELINE_WINDOW = 90
CLASSIFICATION_THRESHOLD = 0.30
MIN_COUNT_RELATIVE_IMPROVEMENT = 0.03
MAX_BRIER_RELATIVE_DEGRADATION = 0.02
MIN_HORIZON_FRACTION_IMPROVED = 2.0 / 3.0
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
    """Build one direct-horizon matrix without using target-day event history."""

    target_indices = np.arange(horizon, len(frame), dtype=int)
    origin_indices = target_indices - horizon
    X = frame.iloc[target_indices][columns].reset_index(drop=True).copy()
    target = pd.to_numeric(
        frame.iloc[target_indices]["EVENTOS"], errors="coerce"
    ).reset_index(drop=True)
    history = pd.to_numeric(frame["EVENTOS"], errors="coerce").to_numpy(dtype=float)

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


def predict_regime(
    details: dict[str, object], X: pd.DataFrame
) -> tuple[np.ndarray, np.ndarray]:
    probability = details["classifier"].predict_proba(X)[:, 1]
    normal = np.clip(details["normal"].predict(X), 0, None)
    high = np.clip(details["high"].predict(X), 0, None)
    count = (1.0 - probability) * normal + probability * high
    return count, probability


def safe_roc_auc(target: np.ndarray, probability: np.ndarray) -> float:
    return (
        float(roc_auc_score(target, probability))
        if np.unique(target).size == 2
        else float("nan")
    )


def metrics(
    y: np.ndarray,
    count: np.ndarray,
    probability: np.ndarray,
) -> dict[str, float]:
    target = (y > CRITICAL_THRESHOLD).astype(int)
    labels = probability >= CLASSIFICATION_THRESHOLD
    mse = float(mean_squared_error(y, count))
    target_std = float(np.std(y, ddof=1))
    return {
        "mae": float(mean_absolute_error(y, count)),
        "mse": mse,
        "rmse": float(mse**0.5),
        "r2": float(r2_score(y, count)),
        "roc_auc": safe_roc_auc(target, probability),
        "brier": float(brier_score_loss(target, probability)),
        "accuracy": float(accuracy_score(target, labels)),
        "precision": float(precision_score(target, labels, zero_division=0)),
        "recall": float(recall_score(target, labels, zero_division=0)),
        "f1": float(f1_score(target, labels, zero_division=0)),
        "prediction_std": float(np.std(count, ddof=1)),
        "target_std": target_std,
        "variability_ratio": (
            float(np.std(count, ddof=1) / target_std)
            if target_std > 0
            else float("nan")
        ),
    }


def count_metrics(y: np.ndarray, prediction: np.ndarray) -> dict[str, float]:
    mse = float(mean_squared_error(y, prediction))
    return {
        "mae": float(mean_absolute_error(y, prediction)),
        "mse": mse,
        "rmse": float(mse**0.5),
        "r2": float(r2_score(y, prediction)),
    }


def probability_metrics(y: np.ndarray, probability: np.ndarray) -> dict[str, float]:
    target = (y > CRITICAL_THRESHOLD).astype(int)
    return {
        "roc_auc": safe_roc_auc(target, probability),
        "brier": float(brier_score_loss(target, probability)),
    }


def baseline_values(history: np.ndarray) -> tuple[float, float, float]:
    """Return persistence, trailing-mean and trailing-risk baselines."""

    persistence = float(history[-1])
    rolling_count = float(np.mean(history[-COUNT_BASELINE_WINDOW:]))
    risk_history = history[-RISK_BASELINE_WINDOW:]
    rolling_risk = float(np.mean(risk_history > CRITICAL_THRESHOLD))
    return persistence, rolling_count, rolling_risk


def evaluate_horizons(predictions: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for horizon, group in predictions.groupby("horizon", sort=True):
        y = group["EVENTOS"].to_numpy(dtype=float)
        candidate = metrics(
            y,
            group["PRED_EVENTOS_WALKFORWARD"].to_numpy(dtype=float),
            group["PROB_ALTA_WALKFORWARD"].to_numpy(dtype=float),
        )
        persistence = count_metrics(
            y,
            group["BASELINE_PERSISTENCE"].to_numpy(dtype=float),
        )
        rolling = count_metrics(
            y,
            group["BASELINE_ROLLING_28D"].to_numpy(dtype=float),
        )
        risk = probability_metrics(
            y,
            group["BASELINE_PROB_HIGH_90D"].to_numpy(dtype=float),
        )
        rows.append({
            "horizon": int(horizon),
            "n_pairs": int(len(group)),
            **{f"candidate_{key}": value for key, value in candidate.items()},
            **{f"persistence_{key}": value for key, value in persistence.items()},
            **{f"rolling_28d_{key}": value for key, value in rolling.items()},
            **{f"probability_90d_{key}": value for key, value in risk.items()},
        })
    return pd.DataFrame(rows)


def main() -> int:
    models_dir = PROJECT_ROOT / "03_model" / "saved_models"
    frame = pd.read_csv(
        PROJECT_ROOT / "02_data" / "augmented_emergency_data.csv",
        sep=";",
    )
    frame = add_weekday_columns(frame.sort_values("FECHA_DIA").reset_index(drop=True))
    frame["EVENTOS"] = pd.to_numeric(frame["EVENTOS"], errors="coerce")
    frame = frame.loc[frame["EVENTOS"].notna()].reset_index(drop=True)
    columns = feature_columns(models_dir, frame)
    matrices = {
        horizon: direct_horizon_matrix(frame, columns, horizon)
        for horizon in HORIZONS
    }
    positions = {
        horizon: {
            int(target_index): position
            for position, target_index in enumerate(target_indices)
        }
        for horizon, (_, _, target_indices) in matrices.items()
    }

    all_pairs = build_origin_horizon_pairs(
        len(frame),
        min_train_days=MIN_TRAIN_DAYS,
        horizons=HORIZONS,
    )
    if not all_pairs:
        raise ValueError("Not enough observations for walk-forward evaluation")

    prediction_rows: list[dict[str, object]] = []
    fold_rows: list[dict[str, object]] = []
    first_origin = all_pairs[0][0]
    last_origin = all_pairs[-1][0]
    fold = 0
    block_start = first_origin

    while block_start <= last_origin:
        block_end = min(block_start + REFIT_BLOCK_DAYS, last_origin + 1)
        fold += 1
        models: dict[int, dict[str, object]] = {}
        for horizon in HORIZONS:
            X_all, y_all, target_indices = matrices[horizon]
            train_mask = target_indices <= block_start
            models[horizon] = fit_regime_models(
                X_all.loc[train_mask].reset_index(drop=True),
                y_all.loc[train_mask].reset_index(drop=True),
            )

        fold_start_row = len(prediction_rows)
        for origin_index in range(block_start, block_end):
            history = frame.loc[:origin_index, "EVENTOS"].to_numpy(dtype=float)
            persistence, rolling_count, rolling_risk = baseline_values(history)

            for horizon in HORIZONS:
                target_index = origin_index + horizon
                if target_index >= len(frame):
                    continue
                X_all, y_all, _ = matrices[horizon]
                row_position = positions[horizon][target_index]
                X_test = X_all.iloc[[row_position]]
                count, probability = predict_regime(models[horizon], X_test)
                actual = float(y_all.iloc[row_position])
                prediction_rows.append({
                    "fold": fold,
                    "ORIGIN_DATE": frame.loc[origin_index, "FECHA_DIA"],
                    "FECHA_DIA": frame.loc[target_index, "FECHA_DIA"],
                    "origin_index": int(origin_index),
                    "target_index": int(target_index),
                    "horizon": int(horizon),
                    "EVENTOS": actual,
                    "PRED_EVENTOS_WALKFORWARD": float(count[0]),
                    "PROB_ALTA_WALKFORWARD": float(probability[0]),
                    "BASELINE_PERSISTENCE": persistence,
                    "BASELINE_ROLLING_28D": rolling_count,
                    "BASELINE_PROB_HIGH_90D": rolling_risk,
                })

        fold_frame = pd.DataFrame(prediction_rows[fold_start_row:])
        fold_metric = metrics(
            fold_frame["EVENTOS"].to_numpy(dtype=float),
            fold_frame["PRED_EVENTOS_WALKFORWARD"].to_numpy(dtype=float),
            fold_frame["PROB_ALTA_WALKFORWARD"].to_numpy(dtype=float),
        )
        fold_rows.append({
            "fold": fold,
            "train_end": frame.loc[block_start, "FECHA_DIA"],
            "first_origin": frame.loc[block_start, "FECHA_DIA"],
            "last_origin": frame.loc[block_end - 1, "FECHA_DIA"],
            "n_train_targets_h1": int(block_start),
            "n_origins": int(block_end - block_start),
            "n_origin_horizon_pairs": int(len(fold_frame)),
            **fold_metric,
        })
        print(
            f"Completed walk-forward fold {fold}: origins "
            f"{block_start}-{block_end - 1}, pairs={len(fold_frame)}",
            flush=True,
        )
        block_start = block_end

    predictions = pd.DataFrame(prediction_rows)
    actual = predictions["EVENTOS"].to_numpy(dtype=float)
    aggregate = metrics(
        actual,
        predictions["PRED_EVENTOS_WALKFORWARD"].to_numpy(dtype=float),
        predictions["PROB_ALTA_WALKFORWARD"].to_numpy(dtype=float),
    )
    persistence_metrics = count_metrics(
        actual,
        predictions["BASELINE_PERSISTENCE"].to_numpy(dtype=float),
    )
    rolling_metrics = count_metrics(
        actual,
        predictions["BASELINE_ROLLING_28D"].to_numpy(dtype=float),
    )
    risk_baseline_metrics = probability_metrics(
        actual,
        predictions["BASELINE_PROB_HIGH_90D"].to_numpy(dtype=float),
    )
    horizon_metrics = evaluate_horizons(predictions)

    candidate_horizon_mae = dict(zip(
        horizon_metrics["horizon"].astype(int),
        horizon_metrics["candidate_mae"].astype(float),
    ))
    baseline_horizon_mae = dict(zip(
        horizon_metrics["horizon"].astype(int),
        horizon_metrics[["persistence_mae", "rolling_28d_mae"]]
        .min(axis=1)
        .astype(float),
    ))
    gate = evaluate_promotion_gate(
        candidate_mae=aggregate["mae"],
        count_baseline_mae={
            "persistence": persistence_metrics["mae"],
            "rolling_28d": rolling_metrics["mae"],
        },
        candidate_brier=aggregate["brier"],
        probability_baseline_brier=risk_baseline_metrics["brier"],
        candidate_horizon_mae=candidate_horizon_mae,
        baseline_horizon_mae=baseline_horizon_mae,
        min_count_relative_improvement=MIN_COUNT_RELATIVE_IMPROVEMENT,
        max_brier_relative_degradation=MAX_BRIER_RELATIVE_DEGRADATION,
        min_horizon_fraction_improved=MIN_HORIZON_FRACTION_IMPROVED,
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
        "validation_protocol": (
            "Blocked Rolling-Origin 28D Refit · Every Origin H1-H6 · Two-Regime"
        ),
        "operational_use": False,
        "is_primary": False,
        "feature_cols": columns,
        "feature_importances": importances,
        "umbral_alta_actividad": CRITICAL_THRESHOLD,
        "classification_threshold": CLASSIFICATION_THRESHOLD,
        "regressor_type": "WalkForwardRegimeXGBRegressor",
        "classifier_type": "WalkForwardRegimeXGBClassifier",
        "forecast_horizons": list(HORIZONS),
        "outer_min_train_size": MIN_TRAIN_DAYS,
        "outer_refit_block_days": REFIT_BLOCK_DAYS,
        "cv_n_folds": fold,
        "forecast_origins": int(predictions["origin_index"].nunique()),
        "evaluation_pairs": int(len(predictions)),
        "train_samples": int(len(frame)),
        "test_samples": int(len(predictions)),
        "train_start_date": str(frame.iloc[0]["FECHA_DIA"]),
        "train_end_date": str(frame.iloc[-1]["FECHA_DIA"]),
        "count_baselines": {
            "persistence": persistence_metrics,
            "rolling_28d": rolling_metrics,
        },
        "risk_baseline_90d": risk_baseline_metrics,
        "promotion_gate": {
            **gate.as_dict(),
            "min_count_relative_improvement": MIN_COUNT_RELATIVE_IMPROVEMENT,
            "max_brier_relative_degradation": MAX_BRIER_RELATIVE_DEGRADATION,
            "min_horizon_fraction_improved": MIN_HORIZON_FRACTION_IMPROVED,
        },
        "passes_baseline_gate": gate.passes,
        **aggregate,
    }

    stem = "walkforward_regime_direct6_xgboost"
    dump_pickle(regressor, models_dir / f"regressor_{stem}.pkl")
    dump_pickle(classifier, models_dir / f"classifier_{stem}.pkl")
    dump_pickle(metadata, models_dir / f"metadata_{stem}.pkl")
    predictions.to_csv(
        models_dir / f"{stem}_oof_predictions.csv",
        sep=";",
        index=False,
    )
    pd.DataFrame(fold_rows).to_csv(
        models_dir / f"{stem}_fold_metrics.csv",
        sep=";",
        index=False,
    )
    horizon_metrics.to_csv(
        models_dir / f"{stem}_horizon_metrics.csv",
        sep=";",
        index=False,
    )
    with (models_dir / f"{stem}_summary.json").open(
        "w", encoding="utf-8"
    ) as stream:
        json.dump(metadata, stream, indent=2, ensure_ascii=False)

    print(json.dumps({
        "candidate": aggregate,
        "count_baselines": metadata["count_baselines"],
        "risk_baseline_90d": risk_baseline_metrics,
        "promotion_gate": metadata["promotion_gate"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
