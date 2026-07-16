"""Build the verified high-resolution candidate without promoting it."""

from __future__ import annotations

import argparse
import json
import os
import pickle
import sys
from pathlib import Path

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import TimeSeriesSplit
from xgboost import XGBClassifier, XGBRegressor

from model_components import RegressorProbabilityClassifier
from search_signal_models import N_SPLITS, TEST_SIZE, load_data, metrics


SUFFIX = "signal_xgb_d3_flexible"
REGRESSOR_PARAMS = {
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
CLASSIFIER_PARAMS = {"method": "Platt scaling over count score", "C": 1.0}


def select_threshold(y, probability, precision_target=0.30):
    rows = []
    for threshold in np.arange(0.05, 0.81, 0.01):
        predicted = probability >= threshold
        rows.append((
            float(threshold),
            float(precision_score(y, predicted, zero_division=0)),
            float(recall_score(y, predicted, zero_division=0)),
            float(f1_score(y, predicted, zero_division=0)),
        ))
    eligible = [row for row in rows if row[1] >= precision_target]
    return max(eligible, key=lambda row: (row[2], row[3])) if eligible else max(rows, key=lambda row: row[3])


def block_bootstrap_improvement(y, official, candidate, block_size=14, repeats=20000):
    mae_delta = np.abs(y - official) - np.abs(y - candidate)
    mse_delta = (y - official) ** 2 - (y - candidate) ** 2
    rng = np.random.default_rng(42)
    starts = np.arange(len(y) - block_size + 1)
    samples = {"mae": [], "mse": []}
    for _ in range(repeats):
        indices = []
        while len(indices) < len(y):
            start = int(rng.choice(starts))
            indices.extend(range(start, start + block_size))
        selected = np.asarray(indices[:len(y)])
        samples["mae"].append(float(np.mean(mae_delta[selected])))
        samples["mse"].append(float(np.mean(mse_delta[selected])))
    result = {}
    for name, values in samples.items():
        values = np.asarray(values)
        result[f"{name}_improvement_mean"] = float(np.mean(values))
        result[f"{name}_improvement_ci_low"] = float(np.quantile(values, 0.025))
        result[f"{name}_improvement_ci_high"] = float(np.quantile(values, 0.975))
        result[f"{name}_probability_improves"] = float(np.mean(values > 0))
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--promote",
        action="store_true",
        help="Mark the verified candidate as active after explicit user approval.",
    )
    args = parser.parse_args()
    base_dir = Path(__file__).resolve().parent.parent
    models_dir = base_dir / "03_model" / "saved_models"
    results_dir = base_dir / "05_research" / "results" / "signal_candidate"
    results_dir.mkdir(parents=True, exist_ok=True)
    df, feature_sets, official_metadata = load_data(base_dir)
    features = feature_sets["all_operational"]
    y_count = df["EVENTOS"].astype(float).reset_index(drop=True)
    high_threshold = 7.0
    y_high = (y_count > high_threshold).astype(int)
    splits = list(TimeSeriesSplit(n_splits=N_SPLITS, test_size=TEST_SIZE).split(df))

    tuned_oof = pd.read_csv(
        base_dir / "05_research" / "results" / "signal_search_xgb_tuning" / "oof_predictions.csv",
        sep=";",
    )
    candidate_count = tuned_oof["d3_flexible"].to_numpy(float)
    official_count = tuned_oof["official_temporal"].to_numpy(float)
    evaluation_y = tuned_oof["EVENTOS"].to_numpy(float)
    evaluation_dates = tuned_oof["FECHA_DIA"].astype(str)

    evaluation_high = evaluation_y > high_threshold
    official_classifier_oof = []
    official_classifier_params = {
        "n_estimators": 100,
        "max_depth": 4,
        "learning_rate": 0.03,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "n_jobs": 1,
        "random_state": 42,
    }
    for train_idx, test_idx in splits:
        official_classifier = XGBClassifier(**official_classifier_params).fit(
            df[feature_sets["official_50"]].iloc[train_idx],
            y_high.iloc[train_idx],
        )
        official_classifier_oof.extend(
            official_classifier.predict_proba(
                df[feature_sets["official_50"]].iloc[test_idx]
            )[:, 1]
        )
    official_classifier_oof = np.asarray(official_classifier_oof)
    fold_numbers = np.repeat(np.arange(1, N_SPLITS + 1), TEST_SIZE)
    classifier_oof = np.empty(len(candidate_count), dtype=float)
    for fold, (train_idx, _) in enumerate(splits, start=1):
        current = fold_numbers == fold
        previous = fold_numbers < fold
        if previous.sum() < TEST_SIZE:
            classifier_oof[current] = float(y_high.iloc[train_idx].mean())
            continue
        temporal_calibrator = LogisticRegression(C=1.0).fit(
            candidate_count[previous].reshape(-1, 1),
            evaluation_high[previous],
        )
        classifier_oof[current] = temporal_calibrator.predict_proba(
            candidate_count[current].reshape(-1, 1)
        )[:, 1]
    threshold, threshold_precision, threshold_recall, _ = select_threshold(
        evaluation_high,
        classifier_oof,
    )
    alert_prediction = classifier_oof >= threshold

    print("training final candidate", flush=True)
    regressor = XGBRegressor(**REGRESSOR_PARAMS).fit(df[features], y_count)
    final_calibrator = LogisticRegression(C=1.0).fit(
        candidate_count.reshape(-1, 1),
        evaluation_high,
    )
    classifier = RegressorProbabilityClassifier(
        regressor=regressor,
        coefficient=float(final_calibrator.coef_[0, 0]),
        intercept=float(final_calibrator.intercept_[0]),
        feature_cols=features,
    )
    feature_importances = {
        column: float(value)
        for column, value in zip(features, regressor.feature_importances_)
    }
    count_metrics = metrics(evaluation_y, candidate_count)
    official_temporal_metrics = metrics(evaluation_y, official_count)
    official_threshold, _, _, _ = select_threshold(
        evaluation_high,
        official_classifier_oof,
    )
    official_alert_prediction = official_classifier_oof >= official_threshold
    official_temporal_metrics.update({
        "roc_auc": float(roc_auc_score(evaluation_high, official_classifier_oof)),
        "brier": float(brier_score_loss(evaluation_high, official_classifier_oof)),
        "classification_threshold": official_threshold,
        "accuracy": float(accuracy_score(evaluation_high, official_alert_prediction)),
        "precision": float(precision_score(evaluation_high, official_alert_prediction, zero_division=0)),
        "recall": float(recall_score(evaluation_high, official_alert_prediction, zero_division=0)),
        "f1": float(f1_score(evaluation_high, official_alert_prediction, zero_division=0)),
    })
    clipped_count = np.clip(candidate_count, 0.05, None)
    negative_binomial_alpha = float(max(
        np.sum((evaluation_y - clipped_count) ** 2 - clipped_count)
        / np.sum(clipped_count ** 2),
        1e-4,
    ))
    bootstrap = block_bootstrap_improvement(
        evaluation_y,
        official_count,
        candidate_count,
    )

    top20_candidate = candidate_count >= np.quantile(candidate_count, 0.80)
    top20_official = official_count >= np.quantile(official_count, 0.80)
    high_day_metrics = {
        "candidate_top20_precision": float(np.mean(evaluation_high[top20_candidate])),
        "candidate_top20_recall": float(np.mean(top20_candidate[evaluation_high])),
        "official_top20_precision": float(np.mean(evaluation_high[top20_official])),
        "official_top20_recall": float(np.mean(top20_official[evaluation_high])),
    }
    official_temporal_metrics.update({
        "top20_precision": high_day_metrics["official_top20_precision"],
        "top20_recall": high_day_metrics["official_top20_recall"],
    })
    metadata = {
        "model_role": "high_resolution_count_candidate",
        "validation_protocol": "Walk-forward 6x120 dias",
        "operational_use": bool(args.promote),
        "is_primary": bool(args.promote),
        "promoted_model": bool(args.promote),
        "candidate_suffix": SUFFIX,
        "regressor_type": "XGBRegressor",
        "classifier_type": "RegressorProbabilityClassifier",
        "feature_cols": features,
        "feature_count": len(features),
        "feature_importances": feature_importances,
        "regressor_params": REGRESSOR_PARAMS,
        "classifier_params": CLASSIFIER_PARAMS,
        "train_samples": int(len(df)),
        "test_samples": int(len(evaluation_y)),
        "train_start_date": str(df["FECHA_DIA"].iloc[0]),
        "train_end_date": str(df["FECHA_DIA"].iloc[-1]),
        "test_start_date": str(evaluation_dates.iloc[0]),
        "test_end_date": str(evaluation_dates.iloc[-1]),
        "mae": count_metrics["mae"],
        "rmse": count_metrics["rmse"],
        "r2": count_metrics["r2"],
        "count_high_auc": count_metrics["high_auc"],
        "prediction_std": count_metrics["prediction_std"],
        "std_ratio": count_metrics["std_ratio"],
        "p05": count_metrics["p05"],
        "p95": count_metrics["p95"],
        "p05_p95_width": count_metrics["p05_p95_width"],
        "boring_4_5_share": count_metrics["boring_4_5_share"],
        "top20_precision": high_day_metrics["candidate_top20_precision"],
        "top20_recall": high_day_metrics["candidate_top20_recall"],
        "official_temporal_metrics": official_temporal_metrics,
        "umbral_alta_actividad": high_threshold,
        "classification_threshold": threshold,
        "threshold_oof_precision": threshold_precision,
        "threshold_oof_recall": threshold_recall,
        "roc_auc": float(roc_auc_score(evaluation_high, classifier_oof)),
        "brier": float(brier_score_loss(evaluation_high, classifier_oof)),
        "accuracy": float(accuracy_score(evaluation_high, alert_prediction)),
        "precision": float(precision_score(evaluation_high, alert_prediction, zero_division=0)),
        "recall": float(recall_score(evaluation_high, alert_prediction, zero_division=0)),
        "f1": float(f1_score(evaluation_high, alert_prediction, zero_division=0)),
        "negative_binomial_alpha": negative_binomial_alpha,
        "activity_score_method": "empirical_cdf_walk_forward_oof",
        "activity_prediction_reference": np.sort(candidate_count).tolist(),
        **high_day_metrics,
        **bootstrap,
    }
    oof = pd.DataFrame({
        "FECHA_DIA": evaluation_dates,
        "EVENTOS": evaluation_y,
        "PRED_EVENTOS_OFFICIAL_TEMPORAL": official_count,
        "PRED_EVENTOS_SIGNAL_OOF": candidate_count,
        "PROB_ALTA_SIGNAL_OOF": classifier_oof,
        "ALERTA_TARGET": evaluation_high.astype(int),
        "fold": fold_numbers,
    })
    oof.to_csv(models_dir / f"{SUFFIX}_oof_predictions.csv", sep=";", index=False)
    oof.to_csv(results_dir / "oof_predictions.csv", sep=";", index=False)
    with (models_dir / f"regressor_{SUFFIX}.pkl").open("wb") as file:
        pickle.dump(regressor, file)
    with (models_dir / f"classifier_{SUFFIX}.pkl").open("wb") as file:
        pickle.dump(classifier, file)
    with (models_dir / f"metadata_{SUFFIX}.pkl").open("wb") as file:
        pickle.dump(metadata, file)
    with (results_dir / "summary.json").open("w", encoding="utf-8") as file:
        json.dump(metadata, file, ensure_ascii=False, indent=2, default=str)
    if args.promote:
        with (models_dir / "active_models.json").open("w", encoding="utf-8") as file:
            json.dump({"climatic_augmented": SUFFIX}, file, indent=2)

    deciles = oof.assign(
        ACTIVITY_INDEX=oof["PRED_EVENTOS_SIGNAL_OOF"].rank(pct=True) * 100,
    )
    deciles["decile"] = pd.cut(deciles["ACTIVITY_INDEX"], np.arange(0, 101, 10), include_lowest=True)
    decile_summary = deciles.groupby("decile", observed=True).agg(
        days=("EVENTOS", "size"),
        prediction_mean=("PRED_EVENTOS_SIGNAL_OOF", "mean"),
        actual_mean=("EVENTOS", "mean"),
        high_activity_rate=("ALERTA_TARGET", "mean"),
    ).reset_index()
    decile_summary.to_csv(results_dir / "activity_deciles.csv", sep=";", index=False)

    print(f"saved={SUFFIX}")
    print(f"mae={metadata['mae']:.3f} rmse={metadata['rmse']:.3f} r2={metadata['r2']:.3f}")
    print(f"classifier_auc={metadata['roc_auc']:.3f} brier={metadata['brier']:.3f}")
    print("Candidate promoted." if args.promote else "Candidate was NOT promoted.")


if __name__ == "__main__":
    main()
