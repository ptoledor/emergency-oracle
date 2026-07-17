"""Train temporally calibrated category-risk classifiers.

Balanced Random Forests provide useful ranking for rare events, but their raw
probabilities are not calibrated prevalence estimates. A sigmoid calibrator is
fit exclusively on temporal out-of-fold predictions. Its effect is then
measured on the untouched final 20% before the serving artifact is rebuilt on
all available history.
"""

from __future__ import annotations

import json
import os
import pickle
import sys
from pathlib import Path

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("SKLEARN_NUM_THREADS", "1")

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    f1_score,
    log_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import TimeSeriesSplit

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from model_components import SigmoidProbabilityCalibratedClassifier


RANDOM_STATE = 42
CLASSIFIER_PARAMS = {
    "n_estimators": 300,
    "max_depth": 7,
    "min_samples_leaf": 5,
    "class_weight": "balanced_subsample",
    "n_jobs": 1,
    "random_state": RANDOM_STATE,
}
RISK_GROUPS = {
    "rescate_vehicular": ["N_RESCATE_VEH"],
    "incendio": ["N_INCENDIO_ESTR", "N_INCENDIO_FOREST"],
    "climaticas": ["N_EMERGENCIAS_CLIMATICAS"],
}
CALIBRATION_METHOD = "temporal_oof_platt_on_raw_probability"


def temporal_oof_probs(X: pd.DataFrame, y: pd.Series) -> np.ndarray:
    probabilities = np.full(len(X), np.nan)
    for train_idx, validation_idx in TimeSeriesSplit(n_splits=5).split(X):
        y_train = y.iloc[train_idx]
        if y_train.nunique() < 2:
            continue
        model = RandomForestClassifier(**CLASSIFIER_PARAMS)
        model.fit(X.iloc[train_idx], y_train)
        probabilities[validation_idx] = model.predict_proba(
            X.iloc[validation_idx]
        )[:, 1]
    return probabilities


def fit_sigmoid(raw_probability: np.ndarray, target: np.ndarray) -> tuple[float, float]:
    calibrator = LogisticRegression(C=1e6, solver="lbfgs", max_iter=2000)
    calibrator.fit(np.asarray(raw_probability).reshape(-1, 1), target)
    return float(calibrator.coef_[0, 0]), float(calibrator.intercept_[0])


def apply_sigmoid(
    raw_probability: np.ndarray, coefficient: float, intercept: float
) -> np.ndarray:
    score = np.clip(
        intercept + coefficient * np.asarray(raw_probability, dtype=float),
        -35,
        35,
    )
    return 1.0 / (1.0 + np.exp(-score))


def safe_auc(target: np.ndarray, probability: np.ndarray) -> float:
    return (
        float(roc_auc_score(target, probability))
        if np.unique(target).size == 2
        else float("nan")
    )


def expected_calibration_error(
    target: np.ndarray, probability: np.ndarray, bins: int = 10
) -> float:
    target = np.asarray(target, dtype=float)
    probability = np.asarray(probability, dtype=float)
    edges = np.linspace(0.0, 1.0, bins + 1)
    result = 0.0
    for lower, upper in zip(edges[:-1], edges[1:]):
        mask = (probability >= lower) & (
            probability < upper if upper < 1.0 else probability <= upper
        )
        if mask.any():
            result += float(mask.mean()) * abs(
                float(probability[mask].mean()) - float(target[mask].mean())
            )
    return float(result)


def probability_metrics(target: np.ndarray, probability: np.ndarray) -> dict:
    return {
        "mean_probability": float(np.mean(probability)),
        "positive_rate": float(np.mean(target)),
        "roc_auc": safe_auc(target, probability),
        "brier": float(brier_score_loss(target, probability)),
        "log_loss": float(log_loss(target, probability, labels=[0, 1])),
        "ece_10": expected_calibration_error(target, probability),
    }


def classification_metrics(
    target: np.ndarray, probability: np.ndarray, threshold: float
) -> dict:
    predicted = (np.asarray(probability) > threshold).astype(int)
    return {
        "accuracy_at_p80": float(accuracy_score(target, predicted)),
        "precision_at_p80": float(
            precision_score(target, predicted, zero_division=0)
        ),
        "recall_at_p80": float(recall_score(target, predicted, zero_division=0)),
        "f1_at_p80": float(f1_score(target, predicted, zero_division=0)),
    }


def main() -> None:
    data_path = PROJECT_ROOT / "02_data" / "augmented_emergency_data.csv"
    models_dir = PROJECT_ROOT / "03_model" / "saved_models"
    results_dir = PROJECT_ROOT / "05_research" / "results" / "category_risk_calibration"
    models_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)

    frame = (
        pd.read_csv(data_path, sep=";")
        .sort_values("FECHA_DIA")
        .reset_index(drop=True)
    )
    existing_artifact_path = models_dir / "category_risk_models.pkl"
    if existing_artifact_path.exists():
        with existing_artifact_path.open("rb") as stream:
            existing_artifact = pickle.load(stream)
        feature_cols = list(existing_artifact["feature_cols"])
    else:
        with (models_dir / "metadata_climatic_augmented.pkl").open("rb") as stream:
            primary_metadata = pickle.load(stream)
        feature_cols = list(primary_metadata["feature_cols"])
    X = frame[feature_cols]
    split_idx = int(len(frame) * 0.8)

    models: dict[str, dict] = {}
    summary_rows: list[dict] = []
    comparison_rows: list[dict] = []

    for group_name, source_columns in RISK_GROUPS.items():
        print(f"calibrating={group_name}", flush=True)
        total = frame[source_columns].sum(axis=1).astype(float)
        count_threshold = float(total.iloc[:split_idx].quantile(0.80))
        target = (total > count_threshold).astype(int)
        X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
        y_train, y_test = target.iloc[:split_idx], target.iloc[split_idx:]

        # Honest calibration fit: temporal OOF predictions from training only.
        train_raw_oof = temporal_oof_probs(X_train, y_train)
        train_valid = np.isfinite(train_raw_oof)
        if not train_valid.any():
            raise RuntimeError(f"No temporal OOF probabilities for {group_name}")
        train_oof_target = y_train.iloc[np.flatnonzero(train_valid)].to_numpy()
        coefficient, intercept = fit_sigmoid(
            train_raw_oof[train_valid], train_oof_target
        )
        train_calibrated_oof = apply_sigmoid(
            train_raw_oof[train_valid], coefficient, intercept
        )

        evaluation_model = RandomForestClassifier(**CLASSIFIER_PARAMS)
        evaluation_model.fit(X_train, y_train)
        test_raw_probability = evaluation_model.predict_proba(X_test)[:, 1]
        test_calibrated_probability = apply_sigmoid(
            test_raw_probability, coefficient, intercept
        )
        raw_test_metrics = probability_metrics(y_test.to_numpy(), test_raw_probability)
        calibrated_test_metrics = probability_metrics(
            y_test.to_numpy(), test_calibrated_probability
        )
        calibration_gates = {
            "brier_not_worse": bool(
                calibrated_test_metrics["brier"] <= raw_test_metrics["brier"]
            ),
            "log_loss_not_worse": bool(
                calibrated_test_metrics["log_loss"] <= raw_test_metrics["log_loss"]
            ),
            "auc_preserved": bool(
                calibrated_test_metrics["roc_auc"]
                >= raw_test_metrics["roc_auc"] - 1e-12
            ),
        }
        if not all(calibration_gates.values()):
            raise RuntimeError(
                f"Calibration gates failed for {group_name}: {calibration_gates}"
            )

        # Production rebuild: raw learner sees all history; its calibrator sees
        # only predictions generated before each OOF validation observation.
        full_raw_oof = temporal_oof_probs(X, target)
        full_valid = np.isfinite(full_raw_oof)
        full_oof_target = target.iloc[np.flatnonzero(full_valid)].to_numpy()
        final_coefficient, final_intercept = fit_sigmoid(
            full_raw_oof[full_valid], full_oof_target
        )
        full_calibrated_oof = apply_sigmoid(
            full_raw_oof[full_valid], final_coefficient, final_intercept
        )
        quantiles = {
            percentile: float(np.quantile(full_calibrated_oof, percentile))
            for percentile in (0.33, 0.50, 0.66, 0.80)
        }
        probability_p80 = quantiles[0.80]

        final_raw_model = RandomForestClassifier(**CLASSIFIER_PARAMS)
        final_raw_model.fit(X, target)
        serving_model = SigmoidProbabilityCalibratedClassifier(
            base_classifier=final_raw_model,
            coefficient=final_coefficient,
            intercept=final_intercept,
            feature_cols=feature_cols,
        )
        serving_probability = serving_model.predict_proba(X)[:, 1]
        reference_metrics = {
            **probability_metrics(full_oof_target, full_calibrated_oof),
            **classification_metrics(
                full_oof_target, full_calibrated_oof, probability_p80
            ),
        }
        test_metrics = {
            **calibrated_test_metrics,
            **classification_metrics(
                y_test.to_numpy(), test_calibrated_probability, probability_p80
            ),
        }

        models[group_name] = {
            "model": serving_model,
            "raw_model": final_raw_model,
            "source_cols": source_columns,
            "feature_cols": feature_cols,
            "count_threshold_p80": count_threshold,
            "probability_p33": quantiles[0.33],
            "probability_p50": quantiles[0.50],
            "probability_p66": quantiles[0.66],
            "probability_p80": probability_p80,
            "oof_probability_mean": float(np.mean(full_calibrated_oof)),
            "oof_probability_p33": quantiles[0.33],
            "oof_probability_p50": quantiles[0.50],
            "oof_probability_p66": quantiles[0.66],
            "oof_probability_p80": probability_p80,
            "oof_alert_rate": float(np.mean(full_calibrated_oof > probability_p80)),
            "oof_probabilities": full_calibrated_oof.tolist(),
            "oof_raw_probabilities": full_raw_oof[full_valid].tolist(),
            "oof_targets": full_oof_target.astype(int).tolist(),
            "oof_dates": frame.loc[full_valid, "FECHA_DIA"].astype(str).tolist(),
            "oof_metrics": reference_metrics,
            "test_metrics": test_metrics,
            "raw_test_metrics": raw_test_metrics,
            "calibration": {
                "method": CALIBRATION_METHOD,
                "coefficient": final_coefficient,
                "intercept": final_intercept,
                "evaluation_coefficient": coefficient,
                "evaluation_intercept": intercept,
                "gates": calibration_gates,
            },
            "serving_probability_mean": float(np.mean(serving_probability)),
            "train_end_date": str(frame["FECHA_DIA"].iloc[-1]),
            "evaluation_train_end_date": str(frame["FECHA_DIA"].iloc[split_idx - 1]),
            "test_start_date": str(frame["FECHA_DIA"].iloc[split_idx]),
            "test_end_date": str(frame["FECHA_DIA"].iloc[-1]),
            "train_samples": int(len(X)),
            "evaluation_train_samples": int(len(X_train)),
            "test_samples": int(len(X_test)),
        }

        summary_rows.append({
            "group": group_name,
            "count_threshold_p80": count_threshold,
            "reference_samples": int(full_valid.sum()),
            **{f"oof_{key}": value for key, value in reference_metrics.items()},
            **{f"test_{key}": value for key, value in test_metrics.items()},
            "raw_test_brier": raw_test_metrics["brier"],
            "raw_test_log_loss": raw_test_metrics["log_loss"],
            "raw_test_ece_10": raw_test_metrics["ece_10"],
        })
        for method, values in (
            ("raw", raw_test_metrics),
            ("calibrated", calibrated_test_metrics),
        ):
            comparison_rows.append({
                "group": group_name,
                "split": "final_20pct_untouched",
                "method": method,
                **values,
            })

    artifact = {
        "models": models,
        "feature_cols": feature_cols,
        "target_rule": "group_total_gt_train_p80",
        "probability_rule": "calibrated_probability_vs_temporal_oof_quantiles",
        "model_type": "SigmoidProbabilityCalibratedClassifier(RandomForestClassifier)",
        "calibration_method": CALIBRATION_METHOD,
        "threshold_source": "calibrated_temporal_oof_all_history",
        "threshold_calibration_source": "calibrated_temporal_oof_all_history",
        "split_ratio": 0.8,
        "training_end_date": str(frame["FECHA_DIA"].iloc[-1]),
    }

    with (models_dir / "category_risk_models.pkl").open("wb") as stream:
        pickle.dump(artifact, stream)
    summary = pd.DataFrame(summary_rows)
    comparison = pd.DataFrame(comparison_rows)
    summary.to_csv(models_dir / "category_risk_models_summary.csv", sep=";", index=False)
    summary.to_csv(results_dir / "summary.csv", sep=";", index=False)
    comparison.to_csv(results_dir / "calibration_comparison.csv", sep=";", index=False)
    (results_dir / "protocol.json").write_text(
        json.dumps({
            "method": CALIBRATION_METHOD,
            "evaluation": "calibrator fit on training temporal OOF; final 20% untouched",
            "serving": "raw model fit all history; calibrator fit full temporal OOF",
            "acceptance_gates": [
                "test Brier not worse",
                "test log loss not worse",
                "test ROC-AUC preserved",
            ],
            "groups": list(RISK_GROUPS),
        }, indent=2),
        encoding="utf-8",
    )
    print("\n" + comparison.to_string(index=False, float_format="%.6f"))


if __name__ == "__main__":
    main()
