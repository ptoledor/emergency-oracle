import os
os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("SKLEARN_NUM_THREADS", "1")
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import TimeSeriesSplit


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


def temporal_oof_probs(X, y):
    probs = np.full(len(X), np.nan)
    for train_idx, val_idx in TimeSeriesSplit(n_splits=5).split(X):
        y_train = y.iloc[train_idx]
        if y_train.nunique() < 2:
            continue
        model = RandomForestClassifier(**CLASSIFIER_PARAMS)
        model.fit(X.iloc[train_idx], y_train)
        probs[val_idx] = model.predict_proba(X.iloc[val_idx])[:, 1]
    return probs


def safe_auc(y_true, y_prob):
    if y_true.nunique() < 2:
        return np.nan
    return roc_auc_score(y_true, y_prob)


def main():
    base_dir = Path(__file__).resolve().parent.parent
    data_path = base_dir / "02_data" / "augmented_emergency_data.csv"
    models_dir = base_dir / "03_model" / "saved_models"
    models_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(data_path, sep=";")
    df = df.sort_values("FECHA_DIA").reset_index(drop=True)
    with open(models_dir / "metadata_climatic_augmented.pkl", "rb") as file:
        primary_metadata = pickle.load(file)

    feature_cols = list(primary_metadata["feature_cols"])
    X = df[feature_cols]
    split_idx = int(len(df) * 0.8)
    X_train = X.iloc[:split_idx]
    X_test = X.iloc[split_idx:]

    models = {}
    summary_rows = []

    for group_name, columns in RISK_GROUPS.items():
        total = df[columns].sum(axis=1).astype(float)
        train_total = total.iloc[:split_idx]
        high_threshold = float(train_total.quantile(0.80))
        y = (total > high_threshold).astype(int)
        y_train = y.iloc[:split_idx]
        y_test = y.iloc[split_idx:]

        oof_prob = temporal_oof_probs(X_train, y_train)
        valid = ~np.isnan(oof_prob)
        if not valid.any():
            raise RuntimeError(f"No OOF probabilities for {group_name}")

        y_valid = y_train.iloc[np.flatnonzero(valid)]
        prob_valid = oof_prob[valid]
        calibration_split = max(1, int(len(X_train) * 0.8))
        X_fit = X_train.iloc[:calibration_split]
        y_fit = y_train.iloc[:calibration_split]
        X_cal = X_train.iloc[calibration_split:]
        y_cal = y_train.iloc[calibration_split:]
        if y_fit.nunique() >= 2 and len(X_cal) > 0:
            calibration_model = RandomForestClassifier(**CLASSIFIER_PARAMS)
            calibration_model.fit(X_fit, y_fit)
            threshold_probabilities = calibration_model.predict_proba(X_cal)[:, 1]
            threshold_target = y_cal
            threshold_source = "temporal_holdout_train_only"
        else:
            threshold_probabilities = prob_valid
            threshold_target = y_valid
            threshold_source = "temporal_oof_train_only"

        prob_p33 = float(np.quantile(threshold_probabilities, 0.33))
        prob_p50 = float(np.quantile(threshold_probabilities, 0.50))
        prob_p66 = float(np.quantile(threshold_probabilities, 0.66))
        prob_p80 = float(np.quantile(threshold_probabilities, 0.80))
        pred_alert = (prob_valid >= prob_p80).astype(int)

        model = RandomForestClassifier(**CLASSIFIER_PARAMS)
        model.fit(X_train, y_train)

        test_prob = model.predict_proba(X_test)[:, 1]
        test_pred_alert = (test_prob >= prob_p80).astype(int)

        models[group_name] = {
            "model": model,
            "source_cols": columns,
            "feature_cols": feature_cols,
            "count_threshold_p80": high_threshold,
            "probability_p33": prob_p33,
            "probability_p50": prob_p50,
            "probability_p66": prob_p66,
            "probability_p80": prob_p80,
            "oof_probability_mean": float(np.mean(prob_valid)),
            "oof_probability_p33": prob_p33,
            "oof_probability_p50": prob_p50,
            "oof_probability_p66": prob_p66,
            "oof_probability_p80": prob_p80,
            "oof_alert_rate": float(np.mean(prob_valid > prob_p80)),
            "oof_metrics": {
                "accuracy_at_p80": float(accuracy_score(y_valid, pred_alert)),
                "precision_at_p80": float(precision_score(y_valid, pred_alert, zero_division=0)),
                "recall_at_p80": float(recall_score(y_valid, pred_alert, zero_division=0)),
                "f1_at_p80": float(f1_score(y_valid, pred_alert, zero_division=0)),
                "roc_auc": float(safe_auc(y_valid, prob_valid)),
                "brier": float(brier_score_loss(y_valid, prob_valid)),
                "positive_rate": float(y_train.mean()),
                "oof_probability_p33": prob_p33,
                "oof_probability_p50": prob_p50,
                "oof_probability_p66": prob_p66,
                "oof_probability_p80": prob_p80,
            },
            "test_metrics": {
                "accuracy_at_p80": float(accuracy_score(y_test, test_pred_alert)),
                "precision_at_p80": float(precision_score(y_test, test_pred_alert, zero_division=0)),
                "recall_at_p80": float(recall_score(y_test, test_pred_alert, zero_division=0)),
                "f1_at_p80": float(f1_score(y_test, test_pred_alert, zero_division=0)),
                "roc_auc": float(safe_auc(y_test, test_prob)),
                "brier": float(brier_score_loss(y_test, test_prob)),
                "positive_rate": float(y_test.mean()),
            },
            "train_end_date": str(df["FECHA_DIA"].iloc[split_idx - 1]),
            "test_start_date": str(df["FECHA_DIA"].iloc[split_idx]),
            "test_end_date": str(df["FECHA_DIA"].iloc[-1]),
            "train_samples": int(len(X_train)),
            "test_samples": int(len(X_test)),
        }

        row = {
            "group": group_name,
            "source_cols": "|".join(columns),
            "count_threshold_p80": high_threshold,
            "positive_rate": float(y_train.mean()),
            **models[group_name]["oof_metrics"],
            "test_roc_auc": models[group_name]["test_metrics"]["roc_auc"],
            "test_brier": models[group_name]["test_metrics"]["brier"],
            "test_f1_at_p80": models[group_name]["test_metrics"]["f1_at_p80"],
        }
        summary_rows.append(row)

    artifact = {
        "models": models,
        "feature_cols": feature_cols,
        "target_rule": "group_total_gt_train_p80",
        "probability_rule": "prealert_gt_oof_p50_alert_gt_oof_p80",
        "model_type": "RandomForestClassifier",
        "threshold_source": "temporal_holdout_train_only",
            "threshold_calibration_source": threshold_source,
            "threshold_calibration_samples": int(len(threshold_target)),
        "split_ratio": 0.8,
    }

    with open(models_dir / "category_risk_models.pkl", "wb") as file:
        pickle.dump(artifact, file)

    summary = pd.DataFrame(summary_rows)
    summary.to_csv(models_dir / "category_risk_models_summary.csv", sep=";", index=False)
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
