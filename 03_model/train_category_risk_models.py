import os
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
    "rescate": ["N_RESCATE_VEH", "N_RESCATE_PERS"],
    "incendio": ["N_INCENDIO_ESTR", "N_INCENDIO_FOREST"],
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
    with open(models_dir / "metadata_climatic_augmented.pkl", "rb") as file:
        primary_metadata = pickle.load(file)

    feature_cols = list(primary_metadata["feature_cols"])
    X = df[feature_cols]

    models = {}
    summary_rows = []

    for group_name, columns in RISK_GROUPS.items():
        total = df[columns].sum(axis=1).astype(float)
        high_threshold = float(total.quantile(0.80))
        y = (total > high_threshold).astype(int)

        oof_prob = temporal_oof_probs(X, y)
        valid = ~np.isnan(oof_prob)
        if not valid.any():
            raise RuntimeError(f"No OOF probabilities for {group_name}")

        y_valid = y.iloc[np.flatnonzero(valid)]
        prob_valid = oof_prob[valid]
        prob_p50 = float(np.quantile(prob_valid, 0.50))
        prob_p80 = float(np.quantile(prob_valid, 0.80))
        pred_alert = (prob_valid > prob_p80).astype(int)

        model = RandomForestClassifier(**CLASSIFIER_PARAMS)
        model.fit(X, y)
        in_sample_prob = model.predict_proba(X)[:, 1]

        models[group_name] = {
            "model": model,
            "source_cols": columns,
            "feature_cols": feature_cols,
            "count_threshold_p80": high_threshold,
            "probability_p50": prob_p50,
            "probability_p80": prob_p80,
            "historical_probability_mean": float(np.mean(in_sample_prob)),
            "historical_probability_p50": float(np.quantile(in_sample_prob, 0.50)),
            "historical_probability_p80": float(np.quantile(in_sample_prob, 0.80)),
            "historical_alert_rate": float(np.mean(in_sample_prob > np.quantile(in_sample_prob, 0.80))),
            "oof_metrics": {
                "accuracy_at_p80": float(accuracy_score(y_valid, pred_alert)),
                "precision_at_p80": float(precision_score(y_valid, pred_alert, zero_division=0)),
                "recall_at_p80": float(recall_score(y_valid, pred_alert, zero_division=0)),
                "f1_at_p80": float(f1_score(y_valid, pred_alert, zero_division=0)),
                "roc_auc": float(safe_auc(y_valid, prob_valid)),
                "brier": float(brier_score_loss(y_valid, prob_valid)),
                "positive_rate": float(y.mean()),
                "oof_probability_p50": prob_p50,
                "oof_probability_p80": prob_p80,
            },
        }

        row = {
            "group": group_name,
            "source_cols": "|".join(columns),
            "count_threshold_p80": high_threshold,
            "positive_rate": float(y.mean()),
            **models[group_name]["oof_metrics"],
        }
        summary_rows.append(row)

    artifact = {
        "models": models,
        "feature_cols": feature_cols,
        "target_rule": "group_total_gt_historical_p80",
        "probability_rule": "prealert_gt_p50_alert_gt_p80",
        "model_type": "RandomForestClassifier",
    }

    with open(models_dir / "category_risk_models.pkl", "wb") as file:
        pickle.dump(artifact, file)

    summary = pd.DataFrame(summary_rows)
    summary.to_csv(models_dir / "category_risk_models_summary.csv", sep=";", index=False)
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
