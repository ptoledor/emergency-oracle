"""Build the verified multi-objective hydrometeorological ensemble."""

from __future__ import annotations

import argparse
import json
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score, brier_score_loss, f1_score, precision_score,
    recall_score, roc_auc_score,
)
from xgboost import XGBRegressor

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from model_components import HydroObjectiveEnsembleRegressor, RegressorProbabilityClassifier
from build_signal_candidate import block_bootstrap_improvement, select_threshold
from search_signal_features_v2 import ACTIVE_PARAMS, D2_PARAMS
from search_signal_models import load_data, metrics


SUFFIX = "signal_hydro_ensemble_v2"
WEIGHTS = {"active": 0.30, "squared": 0.40, "poisson": 0.15, "quantile": 0.15}
CENTER = 5.0
SPREAD_SCALE = 1.15
OFFSET = 0.10


def load_training_frame(root):
    frame, feature_sets, _ = load_data(root)
    weather = pd.read_csv(
        root / "05_research" / "data" / "historical_forecast_features_v2.csv",
        sep=";",
    )
    weather["FECHA_DIA"] = pd.to_datetime(weather["FECHA_DIA"]).dt.strftime("%Y-%m-%d")
    frame = frame.merge(weather, on="FECHA_DIA", how="left", validate="one_to_one")
    hydro = [
        column for column in weather.columns
        if column != "FECHA_DIA"
        and any(token in column for token in (
            "RAIN", "SHOWER", "WET_BULB", "FREEZING", "STORM", "THUNDER",
        ))
        and frame[column].notna().mean() >= 0.99
    ]
    return frame, feature_sets["all_operational"], hydro


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--promote", action="store_true")
    args = parser.parse_args()
    root = PROJECT_ROOT
    models_dir = root / "03_model" / "saved_models"
    results_dir = root / "05_research" / "results" / "hydro_ensemble_candidate"
    results_dir.mkdir(parents=True, exist_ok=True)
    frame, base_features, hydro_features = load_training_frame(root)
    ensemble_features = list(dict.fromkeys(base_features + hydro_features))
    y = frame["EVENTOS"].astype(float)

    objective_oof = pd.read_csv(
        root / "05_research" / "results" / "weather_objectives_v2" / "oof_predictions.csv",
        sep=";",
    )
    active_oof = pd.read_csv(
        root / "05_research" / "results" / "weather_signal_v2" / "oof_predictions.csv",
        sep=";",
    )
    evaluation_y = objective_oof["EVENTOS"].to_numpy(dtype=float)
    active_count = active_oof["d3_base"].to_numpy(dtype=float)
    raw_blend = (
        WEIGHTS["active"] * active_count
        + WEIGHTS["squared"] * objective_oof["d2_squared"].to_numpy(dtype=float)
        + WEIGHTS["poisson"] * objective_oof["d2_poisson"].to_numpy(dtype=float)
        + WEIGHTS["quantile"] * objective_oof["d2_quantile_50"].to_numpy(dtype=float)
    )
    candidate_count = np.clip(
        CENTER + SPREAD_SCALE * (raw_blend - CENTER) + OFFSET, 0, None
    )
    folds = objective_oof["fold"].to_numpy(dtype=int)
    high_threshold = 7.0
    evaluation_high = evaluation_y > high_threshold

    probabilities = np.empty(len(candidate_count), dtype=float)
    for fold in sorted(np.unique(folds)):
        current = folds == fold
        previous = folds < fold
        if previous.sum() < 120:
            probabilities[current] = float(np.mean(evaluation_high[previous])) if previous.any() else float(np.mean(evaluation_high))
            continue
        calibrator = LogisticRegression(C=1.0).fit(
            candidate_count[previous].reshape(-1, 1), evaluation_high[previous]
        )
        probabilities[current] = calibrator.predict_proba(
            candidate_count[current].reshape(-1, 1)
        )[:, 1]
    threshold, threshold_precision, threshold_recall, _ = select_threshold(
        evaluation_high, probabilities
    )
    labels = probabilities >= threshold

    print("Training final ensemble", flush=True)
    models = {
        "active": XGBRegressor(**ACTIVE_PARAMS).fit(frame[base_features], y),
        "squared": XGBRegressor(**D2_PARAMS).fit(frame[ensemble_features], y),
        "poisson": XGBRegressor(**{**D2_PARAMS, "objective": "count:poisson"}).fit(frame[ensemble_features], y),
        "quantile": XGBRegressor(**{
            **D2_PARAMS, "objective": "reg:quantileerror", "quantile_alpha": 0.5,
        }).fit(frame[ensemble_features], y),
    }
    model_feature_cols = {
        "active": base_features,
        "squared": ensemble_features,
        "poisson": ensemble_features,
        "quantile": ensemble_features,
    }
    regressor = HydroObjectiveEnsembleRegressor(
        models=models, model_feature_cols=model_feature_cols, weights=WEIGHTS,
        feature_cols=ensemble_features, center=CENTER,
        spread_scale=SPREAD_SCALE, offset=OFFSET,
    )
    final_calibrator = LogisticRegression(C=1.0).fit(
        candidate_count.reshape(-1, 1), evaluation_high
    )
    classifier = RegressorProbabilityClassifier(
        regressor=regressor,
        coefficient=float(final_calibrator.coef_[0, 0]),
        intercept=float(final_calibrator.intercept_[0]),
        feature_cols=ensemble_features,
    )
    count_metrics = metrics(evaluation_y, candidate_count)
    active_metrics = metrics(evaluation_y, active_count)
    top_candidate = candidate_count >= np.quantile(candidate_count, 0.80)
    top_active = active_count >= np.quantile(active_count, 0.80)
    residuals = evaluation_y - candidate_count
    lower_offset, upper_offset = np.quantile(residuals, [0.10, 0.90])
    bootstrap = block_bootstrap_improvement(
        evaluation_y, active_count, candidate_count, block_size=30
    )
    metadata = {
        "model_role": "multi_objective_hydrometeorological_ensemble",
        "validation_protocol": "Walk-forward 6x120 dias · hydro objective ensemble",
        "operational_use": bool(args.promote), "is_primary": bool(args.promote),
        "promoted_model": bool(args.promote), "candidate_suffix": SUFFIX,
        "regressor_type": "HydroObjectiveEnsembleRegressor",
        "classifier_type": "RegressorProbabilityClassifier",
        "feature_cols": ensemble_features, "feature_count": len(ensemble_features),
        "hydro_feature_cols": hydro_features, "ensemble_weights": WEIGHTS,
        "spread_center": CENTER, "spread_scale": SPREAD_SCALE, "spread_offset": OFFSET,
        "feature_importances": dict(zip(ensemble_features, regressor.feature_importances_)),
        "train_samples": int(len(frame)), "test_samples": int(len(evaluation_y)),
        "train_start_date": str(frame["FECHA_DIA"].iloc[0]),
        "train_end_date": str(frame["FECHA_DIA"].iloc[-1]),
        "test_start_date": str(objective_oof["FECHA_DIA"].iloc[0]),
        "test_end_date": str(objective_oof["FECHA_DIA"].iloc[-1]),
        **count_metrics,
        "official_temporal_metrics": active_metrics,
        "umbral_alta_actividad": high_threshold,
        "classification_threshold": threshold,
        "threshold_oof_precision": threshold_precision,
        "threshold_oof_recall": threshold_recall,
        "roc_auc": float(roc_auc_score(evaluation_high, probabilities)),
        "brier": float(brier_score_loss(evaluation_high, probabilities)),
        "accuracy": float(accuracy_score(evaluation_high, labels)),
        "precision": float(precision_score(evaluation_high, labels, zero_division=0)),
        "recall": float(recall_score(evaluation_high, labels, zero_division=0)),
        "f1": float(f1_score(evaluation_high, labels, zero_division=0)),
        "top20_precision": float(np.mean(evaluation_high[top_candidate])),
        "top20_recall": float(np.mean(top_candidate[evaluation_high])),
        "official_top20_precision": float(np.mean(evaluation_high[top_active])),
        "official_top20_recall": float(np.mean(top_active[evaluation_high])),
        "prediction_interval_80": {
            "method": "walk_forward_residual_quantiles",
            "lower_offset": float(lower_offset), "upper_offset": float(upper_offset),
            "samples": int(len(residuals)), "empirical_coverage": float(np.mean(
                (residuals >= lower_offset) & (residuals <= upper_offset)
            )),
        },
        "activity_score_method": "empirical_cdf_walk_forward_oof",
        "activity_prediction_reference": np.sort(candidate_count).tolist(),
        **bootstrap,
    }
    oof = pd.DataFrame({
        "FECHA_DIA": objective_oof["FECHA_DIA"], "EVENTOS": evaluation_y,
        "PRED_EVENTOS_ACTIVE": active_count,
        "PRED_EVENTOS_HYDRO_ENSEMBLE_OOF": candidate_count,
        "PROB_ALTA_HYDRO_ENSEMBLE_OOF": probabilities,
        "ALERTA_TARGET": evaluation_high.astype(int), "fold": folds,
    })
    with (models_dir / f"regressor_{SUFFIX}.pkl").open("wb") as stream:
        pickle.dump(regressor, stream)
    with (models_dir / f"classifier_{SUFFIX}.pkl").open("wb") as stream:
        pickle.dump(classifier, stream)
    with (models_dir / f"metadata_{SUFFIX}.pkl").open("wb") as stream:
        pickle.dump(metadata, stream)
    oof.to_csv(models_dir / f"{SUFFIX}_oof_predictions.csv", sep=";", index=False)
    oof.to_csv(results_dir / "oof_predictions.csv", sep=";", index=False)
    (results_dir / "summary.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2, default=float), encoding="utf-8"
    )
    if args.promote:
        (models_dir / "active_models.json").write_text(
            json.dumps({
                "climatic_augmented": SUFFIX,
                "principal_backups": [
                    "climatic_augmented",
                    "signal_xgb_d3_flexible",
                    SUFFIX,
                ],
            }, indent=2),
            encoding="utf-8",
        )
    print(json.dumps({
        "suffix": SUFFIX, "promoted": args.promote,
        "mae": metadata["mae"], "rmse": metadata["rmse"], "r2": metadata["r2"],
        "auc": metadata["roc_auc"], "std_ratio": metadata["std_ratio"],
        "boring_4_5_share": metadata["boring_4_5_share"],
        "mae_probability_improves": metadata["mae_probability_improves"],
        "mse_probability_improves": metadata["mse_probability_improves"],
    }, indent=2))


if __name__ == "__main__":
    main()
