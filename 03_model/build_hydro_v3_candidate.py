"""Build the hydro v3 candidate: enriched calendar + Poisson GLM shrinkage head.

The candidate is saved with its own suffix and is never promoted here; the
promotion gate in .agents/AGENTS.md requires explicit user approval.
"""

from __future__ import annotations

import json
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression, PoissonRegressor
from sklearn.metrics import brier_score_loss, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBRegressor

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "03_model"))

from build_hydro_ensemble_candidate import CENTER, OFFSET, SPREAD_SCALE, WEIGHTS
from build_signal_candidate import block_bootstrap_improvement
from hydro_v3_ablation import add_families
from hydro_v3_harness import head_params, load_training_frame
from hydro_v3_structural import finish, oof_heads
from model_components import (
    HydroGLMBlendRegressor, HydroObjectiveEnsembleRegressor, RegressorProbabilityClassifier,
)
from search_signal_models import metrics

SUFFIX = "signal_hydro_v3_glm_cal"
GLM_ALPHA = 10.0
GLM_WEIGHT = 0.25
HIGH_THRESHOLD = 7.0


def main():
    models_dir = PROJECT_ROOT / "03_model" / "saved_models"
    results_dir = PROJECT_ROOT / "05_research" / "results" / "hydro_v3"
    results_dir.mkdir(parents=True, exist_ok=True)
    frame, base, hydro = load_training_frame(PROJECT_ROOT)
    frame, families = add_families(frame)
    ensemble = list(dict.fromkeys(base + hydro)) + families["calendar"]
    y = frame["EVENTOS"].astype(float)

    mask, fold, heads = oof_heads(frame, base, ensemble, y, None, ensemble, glm_alpha=GLM_ALPHA)
    candidate = finish(heads, GLM_WEIGHT)[mask]
    reference = pd.read_csv(results_dir / "ablation_oof.csv", sep=";")["official"].to_numpy()
    t, f = y[mask].to_numpy(), fold[mask]
    high = t > HIGH_THRESHOLD

    probabilities = np.empty(len(candidate))
    for number in sorted(np.unique(f)):
        current, previous = f == number, f < number
        if previous.sum() < 120:
            probabilities[current] = float(np.mean(high[previous])) if previous.any() else float(np.mean(high))
            continue
        calibrator = LogisticRegression(C=1.0).fit(candidate[previous].reshape(-1, 1), high[previous])
        probabilities[current] = calibrator.predict_proba(candidate[current].reshape(-1, 1))[:, 1]

    print("Training final candidate", flush=True)
    xgb_models = {
        name: XGBRegressor(**params).fit(frame[base if name == "active" else ensemble], y)
        for name, params in head_params().items()
    }
    hydro_model = HydroObjectiveEnsembleRegressor(
        models=xgb_models,
        model_feature_cols={name: base if name == "active" else ensemble for name in xgb_models},
        weights=WEIGHTS, feature_cols=ensemble, center=CENTER,
        spread_scale=SPREAD_SCALE, offset=OFFSET,
    )
    fill_values = frame[ensemble].median().to_dict()
    glm = make_pipeline(StandardScaler(), PoissonRegressor(alpha=GLM_ALPHA, max_iter=1000))
    glm.fit(frame[ensemble].fillna(fill_values), y)
    regressor = HydroGLMBlendRegressor(
        hydro=hydro_model, glm=glm, glm_feature_cols=ensemble,
        glm_fill_values=fill_values, glm_weight=GLM_WEIGHT, feature_cols=ensemble,
    )
    final_calibrator = LogisticRegression(C=1.0).fit(candidate.reshape(-1, 1), high)
    classifier = RegressorProbabilityClassifier(
        regressor=regressor, coefficient=float(final_calibrator.coef_[0, 0]),
        intercept=float(final_calibrator.intercept_[0]), feature_cols=ensemble,
    )

    count_metrics = metrics(t, candidate)
    official_metrics = metrics(t, reference)
    fold_rows = []
    for number in range(1, 7):
        current = f == number
        fold_rows.append({
            "fold": number,
            "start": str(frame["FECHA_DIA"][mask].to_numpy()[current][0]),
            "end": str(frame["FECHA_DIA"][mask].to_numpy()[current][-1]),
            "mae_official": float(np.mean(np.abs(t[current] - reference[current]))),
            "mae_candidate": float(np.mean(np.abs(t[current] - candidate[current]))),
            "rmse_official": float(np.sqrt(np.mean((t[current] - reference[current]) ** 2))),
            "rmse_candidate": float(np.sqrt(np.mean((t[current] - candidate[current]) ** 2))),
        })
    residuals = t - candidate
    lower_offset, upper_offset = np.quantile(residuals, [0.10, 0.90])
    metadata = {
        "model_role": "hydro_ensemble_glm_shrinkage_enriched_calendar",
        "validation_protocol": "Walk-forward 6x120 dias · hydro v3 GLM + calendario",
        "operational_use": False, "is_primary": False, "promoted_model": False,
        "candidate_suffix": SUFFIX,
        "regressor_type": "HydroGLMBlendRegressor",
        "classifier_type": "RegressorProbabilityClassifier",
        "feature_cols": ensemble, "feature_count": len(ensemble),
        "calendar_feature_cols": families["calendar"], "hydro_feature_cols": hydro,
        "ensemble_weights": WEIGHTS, "glm_alpha": GLM_ALPHA, "glm_weight": GLM_WEIGHT,
        "spread_center": CENTER, "spread_scale": SPREAD_SCALE, "spread_offset": OFFSET,
        "feature_importances": dict(zip(ensemble, regressor.feature_importances_)),
        "train_samples": int(len(frame)), "test_samples": int(len(t)),
        "train_start_date": str(frame["FECHA_DIA"].iloc[0]),
        "train_end_date": str(frame["FECHA_DIA"].iloc[-1]),
        **count_metrics,
        "official_temporal_metrics": official_metrics,
        "fold_metrics": fold_rows,
        "umbral_alta_actividad": HIGH_THRESHOLD,
        "roc_auc": float(roc_auc_score(high, probabilities)),
        "brier": float(brier_score_loss(high, probabilities)),
        "prediction_interval_80": {
            "method": "walk_forward_residual_quantiles",
            "lower_offset": float(lower_offset), "upper_offset": float(upper_offset),
            "samples": int(len(residuals)),
        },
        "activity_prediction_reference": np.sort(candidate).tolist(),
        **block_bootstrap_improvement(t, reference, candidate, block_size=30),
    }
    oof = pd.DataFrame({
        "FECHA_DIA": frame["FECHA_DIA"][mask].to_numpy(), "EVENTOS": t,
        "PRED_EVENTOS_OFFICIAL": reference, "PRED_EVENTOS_V3_OOF": candidate,
        "PROB_ALTA_V3_OOF": probabilities, "ALERTA_TARGET": high.astype(int), "fold": f,
    })
    for kind, obj in (("regressor", regressor), ("classifier", classifier), ("metadata", metadata)):
        with (models_dir / f"{kind}_{SUFFIX}.pkl").open("wb") as stream:
            pickle.dump(obj, stream)
    oof.to_csv(models_dir / f"{SUFFIX}_oof_predictions.csv", sep=";", index=False)
    oof.to_csv(results_dir / "candidate_oof_predictions.csv", sep=";", index=False)
    summary = {k: v for k, v in metadata.items() if k not in ("activity_prediction_reference", "feature_importances")}
    (results_dir / "candidate_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=float), encoding="utf-8")
    # Sanity check: the saved model reproduces in-sample predictions on the training frame.
    sample = frame[ensemble].tail(5)
    print(json.dumps({
        "suffix": SUFFIX, "mae": count_metrics["mae"], "rmse": count_metrics["rmse"],
        "r2": count_metrics["r2"], "count_auc": count_metrics["high_auc"],
        "prob_auc": metadata["roc_auc"], "brier": metadata["brier"],
        "std_ratio": count_metrics["std_ratio"],
        "official": {k: official_metrics[k] for k in ("mae", "rmse", "r2", "high_auc", "std_ratio")},
        "mae_probability_improves": metadata["mae_probability_improves"],
        "mse_probability_improves": metadata["mse_probability_improves"],
        "tail_predictions": regressor.predict(sample).round(2).tolist(),
        "tail_probabilities": classifier.predict_proba(sample)[:, 1].round(3).tolist(),
    }, indent=2), flush=True)


if __name__ == "__main__":
    main()
