"""Guarda el candidato hydro reentrenado con el dataset Talcahuano tweets+Viper.

Requiere haber corrido ``viper_wf.py <target>`` para reutilizar sus OOF.
Con ``--promote`` la metadata queda marcada como operacional; el cambio de
``active_models.json`` sigue siendo manual (gate de .agents/AGENTS.md).
"""
from __future__ import annotations

import argparse
import json
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression, PoissonRegressor
from sklearn.metrics import (
    accuracy_score, brier_score_loss, f1_score, precision_score, recall_score, roc_auc_score,
)
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBRegressor

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_hydro_ensemble_candidate import CENTER, OFFSET, SPREAD_SCALE, WEIGHTS  # noqa: E402
from build_hydro_v3_candidate import GLM_ALPHA, GLM_WEIGHT  # noqa: E402
from build_signal_candidate import block_bootstrap_improvement, select_threshold  # noqa: E402
from hydro_v3_harness import head_params  # noqa: E402
from search_signal_models import metrics as signal_metrics  # noqa: E402
from model_components import (  # noqa: E402
    HydroGLMBlendRegressor, HydroObjectiveEnsembleRegressor, RegressorProbabilityClassifier,
)
from viper_eval import OUT, PROJECT_ROOT, count_metrics, load  # noqa: E402


def sequential_probabilities(pred, high, fold):
    out = np.empty(len(pred))
    for number in sorted(np.unique(fold)):
        current, previous = fold == number, fold < number
        if previous.sum() < 120:
            out[current] = float(np.mean(high[previous])) if previous.any() else float(np.mean(high))
            continue
        calibrator = LogisticRegression(C=1.0).fit(pred[previous].reshape(-1, 1), high[previous])
        out[current] = calibrator.predict_proba(pred[current].reshape(-1, 1))[:, 1]
    return out


def main(target="mensajes", threshold=7.0, arch="official", promote=False):
    """arch=official: misma clase y features que el modelo en produccion (reemplazo directo).
    arch=v3: agrega calendario CAL_* y cabeza GLM (el serving aun no calcula CAL_*)."""
    suffix = (f"signal_hydro_v3_viper_{target}" if arch == "v3"
              else f"signal_hydro_ensemble_v2_viper_{target}")
    models_dir = PROJECT_ROOT / "03_model" / "saved_models"
    frame, base, ensemble, cal = load(PROJECT_ROOT / "02_data" / f"augmented_emergency_data_viper_{target}.csv")
    if arch == "v3":
        ensemble = ensemble + cal
    y = frame["EVENTOS"].astype(float)

    oof = pd.read_csv(OUT / f"wf_{target}_oof.csv", sep=";")
    t, f = oof["EVENTOS"].to_numpy(), oof["fold"].to_numpy()
    candidate, reference = oof[arch].to_numpy(), oof["official"].to_numpy()
    high = t > threshold
    probabilities = sequential_probabilities(candidate, high, f)
    clf_threshold, threshold_precision, threshold_recall, _ = select_threshold(high, probabilities)
    labels = probabilities >= clf_threshold
    top_candidate = candidate >= np.quantile(candidate, 0.80)
    # Referencia: misma arquitectura entrenada con el target de tweets (modelo previo).
    previous = oof["official_tweet_target"].to_numpy() if "official_tweet_target" in oof else reference

    xgb_models = {
        name: XGBRegressor(**params).fit(frame[base if name == "active" else ensemble], y)
        for name, params in head_params().items()
    }
    hydro_model = HydroObjectiveEnsembleRegressor(
        models=xgb_models,
        model_feature_cols={name: base if name == "active" else ensemble for name in xgb_models},
        weights=WEIGHTS, feature_cols=ensemble, center=CENTER, spread_scale=SPREAD_SCALE, offset=OFFSET,
    )
    regressor = hydro_model
    if arch == "v3":
        fill_values = frame[ensemble].median().to_dict()
        glm = make_pipeline(StandardScaler(), PoissonRegressor(alpha=GLM_ALPHA, max_iter=1000))
        glm.fit(frame[ensemble].fillna(fill_values), y)
        regressor = HydroGLMBlendRegressor(
            hydro=hydro_model, glm=glm, glm_feature_cols=ensemble, glm_fill_values=fill_values,
            glm_weight=GLM_WEIGHT, feature_cols=ensemble,
        )
    final_calibrator = LogisticRegression(C=1.0).fit(candidate.reshape(-1, 1), high)
    classifier = RegressorProbabilityClassifier(
        regressor=regressor, coefficient=float(final_calibrator.coef_[0, 0]),
        intercept=float(final_calibrator.intercept_[0]), feature_cols=ensemble,
    )
    residuals = t - candidate
    lower_offset, upper_offset = np.quantile(residuals, [0.10, 0.90])
    metadata = {
        "model_role": f"hydro_{arch}_viper_history",
        "weather_source": "historical_forecast_fresh_2026-11-01",
        "validation_protocol": "Walk-forward 6x120 dias sobre dataset tweets+Viper",
        "target_definition": target,
        "operational_use": promote, "is_primary": promote, "promoted_model": promote,
        "candidate_suffix": suffix,
        "regressor_type": type(regressor).__name__,
        "classifier_type": "RegressorProbabilityClassifier",
        "feature_cols": ensemble, "feature_count": len(ensemble),
        "calendar_feature_cols": cal if arch == "v3" else [], "base_feature_cols": base,
        "ensemble_weights": WEIGHTS, "glm_alpha": GLM_ALPHA, "glm_weight": GLM_WEIGHT,
        "spread_center": CENTER, "spread_scale": SPREAD_SCALE, "spread_offset": OFFSET,
        "feature_importances": dict(zip(ensemble, regressor.feature_importances_)),
        "train_samples": int(len(frame)), "test_samples": int(len(t)),
        "train_start_date": str(frame["FECHA_DIA"].iloc[0]),
        "train_end_date": str(frame["FECHA_DIA"].iloc[-1]),
        "test_start_date": str(oof["FECHA_DIA"].iloc[0]), "test_end_date": str(oof["FECHA_DIA"].iloc[-1]),
        **signal_metrics(t, candidate),
        **{k: float(v) for k, v in count_metrics(t, candidate, threshold).items()},
        "mse": float(np.mean(residuals ** 2)),
        "official_architecture_metrics": {k: float(v) for k, v in count_metrics(t, reference, threshold).items()},
        "tweet_target_metrics": {k: float(v) for k, v in count_metrics(t, previous, threshold).items()},
        "umbral_alta_actividad": threshold,
        "classification_threshold": clf_threshold,
        "threshold_oof_precision": threshold_precision,
        "threshold_oof_recall": threshold_recall,
        "roc_auc": float(roc_auc_score(high, probabilities)),
        "brier": float(brier_score_loss(high, probabilities)),
        "accuracy": float(accuracy_score(high, labels)),
        "precision": float(precision_score(high, labels, zero_division=0)),
        "recall": float(recall_score(high, labels, zero_division=0)),
        "f1": float(f1_score(high, labels, zero_division=0)),
        "top20_precision": float(np.mean(high[top_candidate])),
        "top20_recall": float(np.mean(top_candidate[high])),
        "prediction_interval_80": {
            "method": "walk_forward_residual_quantiles",
            "lower_offset": float(lower_offset), "upper_offset": float(upper_offset),
            "samples": int(len(residuals)), "empirical_coverage": float(np.mean(
                (residuals >= lower_offset) & (residuals <= upper_offset)
            )),
        },
        "activity_score_method": "empirical_cdf_walk_forward_oof",
        "activity_prediction_reference": np.sort(candidate).tolist(),
        **block_bootstrap_improvement(t, previous, candidate, block_size=30),
    }
    for kind, obj in (("regressor", regressor), ("classifier", classifier), ("metadata", metadata)):
        with (models_dir / f"{kind}_{suffix}.pkl").open("wb") as stream:
            pickle.dump(obj, stream)
    pd.DataFrame({
        "FECHA_DIA": oof["FECHA_DIA"], "EVENTOS": t, "PRED_EVENTOS_OFFICIAL_ARCH": reference,
        "PRED_EVENTOS_VIPER_OOF": candidate, "PROB_ALTA_VIPER_OOF": probabilities,
        "ALERTA_TARGET": high.astype(int), "fold": f,
    }).to_csv(models_dir / f"{suffix}_oof_predictions.csv", sep=";", index=False)
    sample = frame[ensemble].tail(5)
    print(json.dumps({
        "suffix": suffix, "promoted": promote, "mae": metadata["mae"], "rmse": metadata["rmse"],
        "r2": metadata["r2"], "prob_auc": metadata["roc_auc"], "brier": metadata["brier"],
        "classification_threshold": clf_threshold, "precision": metadata["precision"],
        "recall": metadata["recall"], "mae_probability_improves": metadata["mae_probability_improves"],
        "tail_dates": frame["FECHA_DIA"].tail(5).tolist(),
        "tail_actual": y.tail(5).tolist(),
        "tail_predictions": regressor.predict(sample).round(2).tolist(),
        "tail_probabilities": classifier.predict_proba(sample)[:, 1].round(3).tolist(),
    }, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("target", nargs="?", default="mensajes", choices=["mensajes", "incidentes"])
    parser.add_argument("threshold", nargs="?", type=float, default=7.0)
    parser.add_argument("arch", nargs="?", default="official", choices=["official", "v3"])
    parser.add_argument("--promote", action="store_true")
    args = parser.parse_args()
    main(args.target, args.threshold, args.arch, args.promote)
