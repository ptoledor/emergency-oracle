"""Modelo "alarmista": escenario alto plausible del dia (cuantil 75 de llamados).

Promedio de 4 semillas de XGBoost cuantil 0,75 con las mismas features que el modelo
oficial. Validado en ``viper_alert_models.py``: PR-AUC 0,355 vs 0,311 del conteo
(bootstrap 98%). El dashboard lo muestra como superindice sobre las llamadas esperadas.
"""
from __future__ import annotations

import json
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score
from xgboost import XGBRegressor

sys.path.insert(0, str(Path(__file__).resolve().parent))
from viper_eval import PROJECT_ROOT, load  # noqa: E402  (agrega la raiz al path)
from model_components import QuantileSeedEnsembleRegressor  # noqa: E402
from search_signal_features_v2 import D2_PARAMS  # noqa: E402
from viper_alert_models import walk  # noqa: E402

QUANTILE = 0.75
SEEDS = (42, 7, 99, 123)
SUFFIX = "signal_hydro_ensemble_v2_viper_mensajes"


def main():
    import viper_alert_models
    models_dir = PROJECT_ROOT / "03_model" / "saved_models"
    frame, _, ensemble, _ = load(PROJECT_ROOT / "02_data" / "augmented_emergency_data_viper_mensajes.csv")
    y = frame["EVENTOS"].astype(float).to_numpy()
    params = {**D2_PARAMS, "objective": "reg:quantileerror", "quantile_alpha": QUANTILE}

    oof = []
    for seed in SEEDS:
        viper_alert_models.D2_PARAMS = {**D2_PARAMS, "random_state": seed}
        oof.append(walk(f"q{int(QUANTILE * 100)}", frame, ensemble, y))
    oof = np.mean(oof, axis=0)
    mask = ~np.isnan(oof)
    high = y[mask] > 7.0
    reference = pd.read_csv(models_dir / f"{SUFFIX}_oof_predictions.csv", sep=";")
    strict = frame.loc[mask, "FECHA_DIA"].map(
        reference.set_index("FECHA_DIA")["PRED_EVENTOS_VIPER_OOF"]).to_numpy()
    alarm = np.maximum(oof[mask], strict)

    regressor = QuantileSeedEnsembleRegressor(
        models=[XGBRegressor(**{**params, "random_state": seed}).fit(frame[ensemble], y) for seed in SEEDS],
        feature_cols=ensemble, quantile=QUANTILE,
    )
    metadata = {
        "model_role": "alarm_quantile_scenario", "quantile": QUANTILE, "seeds": list(SEEDS),
        "base_model_suffix": SUFFIX, "feature_cols": ensemble,
        "train_start_date": str(frame["FECHA_DIA"].iloc[0]),
        "train_end_date": str(frame["FECHA_DIA"].iloc[-1]),
        "validation_protocol": "Walk-forward 6x120 dias sobre dataset tweets+Viper",
        "oof_coverage": float(np.mean(y[mask] <= alarm)),
        "roc_auc": float(roc_auc_score(high, alarm)),
        "pr_auc": float(average_precision_score(high, alarm)),
        "strict_pr_auc": float(average_precision_score(high, strict)),
    }
    with (models_dir / f"alarm_{SUFFIX}.pkl").open("wb") as stream:
        pickle.dump({"regressor": regressor, "metadata": metadata}, stream)
    print(json.dumps({k: v for k, v in metadata.items() if k != "feature_cols"}, indent=2))


if __name__ == "__main__":
    main()
