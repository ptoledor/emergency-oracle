"""Train count and overload models for Talcahuano's 5th Company."""

from __future__ import annotations

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
from sklearn.model_selection import RepeatedKFold

from train_repeated_kfold import (
    XGB_CLASSIFIER_PARAMS,
    XGB_REGRESSOR_PARAMS,
    add_weekday_columns,
    load_feature_cols,
    select_recall_controlled_threshold,
)


N_SPLITS = 5
N_REPEATS = 30
RANDOM_STATE = 42
OVERLOAD_MIN_COUNT = 3


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


def dump_pickle(value, path: Path) -> None:
    with path.open("wb") as stream:
        pickle.dump(value, stream)


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    models_dir = root / "03_model" / "saved_models"
    frame = pd.read_csv(root / "02_data" / "augmented_emergency_data.csv", sep=";")
    frame = add_weekday_columns(frame.sort_values("FECHA_DIA").reset_index(drop=True))
    if "N_5TA_CIA" not in frame:
        raise KeyError("N_5TA_CIA is missing; run 02_data/clean_and_augment.py first")

    columns = feature_columns(models_dir, frame)
    target = pd.to_numeric(frame["N_5TA_CIA"], errors="coerce")
    valid = target.notna()
    X = frame.loc[valid, columns].reset_index(drop=True)
    y_count = target.loc[valid].reset_index(drop=True)
    dates = frame.loc[valid, "FECHA_DIA"].reset_index(drop=True)
    y_overload = (y_count >= OVERLOAD_MIN_COUNT).astype(int)

    count_predictions = np.zeros((len(X), N_REPEATS), dtype=float)
    overload_probabilities = np.zeros((len(X), N_REPEATS), dtype=float)
    fold_rows = []
    splitter = RepeatedKFold(
        n_splits=N_SPLITS,
        n_repeats=N_REPEATS,
        random_state=RANDOM_STATE,
    )
    total_folds = N_SPLITS * N_REPEATS
    for index, (train_idx, validation_idx) in enumerate(splitter.split(X)):
        repeat = index // N_SPLITS
        fold = (index % N_SPLITS) + 1
        regressor = xgb.XGBRegressor(**XGB_REGRESSOR_PARAMS)
        classifier = xgb.XGBClassifier(**XGB_CLASSIFIER_PARAMS)
        regressor.fit(X.iloc[train_idx], y_count.iloc[train_idx])
        classifier.fit(X.iloc[train_idx], y_overload.iloc[train_idx])
        predicted = np.clip(regressor.predict(X.iloc[validation_idx]), 0, None)
        probability = classifier.predict_proba(X.iloc[validation_idx])[:, 1]
        count_predictions[validation_idx, repeat] = predicted
        overload_probabilities[validation_idx, repeat] = probability
        fold_rows.append(
            {
                "repeat": repeat + 1,
                "fold": fold,
                "mae": float(mean_absolute_error(y_count.iloc[validation_idx], predicted)),
                "roc_auc": float(roc_auc_score(y_overload.iloc[validation_idx], probability)),
                "brier": float(brier_score_loss(y_overload.iloc[validation_idx], probability)),
            }
        )
        if (index + 1) % 25 == 0:
            print(f"Processed {index + 1}/{total_folds} folds", flush=True)

    count_oof = count_predictions.mean(axis=1)
    probability_oof = overload_probabilities.mean(axis=1)
    decision_threshold, threshold_precision, threshold_recall = (
        select_recall_controlled_threshold(y_overload, probability_oof)
    )
    overload_predicted = probability_oof >= decision_threshold
    mse = float(mean_squared_error(y_count, count_oof))

    final_regressor = xgb.XGBRegressor(**XGB_REGRESSOR_PARAMS)
    final_classifier = xgb.XGBClassifier(**XGB_CLASSIFIER_PARAMS)
    final_regressor.fit(X, y_count)
    final_classifier.fit(X, y_overload)
    probability_quantiles = {
        f"probability_p{percent}": float(np.quantile(probability_oof, percent / 100))
        for percent in (33, 50, 66, 80)
    }
    metadata = {
        "model_role": "fifth_company_repeated_kfold",
        "validation_protocol": "Repeated 5-Fold Cross-Validation (30 seeds)",
        "target_column": "N_5TA_CIA",
        "target_units": ["B-5", "RB-5", "RX-5", "MX-5", "BX-5"],
        "target_rule": "incident_like_message_contains_fifth_company_unit",
        "dedup_mode": "disabled",
        "overload_rule": "N_5TA_CIA >= 3",
        "overload_min_count": OVERLOAD_MIN_COUNT,
        "feature_cols": columns,
        "feature_importances": {
            column: float(value)
            for column, value in zip(columns, final_regressor.feature_importances_)
        },
        "regressor_type": "XGBRegressor",
        "classifier_type": "XGBClassifier",
        "cv_n_splits": N_SPLITS,
        "cv_n_repeats": N_REPEATS,
        "train_samples": int(len(X)),
        "train_start_date": str(dates.iloc[0]),
        "train_end_date": str(dates.iloc[-1]),
        "target_mean": float(y_count.mean()),
        "target_std": float(y_count.std()),
        "overload_rate": float(y_overload.mean()),
        "classification_threshold": float(decision_threshold),
        "threshold_oof_precision": float(threshold_precision),
        "threshold_oof_recall": float(threshold_recall),
        "mae": float(mean_absolute_error(y_count, count_oof)),
        "mse": mse,
        "rmse": float(mse**0.5),
        "r2": float(r2_score(y_count, count_oof)),
        "roc_auc": float(roc_auc_score(y_overload, probability_oof)),
        "brier": float(brier_score_loss(y_overload, probability_oof)),
        "accuracy": float(accuracy_score(y_overload, overload_predicted)),
        "precision": float(precision_score(y_overload, overload_predicted, zero_division=0)),
        "recall": float(recall_score(y_overload, overload_predicted, zero_division=0)),
        "f1": float(f1_score(y_overload, overload_predicted, zero_division=0)),
        **probability_quantiles,
    }
    artifact = {
        "regressor": final_regressor,
        "classifier": final_classifier,
        "metadata": metadata,
    }
    dump_pickle(artifact, models_dir / "fifth_company_models.pkl")
    pd.DataFrame(fold_rows).to_csv(
        models_dir / "fifth_company_fold_metrics.csv", sep=";", index=False
    )
    pd.DataFrame(
        {
            "FECHA_DIA": dates,
            "N_5TA_CIA": y_count,
            "PRED_5TA_CIA_OOF": count_oof,
            "PROB_5TA_CIA_ALTA_OOF": probability_oof,
            "TARGET_5TA_CIA_ALTA": y_overload,
        }
    ).to_csv(models_dir / "fifth_company_oof_predictions.csv", sep=";", index=False)
    pd.DataFrame([metadata]).drop(columns=["feature_cols", "feature_importances"]).to_csv(
        models_dir / "fifth_company_model_summary.csv", sep=";", index=False
    )
    print(
        f"MAE={metadata['mae']:.3f} R2={metadata['r2']:.3f} "
        f"ROC_AUC={metadata['roc_auc']:.3f} Brier={metadata['brier']:.3f} "
        f"threshold={decision_threshold:.2f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
