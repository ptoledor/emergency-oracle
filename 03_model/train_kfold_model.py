import os
import pickle
from pathlib import Path

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, HistGradientBoostingRegressor, RandomForestClassifier
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
from sklearn.model_selection import KFold

from train import (
    CLASSIFIER_PARAMS,
    FINAL_MODEL_PARAMS,
    MODEL_PARAMS,
)


RANDOM_STATE = 42
N_SPLITS = 5
PRECISION_TARGET = 0.30
KFOLD_CLASSIFIER_PARAMS = {
    **CLASSIFIER_PARAMS,
    "n_jobs": 1,
}


def pickle_dump(obj, path):
    with Path(path).open("wb") as file:
        pickle.dump(obj, file)


def add_weekday_columns(df):
    weekday_columns = {
        0: "DIA_LUNES",
        1: "DIA_MARTES",
        2: "DIA_MIERCOLES",
        3: "DIA_JUEVES",
        4: "DIA_VIERNES",
        5: "DIA_SABADO",
        6: "DIA_DOMINGO",
    }
    if "DIA_SEMANA" not in df.columns:
        return df
    for weekday, column in weekday_columns.items():
        df[column] = (df["DIA_SEMANA"] == weekday).astype(int)
    return df


def load_feature_cols(models_dir, df):
    preferred_metadata = [
        "metadata_climatic_augmented_direct31.pkl",
        "metadata_climatic_augmented.pkl",
    ]
    for metadata_name in preferred_metadata:
        metadata_path = models_dir / metadata_name
        if not metadata_path.exists():
            continue
        with metadata_path.open("rb") as file:
            metadata = pickle.load(file)
        feature_cols = list(metadata.get("feature_cols", []))
        missing = [column for column in feature_cols if column not in df.columns]
        if missing:
            raise KeyError(
                f"{metadata_name} references missing features: {missing[:8]}"
            )
        return feature_cols, metadata_name
    raise FileNotFoundError("No compatible metadata file was found for KFold model")


def select_recall_controlled_threshold(y_true, probabilities):
    best_threshold = 0.50
    best_precision = 0.0
    best_recall = -1.0
    best_f1 = -1.0
    fallback_threshold = 0.50

    for threshold in np.arange(0.05, 0.85, 0.01):
        predictions = (probabilities >= threshold).astype(int)
        precision = precision_score(y_true, predictions, zero_division=0)
        recall = recall_score(y_true, predictions, zero_division=0)
        f1 = f1_score(y_true, predictions, zero_division=0)

        if f1 > best_f1:
            best_f1 = f1
            fallback_threshold = threshold

        if precision >= PRECISION_TARGET and recall > best_recall:
            best_threshold = threshold
            best_precision = precision
            best_recall = recall

    if best_recall < 0:
        best_threshold = fallback_threshold
        predictions = (probabilities >= best_threshold).astype(int)
        best_precision = precision_score(y_true, predictions, zero_division=0)
        best_recall = recall_score(y_true, predictions, zero_division=0)

    return float(best_threshold), float(best_precision), float(best_recall)


def main():
    base_dir = Path(__file__).resolve().parent.parent
    data_path = base_dir / "02_data" / "augmented_emergency_data.csv"
    models_dir = base_dir / "03_model" / "saved_models"
    models_dir.mkdir(parents=True, exist_ok=True)

    if not data_path.exists():
        raise FileNotFoundError(f"Dataset not found: {data_path}")

    df = pd.read_csv(data_path, sep=";")
    df = df.sort_values("FECHA_DIA").reset_index(drop=True)
    df = add_weekday_columns(df)

    feature_cols, metadata_source = load_feature_cols(models_dir, df)
    X = df[feature_cols].copy()
    y_reg = pd.to_numeric(df["EVENTOS"], errors="coerce")
    valid = y_reg.notna()
    X = X.loc[valid].reset_index(drop=True)
    y_reg = y_reg.loc[valid].reset_index(drop=True)
    dates = df.loc[valid, "FECHA_DIA"].reset_index(drop=True)

    alert_threshold = float(y_reg.quantile(0.80))
    y_clf = (y_reg > alert_threshold).astype(int)

    reg_predictions = np.full(len(X), np.nan)
    clf_probabilities = np.full(len(X), np.nan)
    fold_rows = []

    splitter = KFold(n_splits=N_SPLITS, shuffle=True, random_state=RANDOM_STATE)
    for fold, (train_idx, validation_idx) in enumerate(splitter.split(X), start=1):
        X_train = X.iloc[train_idx]
        X_validation = X.iloc[validation_idx]
        y_train_reg = y_reg.iloc[train_idx]
        y_validation_reg = y_reg.iloc[validation_idx]
        y_train_clf = y_clf.iloc[train_idx]
        y_validation_clf = y_clf.iloc[validation_idx]

        reg_model = HistGradientBoostingRegressor(**FINAL_MODEL_PARAMS)
        reg_model.fit(X_train, y_train_reg)
        fold_reg_predictions = np.clip(reg_model.predict(X_validation), 0, None)
        reg_predictions[validation_idx] = fold_reg_predictions

        clf_model = RandomForestClassifier(**KFOLD_CLASSIFIER_PARAMS)
        if y_train_clf.nunique() < 2:
            fold_probabilities = np.full(len(validation_idx), float(y_train_clf.mean()))
        else:
            clf_model.fit(X_train, y_train_clf)
            fold_probabilities = clf_model.predict_proba(X_validation)[:, 1]
        clf_probabilities[validation_idx] = fold_probabilities

        fold_rows.append(
            {
                "fold": fold,
                "train_samples": int(len(train_idx)),
                "validation_samples": int(len(validation_idx)),
                "mae": float(mean_absolute_error(y_validation_reg, fold_reg_predictions)),
                "mse": float(mean_squared_error(y_validation_reg, fold_reg_predictions)),
                "r2": float(r2_score(y_validation_reg, fold_reg_predictions)),
                "roc_auc": (
                    float(roc_auc_score(y_validation_clf, fold_probabilities))
                    if y_validation_clf.nunique() == 2
                    else np.nan
                ),
                "brier": float(brier_score_loss(y_validation_clf, fold_probabilities)),
            }
        )

    valid_oof = ~np.isnan(reg_predictions) & ~np.isnan(clf_probabilities)
    if not valid_oof.all():
        raise RuntimeError("KFold OOF generation left missing predictions")

    threshold, threshold_precision, threshold_recall = select_recall_controlled_threshold(
        y_clf,
        clf_probabilities,
    )
    y_clf_pred = (clf_probabilities >= threshold).astype(int)

    reg_model_final = HistGradientBoostingRegressor(**FINAL_MODEL_PARAMS)
    reg_model_final.fit(X, y_reg)
    clf_model_final = RandomForestClassifier(**KFOLD_CLASSIFIER_PARAMS)
    clf_model_final.fit(X, y_clf)

    importance_model = GradientBoostingRegressor(**MODEL_PARAMS)
    importance_model.fit(X, y_reg)
    feature_importances = {
        column: float(value)
        for column, value in zip(feature_cols, importance_model.feature_importances_)
    }

    train_mean = float(y_reg.mean())
    baseline_predictions = np.full(len(y_reg), train_mean)
    metadata = {
        "model_role": "structural_kfold",
        "validation_protocol": "KFold random 5 folds",
        "operational_use": False,
        "is_primary": False,
        "metadata_source": metadata_source,
        "feature_cols": feature_cols,
        "feature_importances": feature_importances,
        "umbral_alta_actividad": alert_threshold,
        "classification_threshold": threshold,
        "threshold_metric": "recall_controlled_kfold_oof",
        "precision_target": PRECISION_TARGET,
        "threshold_oof_precision": threshold_precision,
        "threshold_oof_recall": threshold_recall,
        "regressor_type": "HistGradientBoostingRegressor",
        "classifier_type": "RandomForestClassifier",
        "cv_n_splits": N_SPLITS,
        "cv_shuffle": True,
        "cv_random_state": RANDOM_STATE,
        "train_samples": int(len(X)),
        "test_samples": int(len(X)),
        "train_start_date": str(dates.iloc[0]),
        "train_end_date": str(dates.iloc[-1]),
        "baseline_mae": float(mean_absolute_error(y_reg, baseline_predictions)),
        "mae": float(mean_absolute_error(y_reg, reg_predictions)),
        "mse": float(mean_squared_error(y_reg, reg_predictions)),
        "r2": float(r2_score(y_reg, reg_predictions)),
        "accuracy": float(accuracy_score(y_clf, y_clf_pred)),
        "precision": float(precision_score(y_clf, y_clf_pred, zero_division=0)),
        "recall": float(recall_score(y_clf, y_clf_pred, zero_division=0)),
        "f1": float(f1_score(y_clf, y_clf_pred, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_clf, clf_probabilities)),
        "brier": float(brier_score_loss(y_clf, clf_probabilities)),
    }

    pd.DataFrame(fold_rows).to_csv(
        models_dir / "kfold_evaluation.csv",
        sep=";",
        index=False,
    )
    pd.DataFrame(
        {
            "FECHA_DIA": dates,
            "EVENTOS": y_reg,
            "PRED_EVENTOS_KFOLD_OOF": reg_predictions,
            "PROB_ALTA_KFOLD_OOF": clf_probabilities,
            "ALERTA_TARGET_KFOLD": y_clf,
        }
    ).to_csv(
        models_dir / "kfold_oof_predictions.csv",
        sep=";",
        index=False,
    )

    pickle_dump(reg_model_final, models_dir / "regressor_climatic_augmented_kfold.pkl")
    pickle_dump(clf_model_final, models_dir / "classifier_climatic_augmented_kfold.pkl")
    pickle_dump(metadata, models_dir / "metadata_climatic_augmented_kfold.pkl")

    print("KFold structural model saved")
    print(f"  MAE OOF: {metadata['mae']:.3f}")
    print(f"  ROC-AUC OOF: {metadata['roc_auc']:.3f}")
    print(f"  Brier OOF: {metadata['brier']:.3f}")
    print(f"  Threshold: {metadata['classification_threshold']:.2f}")


if __name__ == "__main__":
    main()
