import pickle
from pathlib import Path

import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from sklearn.ensemble import GradientBoostingRegressor, HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import TimeSeriesSplit


RANDOM_STATE = 42
CATEGORY_COLS = [
    "N_INCENDIO_ESTR",
    "N_INCENDIO_FOREST",
    "N_RESCATE_VEH",
    "N_RESCATE_PERS",
    "N_GASES",
    "N_OTROS",
]


def build_model(family):
    if family == "gradient_boosting":
        return GradientBoostingRegressor(
            n_estimators=150,
            max_depth=4,
            min_samples_leaf=5,
            learning_rate=0.05,
            subsample=0.8,
            random_state=RANDOM_STATE,
        )
    if family == "hist_poisson":
        return HistGradientBoostingRegressor(
            loss="poisson",
            max_iter=300,
            learning_rate=0.05,
            max_leaf_nodes=15,
            min_samples_leaf=12,
            l2_regularization=1.0,
            random_state=RANDOM_STATE,
        )
    raise ValueError(f"Familia desconocida: {family}")


def fit_category(family, category, X_train, y_train, X_validation):
    model = build_model(family)
    model.fit(X_train, y_train[category])
    prediction = np.clip(model.predict(X_validation), 0, None)
    return category, prediction


def predict_categories(family, X_train, y_train, X_validation):
    category_predictions = Parallel(n_jobs=6, backend="threading")(
        delayed(fit_category)(
            family,
            category,
            X_train,
            y_train,
            X_validation,
        )
        for category in CATEGORY_COLS
    )
    return {
        category: prediction
        for category, prediction in category_predictions
    }


def evaluate_family(family, X_train, y_categories_train, y_total_train):
    oof_total = np.full(len(X_train), np.nan)
    oof_categories = {
        category: np.full(len(X_train), np.nan)
        for category in CATEGORY_COLS
    }
    fold_maes = []

    for fold, (train_idx, validation_idx) in enumerate(
        TimeSeriesSplit(n_splits=5).split(X_train),
        start=1,
    ):
        predictions = predict_categories(
            family,
            X_train.iloc[train_idx],
            y_categories_train.iloc[train_idx],
            X_train.iloc[validation_idx],
        )
        total = np.sum(list(predictions.values()), axis=0)
        oof_total[validation_idx] = total
        fold_maes.append(
            mean_absolute_error(y_total_train.iloc[validation_idx], total)
        )
        for category, prediction in predictions.items():
            oof_categories[category][validation_idx] = prediction
        print(
            f"  {family} fold {fold}: MAE total={fold_maes[-1]:.3f}",
            flush=True,
        )

    valid = ~np.isnan(oof_total)
    valid_positions = np.flatnonzero(valid)
    result = {
        "family": family,
        "oof_mae": float(
            mean_absolute_error(
                y_total_train.iloc[valid_positions],
                oof_total[valid],
            )
        ),
        "oof_rmse": float(
            mean_squared_error(
                y_total_train.iloc[valid_positions],
                oof_total[valid],
            ) ** 0.5
        ),
        "oof_r2": float(
            r2_score(
                y_total_train.iloc[valid_positions],
                oof_total[valid],
            )
        ),
        "fold_mae_std": float(np.std(fold_maes)),
        "oof_prediction_std": float(np.std(oof_total[valid], ddof=1)),
    }
    for category in CATEGORY_COLS:
        result[f"mae_{category}"] = float(
            mean_absolute_error(
                y_categories_train[category].iloc[valid_positions],
                oof_categories[category][valid],
            )
        )
    return result


def final_test(family, X_train, X_test, y_categories_train, y_total_test):
    predictions = predict_categories(
        family,
        X_train,
        y_categories_train,
        X_test,
    )
    total = np.sum(list(predictions.values()), axis=0)
    result = {
        "test_mae": float(mean_absolute_error(y_total_test, total)),
        "test_rmse": float(mean_squared_error(y_total_test, total) ** 0.5),
        "test_r2": float(r2_score(y_total_test, total)),
        "test_prediction_mean": float(np.mean(total)),
        "test_prediction_std": float(np.std(total, ddof=1)),
    }
    return result, predictions


def main():
    base_dir = Path(__file__).resolve().parent.parent
    models_dir = base_dir / "03_model" / "saved_models"
    df = pd.read_csv(
        base_dir / "02_data" / "augmented_emergency_data.csv",
        sep=";",
    )
    with open(models_dir / "metadata_climatic_augmented.pkl", "rb") as file:
        metadata = pickle.load(file)

    feature_cols = metadata["feature_cols"]
    split_idx = int(len(df) * 0.8)
    X = df[feature_cols]
    y_categories = df[CATEGORY_COLS].astype(float)
    y_total = df["EVENTOS"].astype(float)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_categories_train = y_categories.iloc[:split_idx]
    y_total_train, y_total_test = y_total.iloc[:split_idx], y_total.iloc[split_idx:]
    rows = []

    for family in ["gradient_boosting", "hist_poisson"]:
        print(f"Evaluando modelos por categoría: {family}", flush=True)
        result = evaluate_family(
            family,
            X_train,
            y_categories_train,
            y_total_train,
        )
        test_result, _ = final_test(
            family,
            X_train,
            X_test,
            y_categories_train,
            y_total_test,
        )
        result.update(test_result)
        rows.append(result)

    results = pd.DataFrame(rows)
    results.to_csv(
        models_dir / "category_model_benchmark.csv",
        sep=";",
        index=False,
    )
    print(
        results.to_string(
            index=False,
            float_format="%.3f",
        )
    )


if __name__ == "__main__":
    main()
