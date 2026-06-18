import pickle
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import TimeSeriesSplit

from benchmark_count_models import build_model


def sorted_distribution_distance(actual, predicted):
    return float(
        np.mean(
            np.abs(
                np.sort(np.asarray(actual, dtype=float))
                - np.sort(np.asarray(predicted, dtype=float))
            )
        )
    )


def temporal_predictions(name, X_train, y_train, X_test):
    oof = np.full(len(X_train), np.nan)
    for train_idx, validation_idx in TimeSeriesSplit(n_splits=4).split(X_train):
        model = build_model(name)
        model.fit(X_train.iloc[train_idx], y_train.iloc[train_idx])
        oof[validation_idx] = np.clip(
            model.predict(X_train.iloc[validation_idx]),
            0,
            None,
        )

    model = build_model(name)
    model.fit(X_train, y_train)
    test = np.clip(model.predict(X_test), 0, None)
    return oof, test


def main():
    base_dir = Path(__file__).resolve().parent.parent
    models_dir = base_dir / "03_model" / "saved_models"
    df = pd.read_csv(
        base_dir / "02_data" / "augmented_emergency_data.csv",
        sep=";",
    )
    df = df.sort_values("FECHA_DIA").reset_index(drop=True)
    with open(models_dir / "metadata_climatic_augmented.pkl", "rb") as file:
        metadata = pickle.load(file)

    split_idx = int(len(df) * 0.8)
    X = df[metadata["feature_cols"]]
    y = df["EVENTOS"].astype(float)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    names = ["gradient_boosting", "hist_poisson", "random_forest"]
    predictions = {}

    for name in names:
        print(f"Generando predicciones temporales: {name}", flush=True)
        predictions[name] = temporal_predictions(
            name,
            X_train,
            y_train,
            X_test,
        )

    valid = ~np.isnan(predictions[names[0]][0])
    y_oof = y_train.iloc[np.flatnonzero(valid)]
    rows = []
    for gb_weight in np.arange(0, 1.01, 0.1):
        for hist_weight in np.arange(0, 1.01 - gb_weight, 0.1):
            rf_weight = 1.0 - gb_weight - hist_weight
            if rf_weight < -1e-9:
                continue
            weights = {
                "gradient_boosting": gb_weight,
                "hist_poisson": hist_weight,
                "random_forest": max(0.0, rf_weight),
            }
            oof_prediction = sum(
                weights[name] * predictions[name][0][valid]
                for name in names
            )
            test_prediction = sum(
                weights[name] * predictions[name][1]
                for name in names
            )
            rows.append({
                "weight_gradient_boosting": gb_weight,
                "weight_hist_poisson": hist_weight,
                "weight_random_forest": max(0.0, rf_weight),
                "oof_mae": mean_absolute_error(y_oof, oof_prediction),
                "oof_distribution_distance": sorted_distribution_distance(
                    y_oof,
                    oof_prediction,
                ),
                "test_mae": mean_absolute_error(y_test, test_prediction),
                "test_r2": r2_score(y_test, test_prediction),
                "test_mean": float(np.mean(test_prediction)),
                "test_std": float(np.std(test_prediction, ddof=1)),
                "test_distribution_distance": sorted_distribution_distance(
                    y_test,
                    test_prediction,
                ),
            })

    results = pd.DataFrame(rows)
    results["rank_oof_mae"] = results["oof_mae"].rank()
    results["rank_oof_distribution"] = results[
        "oof_distribution_distance"
    ].rank()
    results["selection_rank"] = results[
        ["rank_oof_mae", "rank_oof_distribution"]
    ].mean(axis=1)
    results = results.sort_values(
        ["selection_rank", "oof_mae", "oof_distribution_distance"]
    )
    results.to_csv(
        models_dir / "count_model_blend_benchmark.csv",
        sep=";",
        index=False,
    )
    print(
        results.head(12).to_string(
            index=False,
            float_format="%.3f",
        )
    )


if __name__ == "__main__":
    main()
