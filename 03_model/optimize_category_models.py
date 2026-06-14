import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from sklearn.ensemble import GradientBoostingRegressor, HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import TimeSeriesSplit

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from model_components import CategoryBlendRegressor

RANDOM_STATE = 42
CATEGORIES = [
    "N_INCENDIO_ESTR",
    "N_INCENDIO_FOREST",
    "N_RESCATE_VEH",
    "N_RESCATE_PERS",
    "N_GASES",
    "N_OTROS",
]
SIZES = [5, 10, 15, 20, 25, 31]


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
    return HistGradientBoostingRegressor(
        loss="poisson",
        max_iter=300,
        learning_rate=0.05,
        max_leaf_nodes=15,
        min_samples_leaf=12,
        l2_regularization=1.0,
        random_state=RANDOM_STATE,
    )


def rank_features(X, y):
    importances = []
    for train_idx, _ in TimeSeriesSplit(n_splits=5).split(X):
        model = GradientBoostingRegressor(
            n_estimators=150,
            max_depth=4,
            min_samples_leaf=5,
            learning_rate=0.05,
            subsample=0.8,
            random_state=RANDOM_STATE,
        )
        model.fit(X.iloc[train_idx], y.iloc[train_idx])
        importances.append(model.feature_importances_)
    mean_importance = np.mean(importances, axis=0)
    return [
        name for name, _ in sorted(
            zip(X.columns, mean_importance),
            key=lambda item: item[1],
            reverse=True,
        )
    ]


def evaluate_candidate(family, features, X, y):
    predictions = np.full(len(X), np.nan)
    fold_maes = []
    for train_idx, validation_idx in TimeSeriesSplit(n_splits=5).split(X):
        model = build_model(family)
        model.fit(X[features].iloc[train_idx], y.iloc[train_idx])
        fold_prediction = np.clip(
            model.predict(X[features].iloc[validation_idx]),
            0,
            None,
        )
        predictions[validation_idx] = fold_prediction
        fold_maes.append(
            mean_absolute_error(y.iloc[validation_idx], fold_prediction)
        )
    valid = ~np.isnan(predictions)
    return {
        "family": family,
        "feature_count": len(features),
        "features": features,
        "oof_mae": float(
            mean_absolute_error(
                y.iloc[np.flatnonzero(valid)],
                predictions[valid],
            )
        ),
        "fold_mae_std": float(np.std(fold_maes)),
        "oof_predictions": predictions,
    }


def optimize_category(category, X_train, y_train):
    ranking = rank_features(X_train, y_train[category])
    candidates = []
    for family in ["gradient_boosting", "hist_poisson"]:
        for size in SIZES:
            features = ranking[:min(size, len(ranking))]
            candidates.append((family, features))
    results = Parallel(n_jobs=4, backend="threading")(
        delayed(evaluate_candidate)(
            family,
            features,
            X_train,
            y_train[category],
        )
        for family, features in candidates
    )
    selected = min(
        results,
        key=lambda result: (
            result["oof_mae"],
            result["fold_mae_std"],
            result["feature_count"],
        ),
    )
    return category, selected, results


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
    y_categories = df[CATEGORIES].astype(float)
    y_total = df["EVENTOS"].astype(float)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train = y_categories.iloc[:split_idx]
    y_total_train, y_total_test = y_total.iloc[:split_idx], y_total.iloc[split_idx:]

    optimized = []
    all_results = []
    for category in CATEGORIES:
        print(f"Optimizando {category}...", flush=True)
        name, selected, results = optimize_category(category, X_train, y_train)
        optimized.append((name, selected))
        for result in results:
            all_results.append({
                "category": category,
                "family": result["family"],
                "feature_count": result["feature_count"],
                "features": "|".join(result["features"]),
                "oof_mae": result["oof_mae"],
                "fold_mae_std": result["fold_mae_std"],
                "selected": result is selected,
            })
        print(
            f"  {selected['family']} · {selected['feature_count']} variables "
            f"· MAE={selected['oof_mae']:.3f}",
            flush=True,
        )

    valid = ~np.isnan(optimized[0][1]["oof_predictions"])
    oof_total = sum(
        selected["oof_predictions"][valid]
        for _, selected in optimized
    )
    valid_positions = np.flatnonzero(valid)
    direct_oof = np.full(len(X_train), np.nan)
    direct_params = {
        "n_estimators": 150,
        "max_depth": 4,
        "min_samples_leaf": 5,
        "learning_rate": 0.05,
        "subsample": 0.8,
        "random_state": RANDOM_STATE,
    }
    for train_idx, validation_idx in TimeSeriesSplit(n_splits=5).split(X_train):
        direct_fold = GradientBoostingRegressor(**direct_params)
        direct_fold.fit(X_train.iloc[train_idx], y_total_train.iloc[train_idx])
        direct_oof[validation_idx] = direct_fold.predict(
            X_train.iloc[validation_idx]
        )
    blend_candidates = []
    for weight_direct in np.arange(0, 1.001, 0.01):
        blended = (
            weight_direct * direct_oof[valid]
            + (1.0 - weight_direct) * oof_total
        )
        blend_candidates.append({
            "weight_direct": float(weight_direct),
            "oof_mae": float(
                mean_absolute_error(
                    y_total_train.iloc[valid_positions],
                    blended,
                )
            ),
            "oof_r2": float(
                r2_score(
                    y_total_train.iloc[valid_positions],
                    blended,
                )
            ),
        })
    selected_blend = min(
        blend_candidates,
        key=lambda candidate: candidate["oof_mae"],
    )
    weight_direct = selected_blend["weight_direct"]
    blended_oof = (
        weight_direct * direct_oof[valid]
        + (1.0 - weight_direct) * oof_total
    )
    blended_oof_clipped = np.clip(blended_oof, 0.05, None)
    y_blend_oof = y_total_train.iloc[valid_positions].to_numpy()
    negative_binomial_alpha = float(max(
        np.sum((y_blend_oof - blended_oof_clipped) ** 2 - blended_oof_clipped)
        / np.sum(blended_oof_clipped ** 2),
        1e-4,
    ))

    test_predictions = {}
    saved_models = {}
    for category, selected in optimized:
        model = build_model(selected["family"])
        model.fit(X_train[selected["features"]], y_train[category])
        prediction = np.clip(model.predict(X_test[selected["features"]]), 0, None)
        test_predictions[category] = prediction
        saved_models[category] = {
            "model": model,
            "family": selected["family"],
            "feature_cols": selected["features"],
            "oof_mae": selected["oof_mae"],
        }

    test_total = sum(test_predictions.values())
    direct_model = GradientBoostingRegressor(**direct_params)
    direct_model.fit(X_train, y_total_train)
    direct_test = np.clip(direct_model.predict(X_test), 0, None)
    blended_test = (
        weight_direct * direct_test
        + (1.0 - weight_direct) * test_total
    )
    blended_regressor = CategoryBlendRegressor(
        direct_model=direct_model,
        category_models=saved_models,
        weight_direct=weight_direct,
        feature_cols=feature_cols,
    )
    summary = {
        "oof_mae": float(
            mean_absolute_error(
                y_total_train.iloc[valid_positions],
                oof_total,
            )
        ),
        "oof_r2": float(
            r2_score(
                y_total_train.iloc[valid_positions],
                oof_total,
            )
        ),
        "test_mae": float(mean_absolute_error(y_total_test, test_total)),
        "test_rmse": float(mean_squared_error(y_total_test, test_total) ** 0.5),
        "test_r2": float(r2_score(y_total_test, test_total)),
        "test_mean": float(np.mean(test_total)),
        "test_std": float(np.std(test_total, ddof=1)),
        "blend_weight_direct": weight_direct,
        "blend_weight_categories": 1.0 - weight_direct,
        "blend_weight_search_step": 0.01,
        "blend_oof_mae": selected_blend["oof_mae"],
        "blend_oof_r2": selected_blend["oof_r2"],
        "blend_test_mae": float(mean_absolute_error(y_total_test, blended_test)),
        "blend_test_mse": float(mean_squared_error(y_total_test, blended_test)),
        "blend_test_rmse": float(
            mean_squared_error(y_total_test, blended_test) ** 0.5
        ),
        "blend_test_r2": float(r2_score(y_total_test, blended_test)),
        "blend_test_mean": float(np.mean(blended_test)),
        "blend_test_std": float(np.std(blended_test, ddof=1)),
        "direct_test_mae": float(mean_absolute_error(y_total_test, direct_test)),
        "direct_test_mse": float(mean_squared_error(y_total_test, direct_test)),
        "direct_test_r2": float(r2_score(y_total_test, direct_test)),
        "negative_binomial_alpha": negative_binomial_alpha,
        "categories": {
            category: {
                key: value
                for key, value in details.items()
                if key != "model"
            }
            for category, details in saved_models.items()
        },
    }
    pd.DataFrame(all_results).to_csv(
        models_dir / "category_model_optimization.csv",
        sep=";",
        index=False,
    )
    pd.DataFrame(blend_candidates).to_csv(
        models_dir / "category_blend_weight_search.csv",
        sep=";",
        index=False,
    )
    with open(models_dir / "category_model_optimized.pkl", "wb") as file:
        pickle.dump(saved_models, file)
    with open(models_dir / "regressor_climatic_augmented_category_blend.pkl", "wb") as file:
        pickle.dump(blended_regressor, file)
    with open(models_dir / "category_model_optimized_metadata.pkl", "wb") as file:
        pickle.dump(summary, file)

    canonical_metadata_path = models_dir / "metadata_climatic_augmented.pkl"
    with open(canonical_metadata_path, "rb") as file:
        canonical_metadata = pickle.load(file)
    with open(models_dir / "regressor_climatic_augmented_direct31.pkl", "wb") as file:
        pickle.dump(direct_model, file)
    direct_metadata = canonical_metadata.copy()
    direct_metadata.update({
        "mae": summary["direct_test_mae"],
        "mse": summary["direct_test_mse"],
        "r2": summary["direct_test_r2"],
        "regressor_type": "GradientBoostingRegressor",
        "selected_variant": "pruned",
        "is_primary": False,
    })
    with open(models_dir / "metadata_climatic_augmented_direct31.pkl", "wb") as file:
        pickle.dump(direct_metadata, file)

    canonical_metadata.update({
        "mae": summary["blend_test_mae"],
        "mse": summary["blend_test_mse"],
        "r2": summary["blend_test_r2"],
        "regressor_type": "CategoryBlendRegressor",
        "selected_variant": "category_blend",
        "blend_weight_direct": weight_direct,
        "blend_weight_categories": 1.0 - weight_direct,
        "blend_weight_search_step": summary["blend_weight_search_step"],
        "blend_oof_mae": summary["blend_oof_mae"],
        "blend_oof_r2": summary["blend_oof_r2"],
        "negative_binomial_alpha": negative_binomial_alpha,
        "category_model_summary": summary["categories"],
    })
    for metadata_name in [
        "metadata_climatic_augmented.pkl",
        "metadata_climatic_augmented_pruned.pkl",
    ]:
        with open(models_dir / metadata_name, "wb") as file:
            pickle.dump(canonical_metadata, file)
    for regressor_name in [
        "regressor_climatic_augmented.pkl",
        "regressor_climatic_augmented_pruned.pkl",
    ]:
        with open(models_dir / regressor_name, "wb") as file:
            pickle.dump(blended_regressor, file)

    print("\nResumen:")
    print(pd.Series(summary).drop("categories").to_string())


if __name__ == "__main__":
    main()
