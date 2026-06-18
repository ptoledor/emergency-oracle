import pickle
import os
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.ensemble import (
    ExtraTreesRegressor,
    GradientBoostingRegressor,
    HistGradientBoostingRegressor,
    RandomForestRegressor,
)
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler


RANDOM_STATE = 42


class StatsmodelsCountRegressor:
    def __init__(self, family):
        self.family = family
        self.scaler = StandardScaler()
        self.result = None

    def fit(self, X, y):
        scaled = self.scaler.fit_transform(X)
        design = sm.add_constant(scaled, has_constant="add")
        if self.family == "poisson":
            family = sm.families.Poisson()
        else:
            family = sm.families.Tweedie(
                var_power=1.5,
                link=sm.families.links.Log(),
            )
        self.result = sm.GLM(y, design, family=family).fit(
            maxiter=200,
            disp=0,
        )
        return self

    def predict(self, X):
        scaled = self.scaler.transform(X)
        design = sm.add_constant(scaled, has_constant="add")
        return np.asarray(self.result.predict(design))


def build_model(name):
    if name == "gradient_boosting":
        return GradientBoostingRegressor(
            n_estimators=150,
            max_depth=4,
            min_samples_leaf=5,
            learning_rate=0.05,
            subsample=0.8,
            random_state=RANDOM_STATE,
        )
    if name == "hist_poisson":
        return HistGradientBoostingRegressor(
            loss="poisson",
            max_iter=300,
            learning_rate=0.05,
            max_leaf_nodes=15,
            min_samples_leaf=12,
            l2_regularization=1.0,
            random_state=RANDOM_STATE,
        )
    if name == "poisson_glm":
        return StatsmodelsCountRegressor("poisson")
    if name == "tweedie_1_5":
        return StatsmodelsCountRegressor("tweedie")
    if name == "random_forest":
        return RandomForestRegressor(
            n_estimators=300,
            max_depth=10,
            min_samples_leaf=4,
            max_features=0.8,
            n_jobs=1,
            random_state=RANDOM_STATE,
        )
    if name == "extra_trees":
        return ExtraTreesRegressor(
            n_estimators=300,
            max_depth=12,
            min_samples_leaf=3,
            max_features=0.8,
            n_jobs=1,
            random_state=RANDOM_STATE,
        )
    if name == "xgb_poisson":
        from xgboost import XGBRegressor

        return XGBRegressor(
            objective="count:poisson",
            n_estimators=300,
            max_depth=4,
            learning_rate=0.03,
            min_child_weight=5,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_lambda=2.0,
            n_jobs=1,
            random_state=RANDOM_STATE,
        )
    if name == "xgb_squared":
        from xgboost import XGBRegressor

        return XGBRegressor(
            objective="reg:squarederror",
            n_estimators=300,
            max_depth=4,
            learning_rate=0.03,
            min_child_weight=5,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_lambda=2.0,
            n_jobs=1,
            random_state=RANDOM_STATE,
        )
    raise ValueError(f"Modelo desconocido: {name}")


def estimate_nb_alpha(y_true, means):
    means = np.clip(np.asarray(means, dtype=float), 0.05, None)
    numerator = np.sum((np.asarray(y_true) - means) ** 2 - means)
    denominator = np.sum(means ** 2)
    return float(max(numerator / denominator, 1e-4))


def distribution_metrics(y_true, point_predictions, alpha, seed):
    means = np.clip(np.asarray(point_predictions, dtype=float), 0.05, None)
    actual = np.asarray(y_true, dtype=float)
    conditional_variance = means + alpha * means ** 2
    predictive_variance = np.mean(conditional_variance) + np.var(
        means,
        ddof=1,
    )
    predictive_std = float(np.sqrt(predictive_variance))
    conditional_std = np.sqrt(conditional_variance)
    lower = np.clip(means - 1.2816 * conditional_std, 0, None)
    upper = means + 1.2816 * conditional_std
    point_wasserstein = float(
        np.mean(np.abs(np.sort(actual) - np.sort(means)))
    )
    actual_std = float(np.std(actual, ddof=1))
    distribution_moment_distance = float(
        abs(np.mean(actual) - np.mean(means))
        + abs(actual_std - predictive_std)
    )

    return {
        "point_mean": float(np.mean(means)),
        "point_std": float(np.std(means, ddof=1)),
        "point_min": float(np.min(means)),
        "point_max": float(np.max(means)),
        "point_wasserstein": point_wasserstein,
        "nb_alpha": alpha,
        "predictive_mean": float(np.mean(means)),
        "predictive_std": predictive_std,
        "predictive_wasserstein": distribution_moment_distance,
        "interval_80_coverage": float(
            np.mean((actual >= lower) & (actual <= upper))
        ),
    }


def evaluate_model(name, X_train, y_train, X_test, y_test):
    oof_predictions = np.full(len(X_train), np.nan)
    fold_maes = []

    for fold, (train_idx, validation_idx) in enumerate(
        TimeSeriesSplit(n_splits=4).split(X_train),
        start=1,
    ):
        print(f"    {name}: fold {fold}/4", flush=True)
        model = build_model(name)
        model.fit(X_train.iloc[train_idx], y_train.iloc[train_idx])
        predictions = np.clip(
            model.predict(X_train.iloc[validation_idx]),
            0,
            None,
        )
        oof_predictions[validation_idx] = predictions
        fold_maes.append(
            mean_absolute_error(y_train.iloc[validation_idx], predictions)
        )

    valid = ~np.isnan(oof_predictions)
    oof_y = y_train.iloc[np.flatnonzero(valid)]
    oof_pred = oof_predictions[valid]
    alpha = estimate_nb_alpha(oof_y, oof_pred)

    final_model = build_model(name)
    print(f"    {name}: ajuste final", flush=True)
    final_model.fit(X_train, y_train)
    test_predictions = np.clip(final_model.predict(X_test), 0, None)
    print(f"    {name}: métricas de distribución", flush=True)
    distribution = distribution_metrics(
        y_test,
        test_predictions,
        alpha,
        seed=RANDOM_STATE,
    )

    return {
        "model": name,
        "oof_mae": float(mean_absolute_error(oof_y, oof_pred)),
        "oof_mae_std": float(np.std(fold_maes)),
        "test_mae": float(mean_absolute_error(y_test, test_predictions)),
        "test_rmse": float(mean_squared_error(y_test, test_predictions) ** 0.5),
        "test_r2": float(r2_score(y_test, test_predictions)),
        "actual_mean": float(y_test.mean()),
        "actual_std": float(y_test.std()),
        "actual_min": float(y_test.min()),
        "actual_max": float(y_test.max()),
        **distribution,
    }


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

    feature_cols = metadata["feature_cols"]
    split_idx = int(len(df) * 0.8)
    X = df[feature_cols]
    y = df["EVENTOS"].astype(float)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    model_names = [
        "gradient_boosting",
        "hist_poisson",
        "random_forest",
        "extra_trees",
        "xgb_poisson",
        "xgb_squared",
    ]
    requested_models = os.getenv("BENCHMARK_MODELS")
    if requested_models:
        model_names = [
            name.strip() for name in requested_models.split(",") if name.strip()
        ]

    print(
        f"Benchmark: {len(model_names)} modelos, "
        f"{len(feature_cols)} variables, {len(X_test)} días de prueba"
    )
    parts_dir = models_dir / "count_model_benchmark_parts"
    parts_dir.mkdir(exist_ok=True)
    results = []
    for name in model_names:
        print(f"Evaluando {name}...", flush=True)
        result = evaluate_model(
            name,
            X_train,
            y_train,
            X_test,
            y_test,
        )
        results.append(result)
        pd.DataFrame([result]).to_csv(
            parts_dir / f"{name}.csv",
            sep=";",
            index=False,
        )
        print(
            f"  OOF MAE={result['oof_mae']:.3f} | "
            f"Test MAE={result['test_mae']:.3f} | "
            f"Std puntual={result['point_std']:.3f} | "
            f"Std predictiva={result['predictive_std']:.3f}",
            flush=True,
        )
    part_files = sorted(parts_dir.glob("*.csv"))
    results_frame = pd.concat(
        [pd.read_csv(path, sep=";") for path in part_files],
        ignore_index=True,
    ).drop_duplicates(subset=["model"], keep="last")
    results_frame["rank_oof_mae"] = results_frame["oof_mae"].rank()
    results_frame["rank_test_mae"] = results_frame["test_mae"].rank()
    results_frame["rank_distribution"] = results_frame[
        "predictive_wasserstein"
    ].rank()
    results_frame["mean_rank"] = results_frame[
        ["rank_oof_mae", "rank_test_mae", "rank_distribution"]
    ].mean(axis=1)
    results_frame = results_frame.sort_values(
        ["mean_rank", "oof_mae", "test_mae"]
    )
    results_frame.to_csv(
        models_dir / "count_model_benchmark.csv",
        sep=";",
        index=False,
    )

    columns = [
        "model",
        "oof_mae",
        "test_mae",
        "test_r2",
        "point_std",
        "predictive_std",
        "point_wasserstein",
        "predictive_wasserstein",
        "interval_80_coverage",
        "mean_rank",
    ]
    print(results_frame[columns].to_string(index=False, float_format="%.3f"))
    print(f"\nGanador del benchmark: {results_frame.iloc[0]['model']}")


if __name__ == "__main__":
    main()
