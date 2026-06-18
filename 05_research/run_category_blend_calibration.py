from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import nbinom
from sklearn.ensemble import GradientBoostingRegressor, HistGradientBoostingRegressor
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    brier_score_loss,
    f1_score,
    mean_absolute_error,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import TimeSeriesSplit

from models.benchmark import NegativeBinomialRegressor


DATASET = Path("05_research/data/weather_experiment_dataset.csv")
ABLATION_SUMMARY = Path(
    "05_research/results/weather_ablation/experiment_summary.json"
)
OUTPUT = Path("05_research/results/category_blend_calibration")
CATEGORIES = [
    "N_INCENDIO_ESTR",
    "N_INCENDIO_FOREST",
    "N_RESCATE_VEH",
    "N_RESCATE_PERS",
    "N_GASES",
    "N_OTROS",
]
CRITICAL_THRESHOLD = 7.0
DECISION_THRESHOLD = 0.30
WEIGHTS = np.arange(0.0, 1.001, 0.05)


def build_model(family: str):
    if family == "gradient_boosting":
        return GradientBoostingRegressor(
            loss="absolute_error",
            n_estimators=200,
            max_depth=4,
            min_samples_leaf=5,
            learning_rate=0.05,
            subsample=0.8,
            random_state=42,
        )
    return HistGradientBoostingRegressor(
        loss="poisson",
        max_iter=300,
        learning_rate=0.05,
        max_leaf_nodes=15,
        min_samples_leaf=12,
        l2_regularization=1.0,
        random_state=42,
    )


def fit_predict_direct(X_train, y_train, X_test) -> np.ndarray:
    model = build_model("gradient_boosting")
    model.fit(X_train, y_train)
    return np.clip(model.predict(X_test), 0.05, None)


def fit_predict_negative_binomial(X_train, y_train, X_test):
    model = NegativeBinomialRegressor()
    model.fit(X_train, y_train)
    return (
        np.clip(model.predict(X_test), 0.05, None),
        model.probability_above(X_test, CRITICAL_THRESHOLD),
    )


def fit_predict_categories(
    family: str,
    X_train: pd.DataFrame,
    y_train: pd.DataFrame,
    X_test: pd.DataFrame,
) -> np.ndarray:
    total = np.zeros(len(X_test), dtype=float)
    for category in CATEGORIES:
        model = build_model(family)
        model.fit(X_train, y_train[category])
        total += np.clip(model.predict(X_test), 0.0, None)
    return np.clip(total, 0.05, None)


def estimate_alpha(target: np.ndarray, means: np.ndarray) -> float:
    means = np.clip(means, 0.05, None)
    value = np.sum((target - means) ** 2 - means) / np.sum(means**2)
    return float(max(value, 1e-4))


def risk_from_count(means: np.ndarray, alpha: float) -> np.ndarray:
    size = 1.0 / alpha
    probability = size / (size + np.clip(means, 0.05, None))
    return nbinom.sf(np.floor(CRITICAL_THRESHOLD), size, probability)


def calibrate(
    method: str,
    train_probability: np.ndarray,
    train_target: np.ndarray,
    test_probability: np.ndarray,
) -> np.ndarray:
    if method == "raw":
        return test_probability
    if method == "sigmoid":
        clipped = np.clip(train_probability, 1e-6, 1 - 1e-6)
        model = LogisticRegression(random_state=42)
        model.fit(np.log(clipped / (1 - clipped)).reshape(-1, 1), train_target)
        test = np.clip(test_probability, 1e-6, 1 - 1e-6)
        return model.predict_proba(
            np.log(test / (1 - test)).reshape(-1, 1)
        )[:, 1]
    model = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
    model.fit(train_probability, train_target)
    return model.predict(test_probability)


def inner_predictions(X, y_total, y_categories):
    direct = np.full(len(X), np.nan)
    negative_binomial = np.full(len(X), np.nan)
    negative_binomial_risk = np.full(len(X), np.nan)
    categories = {
        family: np.full(len(X), np.nan)
        for family in ("gradient_boosting", "hist_poisson")
    }
    for train_index, validation_index in TimeSeriesSplit(n_splits=4).split(X):
        direct[validation_index] = fit_predict_direct(
            X.iloc[train_index],
            y_total.iloc[train_index],
            X.iloc[validation_index],
        )
        nb_count, nb_risk = fit_predict_negative_binomial(
            X.iloc[train_index],
            y_total.iloc[train_index],
            X.iloc[validation_index],
        )
        negative_binomial[validation_index] = nb_count
        negative_binomial_risk[validation_index] = nb_risk
        for family in categories:
            categories[family][validation_index] = fit_predict_categories(
                family,
                X.iloc[train_index],
                y_categories.iloc[train_index],
                X.iloc[validation_index],
            )
    return direct, negative_binomial, negative_binomial_risk, categories


def choose_blend(y, direct, categories):
    valid = ~np.isnan(direct)
    target = y.iloc[np.flatnonzero(valid)].to_numpy()
    candidates = []
    for family, values in categories.items():
        for weight in WEIGHTS:
            prediction = weight * direct[valid] + (1.0 - weight) * values[valid]
            candidates.append(
                {
                    "family": family,
                    "weight_direct": float(weight),
                    "mae": float(mean_absolute_error(target, prediction)),
                }
            )
    return min(candidates, key=lambda item: item["mae"]), valid


def classification_metrics(target, probability):
    labels = probability >= DECISION_THRESHOLD
    return {
        "roc_auc": float(roc_auc_score(target, probability)),
        "brier": float(brier_score_loss(target, probability)),
        "precision": float(precision_score(target, labels, zero_division=0)),
        "recall": float(recall_score(target, labels, zero_division=0)),
        "f1": float(f1_score(target, labels, zero_division=0)),
    }


def main() -> int:
    frame = pd.read_csv(DATASET, sep=";")
    frame["EVENTOS"] = pd.to_numeric(frame["EVENTOS"], errors="coerce")
    frame = frame.loc[frame["EVENTOS"].notna() & frame["EVENTOS"].gt(0)].reset_index(
        drop=True
    )
    with ABLATION_SUMMARY.open("r", encoding="utf-8") as stream:
        features = json.load(stream)["winner"]["features"]
    X = frame[features]
    # Rellenar NaN en features de clima con forward-fill + back-fill
    X = X.ffill().bfill()
    y_total = frame["EVENTOS"].astype(float)
    y_categories = frame[CATEGORIES].astype(float)

    prediction_rows = []
    selection_rows = []
    for fold, (train_index, validation_index) in enumerate(
        TimeSeriesSplit(n_splits=5).split(X), start=1
    ):
        X_train, X_validation = X.iloc[train_index], X.iloc[validation_index]
        total_train = y_total.iloc[train_index]
        category_train = y_categories.iloc[train_index]
        inner_direct, inner_nb, inner_nb_risk, inner_categories = inner_predictions(
            X_train, total_train, category_train
        )
        selection, valid = choose_blend(
            total_train, inner_direct, inner_categories
        )
        family = selection["family"]
        weight = selection["weight_direct"]
        direct_validation = fit_predict_direct(
            X_train, total_train, X_validation
        )
        nb_validation, nb_validation_risk = fit_predict_negative_binomial(
            X_train, total_train, X_validation
        )
        category_validation = fit_predict_categories(
            family, X_train, category_train, X_validation
        )
        blend_validation = (
            weight * direct_validation + (1.0 - weight) * category_validation
        )
        inner_blend = (
            weight * inner_direct[valid]
            + (1.0 - weight) * inner_categories[family][valid]
        )
        inner_target = total_train.iloc[np.flatnonzero(valid)].to_numpy()
        critical_inner = (inner_target > CRITICAL_THRESHOLD).astype(int)
        critical_validation = (
            y_total.iloc[validation_index].to_numpy() > CRITICAL_THRESHOLD
        ).astype(int)

        row = pd.DataFrame(
            {
                "date": frame["FECHA_DIA"].iloc[validation_index],
                "fold": fold,
                "actual": y_total.iloc[validation_index],
                "direct_count": direct_validation,
                "negative_binomial_count": nb_validation,
                "category_count": category_validation,
                "blend_count": blend_validation,
            }
        )
        for name, inner_count, outer_count in (
            ("direct", inner_direct[valid], direct_validation),
            ("category", inner_categories[family][valid], category_validation),
            ("blend", inner_blend, blend_validation),
        ):
            alpha = estimate_alpha(inner_target, inner_count)
            inner_risk = risk_from_count(inner_count, alpha)
            outer_risk = risk_from_count(outer_count, alpha)
            for method in ("raw", "sigmoid", "isotonic"):
                row[f"{name}_{method}_risk"] = calibrate(
                    method, inner_risk, critical_inner, outer_risk
                )
        for method in ("raw", "sigmoid", "isotonic"):
            row[f"negative_binomial_{method}_risk"] = calibrate(
                method,
                inner_nb_risk[valid],
                critical_inner,
                nb_validation_risk,
            )
        prediction_rows.append(row)
        selection_rows.append(
            {
                "fold": fold,
                "category_family": family,
                "weight_direct": weight,
                "inner_mae": selection["mae"],
                "outer_direct_mae": mean_absolute_error(
                    y_total.iloc[validation_index], direct_validation
                ),
                "outer_category_mae": mean_absolute_error(
                    y_total.iloc[validation_index], category_validation
                ),
                "outer_blend_mae": mean_absolute_error(
                    y_total.iloc[validation_index], blend_validation
                ),
            }
        )

    predictions = pd.concat(prediction_rows, ignore_index=True)
    actual = predictions["actual"].to_numpy()
    critical = (actual > CRITICAL_THRESHOLD).astype(int)
    count_rows = [
        {
            "model": name,
            "mae": float(mean_absolute_error(actual, predictions[column])),
        }
        for name, column in (
            ("direct", "direct_count"),
            ("negative_binomial", "negative_binomial_count"),
            ("categories", "category_count"),
            ("blend", "blend_count"),
        )
    ]
    risk_rows = []
    for column in [item for item in predictions if item.endswith("_risk")]:
        fold_metrics = []
        for _, fold_frame in predictions.groupby("fold"):
            fold_target = (
                fold_frame["actual"].to_numpy() > CRITICAL_THRESHOLD
            ).astype(int)
            fold_probability = fold_frame[column].to_numpy()
            fold_metrics.append(
                classification_metrics(fold_target, fold_probability)
            )
        risk_rows.append(
            {"model": column.removesuffix("_risk"), **classification_metrics(
                critical, predictions[column].to_numpy()
            ),
             "mean_fold_roc_auc": float(
                 np.mean([item["roc_auc"] for item in fold_metrics])
             ),
             "mean_fold_brier": float(
                 np.mean([item["brier"] for item in fold_metrics])
             )}
        )
    count_metrics = pd.DataFrame(count_rows).sort_values("mae")
    risk_metrics = pd.DataFrame(risk_rows).sort_values(
        ["brier", "roc_auc"], ascending=[True, False]
    )
    best_count = count_metrics.iloc[0].to_dict()
    best_risk = risk_metrics.iloc[0].to_dict()
    best_fold_ranking = risk_metrics.sort_values(
        ["mean_fold_roc_auc", "mean_fold_brier"],
        ascending=[False, True],
    ).iloc[0].to_dict()
    summary = {
        "best_count": best_count,
        "best_risk": best_risk,
        "best_fold_ranking": best_fold_ranking,
        "mean_weight_direct": float(
            pd.DataFrame(selection_rows)["weight_direct"].mean()
        ),
        "folds_blend_beats_direct": int(
            (
                pd.DataFrame(selection_rows)["outer_blend_mae"]
                < pd.DataFrame(selection_rows)["outer_direct_mae"]
            ).sum()
        ),
        "rows_used": len(frame),
    }
    OUTPUT.mkdir(parents=True, exist_ok=True)
    predictions.to_csv(OUTPUT / "predictions.csv", index=False)
    pd.DataFrame(selection_rows).to_csv(OUTPUT / "fold_selection.csv", index=False)
    count_metrics.to_csv(OUTPUT / "count_metrics.csv", index=False)
    risk_metrics.to_csv(OUTPUT / "risk_metrics.csv", index=False)
    with (OUTPUT / "summary.json").open("w", encoding="utf-8") as stream:
        json.dump(summary, stream, indent=2)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
