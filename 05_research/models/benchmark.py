from __future__ import annotations

import argparse
import json
import pickle
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.special import gammaln
from scipy.stats import nbinom
from sklearn.calibration import calibration_curve
from sklearn.ensemble import (
    GradientBoostingRegressor,
    HistGradientBoostingRegressor,
    RandomForestClassifier,
)
from sklearn.impute import SimpleImputer
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression, PoissonRegressor, TweedieRegressor
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    brier_score_loss,
    f1_score,
    log_loss,
    mean_absolute_error,
    mean_poisson_deviance,
    mean_squared_error,
    precision_score,
    r2_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler


RANDOM_STATE = 42
DEFAULT_DATASET = Path("02_data/augmented_emergency_data.csv")
DEFAULT_METADATA = Path("03_model/saved_models/metadata_climatic_augmented.pkl")

CALENDAR_COLUMNS = {
    "MES",
    "DIA_SEMANA",
    "ES_FIN_SEMANA",
    "ES_FERIADO",
    "ES_FERIADO_IRRENUNCIABLE",
    "MES_SIN",
    "MES_COS",
    "DIA_SIN",
    "DIA_COS",
    "DANO_SIN",
    "DANO_COS",
}
CATEGORY_TARGET_COLUMNS = {
    "N_INCENDIO_ESTR",
    "N_INCENDIO_FOREST",
    "N_RESCATE_VEH",
    "N_RESCATE_PERS",
    "N_GASES",
    "N_OTROS",
}
AUDIT_TARGET_COLUMNS = {
    "EVENTOS_ORIGINAL",
    "EVENTOS_AUDITADOS",
    "TARGET_COVERAGE_COMPLETE",
}
TARGET_DERIVED_PREFIXES = ("EVENTOS_lag_", "EVENTOS_rolling_")


@dataclass(frozen=True)
class BenchmarkConfig:
    dataset: str
    output_dir: str
    date_column: str
    target_column: str
    critical_threshold: float
    decision_threshold: float
    n_splits: int
    inner_splits: int
    feature_source: str
    metadata_path: str | None
    zero_policy: str
    rf_estimators: int
    random_state: int


class NegativeBinomialRegressor:
    """Small NB2 GLM implemented with SciPy and current project dependencies."""

    def __init__(self, l2: float = 0.1, max_iter: int = 250):
        self.l2 = l2
        self.max_iter = max_iter
        self.imputer = SimpleImputer(strategy="median")
        self.scaler = StandardScaler()
        self.coef_: np.ndarray | None = None
        self.alpha_: float | None = None

    def fit(self, X: pd.DataFrame | np.ndarray, y: pd.Series | np.ndarray):
        values = self.imputer.fit_transform(X)
        values = self.scaler.fit_transform(values)
        design = np.column_stack([np.ones(len(values)), values])
        target = np.asarray(y, dtype=float)
        if np.any(target < 0):
            raise ValueError("Negative Binomial requires a non-negative target.")

        initial = np.zeros(design.shape[1] + 1)
        initial[0] = np.log(max(float(target.mean()), 0.1))
        mean = max(float(target.mean()), 1e-6)
        variance = float(target.var())
        initial[-1] = np.log(max((variance - mean) / (mean**2), 1e-3))

        def objective(params: np.ndarray) -> float:
            beta = params[:-1]
            alpha = np.exp(params[-1])
            mu = np.exp(np.clip(design @ beta, -20, 20))
            size = 1.0 / alpha
            probability = size / (size + mu)
            log_likelihood = (
                gammaln(target + size)
                - gammaln(size)
                - gammaln(target + 1)
                + size * np.log(probability)
                + target * np.log1p(-probability)
            )
            penalty = 0.5 * self.l2 * np.sum(beta[1:] ** 2)
            return float(-np.sum(log_likelihood) + penalty)

        result = minimize(
            objective,
            initial,
            method="L-BFGS-B",
            bounds=[(None, None)] * design.shape[1] + [(-9.0, 4.0)],
            options={"maxiter": self.max_iter},
        )
        if not result.success and not np.isfinite(result.fun):
            raise RuntimeError(f"Negative Binomial fit failed: {result.message}")
        self.coef_ = result.x[:-1]
        self.alpha_ = float(np.exp(result.x[-1]))
        return self

    def predict(self, X: pd.DataFrame | np.ndarray) -> np.ndarray:
        if self.coef_ is None:
            raise RuntimeError("Model must be fitted before prediction.")
        values = self.scaler.transform(self.imputer.transform(X))
        design = np.column_stack([np.ones(len(values)), values])
        return np.exp(np.clip(design @ self.coef_, -20, 20))

    def probability_above(
        self, X: pd.DataFrame | np.ndarray, threshold: float
    ) -> np.ndarray:
        if self.alpha_ is None:
            raise RuntimeError("Model must be fitted before prediction.")
        means = np.clip(self.predict(X), 1e-8, None)
        size = 1.0 / self.alpha_
        probability = size / (size + means)
        return nbinom.sf(np.floor(threshold), size, probability)


def read_dataset(path: Path) -> pd.DataFrame:
    with path.open("r", encoding="utf-8-sig") as stream:
        first_line = stream.readline()
    separator = ";" if first_line.count(";") > first_line.count(",") else ","
    return pd.read_csv(path, sep=separator)


def resolve_features(
    frame: pd.DataFrame,
    source: str,
    metadata_path: Path | None,
    target_column: str,
    date_column: str,
) -> list[str]:
    numeric = set(frame.select_dtypes(include=[np.number]).columns)
    forbidden = (
        {target_column, date_column}
        | CATEGORY_TARGET_COLUMNS
        | AUDIT_TARGET_COLUMNS
    )

    if source == "metadata":
        if metadata_path is None:
            raise ValueError("--metadata-path is required for feature-source=metadata.")
        with metadata_path.open("rb") as stream:
            metadata = pickle.load(stream)
        features = list(metadata["feature_cols"])
    elif source == "climatic":
        forbidden |= CALENDAR_COLUMNS
        features = [
            column
            for column in frame.columns
            if column in numeric
            and column not in forbidden
            and not column.startswith(TARGET_DERIVED_PREFIXES)
            and "_lag_" not in column.lower()
        ]
    elif source == "calendar_lags":
        features = [
            column
            for column in frame.columns
            if column in numeric and column not in forbidden
        ]
    elif source == "all_safe":
        features = [
            column
            for column in frame.columns
            if column in numeric
            and column not in forbidden
            and not column.startswith("N_")
            and not column.startswith(TARGET_DERIVED_PREFIXES)
        ]
    else:
        features = [item.strip() for item in source.split(",") if item.strip()]

    missing = [column for column in features if column not in frame.columns]
    non_numeric = [column for column in features if column not in numeric]
    if missing:
        raise ValueError(f"Missing feature columns: {missing}")
    if non_numeric:
        raise ValueError(f"Non-numeric feature columns: {non_numeric}")
    if not features:
        raise ValueError("No usable features were selected.")
    return features


def prepare_data(
    frame: pd.DataFrame,
    date_column: str,
    target_column: str,
    zero_policy: str,
) -> pd.DataFrame:
    required = {date_column, target_column}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Dataset is missing required columns: {sorted(missing)}")

    prepared = frame.copy()
    prepared[date_column] = pd.to_datetime(prepared[date_column], errors="raise")
    prepared[target_column] = pd.to_numeric(
        prepared[target_column], errors="raise"
    )
    if prepared[date_column].duplicated().any():
        raise ValueError("Dates must be unique before temporal benchmarking.")
    if (prepared[target_column] < 0).any():
        raise ValueError("Count target cannot contain negative values.")
    prepared = prepared.loc[prepared[target_column].notna()].copy()
    prepared = prepared.sort_values(date_column).reset_index(drop=True)
    if zero_policy == "exclude":
        prepared = prepared.loc[prepared[target_column] > 0].reset_index(drop=True)
    return prepared


def build_count_models(random_state: int) -> dict[str, Callable[[], object]]:
    return {
        "gradient_boosting_absolute": lambda: make_pipeline(
            SimpleImputer(strategy="median"),
            GradientBoostingRegressor(
                loss="absolute_error",
                n_estimators=200,
                max_depth=4,
                min_samples_leaf=5,
                learning_rate=0.05,
                subsample=0.8,
                random_state=random_state,
            ),
        ),
        "hist_gradient_boosting_poisson": lambda: make_pipeline(
            SimpleImputer(strategy="median"),
            HistGradientBoostingRegressor(
                loss="poisson",
                max_iter=300,
                learning_rate=0.05,
                max_leaf_nodes=15,
                min_samples_leaf=12,
                l2_regularization=1.0,
                random_state=random_state,
            ),
        ),
        "poisson_regressor": lambda: make_pipeline(
            SimpleImputer(strategy="median"),
            StandardScaler(),
            PoissonRegressor(alpha=0.1, max_iter=500),
        ),
        "tweedie_regressor": lambda: make_pipeline(
            SimpleImputer(strategy="median"),
            StandardScaler(),
            TweedieRegressor(power=1.5, alpha=0.1, link="log", max_iter=500),
        ),
        "negative_binomial": lambda: NegativeBinomialRegressor(),
    }


def build_classifier(random_state: int, estimators: int) -> RandomForestClassifier:
    return RandomForestClassifier(
        n_estimators=estimators,
        max_depth=8,
        min_samples_leaf=4,
        class_weight="balanced_subsample",
        n_jobs=1,
        random_state=random_state,
    )


def _fit_calibrator(
    method: str, probabilities: np.ndarray, targets: np.ndarray
) -> Callable[[np.ndarray], np.ndarray]:
    probabilities = np.clip(np.asarray(probabilities, dtype=float), 1e-6, 1 - 1e-6)
    targets = np.asarray(targets, dtype=int)
    if np.unique(targets).size < 2:
        constant = float(targets.mean())
        return lambda values: np.full(len(values), constant)
    if method == "sigmoid":
        model = LogisticRegression(random_state=RANDOM_STATE)
        model.fit(np.log(probabilities / (1 - probabilities)).reshape(-1, 1), targets)
        return lambda values: model.predict_proba(
            np.log(
                np.clip(values, 1e-6, 1 - 1e-6)
                / (1 - np.clip(values, 1e-6, 1 - 1e-6))
            ).reshape(-1, 1)
        )[:, 1]
    model = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
    model.fit(probabilities, targets)
    return lambda values: model.predict(np.asarray(values, dtype=float))


def temporally_calibrated_probabilities(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    raw_validation_probabilities: np.ndarray,
    inner_splits: int,
    classifier_factory: Callable[[], RandomForestClassifier],
) -> dict[str, np.ndarray]:
    usable_splits = min(inner_splits, max(2, len(X_train) // 30))
    splitter = TimeSeriesSplit(n_splits=usable_splits)
    inner_probabilities = np.full(len(X_train), np.nan)
    for inner_train, inner_validation in splitter.split(X_train):
        model = classifier_factory()
        model.fit(X_train.iloc[inner_train], y_train.iloc[inner_train])
        inner_probabilities[inner_validation] = model.predict_proba(
            X_train.iloc[inner_validation]
        )[:, 1]
    valid = ~np.isnan(inner_probabilities)
    calibration_target = y_train.iloc[np.flatnonzero(valid)].to_numpy()
    return {
        method: np.clip(
            _fit_calibrator(
                method,
                inner_probabilities[valid],
                calibration_target,
            )(raw_validation_probabilities),
            0.0,
            1.0,
        )
        for method in ("sigmoid", "isotonic")
    }


def expected_calibration_error(
    target: np.ndarray, probabilities: np.ndarray, bins: int = 10
) -> float:
    observed, predicted = calibration_curve(
        target, probabilities, n_bins=bins, strategy="quantile"
    )
    return float(np.mean(np.abs(observed - predicted)))


def regression_metrics(
    target: np.ndarray, prediction: np.ndarray, baseline_mae: float
) -> dict[str, float]:
    positive_prediction = np.clip(prediction, 1e-8, None)
    return {
        "mae": float(mean_absolute_error(target, prediction)),
        "rmse": float(mean_squared_error(target, prediction) ** 0.5),
        "r2": float(r2_score(target, prediction)),
        "poisson_deviance": float(
            mean_poisson_deviance(target, positive_prediction)
        ),
        "mase_vs_train_mean": float(
            mean_absolute_error(target, prediction) / max(baseline_mae, 1e-8)
        ),
    }


def classification_metrics(
    target: np.ndarray, probabilities: np.ndarray, threshold: float
) -> dict[str, float]:
    labels = probabilities >= threshold
    result = {
        "average_precision": float(average_precision_score(target, probabilities)),
        "brier": float(brier_score_loss(target, probabilities)),
        "log_loss": float(log_loss(target, probabilities, labels=[0, 1])),
        "ece_10": expected_calibration_error(target, probabilities),
        "accuracy": float(accuracy_score(target, labels)),
        "precision": float(precision_score(target, labels, zero_division=0)),
        "recall": float(recall_score(target, labels, zero_division=0)),
        "f1": float(f1_score(target, labels, zero_division=0)),
        "alert_rate": float(labels.mean()),
    }
    result["roc_auc"] = (
        float(roc_auc_score(target, probabilities))
        if np.unique(target).size == 2
        else float("nan")
    )
    return result


def run_benchmark(config: BenchmarkConfig) -> dict[str, Path]:
    dataset_path = Path(config.dataset)
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw = read_dataset(dataset_path)
    frame = prepare_data(
        raw, config.date_column, config.target_column, config.zero_policy
    )
    metadata_path = Path(config.metadata_path) if config.metadata_path else None
    features = resolve_features(
        frame,
        config.feature_source,
        metadata_path,
        config.target_column,
        config.date_column,
    )
    X = frame[features]
    y_count = frame[config.target_column].astype(float)
    y_critical = (y_count > config.critical_threshold).astype(int)

    predictions = pd.DataFrame(
        {
            "date": frame[config.date_column],
            "fold": np.nan,
            "actual_count": y_count,
            "actual_critical": y_critical,
        }
    )
    count_factories = build_count_models(config.random_state)
    for name in count_factories:
        predictions[f"count__{name}"] = np.nan
    predictions["risk__negative_binomial"] = np.nan
    for name in ("rf_raw", "rf_sigmoid", "rf_isotonic"):
        predictions[f"class__{name}"] = np.nan

    fold_rows: list[dict[str, float | int | str]] = []
    splitter = TimeSeriesSplit(n_splits=config.n_splits)
    classifier_factory = lambda: build_classifier(
        config.random_state, config.rf_estimators
    )

    for fold, (train_index, validation_index) in enumerate(splitter.split(X), start=1):
        X_train, X_validation = X.iloc[train_index], X.iloc[validation_index]
        y_train = y_count.iloc[train_index]
        y_validation = y_count.iloc[validation_index]
        critical_train = y_critical.iloc[train_index]
        critical_validation = y_critical.iloc[validation_index]
        predictions.loc[validation_index, "fold"] = fold
        baseline = np.full(len(validation_index), y_train.mean())
        baseline_mae = mean_absolute_error(y_validation, baseline)

        for name, factory in count_factories.items():
            model = factory()
            model.fit(X_train, y_train)
            values = np.clip(model.predict(X_validation), 0.0, None)
            predictions.loc[validation_index, f"count__{name}"] = values
            metrics = regression_metrics(
                y_validation.to_numpy(), values, baseline_mae
            )
            fold_rows.append({"fold": fold, "task": "count", "model": name, **metrics})
            if name == "negative_binomial":
                risk = model.probability_above(
                    X_validation, config.critical_threshold
                )
                predictions.loc[
                    validation_index, "risk__negative_binomial"
                ] = risk

        raw_classifier = classifier_factory()
        raw_classifier.fit(X_train, critical_train)
        raw_probability = raw_classifier.predict_proba(X_validation)[:, 1]
        calibrated = temporally_calibrated_probabilities(
            X_train,
            critical_train,
            raw_probability,
            config.inner_splits,
            classifier_factory,
        )
        probabilities = {
            "rf_raw": raw_probability,
            "rf_sigmoid": calibrated["sigmoid"],
            "rf_isotonic": calibrated["isotonic"],
        }
        for name, values in probabilities.items():
            predictions.loc[validation_index, f"class__{name}"] = values
            metrics = classification_metrics(
                critical_validation.to_numpy(), values, config.decision_threshold
            )
            fold_rows.append(
                {"fold": fold, "task": "classification", "model": name, **metrics}
            )

        nb_risk = predictions.loc[
            validation_index, "risk__negative_binomial"
        ].to_numpy()
        metrics = classification_metrics(
            critical_validation.to_numpy(), nb_risk, config.decision_threshold
        )
        fold_rows.append(
            {
                "fold": fold,
                "task": "classification",
                "model": "negative_binomial_derived_risk",
                **metrics,
            }
        )

    evaluated = predictions["fold"].notna()
    count_rows = []
    first_validation = int(np.flatnonzero(evaluated.to_numpy())[0])
    baseline_mae = mean_absolute_error(
        y_count.iloc[first_validation:],
        np.full(evaluated.sum(), y_count.iloc[:first_validation].mean()),
    )
    for name in count_factories:
        values = predictions.loc[evaluated, f"count__{name}"].to_numpy()
        metrics = regression_metrics(
            y_count.loc[evaluated].to_numpy(), values, baseline_mae
        )
        count_rows.append({"model": name, **metrics})

    classification_rows = []
    probability_columns = {
        "rf_raw": "class__rf_raw",
        "rf_sigmoid": "class__rf_sigmoid",
        "rf_isotonic": "class__rf_isotonic",
        "negative_binomial_derived_risk": "risk__negative_binomial",
    }
    for name, column in probability_columns.items():
        metrics = classification_metrics(
            y_critical.loc[evaluated].to_numpy(),
            predictions.loc[evaluated, column].to_numpy(),
            config.decision_threshold,
        )
        classification_rows.append({"model": name, **metrics})

    count_metrics = pd.DataFrame(count_rows).sort_values("mae")
    classification_metrics_frame = pd.DataFrame(classification_rows).sort_values(
        ["brier", "average_precision"], ascending=[True, False]
    )
    fold_metrics = pd.DataFrame(fold_rows)

    paths = {
        "predictions": output_dir / "predictions.csv",
        "count_metrics": output_dir / "count_metrics.csv",
        "classification_metrics": output_dir / "classification_metrics.csv",
        "fold_metrics": output_dir / "fold_metrics.csv",
        "config": output_dir / "run_config.json",
    }
    predictions.loc[evaluated].to_csv(paths["predictions"], index=False)
    count_metrics.to_csv(paths["count_metrics"], index=False)
    classification_metrics_frame.to_csv(
        paths["classification_metrics"], index=False
    )
    fold_metrics.to_csv(paths["fold_metrics"], index=False)
    with paths["config"].open("w", encoding="utf-8") as stream:
        json.dump(
            {
                **asdict(config),
                "features": features,
                "rows_loaded": len(raw),
                "rows_used": len(frame),
                "zero_count_used": int((y_count == 0).sum()),
                "critical_prevalence": float(y_critical.mean()),
            },
            stream,
            indent=2,
            ensure_ascii=True,
        )
    return paths


def parse_args(argv: list[str] | None = None) -> BenchmarkConfig:
    parser = argparse.ArgumentParser(
        description="Leak-aware temporal benchmark for emergency count and risk models."
    )
    parser.add_argument("--dataset", default=str(DEFAULT_DATASET))
    parser.add_argument("--output-dir", default="05_research/models/results")
    parser.add_argument("--date-column", default="FECHA_DIA")
    parser.add_argument("--target-column", default="EVENTOS")
    parser.add_argument("--critical-threshold", type=float, default=7.0)
    parser.add_argument("--decision-threshold", type=float, default=0.5)
    parser.add_argument("--n-splits", type=int, default=5)
    parser.add_argument("--inner-splits", type=int, default=4)
    parser.add_argument(
        "--feature-source",
        default="metadata",
        help=(
            "metadata, climatic, calendar_lags, all_safe, or a comma-separated "
            "explicit feature list."
        ),
    )
    parser.add_argument("--metadata-path", default=str(DEFAULT_METADATA))
    parser.add_argument(
        "--zero-policy",
        choices=("include", "exclude"),
        default="include",
        help="Include verified zero-count days by default; exclude only for sensitivity.",
    )
    parser.add_argument("--rf-estimators", type=int, default=400)
    parser.add_argument("--random-state", type=int, default=RANDOM_STATE)
    args = parser.parse_args(argv)
    if args.n_splits < 2 or args.inner_splits < 2:
        parser.error("Both split counts must be at least 2.")
    return BenchmarkConfig(**vars(args))


def main(argv: list[str] | None = None) -> int:
    config = parse_args(argv)
    try:
        paths = run_benchmark(config)
    except Exception as error:
        print(f"Benchmark failed: {error}", file=sys.stderr)
        return 1
    print("Benchmark completed.")
    for name, path in paths.items():
        print(f"{name}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
