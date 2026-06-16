"""Experiment evaluator for rolling-origin CV and sealed holdout testing."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .baselines import HistoricalMedianBaseline, MovingAverageBaseline
from .metrics import classification_metrics, regression_metrics
from .splits import BacktestPlan, TemporalFold


@dataclass
class PredictionBundle:
    count: Sequence[float]
    probability: Sequence[float] | None = None


Predictor = Callable[[pd.DataFrame, pd.DataFrame], PredictionBundle]


@dataclass
class EvaluationResult:
    predictions: pd.DataFrame
    fold_metrics: pd.DataFrame
    summary: pd.DataFrame
    split: str

    def export(self, output_directory: str | Path, plan: BacktestPlan) -> None:
        output = Path(output_directory)
        output.mkdir(parents=True, exist_ok=True)
        self.predictions.to_csv(output / f"{self.split}_predictions.csv", index=False)
        self.fold_metrics.to_csv(output / f"{self.split}_fold_metrics.csv", index=False)
        self.summary.to_csv(output / f"{self.split}_summary.csv", index=False)
        with (output / f"{self.split}_results.json").open(
            "w", encoding="utf-8"
        ) as handle:
            json.dump(
                {
                    "split": self.split,
                    "summary": _records(self.summary),
                    "fold_metrics": _records(self.fold_metrics),
                },
                handle,
                indent=2,
                allow_nan=False,
            )
        with (output / "backtest_plan.json").open("w", encoding="utf-8") as handle:
            json.dump(plan.to_dict(), handle, indent=2, allow_nan=False)


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return json.loads(frame.to_json(orient="records", date_format="iso"))


class BacktestEvaluator:
    def __init__(
        self,
        plan: BacktestPlan,
        critical_threshold: float = 7.0,
        decision_threshold: float = 0.5,
        classification_column: str | None = None,
    ) -> None:
        self.plan = plan
        self.critical_threshold = critical_threshold
        self.decision_threshold = decision_threshold
        self.classification_column = classification_column
        if classification_column and classification_column not in plan.frame.columns:
            raise KeyError(f"Missing classification column: {classification_column}")

    def evaluate_cv(
        self,
        predictors: Mapping[str, Predictor] | None = None,
        include_baselines: bool = True,
    ) -> EvaluationResult:
        return self._evaluate(
            folds=self.plan.outer_folds,
            split="cv",
            predictors=predictors,
            include_baselines=include_baselines,
        )

    def evaluate_holdout(
        self,
        predictors: Mapping[str, Predictor] | None = None,
        include_baselines: bool = True,
    ) -> EvaluationResult:
        """Open the blocked holdout explicitly after experiment choices are frozen."""
        return self._evaluate(
            folds=[self.plan.holdout_fold()],
            split="holdout",
            predictors=predictors,
            include_baselines=include_baselines,
        )

    def _evaluate(
        self,
        folds: Sequence[TemporalFold],
        split: str,
        predictors: Mapping[str, Predictor] | None,
        include_baselines: bool,
    ) -> EvaluationResult:
        registered: dict[str, Predictor] = dict(predictors or {})
        if include_baselines:
            registered = {**self._baseline_predictors(), **registered}
        if not registered:
            raise ValueError("Provide at least one predictor or enable baselines")

        prediction_frames: list[pd.DataFrame] = []
        metric_rows: list[dict[str, Any]] = []
        frame = self.plan.frame
        date_column = self.plan.config.date_column
        target_column = self.plan.config.target_column

        for fold in folds:
            train = frame.loc[fold.train_indices].copy()
            test = frame.loc[fold.test_indices].copy()
            y_train = train[target_column].to_numpy(dtype=float)
            y_true = test[target_column].to_numpy(dtype=float)
            y_class = self._classification_target(test)

            for model_name, predictor in registered.items():
                bundle = predictor(train.copy(), test.copy())
                count = np.asarray(bundle.count, dtype=float)
                if len(count) != len(test):
                    raise ValueError(
                        f"{model_name} returned {len(count)} count predictions "
                        f"for {len(test)} rows in {fold.name}"
                    )
                probability = (
                    np.asarray(bundle.probability, dtype=float)
                    if bundle.probability is not None
                    else np.full(len(test), np.nan)
                )
                if len(probability) != len(test):
                    raise ValueError(
                        f"{model_name} returned invalid probability length"
                    )

                regression = regression_metrics(y_true, count, y_train)
                classification = classification_metrics(
                    y_class,
                    probability,
                    decision_threshold=self.decision_threshold,
                    dates=test[date_column],
                )
                metric_rows.append(
                    {
                        "split": split,
                        "fold": fold.name,
                        "model": model_name,
                        "train_start": fold.train_start,
                        "train_end": fold.train_end,
                        "test_start": fold.test_start,
                        "test_end": fold.test_end,
                        "n_train": len(train),
                        "n_test": len(test),
                        **regression,
                        **classification,
                    }
                )
                prediction_frames.append(
                    pd.DataFrame(
                        {
                            "split": split,
                            "fold": fold.name,
                            "model": model_name,
                            "date": test[date_column].to_numpy(),
                            "y_true": y_true,
                            "y_pred": count,
                            "y_class": y_class,
                            "y_probability": probability,
                            "y_predicted_class": (
                                probability >= self.decision_threshold
                            ).astype(float),
                        }
                    )
                )

        predictions = pd.concat(prediction_frames, ignore_index=True)
        fold_metrics = pd.DataFrame(metric_rows)
        metric_columns = [
            "mae",
            "rmse",
            "r2",
            "mase",
            "poisson_deviance",
            "roc_auc",
            "pr_auc",
            "brier",
            "log_loss",
            "precision",
            "recall",
            "f1",
            "alerts_per_week",
        ]
        summary = (
            fold_metrics.groupby("model", as_index=False)[metric_columns]
            .mean(numeric_only=True)
            .assign(folds=fold_metrics.groupby("model").size().to_numpy())
        )
        return EvaluationResult(predictions, fold_metrics, summary, split)

    def _classification_target(self, test: pd.DataFrame) -> np.ndarray:
        if self.classification_column:
            values = pd.to_numeric(
                test[self.classification_column], errors="coerce"
            ).to_numpy(dtype=float)
            if not np.all(np.isin(values, [0.0, 1.0])):
                raise ValueError("Known classification targets must be binary 0/1")
            return values.astype(int)
        target = test[self.plan.config.target_column].to_numpy(dtype=float)
        return (target > self.critical_threshold).astype(int)

    def _baseline_predictors(self) -> dict[str, Predictor]:
        target_column = self.plan.config.target_column
        baselines = [HistoricalMedianBaseline(), MovingAverageBaseline(window=28)]

        def adapt(baseline: Any) -> Predictor:
            def predict(train: pd.DataFrame, test: pd.DataFrame) -> PredictionBundle:
                result = baseline.predict(
                    train[target_column],
                    test[target_column],
                    self.critical_threshold,
                )
                return PredictionBundle(result.count, result.probability)

            return predict

        return {baseline.name: adapt(baseline) for baseline in baselines}
