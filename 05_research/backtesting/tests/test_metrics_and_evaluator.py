from __future__ import annotations

import sys
from pathlib import Path
import unittest
from unittest.mock import mock_open, patch

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from backtesting.evaluator import BacktestEvaluator, PredictionBundle
from backtesting.metrics import classification_metrics, regression_metrics
from backtesting.splits import TemporalBacktestConfig, build_backtest_plan


class MetricTests(unittest.TestCase):
    def test_regression_metrics_include_required_values(self) -> None:
        metrics = regression_metrics([0, 1, 2], [0.1, 1.1, 1.8], [0, 1, 1, 2])
        self.assertEqual(
            set(metrics),
            {"mae", "rmse", "r2", "mase", "poisson_deviance"},
        )
        self.assertTrue(all(np.isfinite(value) for value in metrics.values()))

    def test_classification_metrics_include_alert_rate(self) -> None:
        metrics = classification_metrics(
            [0, 0, 1, 1],
            [0.1, 0.4, 0.7, 0.9],
            dates=pd.date_range("2024-01-01", periods=4),
        )
        self.assertAlmostEqual(metrics["roc_auc"], 1.0)
        self.assertAlmostEqual(metrics["alerts_per_week"], 3.5)


class EvaluatorTests(unittest.TestCase):
    def setUp(self) -> None:
        dates = pd.date_range("2023-01-01", periods=140, freq="D")
        target = (np.arange(140) % 11).astype(float)
        target[12] = np.nan
        data = pd.DataFrame({"date": dates, "target": target})
        config = TemporalBacktestConfig(
            date_column="date",
            target_column="target",
            holdout_start="2023-05-01",
            outer_min_train_size=50,
            outer_test_size=14,
            outer_step_size=14,
            inner_min_train_size=25,
            inner_test_size=7,
            inner_step_size=7,
            max_outer_folds=2,
            max_inner_folds=2,
        )
        self.plan = build_backtest_plan(data, config)

    def test_evaluator_runs_baselines_and_custom_predictor(self) -> None:
        def constant(train: pd.DataFrame, test: pd.DataFrame) -> PredictionBundle:
            count = np.full(len(test), train["target"].mean())
            probability = np.full(len(test), (train["target"] > 7).mean())
            return PredictionBundle(count, probability)

        result = BacktestEvaluator(self.plan).evaluate_cv(
            predictors={"constant": constant}
        )
        self.assertEqual(
            set(result.summary["model"]),
            {"historical_median", "moving_average_28d", "constant"},
        )
        self.assertFalse(result.predictions["y_true"].isna().any())

    def test_export_writes_csv_and_valid_json(self) -> None:
        result = BacktestEvaluator(self.plan).evaluate_cv()
        output = Path("mock-output")
        with (
            patch.object(Path, "mkdir") as mkdir,
            patch.object(Path, "open", mock_open()) as path_open,
            patch.object(pd.DataFrame, "to_csv") as to_csv,
            patch("backtesting.evaluator.json.dump") as json_dump,
        ):
            result.export(output, self.plan)
        mkdir.assert_called_once_with(parents=True, exist_ok=True)
        self.assertEqual(to_csv.call_count, 3)
        self.assertEqual(path_open.call_count, 2)
        self.assertEqual(json_dump.call_count, 2)

    def test_holdout_requires_explicit_call(self) -> None:
        cv = BacktestEvaluator(self.plan).evaluate_cv()
        holdout = BacktestEvaluator(self.plan).evaluate_holdout()
        self.assertTrue((cv.predictions["split"] == "cv").all())
        self.assertTrue((holdout.predictions["split"] == "holdout").all())


if __name__ == "__main__":
    unittest.main()
