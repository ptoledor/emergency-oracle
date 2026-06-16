from __future__ import annotations

import sys
from pathlib import Path
import unittest

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from backtesting.splits import TemporalBacktestConfig, build_backtest_plan


class TemporalSplitTests(unittest.TestCase):
    def setUp(self) -> None:
        dates = pd.date_range("2024-01-01", periods=100, freq="D")
        target = np.arange(100, dtype=float) % 10
        target[5] = np.nan
        target[70] = np.nan
        self.data = pd.DataFrame({"date": dates, "target": target})
        self.config = TemporalBacktestConfig(
            date_column="date",
            target_column="target",
            holdout_start="2024-03-21",
            holdout_end="2024-04-05",
            outer_min_train_size=30,
            outer_test_size=10,
            outer_step_size=10,
            inner_min_train_size=15,
            inner_test_size=5,
            inner_step_size=5,
        )

    def test_unknown_targets_are_excluded_but_zero_is_known(self) -> None:
        plan = build_backtest_plan(self.data, self.config)
        self.assertEqual(len(plan.unknown_target_indices), 2)
        zero_index = plan.frame.index[plan.frame["target"] == 0][0]
        self.assertIn(zero_index, plan.development_indices)
        used = np.concatenate(
            [
                *(fold.train_indices for fold in plan.outer_folds),
                *(fold.test_indices for fold in plan.outer_folds),
                plan.holdout_indices,
            ]
        )
        self.assertFalse(
            np.intersect1d(used, plan.unknown_target_indices).size
        )

    def test_outer_and_inner_folds_preserve_time_order(self) -> None:
        plan = build_backtest_plan(self.data, self.config)
        self.assertGreater(len(plan.outer_folds), 0)
        for outer in plan.outer_folds:
            self.assertLess(outer.train_end, outer.test_start)
            self.assertLess(outer.test_end, pd.Timestamp("2024-03-21"))
            for inner in outer.inner_folds:
                self.assertLess(inner.train_end, inner.test_start)
                self.assertLessEqual(inner.test_end, outer.train_end)

    def test_holdout_is_date_blocked(self) -> None:
        plan = build_backtest_plan(self.data, self.config)
        holdout = plan.holdout_fold()
        self.assertEqual(holdout.test_start, pd.Timestamp("2024-03-21"))
        self.assertEqual(holdout.test_end, pd.Timestamp("2024-04-05"))
        self.assertLess(holdout.train_end, holdout.test_start)


if __name__ == "__main__":
    unittest.main()
