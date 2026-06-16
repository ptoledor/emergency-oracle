import importlib.util
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "benchmark.py"
SPEC = importlib.util.spec_from_file_location("research_benchmark", MODULE_PATH)
benchmark = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = benchmark
SPEC.loader.exec_module(benchmark)


class BenchmarkTests(unittest.TestCase):
    def test_read_dataset_detects_semicolon(self):
        path = Path(__file__).resolve().parent / "fixtures" / "sample_semicolon.csv"
        frame = benchmark.read_dataset(path)
        self.assertEqual(frame.columns.tolist(), ["FECHA_DIA", "EVENTOS", "X"])
        self.assertEqual(int(frame.loc[0, "EVENTOS"]), 0)

    def test_prepare_data_keeps_or_excludes_zero(self):
        frame = pd.DataFrame(
            {
                "FECHA_DIA": ["2026-01-02", "2026-01-01"],
                "EVENTOS": [2, 0],
            }
        )
        included = benchmark.prepare_data(
            frame, "FECHA_DIA", "EVENTOS", "include"
        )
        excluded = benchmark.prepare_data(
            frame, "FECHA_DIA", "EVENTOS", "exclude"
        )
        self.assertEqual(included["EVENTOS"].tolist(), [0, 2])
        self.assertEqual(excluded["EVENTOS"].tolist(), [2])

    def test_prepare_data_excludes_unknown_target_but_keeps_zero(self):
        frame = pd.DataFrame(
            {
                "FECHA_DIA": ["2024-01-01", "2024-01-02", "2024-01-03"],
                "EVENTOS": [0, np.nan, 4],
            }
        )
        prepared = benchmark.prepare_data(
            frame, "FECHA_DIA", "EVENTOS", "include"
        )
        self.assertEqual(prepared["EVENTOS"].tolist(), [0.0, 4.0])

    def test_negative_binomial_returns_nonnegative_values_and_risk(self):
        rng = np.random.default_rng(42)
        X = pd.DataFrame({"x": np.linspace(-1, 1, 80)})
        y = pd.Series(rng.negative_binomial(3, 0.4, size=80))
        model = benchmark.NegativeBinomialRegressor(max_iter=100).fit(X, y)
        prediction = model.predict(X.iloc[-5:])
        risk = model.probability_above(X.iloc[-5:], threshold=7)
        self.assertTrue(np.all(prediction >= 0))
        self.assertTrue(np.all((risk >= 0) & (risk <= 1)))

    def test_feature_resolution_rejects_same_day_targets(self):
        frame = pd.DataFrame(
            {
                "FECHA_DIA": pd.date_range("2026-01-01", periods=3),
                "EVENTOS": [1, 2, 3],
                "TEMP_MAX": [10.0, 11.0, 12.0],
                "N_GASES": [0, 1, 0],
                "EVENTOS_ORIGINAL": [2, 3, 4],
                "EVENTOS_AUDITADOS": [1, 2, 3],
                "EVENTOS_lag_1": [0, 1, 2],
            }
        )
        features = benchmark.resolve_features(
            frame, "all_safe", None, "EVENTOS", "FECHA_DIA"
        )
        self.assertEqual(features, ["TEMP_MAX"])


if __name__ == "__main__":
    unittest.main()
