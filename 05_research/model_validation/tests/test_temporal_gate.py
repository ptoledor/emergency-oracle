import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
MODEL_DIR = PROJECT_ROOT / "03_model"
if str(MODEL_DIR) not in sys.path:
    sys.path.insert(0, str(MODEL_DIR))

from temporal_gate import build_origin_horizon_pairs, evaluate_promotion_gate


class OriginHorizonPairsTest(unittest.TestCase):
    def test_every_origin_has_every_horizon(self):
        pairs = build_origin_horizon_pairs(
            n_rows=20,
            min_train_days=10,
            horizons=(1, 2, 3),
        )
        by_origin = {}
        for origin, horizon, target in pairs:
            by_origin.setdefault(origin, []).append(horizon)
            self.assertEqual(target, origin + horizon)
            self.assertGreater(target, origin)

        self.assertEqual(sorted(by_origin), list(range(9, 17)))
        self.assertTrue(
            all(sorted(horizons) == [1, 2, 3] for horizons in by_origin.values())
        )

    def test_insufficient_history_returns_no_pairs(self):
        self.assertEqual(
            build_origin_horizon_pairs(8, min_train_days=10, horizons=(1, 2)),
            [],
        )


class PromotionGateTest(unittest.TestCase):
    def test_candidate_passes_all_conditions(self):
        result = evaluate_promotion_gate(
            candidate_mae=1.80,
            count_baseline_mae={"rolling_28d": 2.00, "persistence": 2.10},
            candidate_brier=0.101,
            probability_baseline_brier=0.100,
            candidate_horizon_mae={1: 1.7, 2: 1.8, 3: 1.9},
            baseline_horizon_mae={1: 1.8, 2: 1.9, 3: 2.0},
        )
        self.assertTrue(result.passes)
        self.assertEqual(result.best_count_baseline, "rolling_28d")
        self.assertEqual(result.horizons_improved, 3)

    def test_candidate_fails_when_only_average_improves(self):
        result = evaluate_promotion_gate(
            candidate_mae=1.90,
            count_baseline_mae={"rolling_28d": 2.00},
            candidate_brier=0.120,
            probability_baseline_brier=0.100,
            candidate_horizon_mae={1: 1.7, 2: 2.1, 3: 2.2},
            baseline_horizon_mae={1: 1.8, 2: 2.0, 3: 2.0},
        )
        self.assertFalse(result.passes)
        self.assertIn("brier_degradation_above_0.020", result.reasons)
        self.assertIn("only_1_of_3_horizons_improved", result.reasons)


if __name__ == "__main__":
    unittest.main()
