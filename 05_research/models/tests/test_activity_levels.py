import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from activity_levels import operational_activity_level


class OperationalActivityLevelTests(unittest.TestCase):
    def test_operational_boundaries(self):
        cases = {
            0.0: "ACTIVIDAD BAJA",
            3.99: "ACTIVIDAD BAJA",
            4.0: "ACTIVIDAD NORMAL",
            5.99: "ACTIVIDAD NORMAL",
            6.0: "ACTIVIDAD ALTA",
            7.99: "ACTIVIDAD ALTA",
            8.0: "ACTIVIDAD MUY ALTA",
            20.0: "ACTIVIDAD MUY ALTA",
        }
        for calls, expected in cases.items():
            with self.subTest(calls=calls):
                label, css_class = operational_activity_level(calls)
                self.assertEqual(label, expected)
                self.assertTrue(css_class.startswith("activity-"))

    def test_non_finite_value_is_rejected(self):
        with self.assertRaises(ValueError):
            operational_activity_level(float("nan"))


if __name__ == "__main__":
    unittest.main()
