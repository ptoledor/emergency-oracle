import importlib.util
import sys
import unittest
from pathlib import Path

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[2] / "viper_dedup.py"
sys.path.insert(0, str(MODULE_PATH.parent))
SPEC = importlib.util.spec_from_file_location("viper_dedup", MODULE_PATH)
viper_dedup = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = viper_dedup
SPEC.loader.exec_module(viper_dedup)


class ViperDedupTests(unittest.TestCase):
    def test_extracts_lat_lon_from_html(self):
        html = '{"latitud": -36.72, "longitud": -73.11}'
        x, y, kind = viper_dedup.select_coordinate(html)
        self.assertEqual(kind, "lat_lon")
        self.assertAlmostEqual(x, -36.72)
        self.assertAlmostEqual(y, -73.11)

    def test_keeps_first_coordinate_inside_two_hour_window(self):
        messages = pd.DataFrame(
            {
                "Fecha": [
                    "2025-01-01T12:00:00.000Z",
                    "2025-01-01T12:30:00.000Z",
                    "2025-01-01T15:00:00.000Z",
                ],
                "Texto": ["EMERGENCIA", "SALE B-1", "EMERGENCIA NUEVA"],
                "URL": ["u1", "u2", "u3"],
            }
        )
        coords = pd.DataFrame(
            {
                "original_url": ["u1", "u2", "u3"],
                "final_url": ["u1", "u2", "u3"],
                "x": [600000.0, 600000.0, 600000.0],
                "y": [5900000.0, 5900000.0, 5900000.0],
                "coordinate_kind": ["utm_32718", "utm_32718", "utm_32718"],
                "status": ["ok", "ok", "ok"],
            }
        )
        result = viper_dedup.build_deduplicated_messages(messages, coords, 2)
        self.assertEqual(result["incident_rank"].tolist(), [1, 0, 1])

    def test_falls_back_to_same_url_inside_two_hour_window(self):
        messages = pd.DataFrame(
            {
                "Fecha": [
                    "2025-01-01T12:00:00.000Z",
                    "2025-01-01T12:30:00.000Z",
                    "2025-01-01T15:00:00.000Z",
                ],
                "Texto": ["EMERGENCIA", "SALE B-1", "EMERGENCIA NUEVA"],
                "URL": ["https://t.co/a", "https://t.co/a", "https://t.co/a"],
            }
        )
        coords = pd.DataFrame(
            columns=["original_url", "final_url", "x", "y", "coordinate_kind", "status"]
        )
        result = viper_dedup.build_deduplicated_messages(messages, coords, 2)
        self.assertEqual(result["incident_rank"].tolist(), [1, 0, 1])
        self.assertTrue(result.loc[1, "dedup_reason"].startswith("duplicate_url_"))


if __name__ == "__main__":
    unittest.main()
