import importlib.util
import sys
import unittest
from pathlib import Path

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[2] / "weather_features.py"
sys.path.insert(0, str(MODULE_PATH.parent))
SPEC = importlib.util.spec_from_file_location("weather_features", MODULE_PATH)
weather_features = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = weather_features
SPEC.loader.exec_module(weather_features)

ABLATION_PATH = Path(__file__).resolve().parents[2] / "run_weather_ablation.py"
ABLATION_SPEC = importlib.util.spec_from_file_location(
    "run_weather_ablation", ABLATION_PATH
)
ablation = importlib.util.module_from_spec(ABLATION_SPEC)
assert ABLATION_SPEC.loader is not None
sys.modules[ABLATION_SPEC.name] = ablation
ABLATION_SPEC.loader.exec_module(ablation)


class WeatherFeatureTests(unittest.TestCase):
    def test_daily_aggregation_builds_operational_features(self):
        hourly = pd.DataFrame(
            {
                "time": ["2025-01-01T00:00", "2025-01-01T01:00"],
                "temperature_2m": [25, 31],
                "relative_humidity_2m": [40, 20],
                "dew_point_2m": [12, 10],
                "precipitation": [0, 6],
                "precipitation_probability": [10, 80],
                "pressure_msl": [1010, 1005],
                "visibility": [10000, 3000],
                "wind_speed_10m": [10, 20],
                "wind_gusts_10m": [20, 40],
                "vapour_pressure_deficit": [1, 2],
                "cape": [100, 500],
                "soil_moisture_0_to_1cm": [0.2, 0.2],
                "soil_moisture_9_to_27cm": [0.4, 0.4],
                "et0_fao_evapotranspiration": [0.1, 0.2],
            }
        )
        daily = weather_features.aggregate_daily(hourly)
        self.assertEqual(len(daily), 1)
        self.assertEqual(float(daily.loc[0, "WX_GUST_MAX"]), 40)
        self.assertEqual(float(daily.loc[0, "WX_HEAVY_RAIN_HOURS"]), 1)
        self.assertEqual(float(daily.loc[0, "WX_LOW_HUMIDITY_HOURS"]), 1)
        self.assertGreater(float(daily.loc[0, "WX_FIRE_WEATHER_INDEX"]), 0)

    def test_partial_late_features_are_rejected(self):
        frame = pd.DataFrame(
            {
                "complete": range(10),
                "late": [None] * 3 + list(range(7)),
            }
        )
        selected = ablation.eligible_weather_features(
            frame,
            ["complete", "late"],
            minimum_coverage=0.60,
            maximum_initial_gap=0.10,
        )
        self.assertEqual(selected, ["complete"])


if __name__ == "__main__":
    unittest.main()
