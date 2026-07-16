import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from signal_features import aggregate_weather_daily


HYDRO_FEATURES = [
    "WX2_RAIN_SUM",
    "WX2_SHOWERS_SUM",
    "WX2_SHOWERS_MAX",
    "WX2_SHOWER_HOURS",
    "WX2_STORM_HOURS",
    "WX2_THUNDER_HOURS",
    "WX2_FREEZING_LEVEL_MEAN",
    "WX2_FREEZING_LEVEL_MIN",
    "WX2_WET_BULB_MEAN",
    "WX2_WET_BULB_MAX",
]


class SignalFeatureTests(unittest.TestCase):
    def base_hourly(self):
        return pd.DataFrame(
            {
                "time": pd.date_range("2026-01-01", periods=3, freq="h"),
                "temperature_2m": [10.0, 12.0, 14.0],
                "relative_humidity_2m": [90.0, 80.0, 70.0],
                "precipitation": [0.0, 3.0, 1.0],
                "wind_speed_10m": [5.0, 8.0, 6.0],
            }
        )

    def test_hydro_features_have_deterministic_fallbacks(self):
        daily = aggregate_weather_daily(self.base_hourly())

        self.assertTrue(np.isfinite(daily[HYDRO_FEATURES].to_numpy()).all())
        self.assertEqual(float(daily.iloc[0]["WX2_RAIN_SUM"]), 4.0)
        self.assertEqual(float(daily.iloc[0]["WX2_SHOWERS_SUM"]), 0.0)

    def test_partial_hydro_values_are_filled_before_aggregation(self):
        hourly = self.base_hourly().assign(
            rain=[0.0, np.nan, 0.5],
            showers=[0.0, 2.0, np.nan],
            weather_code=[0.0, 95.0, np.nan],
            freezing_level_height=[np.nan, 3000.0, 2500.0],
            wet_bulb_temperature_2m=[8.0, np.nan, 11.0],
        )
        daily = aggregate_weather_daily(hourly)

        self.assertTrue(np.isfinite(daily[HYDRO_FEATURES].to_numpy()).all())
        self.assertEqual(float(daily.iloc[0]["WX2_RAIN_SUM"]), 3.5)
        self.assertEqual(float(daily.iloc[0]["WX2_SHOWERS_SUM"]), 2.0)
        self.assertEqual(float(daily.iloc[0]["WX2_SHOWER_HOURS"]), 1.0)
        self.assertEqual(float(daily.iloc[0]["WX2_STORM_HOURS"]), 1.0)
        self.assertEqual(float(daily.iloc[0]["WX2_THUNDER_HOURS"]), 1.0)


if __name__ == "__main__":
    unittest.main()
