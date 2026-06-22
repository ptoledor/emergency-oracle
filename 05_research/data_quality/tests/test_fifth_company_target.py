import sys
import unittest
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT / "02_data"))

from clean_and_augment import fifth_company_dispatch_mask


class FifthCompanyTargetTests(unittest.TestCase):
    def test_recognizes_known_units_and_aliases(self):
        texts = pd.Series([
            "SALE B-5 A 10-8",
            "CONCURRE RB5",
            "RX-5, B-9",
            "SALE MX5 A 10-4",
            "FUTURO BX-5",
        ])
        self.assertEqual(fifth_company_dispatch_mask(texts).tolist(), [True] * 5)

    def test_rejects_geocode_other_units_and_url_tokens(self):
        texts = pd.Series([
            "6WQG J5 TALCAHUANO",
            "B-50 EN SERVICIO",
            "CONCURRE B-4 https://t.co/B5abc123",
            "RX-7, B-9",
        ])
        self.assertEqual(fifth_company_dispatch_mask(texts).tolist(), [False] * 4)


if __name__ == "__main__":
    unittest.main()
