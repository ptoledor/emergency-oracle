"""Guard the dashboard forecast against unintended changes.

Regenerate the golden file only when a model or forecast change is intended:
    python 05_research/models/tests/forecast_harness.py
"""

import json
import math

import pytest

from forecast_harness import GOLDEN_CASES, GOLDEN_PATH, load_forecast_inputs, run_case

# XGBoost predictions are float32; allow cross-platform rounding only.
TOLERANCE = 1e-6


def assert_close(expected, actual, path="root"):
    if isinstance(expected, dict):
        assert set(expected) == set(actual), f"{path}: claves distintas"
        for key in expected:
            assert_close(expected[key], actual[key], f"{path}.{key}")
    elif isinstance(expected, list):
        assert len(expected) == len(actual), f"{path}: largo distinto"
        for index, (left, right) in enumerate(zip(expected, actual)):
            assert_close(left, right, f"{path}[{index}]")
    elif isinstance(expected, float):
        assert actual is not None and math.isclose(
            expected, actual, rel_tol=TOLERANCE, abs_tol=TOLERANCE
        ), f"{path}: {expected} != {actual}"
    else:
        assert expected == actual, f"{path}: {expected!r} != {actual!r}"


@pytest.fixture(scope="module")
def forecast_inputs():
    return load_forecast_inputs()


@pytest.mark.parametrize("case", GOLDEN_CASES, ids=[case["name"] for case in GOLDEN_CASES])
def test_forecast_matches_golden(forecast_inputs, case):
    golden = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))
    assert_close(golden[case["name"]], run_case(forecast_inputs, case), case["name"])
