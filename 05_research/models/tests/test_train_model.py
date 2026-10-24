"""Saved count models must reproduce the fitted models after a JSON round trip."""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "03_model"))

from model_components import CountModel, HorizonProbabilityClassifier
from train_model import CANDIDATES, GLM_FEATURES, save_model


def synthetic_frame(rows=400, seed=0):
    rng = np.random.default_rng(seed)
    frame = pd.DataFrame({column: rng.gamma(2.0, 2.0, rows) for column in GLM_FEATURES})
    frame["EVENTOS_lag_1"] = rng.poisson(5, rows).astype(float)
    frame.loc[frame.index[::17], "EVENTOS_lag_1"] = np.nan  # unknown coverage days
    frame["WX2_RAIN_SUM"] = frame["LLUVIA"] * 0.9
    frame["EVENTOS"] = rng.poisson(np.exp(1.2 + 0.1 * np.log1p(frame["LLUVIA"])))
    frame["FECHA_DIA"] = pd.date_range("2024-01-01", periods=rows).strftime("%Y-%m-%d")
    return frame


@pytest.mark.parametrize("candidate", CANDIDATES, ids=[c.name for c in CANDIDATES])
def test_saved_model_reproduces_fit(tmp_path, candidate):
    frame = synthetic_frame()
    features = [c for c in frame.columns if c not in ("EVENTOS", "FECHA_DIA")]
    columns = candidate.feature_cols(features)
    fitted = candidate.fit(frame[columns], frame["EVENTOS"].astype(float))
    horizons = {"1": {"prob_coef": 0.5, "prob_intercept": -3.0}}
    save_model(candidate, fitted, frame, {"horizons": horizons}, tmp_path)

    loaded = CountModel.load(tmp_path)
    np.testing.assert_allclose(loaded.predict(frame), fitted.predict(frame), atol=1e-3)
    assert loaded.metadata["uses_event_history"] == candidate.uses_history
    probability = HorizonProbabilityClassifier(loaded).predict_proba_horizon(frame, 6)[:, 1]
    assert ((probability > 0) & (probability < 1)).all()
