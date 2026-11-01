"""Walk-forward harness to reproduce the official hydro ensemble and test v3 ideas."""

from __future__ import annotations

import os
import sys
from pathlib import Path

os.environ.setdefault("OMP_NUM_THREADS", "1")

import numpy as np
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit
from xgboost import XGBRegressor

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "03_model"))

from build_hydro_ensemble_candidate import (
    CENTER, OFFSET, SPREAD_SCALE, WEIGHTS, load_training_frame,
)
from search_signal_features_v2 import ACTIVE_PARAMS, D2_PARAMS
from search_signal_models import N_SPLITS, TEST_SIZE, metrics


def head_params():
    return {
        "active": ACTIVE_PARAMS,
        "squared": D2_PARAMS,
        "poisson": {**D2_PARAMS, "objective": "count:poisson"},
        "quantile": {**D2_PARAMS, "objective": "reg:quantileerror", "quantile_alpha": 0.5},
    }


def ensemble_oof(frame, base_features, ensemble_features, y, params=None, weights=WEIGHTS,
                 center=CENTER, spread=SPREAD_SCALE, offset=OFFSET):
    params = params or head_params()
    splits = list(TimeSeriesSplit(n_splits=N_SPLITS, test_size=TEST_SIZE).split(frame))
    heads = {name: np.full(len(frame), np.nan) for name in params}
    fold = np.zeros(len(frame), dtype=int)
    for number, (train, test) in enumerate(splits, start=1):
        fold[test] = number
        for name, p in params.items():
            columns = base_features if name == "active" else ensemble_features
            model = XGBRegressor(**p).fit(frame.iloc[train][columns], y.iloc[train])
            heads[name][test] = model.predict(frame.iloc[test][columns])
    mask = fold > 0
    raw = sum(weights[name] * heads[name] for name in params)
    pred = np.clip(center + spread * (raw - center) + offset, 0, None)
    return mask, fold, pred, heads


if __name__ == "__main__":
    frame, base, hydro = load_training_frame(PROJECT_ROOT)
    ensemble = list(dict.fromkeys(base + hydro))
    y = frame["EVENTOS"].astype(float)
    mask, fold, pred, heads = ensemble_oof(frame, base, ensemble, y)
    print(len(frame), len(base), len(ensemble))
    print({k: round(v, 4) for k, v in metrics(y[mask], pred[mask]).items() if k in ("mae", "rmse", "r2", "high_auc", "std_ratio")})
    out = pd.DataFrame({"FECHA_DIA": frame["FECHA_DIA"][mask], "EVENTOS": y[mask], "fold": fold[mask], "pred": pred[mask]})
    out.to_csv(PROJECT_ROOT / "05_research/results/hydro_v3/official_reproduction_oof.csv", sep=";", index=False)
