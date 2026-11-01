"""Structural variants: recency weighting and a regularized Poisson GLM head."""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import PoissonRegressor
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBRegressor

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "03_model"))

from build_hydro_ensemble_candidate import CENTER, OFFSET, SPREAD_SCALE, WEIGHTS
from hydro_v3_ablation import OUT, add_families
from hydro_v3_harness import head_params, load_training_frame
from search_signal_models import N_SPLITS, TEST_SIZE, metrics


def oof_heads(frame, base, ensemble, y, half_life=None, glm_cols=None, glm_alpha=1.0):
    splits = list(TimeSeriesSplit(n_splits=N_SPLITS, test_size=TEST_SIZE).split(frame))
    dates = pd.to_datetime(frame["FECHA_DIA"])
    heads = {name: np.full(len(frame), np.nan) for name in [*head_params(), "glm"]}
    fold = np.zeros(len(frame), dtype=int)
    for number, (train, test) in enumerate(splits, start=1):
        fold[test] = number
        weight = None
        if half_life:
            age = (dates.iloc[train].max() - dates.iloc[train]).dt.days.to_numpy()
            weight = 0.5 ** (age / half_life)
        for name, p in head_params().items():
            columns = base if name == "active" else ensemble
            model = XGBRegressor(**p).fit(frame.iloc[train][columns], y.iloc[train], sample_weight=weight)
            heads[name][test] = model.predict(frame.iloc[test][columns])
        if glm_cols:
            X = frame[glm_cols].fillna(frame[glm_cols].iloc[train].median())
            glm = make_pipeline(StandardScaler(), PoissonRegressor(alpha=glm_alpha, max_iter=1000))
            glm.fit(X.iloc[train], y.iloc[train], poissonregressor__sample_weight=weight)
            heads["glm"][test] = glm.predict(X.iloc[test])
    return fold > 0, fold, heads


def finish(heads, glm_weight=0.0):
    raw = sum(WEIGHTS[name] * heads[name] for name in WEIGHTS)
    if glm_weight:
        raw = (1 - glm_weight) * raw + glm_weight * heads["glm"]
    return np.clip(CENTER + SPREAD_SCALE * (raw - CENTER) + OFFSET, 0, None)


if __name__ == "__main__":
    frame, base, hydro = load_training_frame(PROJECT_ROOT)
    frame, families = add_families(frame)
    ensemble = list(dict.fromkeys(base + hydro)) + families["calendar"]
    y = frame["EVENTOS"].astype(float)
    reference = pd.read_csv(OUT / "ablation_oof.csv", sep=";")["official"].to_numpy()
    rows, oof = [], {}

    def report(label, mask, fold, pred):
        p, t, f = pred[mask], y[mask].to_numpy(), fold[mask]
        m = metrics(t, p)
        wm = sum(np.mean(np.abs(t[f == k] - p[f == k])) < np.mean(np.abs(t[f == k] - reference[f == k])) for k in range(1, 7))
        wr = sum(np.mean((t[f == k] - p[f == k]) ** 2) < np.mean((t[f == k] - reference[f == k]) ** 2) for k in range(1, 7))
        rows.append({"config": label, **{k: round(m[k], 4) for k in ("mae", "rmse", "r2", "high_auc", "std_ratio")},
                     "folds_mae_better": int(wm), "folds_rmse_better": int(wr)})
        oof[label] = p
        print(rows[-1], flush=True)

    glm_cols = ensemble
    for half_life in (None, 730, 365):
        mask, fold, heads = oof_heads(frame, base, ensemble, y, half_life, glm_cols, glm_alpha=1.0)
        tag = f"hl{half_life or 'none'}"
        report(f"{tag}+cal", mask, fold, finish(heads))
        for gw in (0.2, 0.35):
            report(f"{tag}+cal+glm{int(gw*100)}", mask, fold, finish(heads, gw))
    pd.DataFrame(rows).to_csv(OUT / "structural.csv", sep=";", index=False)
    pd.DataFrame(oof).to_csv(OUT / "structural_oof.csv", sep=";", index=False)
