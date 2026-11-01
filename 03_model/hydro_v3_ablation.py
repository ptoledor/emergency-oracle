"""Pre-declared ablation of v3 feature families on the official walk-forward."""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "03_model"))

from hydro_v3_harness import ensemble_oof, head_params, load_training_frame
from search_signal_models import metrics
from v3_features import anomaly_features, calendar_features, level_features

OUT = PROJECT_ROOT / "05_research" / "results" / "hydro_v3"


def add_families(frame):
    daily = frame.set_index(pd.to_datetime(frame["FECHA_DIA"]))["EVENTOS"]
    full = daily.reindex(pd.date_range(daily.index.min(), daily.index.max(), freq="D"))
    level = level_features(full).reindex(daily.index).reset_index(drop=True)
    calendar = calendar_features(frame["FECHA_DIA"]).reset_index(drop=True)
    anomaly = anomaly_features(frame).reset_index(drop=True)
    families = {"calendar": list(calendar.columns), "level": list(level.columns), "anomaly": list(anomaly.columns)}
    return pd.concat([frame.reset_index(drop=True), calendar, level, anomaly], axis=1), families


def seeded(params, seed):
    return {name: {**p, "random_state": seed} for name, p in params.items()}


def run(frame, base, ensemble, y, extra, seeds=(42,), extra_in_active=False):
    preds = []
    for seed in seeds:
        b = base + extra if extra_in_active else base
        mask, fold, pred, _ = ensemble_oof(frame, b, ensemble + extra, y, params=seeded(head_params(), seed))
        preds.append(pred)
    return mask, fold, np.mean(preds, axis=0)


if __name__ == "__main__":
    frame, base, hydro = load_training_frame(PROJECT_ROOT)
    frame, families = add_families(frame)
    ensemble = list(dict.fromkeys(base + hydro))
    y = frame["EVENTOS"].astype(float)
    configs = {
        "official": ([], (42,), False),
        "calendar": (families["calendar"], (42,), False),
        "level": (families["level"], (42,), False),
        "anomaly": (families["anomaly"], (42,), False),
        "all3": (families["calendar"] + families["level"] + families["anomaly"], (42,), False),
        "all3_active": (families["calendar"] + families["level"] + families["anomaly"], (42,), True),
        "official_5seeds": ([], (42, 7, 11, 23, 99), False),
    }
    rows, oof = [], {}
    for name, (extra, seeds, in_active) in configs.items():
        mask, fold, pred = run(frame, base, ensemble, y, extra, seeds, in_active)
        oof[name] = pred[mask]
        m = metrics(y[mask], pred[mask])
        per_fold = [float(np.mean(np.abs(y[mask][fold[mask] == k] - pred[mask][fold[mask] == k]))) for k in range(1, 7)]
        rows.append({"config": name, **{k: m[k] for k in ("mae", "rmse", "r2", "high_auc", "std_ratio")},
                     **{f"mae_f{k+1}": v for k, v in enumerate(per_fold)}})
        print(rows[-1], flush=True)
    pd.DataFrame(rows).to_csv(OUT / "ablation.csv", sep=";", index=False)
    pd.DataFrame({"FECHA_DIA": frame["FECHA_DIA"][mask].values, "EVENTOS": y[mask].values, "fold": fold[mask], **oof}).to_csv(
        OUT / "ablation_oof.csv", sep=";", index=False)
