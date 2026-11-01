"""Pre-declared regularization grid for the hydro ensemble heads (+ enriched calendar)."""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "03_model"))

from hydro_v3_ablation import OUT, add_families
from hydro_v3_harness import ensemble_oof, head_params, load_training_frame
from search_signal_models import metrics

TWEAKS = {
    "base": {},
    "slow_lr": {"learning_rate_scale": 0.6, "n_estimators_scale": 1.7, "min_child_weight": 8},
    "colsample50": {"colsample_bytree": 0.5},
    "colsample30": {"colsample_bytree": 0.3},
    "robust": {"subsample": 0.7, "colsample_bytree": 0.5, "min_child_weight": 10, "reg_lambda": 5.0},
}


def tweak(params, spec):
    out = {}
    for name, p in params.items():
        q = dict(p)
        for key, value in spec.items():
            if key == "learning_rate_scale":
                q["learning_rate"] = p["learning_rate"] * value
            elif key == "n_estimators_scale":
                q["n_estimators"] = int(p["n_estimators"] * value)
            else:
                q[key] = value
        out[name] = q
    return out


if __name__ == "__main__":
    frame, base, hydro = load_training_frame(PROJECT_ROOT)
    frame, families = add_families(frame)
    ensemble = list(dict.fromkeys(base + hydro))
    y = frame["EVENTOS"].astype(float)
    reference = pd.read_csv(OUT / "ablation_oof.csv", sep=";")["official"].to_numpy()
    rows, oof = [], {}
    for calendar in (False, True):
        extra = families["calendar"] if calendar else []
        for name, spec in TWEAKS.items():
            if name == "base" and not calendar:
                continue
            label = f"{name}{'+cal' if calendar else ''}"
            mask, fold, pred, _ = ensemble_oof(frame, base, ensemble + extra, y, params=tweak(head_params(), spec))
            p, t, f = pred[mask], y[mask].to_numpy(), fold[mask]
            m = metrics(t, p)
            wins_mae = sum(np.mean(np.abs(t[f == k] - p[f == k])) < np.mean(np.abs(t[f == k] - reference[f == k])) for k in range(1, 7))
            wins_rmse = sum(np.mean((t[f == k] - p[f == k]) ** 2) < np.mean((t[f == k] - reference[f == k]) ** 2) for k in range(1, 7))
            rows.append({"config": label, **{k: round(m[k], 4) for k in ("mae", "rmse", "r2", "high_auc", "std_ratio")},
                         "folds_mae_better": wins_mae, "folds_rmse_better": wins_rmse})
            oof[label] = p
            print(rows[-1], flush=True)
    pd.DataFrame(rows).to_csv(OUT / "regularization.csv", sep=";", index=False)
    pd.DataFrame(oof).to_csv(OUT / "regularization_oof.csv", sep=";", index=False)
