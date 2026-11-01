"""Neighbourhood sensitivity and block bootstrap for the cal+glm candidate."""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "03_model"))

from build_signal_candidate import block_bootstrap_improvement
from hydro_v3_ablation import OUT, add_families
from hydro_v3_harness import load_training_frame
from hydro_v3_structural import finish, oof_heads
from search_signal_models import metrics

if __name__ == "__main__":
    frame, base, hydro = load_training_frame(PROJECT_ROOT)
    frame, families = add_families(frame)
    plain = list(dict.fromkeys(base + hydro))
    ensemble = plain + families["calendar"]
    y = frame["EVENTOS"].astype(float)
    reference = pd.read_csv(OUT / "ablation_oof.csv", sep=";")["official"].to_numpy()
    rows = []
    for alpha in (0.3, 1.0, 3.0):
        for cols_name, cols in (("cal", ensemble), ("nocal", plain)):
            heads_ens = ensemble if cols_name == "cal" else plain
            mask, fold, heads = oof_heads(frame, base, heads_ens, y, None, cols, glm_alpha=alpha)
            t, f = y[mask].to_numpy(), fold[mask]
            for gw in (0.1, 0.15, 0.2, 0.25, 0.3):
                p = finish(heads, gw)[mask]
                m = metrics(t, p)
                row = {"glm_alpha": alpha, "features": cols_name, "glm_weight": gw,
                       **{k: round(m[k], 4) for k in ("mae", "rmse", "r2", "high_auc")},
                       "folds_mae_better": int(sum(np.mean(np.abs(t[f == k] - p[f == k])) < np.mean(np.abs(t[f == k] - reference[f == k])) for k in range(1, 7))),
                       "folds_rmse_better": int(sum(np.mean((t[f == k] - p[f == k]) ** 2) < np.mean((t[f == k] - reference[f == k]) ** 2) for k in range(1, 7)))}
                if alpha == 1.0 and gw == 0.2:
                    boot = block_bootstrap_improvement(t, reference, p, block_size=30)
                    row.update({k: round(v, 4) for k, v in boot.items() if "probability" in k or "mean" in k})
                rows.append(row)
                print(row, flush=True)
    pd.DataFrame(rows).to_csv(OUT / "robustness.csv", sep=";", index=False)
