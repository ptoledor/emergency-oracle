"""Final pre-declared check: GLM alpha plateau and bootstrap of the chosen candidate."""

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
    ensemble = list(dict.fromkeys(base + hydro)) + families["calendar"]
    y = frame["EVENTOS"].astype(float)
    reference = pd.read_csv(OUT / "ablation_oof.csv", sep=";")["official"].to_numpy()
    rows, keep = [], {}
    for alpha in (3.0, 10.0, 30.0):
        mask, fold, heads = oof_heads(frame, base, ensemble, y, None, ensemble, glm_alpha=alpha)
        t, f = y[mask].to_numpy(), fold[mask]
        glm_only = metrics(t, heads["glm"][mask])
        for gw in (0.25,):
            p = finish(heads, gw)[mask]
            m = metrics(t, p)
            boot = block_bootstrap_improvement(t, reference, p, block_size=30)
            fold_mae = [round(float(np.mean(np.abs(t[f == k] - p[f == k])) - np.mean(np.abs(t[f == k] - reference[f == k]))), 4) for k in range(1, 7)]
            fold_mse = [round(float(np.mean((t[f == k] - p[f == k]) ** 2) - np.mean((t[f == k] - reference[f == k]) ** 2)), 4) for k in range(1, 7)]
            row = {"glm_alpha": alpha, "glm_weight": gw, **{k: round(m[k], 4) for k in ("mae", "rmse", "r2", "high_auc", "std_ratio")},
                   "glm_only_mae": round(glm_only["mae"], 4), "glm_only_r2": round(glm_only["r2"], 4),
                   "fold_mae_delta": fold_mae, "fold_mse_delta": fold_mse,
                   **{k: round(v, 4) for k, v in boot.items()}}
            rows.append(row)
            keep[f"a{alpha:g}_w{gw:g}"] = p
            print(row, flush=True)
    pd.DataFrame(rows).to_csv(OUT / "final_check.csv", sep=";", index=False)
    pd.DataFrame({"FECHA_DIA": frame["FECHA_DIA"][mask].values, "EVENTOS": y[mask].values, "fold": fold[mask],
                  "official": reference, **keep}).to_csv(OUT / "final_check_oof.csv", sep=";", index=False)
