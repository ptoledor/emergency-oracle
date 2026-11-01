"""Experimentos de reentrenamiento con Viper Talcahuano (holdout 2026-04-17..2026-10-31)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from joblib import Parallel, delayed

sys.path.insert(0, str(Path(__file__).resolve().parent))
from viper_eval import (  # noqa: E402
    HOLDOUT_START, OUT, PROJECT_ROOT, baselines, count_metrics, fit_predict, frozen_predictions,
    load, run_holdout,
)

DATA = PROJECT_ROOT / "02_data"


def old_history_predict(old, new, base, ensemble, glm_cols=None):
    """Entrena con el dataset oficial anterior (solo tweets) y predice el holdout nuevo."""
    test_rows = new[new["FECHA_DIA"] >= HOLDOUT_START]
    combo = pd.concat([old, test_rows], ignore_index=True)
    train = np.r_[np.ones(len(old), bool), np.zeros(len(test_rows), bool)]
    return fit_predict(combo, train, ~train, base, ensemble, old["EVENTOS"].astype(float).to_numpy(),
                       glm_cols=glm_cols)


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    new, base, ensemble, cal = load(DATA / "augmented_emergency_data_viper_mensajes.csv")
    old, _, _, _ = load(DATA / "augmented_emergency_data.csv")
    y = new["EVENTOS"].astype(float)
    hold = (new["FECHA_DIA"] >= HOLDOUT_START).to_numpy()
    ens_cal = ensemble + cal
    clip25 = y.clip(upper=25)
    clip_q99 = y.clip(upper=float(y[~hold].quantile(0.99)))

    jobs = {
        "old_hist_official": lambda: old_history_predict(old, new, base, ensemble),
        "old_hist_v3": lambda: old_history_predict(old, new, base, ens_cal, glm_cols=ens_cal),
        "static_official": lambda: run_holdout(new, base, ensemble, y, "static"),
        "static_v3": lambda: run_holdout(new, base, ens_cal, y, "static", glm_cols=ens_cal),
        "monthly_official": lambda: run_holdout(new, base, ensemble, y, "monthly"),
        "monthly_v3": lambda: run_holdout(new, base, ens_cal, y, "monthly", glm_cols=ens_cal),
        "monthly_v3_clip25": lambda: run_holdout(new, base, ens_cal, y, "monthly", glm_cols=ens_cal,
                                                 train_y=clip25),
        "monthly_v3_clipq99": lambda: run_holdout(new, base, ens_cal, y, "monthly", glm_cols=ens_cal,
                                                  train_y=clip_q99),
        "monthly_official_clip25": lambda: run_holdout(new, base, ensemble, y, "monthly", train_y=clip25),
    }
    results = Parallel(n_jobs=4)(delayed(fn)() for fn in jobs.values())
    preds = dict(zip(jobs, (np.asarray(r)[hold] if len(r) == len(new) else np.asarray(r) for r in results)))
    preds["frozen_official"] = frozen_predictions(new, "signal_hydro_ensemble_v2")[hold]
    preds["frozen_v3"] = frozen_predictions(new, "signal_hydro_v3_glm_cal")[hold]
    preds.update(baselines(new, y, hold))

    target = y[hold].to_numpy()
    rows = [{"config": name, **count_metrics(target, p)} for name, p in preds.items()]
    table = pd.DataFrame(rows).set_index("config").round(4)
    table.to_csv(OUT / "holdout_mensajes.csv", sep=";")
    oof = pd.DataFrame({"FECHA_DIA": new.loc[hold, "FECHA_DIA"].to_numpy(), "EVENTOS": target, **preds})
    oof.to_csv(OUT / "holdout_mensajes_predictions.csv", sep=";", index=False)
    print(table.to_string())


if __name__ == "__main__":
    main()
