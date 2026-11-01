"""Usos de Viper Concepcion: entrenamiento conjunto para Talcahuano y modelo propio."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from sklearn.model_selection import TimeSeriesSplit

sys.path.insert(0, str(Path(__file__).resolve().parent))
from viper_eval import OUT, PROJECT_ROOT, baselines, count_metrics, fit_predict, load  # noqa: E402
from viper_wf import block_bootstrap  # noqa: E402

DATA = PROJECT_ROOT / "02_data"


def pooled_walk_forward(talca, conce, base, ensemble, glm_cols, conce_mode, n_splits=6, test_size=120):
    """Walk-forward sobre Talcahuano; cada fold agrega filas de Concepcion anteriores al test."""
    y_t = talca["EVENTOS"].astype(float).to_numpy()
    y_c = conce["EVENTOS"].astype(float).to_numpy()
    if conce_mode == "scaled":
        y_c = y_c * talca["EVENTOS"].mean() / conce["EVENTOS"].mean()
    combo = pd.concat([talca.assign(CIUDAD=0), conce.assign(CIUDAD=1)], ignore_index=True)
    y_all = np.r_[y_t, y_c]
    feats_base, feats_ens = base + ["CIUDAD"], ensemble + ["CIUDAD"]
    glm = glm_cols + ["CIUDAD"] if glm_cols else None
    n_t = len(talca)
    pred = np.full(n_t, np.nan)
    fold = np.zeros(n_t, dtype=int)
    for number, (train, test) in enumerate(TimeSeriesSplit(n_splits=n_splits, test_size=test_size).split(talca), 1):
        start = talca["FECHA_DIA"].iloc[test[0]]
        tr = np.zeros(len(combo), bool)
        tr[train] = True
        if conce_mode != "none":
            tr[n_t:] = (conce["FECHA_DIA"] < start).to_numpy()
        te = np.zeros(len(combo), bool)
        te[test] = True
        pred[test] = fit_predict(combo, tr, te, feats_base, feats_ens, y_all[tr], glm_cols=glm)
        fold[test] = number
    return pred, fold


def main(target="mensajes", threshold=7.0):
    talca, base, ensemble, cal = load(DATA / f"augmented_emergency_data_viper_{target}.csv")
    conce, _, _, _ = load(DATA / f"augmented_emergency_data_concepcion_{target}.csv")
    ens_cal = ensemble + cal
    modes = ["none", "flag", "scaled"]
    results = Parallel(n_jobs=3)(
        delayed(pooled_walk_forward)(talca, conce, base, ens_cal, ens_cal, mode) for mode in modes
    )
    fold = results[0][1]
    mask = fold > 0
    t, f = talca.loc[mask, "EVENTOS"].astype(float).to_numpy(), fold[mask]
    ref = results[0][0][mask]
    rows = []
    for mode, (pred, _) in zip(modes, results):
        p = pred[mask]
        m = count_metrics(t, p, threshold)
        m["boot_mae"], m["boot_mse"] = block_bootstrap(t, ref, p, f)
        m["folds_mae_better"] = int(sum(np.mean(np.abs(t[f == k] - p[f == k])) < np.mean(np.abs(t[f == k] - ref[f == k])) for k in np.unique(f)))
        m["mae_le12"] = np.mean(np.abs(t[t <= 12] - p[t <= 12]))
        rows.append({"config": f"talca_v3_conce_{mode}", **m})
    table = pd.DataFrame(rows).set_index("config").round(4)
    table.to_csv(OUT / f"pooled_concepcion_{target}.csv", sep=";")
    print(table.to_string())

    # Modelo propio de Concepcion: walk-forward 4x120 (solo ~780 dias de historia).
    y_c = conce["EVENTOS"].astype(float)
    pred = np.full(len(conce), np.nan)
    fold = np.zeros(len(conce), dtype=int)
    for number, (train, test) in enumerate(TimeSeriesSplit(n_splits=4, test_size=120).split(conce), 1):
        tr = np.zeros(len(conce), bool); tr[train] = True
        te = np.zeros(len(conce), bool); te[test] = True
        pred[te] = fit_predict(conce, tr, te, base, ens_cal, y_c[tr].to_numpy(), glm_cols=ens_cal)
        fold[te] = number
    mask = fold > 0
    first_test = conce.loc[mask, "FECHA_DIA"].min()
    rows = [{"config": "conce_v3", **count_metrics(y_c[mask], pred[mask], threshold)}]
    for name, p in baselines(conce, y_c, (conce["FECHA_DIA"] >= first_test).to_numpy()).items():
        rows.append({"config": f"conce_{name}", **count_metrics(y_c[mask], p, threshold)})
    table = pd.DataFrame(rows).set_index("config").round(4)
    table.to_csv(OUT / f"concepcion_own_model_{target}.csv", sep=";")
    print(first_test, conce["FECHA_DIA"].max())
    print(table.to_string())


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "mensajes", float(sys.argv[2]) if len(sys.argv) > 2 else 7.0)
