"""Seleccion de variables con Talcahuano + Concepcion y reentrenamiento de Talcahuano.

Idea: una senal real (clima, calendario) deberia ayudar en ambas ciudades; una que solo
"ayuda" en Talcahuano puede ser ruido aprendido. Seleccion anidada: en cada fold del
walk-forward de Talcahuano las variables se eligen solo con datos anteriores al test.

Importancia = aumento de la deviance Poisson al permutar la variable en una ventana de
validacion (dos ventanas de 120 dias x dos semillas), con un XGBoost Poisson D2.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from sklearn.metrics import average_precision_score, mean_poisson_deviance, roc_auc_score
from sklearn.model_selection import TimeSeriesSplit
from xgboost import XGBRegressor

sys.path.insert(0, str(Path(__file__).resolve().parent))
from search_signal_features_v2 import D2_PARAMS  # noqa: E402
from viper_eval import OUT, PROJECT_ROOT, count_metrics, fit_predict, load  # noqa: E402
from viper_wf import block_bootstrap  # noqa: E402

DATA = PROJECT_ROOT / "02_data"
SEEDS = tuple(int(x) for x in sys.argv[1].split(",")) if len(sys.argv) > 1 else (42, 7)
TAG = "" if len(sys.argv) <= 1 else "_" + sys.argv[1].replace(",", "_")
WINDOW = 120
MIN_CONCE_DAYS = 360
POISSON = {**D2_PARAMS, "objective": "count:poisson"}


def permutation_importance(frame, y, cols, cutoff, seed_offset=0):
    """Importancia por ventana x semilla, usando solo filas anteriores a ``cutoff``."""
    dates = frame["FECHA_DIA"].to_numpy()
    before = np.flatnonzero(dates < cutoff)
    rows = []
    for w in (1, 2):
        val = before[-w * WINDOW: len(before) - (w - 1) * WINDOW]
        train = before[: len(before) - w * WINDOW]
        X_tr, X_val, y_tr, y_val = frame.iloc[train][cols], frame.iloc[val][cols], y[train], y[val]
        for seed in SEEDS:
            model = XGBRegressor(**{**POISSON, "random_state": seed}).fit(X_tr, y_tr)
            reference = mean_poisson_deviance(y_val, np.clip(model.predict(X_val), 1e-6, None))
            rng = np.random.default_rng(seed + seed_offset + w)
            scores = {}
            for col in cols:
                deltas = []
                for _ in range(3):
                    shuffled = X_val.copy()
                    shuffled[col] = rng.permutation(shuffled[col].to_numpy())
                    deltas.append(mean_poisson_deviance(y_val, np.clip(model.predict(shuffled), 1e-6, None)) - reference)
                scores[col] = np.mean(deltas) / reference
            rows.append(scores)
    table = pd.DataFrame(rows)
    return pd.DataFrame({"mean": table.mean(), "frac_pos": (table > 0).mean()})


def pooled_frame(talca, conce):
    scale = talca["EVENTOS"].mean() / conce["EVENTOS"].mean()
    combo = pd.concat([talca.assign(CIUDAD=0), conce.assign(CIUDAD=1)], ignore_index=True)
    y = np.r_[talca["EVENTOS"].to_numpy(float), conce["EVENTOS"].to_numpy(float) * scale]
    order = np.argsort(combo["FECHA_DIA"].to_numpy(), kind="stable")
    return combo.iloc[order].reset_index(drop=True), y[order]


def select(talca, conce, cols, cutoff):
    """Conjuntos de variables elegidos con datos anteriores a ``cutoff``."""
    y_t, y_c = talca["EVENTOS"].to_numpy(float), conce["EVENTOS"].to_numpy(float)
    imp_t = permutation_importance(talca, y_t, cols, cutoff)
    imp_c = permutation_importance(conce, y_c, cols, cutoff, seed_offset=100)
    combo, y_p = pooled_frame(talca, conce)
    # Ventanas del pooled al doble de filas: dos ciudades comparten cada fecha.
    imp_p = permutation_importance(combo, y_p, cols + ["CIUDAD"], cutoff, seed_offset=200).drop("CIUDAD")
    stable_t = (imp_t["mean"] > 0) & (imp_t["frac_pos"] >= 0.75)
    stable_c = (imp_c["mean"] > 0) & (imp_c["frac_pos"] >= 0.5)
    stable_p = (imp_p["mean"] > 0) & (imp_p["frac_pos"] >= 0.75)
    sets = {
        "talca_estable": stable_t[stable_t].index.tolist(),
        "ambas_ciudades": stable_t[stable_t & stable_c].index.tolist(),
        "conjunto_pooled": stable_p[stable_p].index.tolist(),
    }
    importances = pd.concat({"talca": imp_t, "conce": imp_c, "pooled": imp_p}, axis=1)
    return sets, importances


def run_fold(talca, conce, base, ensemble, train, test):
    cutoff = talca["FECHA_DIA"].iloc[test[0]]
    sets, importances = select(talca, conce, ensemble, cutoff)
    sets["completo"] = ensemble
    y = talca["EVENTOS"].to_numpy(float)
    tr = np.zeros(len(talca), bool); tr[train] = True
    te = np.zeros(len(talca), bool); te[test] = True
    preds = {}
    for name, cols in sets.items():
        base_cols = [c for c in base if c in cols] or cols
        preds[name] = fit_predict(talca, tr, te, base_cols, cols, y[tr])
    return cutoff, test, preds, {k: len(v) for k, v in sets.items()}, importances, sets


def main():
    talca, base, ensemble, _ = load(DATA / "augmented_emergency_data_viper_mensajes.csv")
    conce, _, _, _ = load(DATA / "augmented_emergency_data_concepcion_mensajes.csv")
    conce_start = pd.Timestamp(conce["FECHA_DIA"].min())
    folds = [
        (train, test) for train, test in TimeSeriesSplit(n_splits=6, test_size=120).split(talca)
        if (pd.Timestamp(talca["FECHA_DIA"].iloc[test[0]]) - conce_start).days >= MIN_CONCE_DAYS
    ]
    results = Parallel(n_jobs=len(folds))(
        delayed(run_fold)(talca, conce, base, ensemble, train, test) for train, test in folds
    )
    idx = np.concatenate([r[1] for r in results])
    fold = np.concatenate([np.full(len(r[1]), k) for k, r in enumerate(results, 1)])
    t = talca["EVENTOS"].to_numpy(float)[idx]
    names = list(results[0][2])
    preds = {n: np.concatenate([r[2][n] for r in results]) for n in names}
    rows = []
    for name, p in preds.items():
        m = count_metrics(t, p)
        m["pr_auc_high"] = average_precision_score(t > 7, p)
        m["n_features_por_fold"] = "/".join(str(r[3].get(name, len(ensemble))) for r in results)
        m["boot_mae"], m["boot_mse"] = block_bootstrap(t, preds["completo"], p, fold)
        rows.append({"config": name, **m})
    table = pd.DataFrame(rows).set_index("config")
    print("Folds evaluados:", [r[0] for r in results], "dias:", len(t))
    print(table[["mae", "rmse", "r2", "poisson_dev", "auc_high", "pr_auc_high", "boot_mae", "boot_mse",
                 "n_features_por_fold"]].round(4).to_string())
    table.round(4).to_csv(OUT / f"joint_selection{TAG}.csv", sep=";")
    pd.DataFrame({"FECHA_DIA": talca["FECHA_DIA"].to_numpy()[idx], "EVENTOS": t, "fold": fold, **preds}).to_csv(
        OUT / f"joint_selection_oof{TAG}.csv", sep=";", index=False)

    # Seleccion final con todo el historial, para interpretar que senales comparten las ciudades.
    final_sets, final_imp = select(talca, conce, ensemble, "9999-12-31")
    final_imp.round(5).to_csv(OUT / f"joint_selection_importances{TAG}.csv", sep=";")
    for name, cols in final_sets.items():
        print(f"\n{name} ({len(cols)}):", ", ".join(cols))
    stable_every_fold = set.intersection(*[set(r[5]["ambas_ciudades"]) for r in results])
    print(f"\nambas_ciudades en todos los folds ({len(stable_every_fold)}):", ", ".join(sorted(stable_every_fold)))


if __name__ == "__main__":
    main()
