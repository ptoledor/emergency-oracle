"""Walk-forward 6x120 sobre el dataset extendido con Viper: arquitecturas y variantes."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from joblib import Parallel, delayed

sys.path.insert(0, str(Path(__file__).resolve().parent))
from viper_eval import OUT, PROJECT_ROOT, count_metrics, load, walk_forward  # noqa: E402

DATA = PROJECT_ROOT / "02_data"


def block_bootstrap(y, a, b, fold, block=30, reps=2000, seed=0):
    """Fraccion de remuestreos por bloques donde b mejora a a (MAE, MSE)."""
    rng = np.random.default_rng(seed)
    n = len(y)
    starts = np.arange(0, n - block + 1)
    da, db = np.abs(y - a), np.abs(y - b)
    sa, sb = (y - a) ** 2, (y - b) ** 2
    mae = mse = 0
    for _ in range(reps):
        idx = np.concatenate([np.arange(s, s + block) for s in rng.choice(starts, n // block)])
        mae += db[idx].mean() < da[idx].mean()
        mse += sb[idx].mean() < sa[idx].mean()
    return mae / reps, mse / reps


def main(target="mensajes", threshold=7.0):
    OUT.mkdir(parents=True, exist_ok=True)
    new, base, ensemble, cal = load(DATA / f"augmented_emergency_data_viper_{target}.csv")
    y = new["EVENTOS"].astype(float)
    ens_cal = ensemble + cal

    jobs = {
        "official": dict(base=base, ensemble=ensemble),
        "v3": dict(base=base, ensemble=ens_cal, glm_cols=ens_cal),
        "v3_glm40": dict(base=base, ensemble=ens_cal, glm_cols=ens_cal, glm_weight=0.40),
        "v3_seed7": dict(base=base, ensemble=ens_cal, glm_cols=ens_cal, seed=7),
        "v3_seed99": dict(base=base, ensemble=ens_cal, glm_cols=ens_cal, seed=99),
    }
    if target == "mensajes":
        old = pd.read_csv(DATA / "augmented_emergency_data_twitter.csv", sep=";")[["FECHA_DIA", "EVENTOS"]]
        tweet_y = new["FECHA_DIA"].map(old.set_index("FECHA_DIA")["EVENTOS"]).fillna(y)
        jobs["official_tweet_target"] = dict(base=base, ensemble=ensemble, train_y=tweet_y)
        jobs["v3_tweet_target"] = dict(base=base, ensemble=ens_cal, glm_cols=ens_cal, train_y=tweet_y)

    def run(kw):
        kw = dict(kw)
        return walk_forward(new, kw.pop("base"), kw.pop("ensemble"), y, **kw)

    results = dict(zip(jobs, Parallel(n_jobs=4)(delayed(run)(kw) for kw in jobs.values())))
    fold = results["official"][1]
    mask = fold > 0
    preds = {name: pred[mask] for name, (pred, _) in results.items()}
    preds["v3_3seeds"] = np.mean([preds["v3"], preds["v3_seed7"], preds["v3_seed99"]], axis=0)
    t, f = y[mask].to_numpy(), fold[mask]

    rows = []
    for name, p in preds.items():
        m = count_metrics(t, p, threshold)
        m["folds_mae_better"] = int(sum(
            np.mean(np.abs(t[f == k] - p[f == k])) < np.mean(np.abs(t[f == k] - preds["official"][f == k]))
            for k in np.unique(f)
        ))
        m["boot_mae"], m["boot_mse"] = block_bootstrap(t, preds["official"], p, f)
        normal = t <= 12
        m["mae_le12"] = np.mean(np.abs(t[normal] - p[normal]))
        rows.append({"config": name, **m})
    table = pd.DataFrame(rows).set_index("config").round(4)
    table.to_csv(OUT / f"wf_{target}.csv", sep=";")
    pd.DataFrame({"FECHA_DIA": new.loc[mask, "FECHA_DIA"].to_numpy(), "EVENTOS": t, "fold": f, **preds}).to_csv(
        OUT / f"wf_{target}_oof.csv", sep=";", index=False)
    print(new.loc[mask, "FECHA_DIA"].min(), new.loc[mask, "FECHA_DIA"].max())
    print(table.to_string())


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "mensajes"
    main(target, float(sys.argv[2]) if len(sys.argv) > 2 else 7.0)
