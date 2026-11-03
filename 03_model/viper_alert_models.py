"""Modelo "alarmista" vs modelo de conteo: ¿un clasificador dedicado detecta mejor los dias altos?

Mismo walk-forward 6x120 y mismas features que el modelo oficial Viper. Todas las
variantes se evaluan contra el mismo objetivo (dia alto = >7 llamados) y se comparan
con usar directamente el conteo predicho por el modelo oficial como puntaje.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, precision_recall_curve, roc_auc_score
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier, XGBRegressor

sys.path.insert(0, str(Path(__file__).resolve().parent))
from search_signal_features_v2 import D2_PARAMS  # noqa: E402
from search_signal_models import N_SPLITS, TEST_SIZE  # noqa: E402
from viper_eval import OUT, PROJECT_ROOT, load  # noqa: E402

HIGH = 7.0


def score_fold(kind, frame, train, test, cols, y):
    X_tr, X_te, y_tr = frame.loc[train, cols], frame.loc[test, cols], y[train]
    if kind.startswith("clf"):
        cut = 5.0 if kind.endswith("gt5") else HIGH
        target = (y_tr > cut).astype(int)
        weight = (1 - target.mean()) / target.mean() if "wt" in kind else 1.0
        params = {k: v for k, v in D2_PARAMS.items() if k != "objective"}
        model = XGBClassifier(**params, objective="binary:logistic", scale_pos_weight=weight)
        return model.fit(X_tr, target).predict_proba(X_te)[:, 1]
    if kind.startswith("q"):
        params = {**D2_PARAMS, "objective": "reg:quantileerror", "quantile_alpha": int(kind[1:]) / 100}
        return XGBRegressor(**params).fit(X_tr, y_tr).predict(X_te)
    if kind == "logit":
        fill = X_tr.median()
        model = make_pipeline(StandardScaler(), LogisticRegression(C=0.05, max_iter=2000))
        return model.fit(X_tr.fillna(fill), (y_tr > HIGH).astype(int)).predict_proba(X_te.fillna(fill))[:, 1]
    raise ValueError(kind)


def walk(kind, frame, cols, y):
    out = np.full(len(frame), np.nan)
    for train, test in TimeSeriesSplit(n_splits=N_SPLITS, test_size=TEST_SIZE).split(frame):
        tr = np.zeros(len(frame), bool); tr[train] = True
        te = np.zeros(len(frame), bool); te[test] = True
        out[te] = score_fold(kind, frame, tr, te, cols, y)
    return out


def ranks(x):
    return pd.Series(x).rank(pct=True).to_numpy()


def alert_metrics(high, score, fold):
    precision, recall, _ = precision_recall_curve(high, score)
    best_recall = lambda p_min: float(recall[precision >= p_min].max()) if (precision >= p_min).any() else 0.0
    best_precision = lambda r_min: float(precision[recall >= r_min].max())
    per_fold = [roc_auc_score(high[fold == k], score[fold == k]) for k in np.unique(fold)]
    return {
        "roc_auc": roc_auc_score(high, score), "pr_auc": average_precision_score(high, score),
        "recall_at_p25": best_recall(0.25), "recall_at_p30": best_recall(0.30),
        "precision_at_r60": best_precision(0.60), "precision_at_r75": best_precision(0.75),
        "fold_auc_min": min(per_fold), "fold_auc_mean": float(np.mean(per_fold)),
    }


def main():
    frame, base, ensemble, cal = load(PROJECT_ROOT / "02_data" / "augmented_emergency_data_viper_mensajes.csv")
    y = frame["EVENTOS"].astype(float).to_numpy()
    oof = pd.read_csv(OUT / "wf_mensajes_oof.csv", sep=";")
    kinds = ["clf", "clf_wt", "clf_gt5", "clf_wt_gt5", "q75", "q90", "logit"]
    scores = dict(zip(kinds, Parallel(n_jobs=4)(delayed(walk)(k, frame, ensemble, y) for k in kinds)))
    mask = frame["FECHA_DIA"].isin(oof["FECHA_DIA"]).to_numpy()
    assert (frame.loc[mask, "FECHA_DIA"].to_numpy() == oof["FECHA_DIA"].to_numpy()).all()
    preds = {"conteo_oficial": oof["official"].to_numpy(), "conteo_tweets": oof["official_tweet_target"].to_numpy(),
             **{k: v[mask] for k, v in scores.items()}}
    preds["blend_conteo_clf_wt"] = ranks(preds["conteo_oficial"]) + ranks(preds["clf_wt"])
    preds["blend_conteo_q90"] = ranks(preds["conteo_oficial"]) + ranks(preds["q90"])
    high = oof["EVENTOS"].to_numpy() > HIGH
    fold = oof["fold"].to_numpy()
    table = pd.DataFrame({name: alert_metrics(high, s, fold) for name, s in preds.items()}).T.round(3)
    table.to_csv(OUT / "alert_models.csv", sep=";")
    pd.DataFrame({"FECHA_DIA": oof["FECHA_DIA"], "EVENTOS": oof["EVENTOS"], "fold": fold, **preds}).to_csv(
        OUT / "alert_models_oof.csv", sep=";", index=False)
    print(table.sort_values("pr_auc", ascending=False).to_string())


if __name__ == "__main__":
    main()
