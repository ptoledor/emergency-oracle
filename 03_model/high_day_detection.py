"""Walk-forward comparison of direct high-activity classifiers vs count ranking."""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import rankdata
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "03_model"))

from hydro_v3_ablation import OUT, add_families
from hydro_v3_harness import load_training_frame
from search_signal_models import N_SPLITS, TEST_SIZE

THRESHOLD = 7.0


def detection(h, score, rates=(0.1, 0.2, 0.3)):
    out = {"auc": roc_auc_score(h, score), "avg_precision": average_precision_score(h, score)}
    for rate in rates:
        alert = score >= np.quantile(score, 1 - rate)
        out[f"recall@{int(rate*100)}"] = alert[h].mean()
        out[f"precision@{int(rate*100)}"] = h[alert].mean()
    return {k: round(float(v), 3) for k, v in out.items()}


if __name__ == "__main__":
    frame, base, hydro = load_training_frame(PROJECT_ROOT)
    frame, families = add_families(frame)
    features = list(dict.fromkeys(base + hydro)) + families["calendar"]
    y = frame["EVENTOS"].astype(float)
    high = (y > THRESHOLD).astype(int)
    splits = list(TimeSeriesSplit(n_splits=N_SPLITS, test_size=TEST_SIZE).split(frame))
    scores = {name: np.full(len(frame), np.nan) for name in ("xgb_clf", "xgb_clf_balanced", "logit_l2")}
    for train, test in splits:
        X_tr, X_te = frame.iloc[train][features], frame.iloc[test][features]
        h_tr = high.iloc[train]
        common = dict(n_estimators=300, max_depth=2, learning_rate=0.03, min_child_weight=5,
                      subsample=0.85, colsample_bytree=0.6, reg_lambda=3.0, n_jobs=1, random_state=42,
                      eval_metric="logloss")
        scores["xgb_clf"][test] = XGBClassifier(**common).fit(X_tr, h_tr).predict_proba(X_te)[:, 1]
        ratio = float((1 - h_tr.mean()) / h_tr.mean())
        scores["xgb_clf_balanced"][test] = XGBClassifier(**common, scale_pos_weight=ratio).fit(X_tr, h_tr).predict_proba(X_te)[:, 1]
        fill = X_tr.median()
        logit = make_pipeline(StandardScaler(), LogisticRegression(C=0.05, max_iter=2000))
        scores["logit_l2"][test] = logit.fit(X_tr.fillna(fill), h_tr).predict_proba(X_te.fillna(fill))[:, 1]
    oof = pd.read_csv(OUT / "candidate_oof_predictions.csv", sep=";")
    mask = ~np.isnan(scores["xgb_clf"])
    h = high[mask].to_numpy().astype(bool)
    table = {
        "oficial_conteo": oof["PRED_EVENTOS_OFFICIAL"].to_numpy(),
        "v3_conteo": oof["PRED_EVENTOS_V3_OOF"].to_numpy(),
        **{k: v[mask] for k, v in scores.items()},
    }
    table["rank_mix_v3_xgbclf_logit"] = (rankdata(table["v3_conteo"]) + rankdata(table["xgb_clf"]) + rankdata(table["logit_l2"])) / 3
    rows = [{"modelo": k, **detection(h, v)} for k, v in table.items()]
    result = pd.DataFrame(rows)
    print(result.to_string(index=False))
    result.to_csv(OUT / "high_day_detection.csv", sep=";", index=False)
    pd.DataFrame({"FECHA_DIA": oof["FECHA_DIA"], "EVENTOS": oof["EVENTOS"], "fold": oof["fold"], **table}).to_csv(
        OUT / "high_day_detection_oof.csv", sep=";", index=False)
