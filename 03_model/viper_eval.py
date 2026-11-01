"""Evaluacion de la incorporacion de Viper Talcahuano al entrenamiento.

Reutiliza ``load_training_frame`` apuntandolo a un root temporal con enlaces
simbolicos, de modo que el armado de features es exactamente el oficial.

Esquemas:
- frozen: modelos guardados (sin reentrenar) sobre el holdout nuevo.
- static: reentrena una vez con todo lo anterior al holdout.
- monthly: reentrena al inicio de cada mes del holdout con todo lo previo.
- wf: walk-forward oficial (TimeSeriesSplit 6x120) sobre el dataset extendido.
"""
from __future__ import annotations

import os
import pickle
import sys
import tempfile
from pathlib import Path

os.environ.setdefault("OMP_NUM_THREADS", "1")

import numpy as np
import pandas as pd
from sklearn.linear_model import PoissonRegressor
from sklearn.metrics import mean_poisson_deviance, roc_auc_score
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBRegressor

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "03_model"))

from build_hydro_ensemble_candidate import CENTER, OFFSET, SPREAD_SCALE, WEIGHTS, load_training_frame  # noqa: E402
from hydro_v3_ablation import add_families  # noqa: E402
from hydro_v3_harness import head_params  # noqa: E402
from search_signal_models import N_SPLITS, TEST_SIZE  # noqa: E402

HOLDOUT_START = "2026-04-17"
OUT = PROJECT_ROOT / "05_research" / "results" / "viper_talcahuano"
WEATHER = {
    # Cache oficial (el que lee el serving). Desde la promocion del 2026-11-01 contiene la
    # descarga fresca; extend_weather.py le agrega los dias nuevos en cada refresco.
    "official": {
        "wx": PROJECT_ROOT / "05_research/data/historical_forecast_features.csv",
        "wx2": PROJECT_ROOT / "05_research/data/historical_forecast_features_v2.csv",
    },
    # Historial completo re-descargado el 2026-11-01: misma version de Open-Meteo en
    # todas las filas. Congelado para reproducir los experimentos de esa fecha.
    "fresh": {
        "wx": PROJECT_ROOT / "05_research/data/historical_forecast_features_fresh.csv",
        "wx2": PROJECT_ROOT / "05_research/data/historical_forecast_features_v2_fresh.csv",
    },
}
DEFAULT_WEATHER = "official"


def make_root(augmented: Path, wx: Path, wx2: Path) -> Path:
    root = Path(tempfile.mkdtemp(prefix="viper_root_"))
    (root / "02_data").mkdir()
    (root / "05_research" / "data").mkdir(parents=True)
    (root / "03_model").mkdir()
    (root / "02_data" / "augmented_emergency_data.csv").symlink_to(augmented.resolve())
    (root / "05_research/data/historical_forecast_features.csv").symlink_to(wx.resolve())
    (root / "05_research/data/historical_forecast_features_v2.csv").symlink_to(wx2.resolve())
    (root / "03_model" / "saved_models").symlink_to(PROJECT_ROOT / "03_model" / "saved_models")
    return root


def load(augmented: Path, weather=DEFAULT_WEATHER):
    paths = WEATHER[weather]
    frame, base, hydro = load_training_frame(make_root(augmented, paths["wx"], paths["wx2"]))
    frame, families = add_families(frame)
    ensemble = list(dict.fromkeys(base + hydro))
    return frame.reset_index(drop=True), base, ensemble, families["calendar"]


def fit_predict(frame, train, test, base, ensemble, y_train, glm_cols=None, glm_alpha=10.0,
                glm_weight=0.25, seed=42):
    heads = {}
    for name, params in head_params().items():
        columns = base if name == "active" else ensemble
        model = XGBRegressor(**{**params, "random_state": seed}).fit(frame.loc[train, columns], y_train)
        heads[name] = model.predict(frame.loc[test, columns])
    raw = sum(WEIGHTS[name] * heads[name] for name in WEIGHTS)
    if glm_cols:
        fill = frame.loc[train, glm_cols].median()
        glm = make_pipeline(StandardScaler(), PoissonRegressor(alpha=glm_alpha, max_iter=1000))
        glm.fit(frame.loc[train, glm_cols].fillna(fill), y_train)
        raw = (1 - glm_weight) * raw + glm_weight * glm.predict(frame.loc[test, glm_cols].fillna(fill))
    return np.clip(CENTER + SPREAD_SCALE * (raw - CENTER) + OFFSET, 0, None)


def count_metrics(y, p, threshold=7.0):
    y, p = np.asarray(y, float), np.clip(np.asarray(p, float), 1e-6, None)
    high = y > threshold
    return {
        "n": len(y), "mae": np.mean(np.abs(y - p)), "rmse": np.sqrt(np.mean((y - p) ** 2)),
        "r2": 1 - np.sum((y - p) ** 2) / np.sum((y - y.mean()) ** 2),
        "poisson_dev": mean_poisson_deviance(y, p),
        "auc_high": roc_auc_score(high, p) if 0 < high.sum() < len(y) else np.nan,
        "bias": p.mean() - y.mean(), "y_mean": y.mean(),
        "std_ratio": np.std(p, ddof=1) / np.std(y, ddof=1),
    }


def baselines(frame, y, test_mask):
    """Media movil 28 dias (lags disponibles) y mediana mes x dia-semana del pasado."""
    series = pd.Series(y.values, index=pd.to_datetime(frame["FECHA_DIA"]))
    full = series.reindex(pd.date_range(series.index.min(), series.index.max()))
    roll28 = full.shift(1).rolling(28, min_periods=14).mean().reindex(series.index).to_numpy()
    train = ~test_mask
    key = frame["MES"].astype(str) + "_" + frame["DIA_SEMANA"].astype(str)
    med = y[train].groupby(key[train]).median()
    seasonal = key.map(med).fillna(y[train].median()).to_numpy()
    return {"baseline_roll28": roll28[test_mask], "baseline_mes_dow_median": seasonal[test_mask]}


def schedule(frame, scheme):
    dates = frame["FECHA_DIA"]
    test_all = (dates >= HOLDOUT_START).to_numpy()
    if scheme == "static":
        return [(~test_all, test_all)]
    months = sorted(dates[test_all].str[:7].unique())
    out = []
    for month in months:
        test = (dates.str[:7] == month).to_numpy() & test_all
        train = (dates < dates[test].min()).to_numpy()
        out.append((train, test))
    return out


def run_holdout(frame, base, ensemble, y, scheme, glm_cols=None, train_y=None, **kw):
    pred = np.full(len(frame), np.nan)
    train_y = y if train_y is None else train_y
    for train, test in schedule(frame, scheme):
        pred[test] = fit_predict(frame, train, test, base, ensemble, train_y[train], glm_cols=glm_cols, **kw)
    return pred


def walk_forward(frame, base, ensemble, y, glm_cols=None, train_y=None, n_splits=N_SPLITS, test_size=TEST_SIZE, **kw):
    pred = np.full(len(frame), np.nan)
    fold = np.zeros(len(frame), dtype=int)
    train_y = y if train_y is None else train_y
    for number, (train, test) in enumerate(
        TimeSeriesSplit(n_splits=n_splits, test_size=test_size).split(frame), start=1
    ):
        tr = np.zeros(len(frame), bool); tr[train] = True
        te = np.zeros(len(frame), bool); te[test] = True
        pred[te] = fit_predict(frame, tr, te, base, ensemble, train_y[tr], glm_cols=glm_cols, **kw)
        fold[te] = number
    return pred, fold


def frozen_predictions(frame, suffix):
    with (PROJECT_ROOT / "03_model/saved_models" / f"regressor_{suffix}.pkl").open("rb") as stream:
        model = pickle.load(stream)
    return np.asarray(model.predict(frame[model.feature_cols]), float)
