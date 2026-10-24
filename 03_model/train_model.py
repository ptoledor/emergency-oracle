"""Select and train the daily count model with an honest walk-forward protocol.

Candidates are declared up front, from simplest to most complex, and nothing is
tuned on the evaluated folds. Each candidate is replayed as the dashboard uses
it (six-day recursive forecast) on the same 6x120-day walk-forward folds, with
dataset weather and with weather issued at the real lead (Open-Meteo Previous
Runs, see horizon_evaluation.py).

Selection, on real-lead weather: the reference is the candidate with the lowest
mean MAE over days 1-6. The simplest candidate is chosen among those that
(1) are not significantly worse than the reference (95% block-bootstrap CI of
the MAE difference includes zero), (2) lose at most 0.01 of mean AUC for days
with more than 7 calls and (3) beat the 30-day mean on days 1-5 (P >= 0.95).

The chosen model is refit on every day and saved as JSON in
03_model/saved_models/count_model_v3/. --promote makes it the active model;
use it only after the metrics were reviewed and approved.

Usage: python 03_model/train_model.py [--promote]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression, PoissonRegressor
from sklearn.metrics import brier_score_loss
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler
from xgboost import XGBRegressor

PROJECT_ROOT = Path(__file__).resolve().parent.parent
for path in (PROJECT_ROOT, PROJECT_ROOT / "03_model"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from features import CATEGORY_SOURCES, EVENT_HISTORY_COLUMNS, WEEKDAY_COLUMNS, calendar_features
from horizon_evaluation import (
    HIGH_ACTIVITY_THRESHOLD, HORIZONS, WalkForward, block_bootstrap_improvement,
    horizon_metrics, lead0_parity, safe_auc,
)
from model_components import CountModel


DATA_PATH = PROJECT_ROOT / "02_data" / "augmented_emergency_data.csv"
MODELS_DIR = PROJECT_ROOT / "03_model" / "saved_models"
MODEL_NAME = "count_model_v3"
RESULTS_DIR = PROJECT_ROOT / "05_research" / "results" / "model_selection_v3"
N_SPLITS, TEST_SIZE = 6, 120
SELECTION_SCENARIO = "previous_runs_lead"
TARGET_COLUMNS = {"FECHA_DIA", "EVENTOS", "N_OTROS", *CATEGORY_SOURCES.values()}

XGB_D3 = {
    "objective": "reg:squarederror", "n_estimators": 300, "max_depth": 3,
    "learning_rate": 0.03, "min_child_weight": 2, "reg_lambda": 1.0,
    "subsample": 0.85, "colsample_bytree": 0.85, "n_jobs": 1, "random_state": 42,
}
XGB_D2 = {**XGB_D3, "n_estimators": 450, "max_depth": 2}
XGB_D2_POISSON = {**XGB_D2, "objective": "count:poisson"}
# Pre-declared climate and calendar signals for the GLM (no selection on folds).
GLM_FEATURES = [
    "MES_SIN", "MES_COS", "DANO_SIN", "DANO_COS",
    "DIA_LUNES", "DIA_MARTES", "DIA_MIERCOLES", "DIA_JUEVES", "DIA_VIERNES", "DIA_SABADO",
    "ES_FERIADO", "ES_PRE_FERIADO",
    "LLUVIA", "WX_PRECIP_HOURS", "WX_HEAVY_RAIN_HOURS", "WX_PRECIP_MAX_HOURLY",
    "LLUVIA_TOTAL_3D_PREV", "LLUVIA_TOTAL_7D_PREV", "DIAS_DESDE_ULTIMA_LLUVIA",
    "VIENTO_MAX", "WX_GUST_MAX", "STORM_COMPOUND_INDEX", "VIENTO_LLUVIA_INDEX",
    "TEMP_MAX", "TEMP_MIN", "HUM_MIN", "VPD_MAX", "WX_FIRE_WEATHER_INDEX",
    "WX_PRESSURE_MEAN", "WX_CAPE_MAX",
]
GLM_LOG1P = {
    "LLUVIA", "WX_HEAVY_RAIN_HOURS", "WX_PRECIP_MAX_HOURLY", "LLUVIA_TOTAL_3D_PREV",
    "LLUVIA_TOTAL_7D_PREV", "DIAS_DESDE_ULTIMA_LLUVIA", "STORM_COMPOUND_INDEX",
    "VIENTO_LLUVIA_INDEX", "WX_CAPE_MAX",
}
GLM_ALPHA = 0.01
LEGACY_ENSEMBLE = {"weights": {"d3": 0.30, "squared": 0.40, "poisson": 0.15, "quantile": 0.15},
                   "spread": {"center": 5.0, "scale": 1.15, "offset": 0.10}}
IDENTITY_SPREAD = {"center": 0.0, "scale": 1.0, "offset": 0.0}


def load_dataset(path=DATA_PATH):
    frame = pd.read_csv(path, sep=";").sort_values("FECHA_DIA").reset_index(drop=True)
    weekdays = calendar_features(frame["FECHA_DIA"])[WEEKDAY_COLUMNS].reset_index(drop=True)
    return pd.concat([frame, weekdays], axis=1)


def operational_features(frame):
    return [
        column for column in frame.columns
        if column not in TARGET_COLUMNS and pd.api.types.is_numeric_dtype(frame[column])
    ]


def without_history(columns):
    return [column for column in columns if column not in EVENT_HISTORY_COLUMNS]


class PoissonGLM:
    """log1p on rain-like signals, standardisation and a ridge Poisson GLM."""

    def __init__(self, feature_cols, alpha=GLM_ALPHA):
        self.feature_cols = list(feature_cols)
        self.alpha = alpha

    def _transform(self, X):
        values = X[self.feature_cols].to_numpy(dtype=float).copy()
        for position, column in enumerate(self.feature_cols):
            if column in GLM_LOG1P:
                values[:, position] = np.log1p(np.clip(values[:, position], 0, None))
        return values

    def fit(self, X, y):
        values = self._transform(X)
        self.scaler = StandardScaler().fit(values)
        self.model = PoissonRegressor(alpha=self.alpha, max_iter=3000).fit(
            self.scaler.transform(values), y
        )
        return self

    def predict(self, X):
        return self.model.predict(self.scaler.transform(self._transform(X)))

    def component(self, name, weight):
        scale = np.where(self.scaler.scale_ == 0, 1.0, self.scaler.scale_)
        return {
            "name": name, "kind": "poisson_glm", "weight": weight,
            "feature_cols": self.feature_cols,
            "log1p_cols": [column for column in self.feature_cols if column in GLM_LOG1P],
            "mean": self.scaler.mean_.tolist(), "scale": scale.tolist(),
            "coef": self.model.coef_.tolist(), "intercept": float(self.model.intercept_),
        }

    def importances(self):
        magnitude = np.abs(self.model.coef_)
        return dict(zip(self.feature_cols, magnitude / magnitude.sum()))


class Blend:
    """Weighted components plus the optional spread transform of the legacy ensemble."""

    def __init__(self, parts, spread):
        self.parts = parts  # list of (name, model, feature_cols, weight)
        self.spread = spread

    def predict(self, X):
        blended = sum(weight * np.asarray(model.predict(X[cols]), dtype=float)
                      for _, model, cols, weight in self.parts)
        spread = self.spread
        return np.clip(spread["center"] + spread["scale"] * (blended - spread["center"]) + spread["offset"], 0, None)


@dataclass(frozen=True)
class Candidate:
    name: str
    description: str
    rank: int  # lower is simpler
    uses_history: bool
    kind: str  # glm, xgb_poisson or legacy_ensemble

    def feature_cols(self, all_features):
        if self.kind == "glm":
            return list(GLM_FEATURES)
        return list(all_features) if self.uses_history else without_history(all_features)

    def fit(self, X, y):
        columns = list(X.columns)
        if self.kind == "glm":
            return Blend([("glm", PoissonGLM(columns).fit(X, y), columns, 1.0)], IDENTITY_SPREAD)
        if self.kind == "xgb_poisson":
            return Blend([("xgb_poisson", XGBRegressor(**XGB_D2_POISSON).fit(X, y), columns, 1.0)], IDENTITY_SPREAD)
        # Structure, weights and spread of signal_hydro_ensemble_v2 (the previous active model).
        d3_cols = [column for column in columns if not column.startswith("WX2_")]
        weights = LEGACY_ENSEMBLE["weights"]
        parts = [
            ("d3", XGBRegressor(**XGB_D3).fit(X[d3_cols], y), d3_cols, weights["d3"]),
            ("squared", XGBRegressor(**XGB_D2).fit(X, y), columns, weights["squared"]),
            ("poisson", XGBRegressor(**XGB_D2_POISSON).fit(X, y), columns, weights["poisson"]),
            ("quantile", XGBRegressor(**{**XGB_D2, "objective": "reg:quantileerror", "quantile_alpha": 0.5}).fit(X, y),
             columns, weights["quantile"]),
        ]
        return Blend(parts, LEGACY_ENSEMBLE["spread"])


CANDIDATES = [
    Candidate("glm_poisson_clima_calendario", "GLM Poisson con 30 variables de clima y calendario", 1, False, "glm"),
    Candidate("xgb_poisson_sin_historial", "XGBoost Poisson sin historial de llamados", 2, False, "xgb_poisson"),
    Candidate("xgb_poisson", "XGBoost Poisson con historial de llamados", 3, True, "xgb_poisson"),
    Candidate("ensemble_v2_sin_historial", "Ensemble de 4 XGBoost (estructura v2) sin historial", 4, False, "legacy_ensemble"),
    Candidate("ensemble_v2", "Ensemble de 4 XGBoost (estructura v2, modelo activo anterior)", 5, True, "legacy_ensemble"),
]


def nested_probabilities(group):
    """P(>7) per fold from a logistic fit on earlier folds only (fold 1 excluded)."""
    probabilities = pd.Series(np.nan, index=group.index)
    for fold in sorted(group["fold"].unique())[1:]:
        past = group[group["fold"] < fold]
        target = past["EVENTOS"] > HIGH_ACTIVITY_THRESHOLD
        if target.nunique() < 2:
            continue
        model = LogisticRegression().fit(past[["prediction"]], target)
        current = group["fold"] == fold
        probabilities[current] = model.predict_proba(group.loc[current, ["prediction"]])[:, 1]
    return probabilities


def evaluate_candidates(walk_forward, all_features):
    baselines = walk_forward.baselines()
    rows, predictions = [], []
    for candidate in CANDIDATES:
        print(f"Evaluando {candidate.name}", flush=True)
        columns = candidate.feature_cols(all_features)
        result = walk_forward.evaluate(candidate.fit, columns)
        result["candidate"] = candidate.name
        predictions.append(result)
    predictions = pd.concat(predictions, ignore_index=True)
    merged = predictions.merge(
        baselines.drop(columns="origin"), on=["FECHA", "horizon", "fold"], how="inner"
    ).dropna(subset=["prediction", "promedio_30d", "estacional_mes_dia"])
    # Compare every candidate on exactly the same (scenario, date, horizon) pairs.
    key = ["weather", "FECHA", "horizon"]
    complete = merged.groupby(key)["candidate"].transform("nunique") == len(CANDIDATES)
    merged = merged[complete].sort_values(["weather", "candidate", "horizon", "FECHA"])
    merged["probability"] = np.nan
    for _, group in merged.groupby(["weather", "candidate", "horizon"]):
        merged.loc[group.index, "probability"] = nested_probabilities(group)

    for (scenario, name, horizon), group in merged.groupby(["weather", "candidate", "horizon"]):
        scored = group.dropna(subset=["probability"])
        target = (scored["EVENTOS"] > HIGH_ACTIVITY_THRESHOLD).astype(int)
        improvement = block_bootstrap_improvement(
            group["EVENTOS"].to_numpy(), group["promedio_30d"].to_numpy(), group["prediction"].to_numpy()
        )
        rows.append({
            "weather": scenario, "candidate": name, "horizon": horizon,
            **horizon_metrics(group["EVENTOS"], group["prediction"]),
            "brier_nested": float(brier_score_loss(target, scored["probability"])),
            "auc_nested": safe_auc(scored["EVENTOS"], scored["probability"]),
            "mae_promedio_30d": float(np.mean(np.abs(group["EVENTOS"] - group["promedio_30d"]))),
            "mae_estacional": float(np.mean(np.abs(group["EVENTOS"] - group["estacional_mes_dia"]))),
            **{f"vs_30d_{key}": value for key, value in improvement.items()},
        })
    return pd.DataFrame(rows), merged


def select_candidate(leaderboard, merged, scenario):
    board = leaderboard[leaderboard["weather"] == scenario]
    summary = board.groupby("candidate").agg(mean_mae=("mae", "mean"), mean_auc=("high_auc", "mean"))
    reference = summary["mean_mae"].idxmin()
    data = merged[merged["weather"] == scenario].sort_values(["FECHA", "horizon"])
    reference_pred = data[data["candidate"] == reference].set_index(["FECHA", "horizon"])
    decisions = {}
    for candidate in CANDIDATES:
        pred = data[data["candidate"] == candidate.name].set_index(["FECHA", "horizon"])
        aligned = reference_pred.join(pred[["prediction"]], rsuffix="_candidate")
        vs_reference = block_bootstrap_improvement(
            aligned["EVENTOS"].to_numpy(), aligned["prediction"].to_numpy(),
            aligned["prediction_candidate"].to_numpy(), block_size=30 * len(HORIZONS),
        )
        per_horizon = board[board["candidate"] == candidate.name].set_index("horizon")
        checks = {
            "not_worse_than_reference": bool(vs_reference["mae_improvement_ci_high"] >= 0),
            "auc_within_0_01": bool(
                summary.loc[candidate.name, "mean_auc"] >= summary.loc[reference, "mean_auc"] - 0.01
            ),
            "beats_30d_days_1_5": bool(
                (per_horizon.loc[1:5, "vs_30d_mae_probability_improves"] >= 0.95).all()
            ),
        }
        decisions[candidate.name] = {
            "rank": candidate.rank, "mean_mae": float(summary.loc[candidate.name, "mean_mae"]),
            "mean_auc": float(summary.loc[candidate.name, "mean_auc"]),
            "vs_reference": vs_reference, "checks": checks, "eligible": all(checks.values()),
        }
    eligible = [c for c in CANDIDATES if decisions[c.name]["eligible"]]
    chosen = min(eligible, key=lambda c: c.rank).name if eligible else reference
    return chosen, reference, decisions


def horizon_calibration(leaderboard, merged, name, scenario):
    """Per-day interval, P(>7) logistic and publication flag from real-lead OOF."""
    horizons = {}
    data = merged[(merged["weather"] == scenario) & (merged["candidate"] == name)]
    board = leaderboard[(leaderboard["weather"] == scenario) & (leaderboard["candidate"] == name)]
    for horizon, group in data.groupby("horizon"):
        residuals = group["EVENTOS"] - group["prediction"]
        lower, upper = np.quantile(residuals, [0.10, 0.90])
        target = group["EVENTOS"] > HIGH_ACTIVITY_THRESHOLD
        logistic = LogisticRegression().fit(group[["prediction"]], target)
        row = board[board["horizon"] == horizon].iloc[0]
        horizons[str(int(horizon))] = {
            "interval_80": [float(lower), float(upper)],
            "interval_coverage": float(np.mean((residuals >= lower) & (residuals <= upper))),
            "prob_coef": float(logistic.coef_[0, 0]),
            "prob_intercept": float(logistic.intercept_[0]),
            "publish": bool(row["vs_30d_mae_probability_improves"] >= 0.95),
            "mae": float(row["mae"]), "mae_promedio_30d": float(row["mae_promedio_30d"]),
            "high_auc": float(row["high_auc"]), "auc_nested": float(row["auc_nested"]),
            "brier_nested": float(row["brier_nested"]),
            "p_beats_30d": float(row["vs_30d_mae_probability_improves"]),
        }
    return horizons


def alert_threshold(merged, name, scenario):
    """Probability threshold that maximises F1 on day-1 nested probabilities."""
    data = merged[(merged["weather"] == scenario) & (merged["candidate"] == name)
                  & (merged["horizon"] == 1)].dropna(subset=["probability"])
    target = data["EVENTOS"] > HIGH_ACTIVITY_THRESHOLD
    best = (0.0, 0.25)
    for threshold in np.arange(0.05, 0.61, 0.01):
        alert = data["probability"] >= threshold
        precision = (alert & target).sum() / max(alert.sum(), 1)
        recall = (alert & target).sum() / max(target.sum(), 1)
        f1 = 2 * precision * recall / max(precision + recall, 1e-9)
        best = max(best, (f1, round(float(threshold), 2)))
    return best[1]


def save_model(candidate, fitted, frame, metadata_extra, model_dir):
    model_dir.mkdir(parents=True, exist_ok=True)
    for old in model_dir.glob("*.json"):
        old.unlink()
    components, importances = [], {}
    for name, model, cols, weight in fitted.parts:
        if isinstance(model, PoissonGLM):
            components.append(model.component(name, weight))
            part_importance = model.importances()
        else:
            file_name = f"{name}.json"
            model.get_booster().save_model(model_dir / file_name)
            components.append({
                "name": name, "kind": "xgboost", "weight": weight,
                "feature_cols": cols, "file": file_name,
            })
            part_importance = dict(zip(cols, model.feature_importances_.astype(float)))
        for column, value in part_importance.items():
            importances[column] = importances.get(column, 0.0) + weight * float(value)
    feature_cols = list(dict.fromkeys(col for _, _, cols, _ in fitted.parts for col in cols))
    metadata = {
        "model_name": MODEL_NAME, "candidate": candidate.name,
        "description": candidate.description, "regressor_type": f"CountModel ({candidate.kind})",
        "validation_protocol": f"Walk-forward {N_SPLITS}x{TEST_SIZE} dias · forecast recursivo de 6 dias",
        "feature_cols": feature_cols, "feature_count": len(feature_cols),
        "uses_event_history": candidate.uses_history,
        "components": components, "spread": fitted.spread,
        "feature_importances": importances,
        "train_start_date": str(frame["FECHA_DIA"].iloc[0]),
        "train_end_date": str(frame["FECHA_DIA"].iloc[-1]),
        "train_samples": int(len(frame)),
        "umbral_alta_actividad": HIGH_ACTIVITY_THRESHOLD,
        **metadata_extra,
    }
    (model_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    reloaded = CountModel.load(model_dir).predict(frame)
    gap = float(np.max(np.abs(reloaded - fitted.predict(frame))))
    if gap > 1e-3:
        raise RuntimeError(f"El modelo guardado no reproduce las predicciones (diferencia {gap})")
    return metadata


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--promote", action="store_true",
                        help="Activa el modelo guardado (solo con aprobacion explicita).")
    args = parser.parse_args()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    frame = load_dataset()
    all_features = operational_features(frame)
    splits = list(TimeSeriesSplit(n_splits=N_SPLITS, test_size=TEST_SIZE).split(frame))
    walk_forward = WalkForward(frame, all_features, splits)
    scenario = SELECTION_SCENARIO if SELECTION_SCENARIO in walk_forward.scenarios else "historical_forecast"
    if walk_forward.lead_weather is not None:
        test_dates = frame.loc[np.concatenate([test for _, test in splits]), "FECHA_DIA"]
        parity = lead0_parity(walk_forward.lead_weather, frame, all_features, pd.to_datetime(test_dates))
        parity.to_csv(RESULTS_DIR / "previous_runs_lead0_parity.csv", sep=";", index=False)

    leaderboard, merged = evaluate_candidates(walk_forward, all_features)
    chosen_name, reference, decisions = select_candidate(leaderboard, merged, scenario)
    chosen = next(c for c in CANDIDATES if c.name == chosen_name)
    print(f"Referencia: {reference} | Elegido: {chosen_name}", flush=True)

    columns = chosen.feature_cols(all_features)
    fitted = chosen.fit(frame[columns], frame["EVENTOS"].astype(float))
    horizons = horizon_calibration(leaderboard, merged, chosen_name, scenario)
    oof_day1 = merged[(merged["candidate"] == chosen_name) & (merged["horizon"] == 1)
                      & (merged["weather"] == "historical_forecast")]
    model_dir = MODELS_DIR / MODEL_NAME
    metadata = save_model(chosen, fitted, frame, {
        "selection_scenario": scenario,
        "selection_reference": reference,
        "horizons": horizons,
        "classification_threshold": alert_threshold(merged, chosen_name, scenario),
        "mae": horizons["1"]["mae"], "high_auc": horizons["1"]["high_auc"],
        "test_start_date": str(oof_day1["FECHA"].min().date()),
        "test_end_date": str(oof_day1["FECHA"].max().date()),
    }, model_dir)
    oof_day1[["FECHA", "EVENTOS", "prediction", "probability", "fold"]].rename(
        columns={"FECHA": "FECHA_DIA", "prediction": "PRED_EVENTOS_OOF", "probability": "PROB_ALTA_OOF"}
    ).to_csv(model_dir / "oof_predictions.csv", sep=";", index=False)

    leaderboard.to_csv(RESULTS_DIR / "leaderboard.csv", sep=";", index=False)
    merged.to_csv(RESULTS_DIR / "predictions.csv", sep=";", index=False)
    (RESULTS_DIR / "selection.json").write_text(json.dumps({
        "selection_scenario": scenario, "reference": reference, "chosen": chosen_name,
        "rule": "mas simple entre los no peores que la referencia, AUC -0.01 y mejor que promedio 30d en dias 1-5",
        "candidates": decisions, "horizons": horizons,
    }, ensure_ascii=False, indent=1, default=float), encoding="utf-8")

    pivot = leaderboard[leaderboard["weather"] == scenario].pivot(index="candidate", columns="horizon", values="mae")
    pivot["30d"] = leaderboard[leaderboard["weather"] == scenario].groupby("candidate")["mae_promedio_30d"].mean()
    print(f"\nMAE por dia ({scenario})\n{pivot.to_string(float_format='%.3f')}")
    print(json.dumps({name: {k: v for k, v in d.items() if k in ("mean_mae", "mean_auc", "eligible", "checks")}
                      for name, d in decisions.items()}, indent=1))

    if args.promote:
        (MODELS_DIR / "active_models.json").write_text(
            json.dumps({"count_model": MODEL_NAME}, indent=2), encoding="utf-8"
        )
        print(f"Modelo activo: {MODEL_NAME}")
    print(f"Modelo guardado en {model_dir} (features: {metadata['feature_count']})")


if __name__ == "__main__":
    main()
