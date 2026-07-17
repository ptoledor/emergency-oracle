"""Train additive category composition models with an explicit habitual base."""

from __future__ import annotations

import json
import os
import pickle
import sys
from pathlib import Path

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import PoissonRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT / "03_model") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "03_model"))

from build_hydro_ensemble_candidate import load_training_frame


RANDOM_STATE = 42
GROUPS = {
    "incendios": ["N_INCENDIO_ESTR", "N_INCENDIO_FOREST"],
    "rescates": ["N_RESCATE_VEH", "N_RESCATE_PERS"],
    "climaticas": ["N_EMERGENCIAS_CLIMATICAS"],
    "otros": ["N_GASES", "N_OTROS"],
}
GROUP_LABELS = {
    "incendios": "Incendios",
    "rescates": "Rescates",
    "climaticas": "Climáticas",
    "otros": "Otros",
}
BASELINE_FEATURES = [
    "ES_FIN_SEMANA",
    "ES_FERIADO",
    "ES_FERIADO_IRRENUNCIABLE",
    "ES_PRE_FERIADO",
    "MES_SIN",
    "MES_COS",
    "DIA_SIN",
    "DIA_COS",
    "DANO_SIN",
    "DANO_COS",
    "DIA_LUNES",
    "DIA_MARTES",
    "DIA_MIERCOLES",
    "DIA_JUEVES",
    "DIA_VIERNES",
    "DIA_SABADO",
    "DIA_DOMINGO",
]
SIGNAL_FEATURES = [
    "TEMP_MAX",
    "TEMP_MIN",
    "HUM_MIN",
    "HUM_MEDIA",
    "VIENTO_MAX",
    "VIENTO_MEDIO",
    "LLUVIA",
    "EVENTOS_lag_1",
    "EVENTOS_lag_2",
    "EVENTOS_lag_3",
    "EVENTOS_lag_7",
    "EVENTOS_rolling_mean_3d",
    "EVENTOS_rolling_mean_7d",
    "EVENTOS_rolling_mean_14d",
    "EVENTOS_rolling_mean_30d",
    "EVENTOS_rolling_std_7d",
    "DIAS_DESDE_ULTIMA_LLUVIA",
    "VPD_MAX",
    "WX_GUST_MAX",
    "WX_GUST_MEAN",
    "WX_PRECIP_MAX_HOURLY",
    "WX_PRECIP_HOURS",
    "WX_CAPE_MAX",
    "WX_FIRE_WEATHER_INDEX",
    "WX2_SHOWERS_SUM",
    "WX2_WET_BULB_MEAN",
    *BASELINE_FEATURES,
]
CANDIDATES = ("baseline", "huber_signal", "poisson_signal")
MIN_RELATIVE_MAE_GAIN = 0.01
MIN_MEAN_RATIO = 0.50


def build_model(name: str):
    if name == "baseline":
        return make_pipeline(
            SimpleImputer(strategy="median"),
            StandardScaler(),
            PoissonRegressor(alpha=1.0, max_iter=2000),
        )
    if name == "huber_signal":
        return GradientBoostingRegressor(
            loss="huber",
            n_estimators=150,
            learning_rate=0.03,
            max_depth=2,
            min_samples_leaf=20,
            subsample=0.8,
            random_state=RANDOM_STATE,
        )
    if name == "poisson_signal":
        return HistGradientBoostingRegressor(
            loss="poisson",
            max_iter=150,
            learning_rate=0.03,
            max_leaf_nodes=7,
            min_samples_leaf=30,
            l2_regularization=5.0,
            random_state=RANDOM_STATE,
        )
    raise KeyError(name)


def model_features(name: str) -> list[str]:
    return BASELINE_FEATURES if name == "baseline" else SIGNAL_FEATURES


def temporal_oof(
    frame: pd.DataFrame, target: pd.Series, model_name: str
) -> tuple[np.ndarray, list[float]]:
    prediction = np.full(len(frame), np.nan)
    fold_maes: list[float] = []
    features = model_features(model_name)
    for train_idx, validation_idx in TimeSeriesSplit(n_splits=5).split(frame):
        model = build_model(model_name)
        model.fit(frame[features].iloc[train_idx], target.iloc[train_idx])
        fold_prediction = np.clip(
            model.predict(frame[features].iloc[validation_idx]), 0, None
        )
        prediction[validation_idx] = fold_prediction
        fold_maes.append(
            float(mean_absolute_error(target.iloc[validation_idx], fold_prediction))
        )
    return prediction, fold_maes


def main() -> None:
    models_dir = PROJECT_ROOT / "03_model" / "saved_models"
    results_dir = PROJECT_ROOT / "05_research" / "results" / "category_composition"
    models_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)

    frame, _, _ = load_training_frame(PROJECT_ROOT)
    frame = frame.sort_values("FECHA_DIA").reset_index(drop=True)
    missing = [column for column in SIGNAL_FEATURES if column not in frame]
    if missing:
        raise KeyError(f"Missing composition features: {missing}")
    split_idx = int(len(frame) * 0.8)
    development = frame.iloc[:split_idx]
    holdout = frame.iloc[split_idx:]

    artifact_groups: dict[str, dict] = {}
    leaderboard_rows: list[dict] = []
    holdout_rows: list[dict] = []
    oof_rows: list[pd.DataFrame] = []

    for group_name, source_columns in GROUPS.items():
        target = frame[source_columns].sum(axis=1).astype(float)
        development_target = target.iloc[:split_idx]
        holdout_target = target.iloc[split_idx:]
        candidate_predictions: dict[str, np.ndarray] = {}

        for candidate in CANDIDATES:
            prediction, fold_maes = temporal_oof(
                development, development_target, candidate
            )
            valid = np.isfinite(prediction)
            actual = development_target.iloc[np.flatnonzero(valid)].to_numpy()
            candidate_predictions[candidate] = prediction
            mean_ratio = float(
                np.mean(prediction[valid]) / max(float(np.mean(actual)), 1e-9)
            )
            leaderboard_rows.append({
                "group": group_name,
                "candidate": candidate,
                "oof_mae": float(mean_absolute_error(actual, prediction[valid])),
                "oof_rmse": float(mean_squared_error(actual, prediction[valid]) ** 0.5),
                "oof_prediction_mean": float(np.mean(prediction[valid])),
                "oof_actual_mean": float(np.mean(actual)),
                "oof_mean_ratio": mean_ratio,
                "fold_mae_std": float(np.std(fold_maes)),
            })

        group_leaderboard = [
            row for row in leaderboard_rows if row["group"] == group_name
        ]
        baseline_row = next(
            row for row in group_leaderboard if row["candidate"] == "baseline"
        )
        eligible = [
            row
            for row in group_leaderboard
            if row["oof_mean_ratio"] >= MIN_MEAN_RATIO
            and row["oof_mae"]
            <= baseline_row["oof_mae"] * (1.0 - MIN_RELATIVE_MAE_GAIN)
        ]
        selected_row = min(
            eligible or [baseline_row], key=lambda row: (row["oof_mae"], row["fold_mae_std"])
        )
        selected_name = selected_row["candidate"]

        baseline_model = build_model("baseline")
        baseline_model.fit(
            development[BASELINE_FEATURES], development_target
        )
        expected_model = build_model(selected_name)
        expected_features = model_features(selected_name)
        expected_model.fit(development[expected_features], development_target)
        baseline_holdout = np.clip(
            baseline_model.predict(holdout[BASELINE_FEATURES]), 0, None
        )
        expected_holdout = np.clip(
            expected_model.predict(holdout[expected_features]), 0, None
        )
        holdout_rows.append({
            "group": group_name,
            "selected_model": selected_name,
            "baseline_mae": float(mean_absolute_error(holdout_target, baseline_holdout)),
            "selected_mae": float(mean_absolute_error(holdout_target, expected_holdout)),
            "baseline_rmse": float(mean_squared_error(holdout_target, baseline_holdout) ** 0.5),
            "selected_rmse": float(mean_squared_error(holdout_target, expected_holdout) ** 0.5),
            "actual_mean": float(holdout_target.mean()),
            "baseline_mean": float(np.mean(baseline_holdout)),
            "selected_mean": float(np.mean(expected_holdout)),
        })

        # Final serving models see all history after the temporal decision.
        baseline_model = build_model("baseline")
        baseline_model.fit(frame[BASELINE_FEATURES], target)
        expected_model = build_model(selected_name)
        expected_model.fit(frame[expected_features], target)
        artifact_groups[group_name] = {
            "label": GROUP_LABELS[group_name],
            "source_cols": source_columns,
            "baseline_model": baseline_model,
            "baseline_feature_cols": BASELINE_FEATURES,
            "expected_model": expected_model,
            "expected_feature_cols": expected_features,
            "selected_model": selected_name,
            "selection_oof_mae": selected_row["oof_mae"],
            "baseline_oof_mae": baseline_row["oof_mae"],
        }

        valid = np.isfinite(candidate_predictions[selected_name])
        oof_rows.append(pd.DataFrame({
            "FECHA_DIA": development.loc[valid, "FECHA_DIA"].astype(str),
            "group": group_name,
            "actual": development_target.iloc[np.flatnonzero(valid)].to_numpy(),
            "baseline_prediction": candidate_predictions["baseline"][valid],
            "selected_prediction": candidate_predictions[selected_name][valid],
            "selected_model": selected_name,
        }))
        print(
            f"{group_name}: selected={selected_name} "
            f"oof_mae={selected_row['oof_mae']:.4f} "
            f"baseline={baseline_row['oof_mae']:.4f}",
            flush=True,
        )

    artifact = {
        "groups": artifact_groups,
        "group_order": list(GROUPS),
        "baseline_definition": "regularized Poisson calendar/season expectation",
        "reconciliation": "expected category counts scaled to official total forecast",
        "selection_protocol": (
            "five-fold temporal OOF on first 80%; >=1% MAE gain and "
            ">=50% actual-mean viability gate"
        ),
        "holdout_protocol": "untouched final 20% reported after selection",
        "train_start_date": str(frame["FECHA_DIA"].iloc[0]),
        "train_end_date": str(frame["FECHA_DIA"].iloc[-1]),
        "train_samples": int(len(frame)),
    }
    with (models_dir / "category_composition_models.pkl").open("wb") as stream:
        pickle.dump(artifact, stream)

    leaderboard = pd.DataFrame(leaderboard_rows)
    holdout_metrics = pd.DataFrame(holdout_rows)
    oof_predictions = pd.concat(oof_rows, ignore_index=True)
    leaderboard.to_csv(results_dir / "leaderboard.csv", sep=";", index=False)
    holdout_metrics.to_csv(results_dir / "holdout_metrics.csv", sep=";", index=False)
    oof_predictions.to_csv(results_dir / "oof_predictions.csv", sep=";", index=False)
    (results_dir / "protocol.json").write_text(
        json.dumps({key: value for key, value in artifact.items() if key != "groups"}, indent=2),
        encoding="utf-8",
    )
    print("\n" + holdout_metrics.to_string(index=False, float_format="%.4f"))


if __name__ == "__main__":
    main()
