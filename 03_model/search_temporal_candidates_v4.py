"""Fourth temporal search: robust median baseline plus leak-safe residual correction.

The count base is the 42-day rolling median found in iteration 1. A small
residual model uses only target-day forecast weather/calendar variables and
history summaries known at the origin. Model and shrinkage are selected on an
inner temporal validation segment, then refit on all development origins and
confirmed on the same final 25% holdout used by prior iterations.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import brier_score_loss, mean_absolute_error
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import HuberRegressor, Ridge

from search_temporal_candidates import (
    CRITICAL_THRESHOLD,
    HOLDOUT_FRACTION,
    HORIZONS,
    MIN_TRAIN_DAYS,
    OUTPUT_DIR,
    PROJECT_ROOT,
    beta_rate,
    count_metrics,
    risk_metrics,
    seasonal_values,
    trailing,
)
from temporal_gate import evaluate_promotion_gate


INNER_VALIDATION_FRACTION = 0.20
SHRINK_SCALES = (0.0, 0.15, 0.25, 0.35, 0.50, 0.75, 1.0)
BASE_WINDOW = 42
RISK_WINDOW = 90

TARGET_FEATURES = (
    "DIA_SEMANA",
    "MES",
    "DIA_ANO",
    "ES_FIN_SEMANA",
    "ES_FERIADO",
    "ES_PRE_FERIADO",
    "TEMP_MAX",
    "TEMP_MIN",
    "TEMP_MEDIA",
    "HUM_MAX",
    "HUM_MIN",
    "HUM_MEDIA",
    "VIENTO_MAX",
    "VIENTO_MEDIO",
    "LLUVIA",
    "VPD",
    "VPD_MAX",
    "TEMP_HUM_INDEX",
    "VIENTO_LLUVIA_INDEX",
    "STORM_COMPOUND_INDEX",
    "FIRE_DRY_INDEX_7D",
)


def available_target_features(frame: pd.DataFrame) -> list[str]:
    return [column for column in TARGET_FEATURES if column in frame.columns]


def history_features(
    values: np.ndarray,
    dates: pd.DatetimeIndex,
    origin: int,
    target: int,
) -> dict[str, float]:
    result: dict[str, float] = {
        "history_last": float(values[origin]),
        "history_horizon": float(target - origin),
    }
    for window in (7, 14, 21, 28, 42, 56, 84):
        known = trailing(values, origin, window)
        result[f"history_mean_{window}"] = float(np.mean(known))
        result[f"history_median_{window}"] = float(np.median(known))
        result[f"history_std_{window}"] = float(np.std(known, ddof=0))
    for weeks in (8, 12, 26):
        known = seasonal_values(values, dates, origin, target, weeks)
        result[f"weekday_mean_{weeks}w"] = float(np.mean(known))
        result[f"weekday_median_{weeks}w"] = float(np.median(known))
    result["history_risk_90d"] = beta_rate(
        (trailing(values, origin, RISK_WINDOW) > CRITICAL_THRESHOLD).astype(float)
    )
    origin_date = dates[origin]
    target_date = dates[target]
    result["calendar_month_sin"] = float(
        np.sin(2.0 * np.pi * target_date.month / 12.0)
    )
    result["calendar_month_cos"] = float(
        np.cos(2.0 * np.pi * target_date.month / 12.0)
    )
    result["calendar_doy_sin"] = float(
        np.sin(2.0 * np.pi * target_date.dayofyear / 365.25)
    )
    result["calendar_doy_cos"] = float(
        np.cos(2.0 * np.pi * target_date.dayofyear / 365.25)
    )
    result["days_since_origin_start"] = float(origin)
    result["target_weekday"] = float(target_date.weekday())
    result["origin_weekday"] = float(origin_date.weekday())
    return result


def build_dataset(frame: pd.DataFrame) -> pd.DataFrame:
    values = pd.to_numeric(frame["EVENTOS"], errors="coerce").to_numpy(dtype=float)
    dates = pd.DatetimeIndex(pd.to_datetime(frame["FECHA_DIA"]))
    target_features = available_target_features(frame)
    rows: list[dict[str, float | int | str]] = []

    first_origin = MIN_TRAIN_DAYS - 1
    last_origin = len(frame) - max(HORIZONS) - 1
    for origin in range(first_origin, last_origin + 1):
        base = float(np.median(trailing(values, origin, BASE_WINDOW)))
        risk = beta_rate(
            (trailing(values, origin, RISK_WINDOW) > CRITICAL_THRESHOLD).astype(float)
        )
        for horizon in HORIZONS:
            target = origin + horizon
            actual = float(values[target])
            row: dict[str, float | int | str] = {
                "origin_index": origin,
                "target_index": target,
                "origin_date": str(dates[origin].date()),
                "target_date": str(dates[target].date()),
                "horizon": horizon,
                "EVENTOS": actual,
                "base_count": base,
                "base_risk": risk,
                "baseline_persistence": float(values[origin]),
                "baseline_rolling_28d": float(np.mean(trailing(values, origin, 28))),
            }
            row.update(history_features(values, dates, origin, target))
            for column in target_features:
                row[f"target_{column}"] = pd.to_numeric(
                    pd.Series([frame.iloc[target][column]]), errors="coerce"
                ).iloc[0]
            rows.append(row)
    return pd.DataFrame(rows)


def feature_columns(dataset: pd.DataFrame) -> list[str]:
    excluded = {
        "origin_index",
        "target_index",
        "origin_date",
        "target_date",
        "EVENTOS",
        "base_count",
        "base_risk",
        "baseline_persistence",
        "baseline_rolling_28d",
    }
    return [
        column
        for column in dataset.columns
        if column not in excluded and pd.api.types.is_numeric_dtype(dataset[column])
    ]


def model_factories() -> dict[str, object]:
    return {
        "hgb_abs_leaf15": HistGradientBoostingRegressor(
            loss="absolute_error",
            max_iter=180,
            learning_rate=0.035,
            max_leaf_nodes=15,
            min_samples_leaf=30,
            l2_regularization=2.0,
            random_state=42,
        ),
        "hgb_abs_leaf7": HistGradientBoostingRegressor(
            loss="absolute_error",
            max_iter=180,
            learning_rate=0.035,
            max_leaf_nodes=7,
            min_samples_leaf=40,
            l2_regularization=3.0,
            random_state=42,
        ),
        "hgb_quantile": HistGradientBoostingRegressor(
            loss="quantile",
            quantile=0.5,
            max_iter=180,
            learning_rate=0.035,
            max_leaf_nodes=7,
            min_samples_leaf=40,
            l2_regularization=3.0,
            random_state=42,
        ),
        "gbr_abs": make_pipeline(
            SimpleImputer(strategy="median"),
            GradientBoostingRegressor(
                loss="absolute_error",
                n_estimators=160,
                learning_rate=0.025,
                max_depth=2,
                min_samples_leaf=30,
                subsample=0.8,
                random_state=42,
            ),
        ),
        "huber": make_pipeline(
            SimpleImputer(strategy="median"),
            StandardScaler(),
            HuberRegressor(epsilon=1.35, alpha=1.0, max_iter=500),
        ),
        "ridge": make_pipeline(
            SimpleImputer(strategy="median"),
            StandardScaler(),
            Ridge(alpha=25.0),
        ),
    }


def select_model(
    development: pd.DataFrame,
    features: list[str],
) -> tuple[str, float, pd.DataFrame]:
    origins = np.sort(development["origin_index"].unique())
    validation_count = max(
        1, int(math.ceil(len(origins) * INNER_VALIDATION_FRACTION))
    )
    validation_start = int(origins[-validation_count])
    train = development.loc[development["origin_index"] < validation_start]
    validation = development.loc[development["origin_index"] >= validation_start]

    X_train = train[features]
    y_train = train["EVENTOS"].to_numpy(dtype=float) - train["base_count"].to_numpy(dtype=float)
    X_validation = validation[features]
    actual = validation["EVENTOS"].to_numpy(dtype=float)
    base = validation["base_count"].to_numpy(dtype=float)

    rows: list[dict[str, float | str]] = []
    best: tuple[float, str, float] | None = None
    for name, model in model_factories().items():
        model.fit(X_train, y_train)
        correction = np.asarray(model.predict(X_validation), dtype=float)
        for scale in SHRINK_SCALES:
            prediction = np.clip(base + scale * correction, 0.0, None)
            mae = float(mean_absolute_error(actual, prediction))
            rows.append({
                "model": name,
                "scale": float(scale),
                "inner_validation_mae": mae,
            })
            candidate = (mae, name, float(scale))
            if best is None or candidate < best:
                best = candidate

    assert best is not None
    leaderboard = pd.DataFrame(rows).sort_values(
        ["inner_validation_mae", "model", "scale"], ignore_index=True
    )
    return best[1], best[2], leaderboard


def make_horizon_metrics(
    holdout: pd.DataFrame,
    prediction: np.ndarray,
) -> pd.DataFrame:
    working = holdout.copy()
    working["candidate_count"] = prediction
    rows: list[dict[str, float | int]] = []
    for horizon, group in working.groupby("horizon", sort=True):
        actual = group["EVENTOS"].to_numpy(dtype=float)
        candidate = group["candidate_count"].to_numpy(dtype=float)
        persistence = group["baseline_persistence"].to_numpy(dtype=float)
        rolling = group["baseline_rolling_28d"].to_numpy(dtype=float)
        risk = group["base_risk"].to_numpy(dtype=float)
        target = (actual > CRITICAL_THRESHOLD).astype(int)
        rows.append({
            "horizon": int(horizon),
            "n_pairs": int(len(group)),
            "candidate_mae": float(mean_absolute_error(actual, candidate)),
            "persistence_mae": float(mean_absolute_error(actual, persistence)),
            "rolling_28d_mae": float(mean_absolute_error(actual, rolling)),
            "candidate_brier": float(brier_score_loss(target, risk)),
            "risk_90d_brier": float(brier_score_loss(target, risk)),
        })
    return pd.DataFrame(rows)


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    frame = pd.read_csv(
        PROJECT_ROOT / "02_data" / "augmented_emergency_data.csv", sep=";"
    )
    frame["EVENTOS"] = pd.to_numeric(frame["EVENTOS"], errors="coerce")
    frame = frame.loc[frame["EVENTOS"].notna()].sort_values("FECHA_DIA").reset_index(drop=True)

    dataset = build_dataset(frame)
    features = feature_columns(dataset)
    origins = np.sort(dataset["origin_index"].unique())
    holdout_count = max(1, int(math.ceil(len(origins) * HOLDOUT_FRACTION)))
    holdout_start = int(origins[-holdout_count])
    development = dataset.loc[dataset["origin_index"] < holdout_start].copy()
    holdout = dataset.loc[dataset["origin_index"] >= holdout_start].copy()

    selected_name, selected_scale, leaderboard = select_model(development, features)
    selected_model = model_factories()[selected_name]
    X_development = development[features]
    residual = (
        development["EVENTOS"].to_numpy(dtype=float)
        - development["base_count"].to_numpy(dtype=float)
    )
    selected_model.fit(X_development, residual)

    correction = np.asarray(selected_model.predict(holdout[features]), dtype=float)
    base = holdout["base_count"].to_numpy(dtype=float)
    candidate_count = np.clip(base + selected_scale * correction, 0.0, None)
    actual = holdout["EVENTOS"].to_numpy(dtype=float)
    probability = holdout["base_risk"].to_numpy(dtype=float)
    persistence = holdout["baseline_persistence"].to_numpy(dtype=float)
    rolling = holdout["baseline_rolling_28d"].to_numpy(dtype=float)

    candidate_count_metrics = count_metrics(actual, candidate_count)
    candidate_risk_metrics = risk_metrics(actual, probability)
    persistence_metrics = count_metrics(actual, persistence)
    rolling_metrics = count_metrics(actual, rolling)
    baseline_risk_metrics = risk_metrics(actual, probability)

    by_horizon = make_horizon_metrics(holdout, candidate_count)
    candidate_horizon = {
        int(row.horizon): float(row.candidate_mae)
        for row in by_horizon.itertuples(index=False)
    }
    baseline_horizon = {
        int(row.horizon): min(float(row.persistence_mae), float(row.rolling_28d_mae))
        for row in by_horizon.itertuples(index=False)
    }
    gate = evaluate_promotion_gate(
        candidate_mae=candidate_count_metrics["mae"],
        count_baseline_mae={
            "persistence": persistence_metrics["mae"],
            "rolling_28d": rolling_metrics["mae"],
        },
        candidate_brier=candidate_risk_metrics["brier"],
        probability_baseline_brier=baseline_risk_metrics["brier"],
        candidate_horizon_mae=candidate_horizon,
        baseline_horizon_mae=baseline_horizon,
    )

    predictions = holdout[[
        "origin_date",
        "target_date",
        "horizon",
        "EVENTOS",
        "base_count",
        "base_risk",
        "baseline_persistence",
        "baseline_rolling_28d",
    ]].copy()
    predictions["candidate_count"] = candidate_count
    predictions["residual_correction"] = correction

    summary = {
        "protocol": "inner temporal model selection + final 25% holdout",
        "base_count_candidate": "rolling_median_42d",
        "selected_residual_model": selected_name,
        "selected_correction_scale": selected_scale,
        "feature_count": len(features),
        "development_origin_count": int(development["origin_index"].nunique()),
        "holdout_origin_count": int(holdout["origin_index"].nunique()),
        "holdout_pair_count": int(len(holdout)),
        "holdout_start_date": str(holdout.iloc[0]["origin_date"]),
        "candidate_count": candidate_count_metrics,
        "candidate_risk": candidate_risk_metrics,
        "baselines": {
            "persistence": persistence_metrics,
            "rolling_28d": rolling_metrics,
            "risk_90d": baseline_risk_metrics,
        },
        "promotion_gate": gate.as_dict(),
        "operational_use": False,
    }

    (OUTPUT_DIR / "temporal_candidate_search_v4_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    leaderboard.to_csv(
        OUTPUT_DIR / "temporal_candidate_search_v4_leaderboard.csv", index=False
    )
    by_horizon.to_csv(
        OUTPUT_DIR / "temporal_candidate_search_v4_horizon_metrics.csv", index=False
    )
    predictions.to_csv(
        OUTPUT_DIR / "temporal_candidate_search_v4_holdout_predictions.csv", index=False
    )
    (OUTPUT_DIR / "temporal_candidate_search_v4_features.json").write_text(
        json.dumps(features, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
