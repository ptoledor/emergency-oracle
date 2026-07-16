"""Search leakage-safe temporal forecasting candidates with a final holdout.

The search uses only information available at each forecast origin. Candidate
selection is performed on the earlier development origins and the promotion
decision is evaluated on the final 25% of origins, which remains untouched
while choosing the count and risk estimators.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import (
    brier_score_loss,
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    roc_auc_score,
)

from temporal_gate import evaluate_promotion_gate


PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "03_model" / "saved_models"
HORIZONS = tuple(range(1, 7))
MIN_TRAIN_DAYS = 365
HOLDOUT_FRACTION = 0.25
CRITICAL_THRESHOLD = 7.0

COUNT_WINDOWS = (7, 14, 21, 28, 42, 56, 84, 112)
MEDIAN_WINDOWS = (14, 28, 42, 56, 84)
EWMA_HALFLIVES = (7, 14, 21, 28, 42, 56)
SEASONAL_WEEKS = (2, 4, 8, 12, 16, 26)
RISK_WINDOWS = (30, 60, 90, 120, 180, 365)
RISK_SEASONAL_WEEKS = (8, 12, 16, 26, 52)
BLEND_WEIGHTS = (0.25, 0.50, 0.75)


def safe_auc(target: np.ndarray, probability: np.ndarray) -> float:
    if np.unique(target).size < 2:
        return float("nan")
    return float(roc_auc_score(target, probability))


def count_metrics(actual: np.ndarray, prediction: np.ndarray) -> dict[str, float]:
    mse = float(mean_squared_error(actual, prediction))
    return {
        "mae": float(mean_absolute_error(actual, prediction)),
        "rmse": float(math.sqrt(mse)),
        "r2": float(r2_score(actual, prediction)),
    }


def risk_metrics(actual: np.ndarray, probability: np.ndarray) -> dict[str, float]:
    target = (actual > CRITICAL_THRESHOLD).astype(int)
    clipped = np.clip(probability, 0.0, 1.0)
    return {
        "brier": float(brier_score_loss(target, clipped)),
        "roc_auc": safe_auc(target, clipped),
    }


def trailing(values: np.ndarray, origin: int, window: int) -> np.ndarray:
    return values[max(0, origin - window + 1): origin + 1]


def ewma(values: np.ndarray, halflife: int) -> float:
    if values.size == 0:
        return float("nan")
    alpha = 1.0 - math.exp(math.log(0.5) / float(halflife))
    ages = np.arange(values.size - 1, -1, -1, dtype=float)
    weights = np.power(1.0 - alpha, ages)
    return float(np.dot(values, weights) / weights.sum())


def seasonal_values(
    values: np.ndarray,
    dates: pd.DatetimeIndex,
    origin: int,
    target: int,
    weeks: int,
) -> np.ndarray:
    """Return known observations matching the target weekday, newest first."""

    target_weekday = int(dates[target].weekday())
    candidate_indices = np.flatnonzero(
        np.asarray(dates[: origin + 1].weekday) == target_weekday
    )
    if candidate_indices.size == 0:
        return np.asarray([], dtype=float)
    return values[candidate_indices[-weeks:]]


def beta_rate(binary_values: np.ndarray) -> float:
    """Jeffreys-smoothed empirical probability."""

    if binary_values.size == 0:
        return 0.0
    return float((binary_values.sum() + 0.5) / (binary_values.size + 1.0))


def build_prediction_frame(frame: pd.DataFrame) -> pd.DataFrame:
    values = pd.to_numeric(frame["EVENTOS"], errors="coerce").to_numpy(dtype=float)
    dates = pd.DatetimeIndex(pd.to_datetime(frame["FECHA_DIA"]))
    rows: list[dict[str, float | int | str]] = []

    first_origin = MIN_TRAIN_DAYS - 1
    last_origin = len(frame) - max(HORIZONS) - 1
    if last_origin < first_origin:
        raise ValueError("Not enough observations for H1-H6 temporal search")

    for origin in range(first_origin, last_origin + 1):
        history = values[: origin + 1]
        persistence = float(history[-1])
        rolling_means = {
            window: float(np.mean(trailing(values, origin, window)))
            for window in COUNT_WINDOWS
        }
        rolling_medians = {
            window: float(np.median(trailing(values, origin, window)))
            for window in MEDIAN_WINDOWS
        }
        ewma_values = {
            halflife: ewma(history, halflife)
            for halflife in EWMA_HALFLIVES
        }
        risk_rates = {
            window: beta_rate(
                (trailing(values, origin, window) > CRITICAL_THRESHOLD).astype(float)
            )
            for window in RISK_WINDOWS
        }

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
                "baseline_persistence": persistence,
                "baseline_rolling_28d": rolling_means[28],
                "baseline_risk_90d": risk_rates[90],
            }

            for window, prediction in rolling_means.items():
                row[f"count_mean_{window}d"] = prediction
            for window, prediction in rolling_medians.items():
                row[f"count_median_{window}d"] = prediction
            for halflife, prediction in ewma_values.items():
                row[f"count_ewma_hl{halflife}"] = prediction

            seasonal_means: dict[int, float] = {}
            for weeks in SEASONAL_WEEKS:
                known = seasonal_values(values, dates, origin, target, weeks)
                prediction = float(np.mean(known)) if known.size else rolling_means[28]
                seasonal_means[weeks] = prediction
                row[f"count_weekday_mean_{weeks}w"] = prediction

            # Horizon-specific seasonal naive observations are always known for H1-H6.
            for weeks_back in (1, 2, 3, 4):
                seasonal_index = target - 7 * weeks_back
                row[f"count_seasonal_lag_{weeks_back}w"] = (
                    float(values[seasonal_index])
                    if seasonal_index >= 0 and seasonal_index <= origin
                    else rolling_means[28]
                )
            seasonal_4w = float(
                np.mean([row[f"count_seasonal_lag_{week}w"] for week in (1, 2, 3, 4)])
            )
            row["count_seasonal_mean_4w"] = seasonal_4w

            for weeks in (4, 8, 12, 16, 26):
                seasonal = seasonal_means[weeks]
                for weight in BLEND_WEIGHTS:
                    row[
                        f"count_blend_mean28_weekday{weeks}w_w{int(weight * 100)}"
                    ] = float(weight * rolling_means[28] + (1.0 - weight) * seasonal)

            for weight in BLEND_WEIGHTS:
                row[f"count_blend_mean28_seasonal4w_w{int(weight * 100)}"] = float(
                    weight * rolling_means[28] + (1.0 - weight) * seasonal_4w
                )

            # A conservative weekday adjustment around the long-run local level.
            long_level = rolling_means[56]
            for weeks in (8, 12, 16, 26):
                weekday_effect = seasonal_means[weeks] - long_level
                row[f"count_adjusted_mean28_weekday{weeks}w"] = float(
                    max(0.0, rolling_means[28] + weekday_effect)
                )

            for window, probability in risk_rates.items():
                row[f"risk_rate_{window}d"] = probability

            seasonal_risks: dict[int, float] = {}
            for weeks in RISK_SEASONAL_WEEKS:
                known = seasonal_values(values, dates, origin, target, weeks)
                probability = beta_rate((known > CRITICAL_THRESHOLD).astype(float))
                seasonal_risks[weeks] = probability
                row[f"risk_weekday_{weeks}w"] = probability

            for weeks in (12, 26, 52):
                for weight in BLEND_WEIGHTS:
                    row[f"risk_blend_90d_weekday{weeks}w_w{int(weight * 100)}"] = float(
                        weight * risk_rates[90]
                        + (1.0 - weight) * seasonal_risks[weeks]
                    )

            rows.append(row)

    return pd.DataFrame(rows)


def select_candidate(
    predictions: pd.DataFrame,
    columns: list[str],
    metric: str,
) -> tuple[str, pd.DataFrame]:
    actual = predictions["EVENTOS"].to_numpy(dtype=float)
    records: list[dict[str, float | str]] = []
    for column in columns:
        values = predictions[column].to_numpy(dtype=float)
        if metric == "mae":
            score = float(mean_absolute_error(actual, values))
        elif metric == "brier":
            score = float(
                brier_score_loss(
                    (actual > CRITICAL_THRESHOLD).astype(int),
                    np.clip(values, 0.0, 1.0),
                )
            )
        else:
            raise ValueError(f"Unsupported selection metric: {metric}")
        records.append({"candidate": column, metric: score})
    leaderboard = pd.DataFrame(records).sort_values(metric, ignore_index=True)
    return str(leaderboard.iloc[0]["candidate"]), leaderboard


def horizon_metrics(
    holdout: pd.DataFrame,
    count_candidate: str,
    risk_candidate: str,
) -> pd.DataFrame:
    rows: list[dict[str, float | int]] = []
    for horizon, group in holdout.groupby("horizon", sort=True):
        actual = group["EVENTOS"].to_numpy(dtype=float)
        candidate_count = group[count_candidate].to_numpy(dtype=float)
        persistence = group["baseline_persistence"].to_numpy(dtype=float)
        rolling = group["baseline_rolling_28d"].to_numpy(dtype=float)
        probability = group[risk_candidate].to_numpy(dtype=float)
        baseline_probability = group["baseline_risk_90d"].to_numpy(dtype=float)
        rows.append({
            "horizon": int(horizon),
            "n_pairs": int(len(group)),
            "candidate_mae": float(mean_absolute_error(actual, candidate_count)),
            "persistence_mae": float(mean_absolute_error(actual, persistence)),
            "rolling_28d_mae": float(mean_absolute_error(actual, rolling)),
            "candidate_brier": float(
                brier_score_loss(
                    (actual > CRITICAL_THRESHOLD).astype(int),
                    np.clip(probability, 0.0, 1.0),
                )
            ),
            "risk_90d_brier": float(
                brier_score_loss(
                    (actual > CRITICAL_THRESHOLD).astype(int),
                    np.clip(baseline_probability, 0.0, 1.0),
                )
            ),
        })
    return pd.DataFrame(rows)


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    frame = pd.read_csv(
        PROJECT_ROOT / "02_data" / "augmented_emergency_data.csv",
        sep=";",
    )
    frame["EVENTOS"] = pd.to_numeric(frame["EVENTOS"], errors="coerce")
    frame = frame.loc[frame["EVENTOS"].notna()].sort_values("FECHA_DIA").reset_index(drop=True)

    predictions = build_prediction_frame(frame)
    unique_origins = np.sort(predictions["origin_index"].unique())
    holdout_count = max(1, int(math.ceil(len(unique_origins) * HOLDOUT_FRACTION)))
    holdout_start_origin = int(unique_origins[-holdout_count])
    development = predictions.loc[predictions["origin_index"] < holdout_start_origin].copy()
    holdout = predictions.loc[predictions["origin_index"] >= holdout_start_origin].copy()

    count_columns = [
        column
        for column in predictions.columns
        if column.startswith("count_") and column not in {"count_mean_28d"}
    ]
    risk_columns = [
        column
        for column in predictions.columns
        if column.startswith("risk_") and column not in {"risk_rate_90d"}
    ]
    count_candidate, count_leaderboard = select_candidate(
        development,
        count_columns,
        metric="mae",
    )
    risk_candidate, risk_leaderboard = select_candidate(
        development,
        risk_columns,
        metric="brier",
    )

    actual = holdout["EVENTOS"].to_numpy(dtype=float)
    candidate_count = holdout[count_candidate].to_numpy(dtype=float)
    candidate_probability = holdout[risk_candidate].to_numpy(dtype=float)
    persistence = holdout["baseline_persistence"].to_numpy(dtype=float)
    rolling = holdout["baseline_rolling_28d"].to_numpy(dtype=float)
    baseline_probability = holdout["baseline_risk_90d"].to_numpy(dtype=float)

    candidate_count_metrics = count_metrics(actual, candidate_count)
    candidate_risk_metrics = risk_metrics(actual, candidate_probability)
    persistence_metrics = count_metrics(actual, persistence)
    rolling_metrics = count_metrics(actual, rolling)
    baseline_risk_metrics = risk_metrics(actual, baseline_probability)

    by_horizon = horizon_metrics(holdout, count_candidate, risk_candidate)
    best_baseline_horizon = {
        int(row.horizon): min(float(row.persistence_mae), float(row.rolling_28d_mae))
        for row in by_horizon.itertuples(index=False)
    }
    candidate_horizon = {
        int(row.horizon): float(row.candidate_mae)
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
        baseline_horizon_mae=best_baseline_horizon,
    )

    selected_holdout = holdout[[
        "origin_date",
        "target_date",
        "horizon",
        "EVENTOS",
        "baseline_persistence",
        "baseline_rolling_28d",
        "baseline_risk_90d",
        count_candidate,
        risk_candidate,
    ]].copy()
    selected_holdout = selected_holdout.rename(columns={
        count_candidate: "candidate_count",
        risk_candidate: "candidate_probability",
    })

    summary = {
        "protocol": "development selection + final 25% rolling-origin holdout H1-H6",
        "selected_count_candidate": count_candidate,
        "selected_risk_candidate": risk_candidate,
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

    (OUTPUT_DIR / "temporal_candidate_search_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    by_horizon.to_csv(
        OUTPUT_DIR / "temporal_candidate_search_horizon_metrics.csv",
        index=False,
    )
    pd.concat([
        count_leaderboard.assign(objective="count_mae"),
        risk_leaderboard.assign(objective="risk_brier"),
    ], ignore_index=True).to_csv(
        OUTPUT_DIR / "temporal_candidate_search_leaderboard.csv",
        index=False,
    )
    selected_holdout.to_csv(
        OUTPUT_DIR / "temporal_candidate_search_holdout_predictions.csv",
        index=False,
    )

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
