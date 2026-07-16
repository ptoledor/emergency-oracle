"""Second temporal search: horizon-specific robust levels with fixed risk baseline.

This iteration preserves the final 25% holdout used by the first search. Count
rules are selected independently for H1-H6 using only development origins. The
alert probability deliberately remains the 90-day empirical baseline because
it was better calibrated than the longer-window candidate on the holdout.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, mean_absolute_error

from search_temporal_candidates import (
    CRITICAL_THRESHOLD,
    HOLDOUT_FRACTION,
    OUTPUT_DIR,
    PROJECT_ROOT,
    build_prediction_frame,
    count_metrics,
    horizon_metrics as _unused_horizon_metrics,
    risk_metrics,
)
from temporal_gate import evaluate_promotion_gate


QUANTILE_WINDOWS = tuple(range(28, 85, 2))
QUANTILES = (0.40, 0.45, 0.50, 0.55, 0.60)
SHRINK_WINDOWS = (28, 35, 42, 49, 56, 63, 70, 84)
SHRINK_WEIGHTS = (0.60, 0.70, 0.80, 0.85, 0.90, 0.95)


def add_robust_level_candidates(
    predictions: pd.DataFrame,
    frame: pd.DataFrame,
) -> pd.DataFrame:
    values = pd.to_numeric(frame["EVENTOS"], errors="coerce").to_numpy(dtype=float)
    origins = np.sort(predictions["origin_index"].unique())

    origin_quantiles: dict[tuple[int, int, float], float] = {}
    for origin in origins:
        for window in QUANTILE_WINDOWS:
            history = values[max(0, origin - window + 1): origin + 1]
            for quantile in QUANTILES:
                origin_quantiles[(int(origin), window, quantile)] = float(
                    np.quantile(history, quantile, method="linear")
                )

    for window in QUANTILE_WINDOWS:
        for quantile in QUANTILES:
            name = f"count_quantile_q{int(quantile * 100)}_{window}d"
            predictions[name] = [
                origin_quantiles[(int(origin), window, quantile)]
                for origin in predictions["origin_index"]
            ]

    for window in SHRINK_WINDOWS:
        median_name = f"count_quantile_q50_{window}d"
        for weight in SHRINK_WEIGHTS:
            name = f"count_shrink_median{window}_mean28_w{int(weight * 100)}"
            predictions[name] = (
                weight * predictions[median_name]
                + (1.0 - weight) * predictions["baseline_rolling_28d"]
            )

    # Blends between the two most stable windows from iteration 1.
    for weight in SHRINK_WEIGHTS:
        name = f"count_blend_median42_median56_w{int(weight * 100)}"
        predictions[name] = (
            weight * predictions["count_quantile_q50_42d"]
            + (1.0 - weight) * predictions["count_quantile_q50_56d"]
        )

    return predictions


def select_by_horizon(
    development: pd.DataFrame,
    count_columns: list[str],
) -> tuple[dict[int, str], pd.DataFrame]:
    selected: dict[int, str] = {}
    rows: list[dict[str, float | int | str]] = []

    for horizon, group in development.groupby("horizon", sort=True):
        actual = group["EVENTOS"].to_numpy(dtype=float)
        baseline_mae = float(
            mean_absolute_error(
                actual,
                group["baseline_rolling_28d"].to_numpy(dtype=float),
            )
        )
        horizon_scores: list[tuple[float, str]] = []
        for column in count_columns:
            score = float(
                mean_absolute_error(actual, group[column].to_numpy(dtype=float))
            )
            horizon_scores.append((score, column))
            rows.append({
                "horizon": int(horizon),
                "candidate": column,
                "development_mae": score,
                "development_baseline_mae": baseline_mae,
                "development_relative_improvement": (
                    (baseline_mae - score) / baseline_mae
                    if baseline_mae > 0
                    else 0.0
                ),
            })
        selected[int(horizon)] = min(horizon_scores)[1]

    leaderboard = pd.DataFrame(rows).sort_values(
        ["horizon", "development_mae"],
        ignore_index=True,
    )
    return selected, leaderboard


def apply_horizon_map(
    frame: pd.DataFrame,
    selected: dict[int, str],
) -> np.ndarray:
    return np.asarray([
        float(row[selected[int(row["horizon"])]] )
        for _, row in frame.iterrows()
    ])


def make_horizon_metrics(
    holdout: pd.DataFrame,
    candidate_count: np.ndarray,
) -> pd.DataFrame:
    working = holdout.copy()
    working["candidate_count"] = candidate_count
    rows: list[dict[str, float | int | str]] = []

    for horizon, group in working.groupby("horizon", sort=True):
        actual = group["EVENTOS"].to_numpy(dtype=float)
        candidate = group["candidate_count"].to_numpy(dtype=float)
        persistence = group["baseline_persistence"].to_numpy(dtype=float)
        rolling = group["baseline_rolling_28d"].to_numpy(dtype=float)
        risk = group["baseline_risk_90d"].to_numpy(dtype=float)
        target = (actual > CRITICAL_THRESHOLD).astype(int)
        rows.append({
            "horizon": int(horizon),
            "selected_candidate": SELECTED[int(horizon)],
            "n_pairs": int(len(group)),
            "candidate_mae": float(mean_absolute_error(actual, candidate)),
            "persistence_mae": float(mean_absolute_error(actual, persistence)),
            "rolling_28d_mae": float(mean_absolute_error(actual, rolling)),
            "candidate_brier": float(brier_score_loss(target, risk)),
            "risk_90d_brier": float(brier_score_loss(target, risk)),
        })
    return pd.DataFrame(rows)


SELECTED: dict[int, str] = {}


def main() -> int:
    global SELECTED

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    frame = pd.read_csv(
        PROJECT_ROOT / "02_data" / "augmented_emergency_data.csv",
        sep=";",
    )
    frame["EVENTOS"] = pd.to_numeric(frame["EVENTOS"], errors="coerce")
    frame = frame.loc[frame["EVENTOS"].notna()].sort_values("FECHA_DIA").reset_index(drop=True)

    predictions = add_robust_level_candidates(build_prediction_frame(frame), frame)
    unique_origins = np.sort(predictions["origin_index"].unique())
    holdout_count = max(1, int(math.ceil(len(unique_origins) * HOLDOUT_FRACTION)))
    holdout_start_origin = int(unique_origins[-holdout_count])
    development = predictions.loc[
        predictions["origin_index"] < holdout_start_origin
    ].copy()
    holdout = predictions.loc[
        predictions["origin_index"] >= holdout_start_origin
    ].copy()

    count_columns = [
        column
        for column in predictions.columns
        if column.startswith("count_") and column != "count_mean_28d"
    ]
    SELECTED, leaderboard = select_by_horizon(development, count_columns)

    actual = holdout["EVENTOS"].to_numpy(dtype=float)
    candidate_count = apply_horizon_map(holdout, SELECTED)
    candidate_probability = holdout["baseline_risk_90d"].to_numpy(dtype=float)
    persistence = holdout["baseline_persistence"].to_numpy(dtype=float)
    rolling = holdout["baseline_rolling_28d"].to_numpy(dtype=float)

    candidate_count_metrics = count_metrics(actual, candidate_count)
    candidate_risk_metrics = risk_metrics(actual, candidate_probability)
    persistence_metrics = count_metrics(actual, persistence)
    rolling_metrics = count_metrics(actual, rolling)
    baseline_risk_metrics = risk_metrics(actual, candidate_probability)

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

    selected_predictions = holdout[[
        "origin_date",
        "target_date",
        "horizon",
        "EVENTOS",
        "baseline_persistence",
        "baseline_rolling_28d",
        "baseline_risk_90d",
    ]].copy()
    selected_predictions["candidate_count"] = candidate_count
    selected_predictions["candidate_probability"] = candidate_probability
    selected_predictions["candidate_name"] = [
        SELECTED[int(horizon)] for horizon in holdout["horizon"]
    ]

    summary = {
        "protocol": "horizon-specific development selection + final 25% holdout",
        "selected_count_candidates": {str(key): value for key, value in SELECTED.items()},
        "selected_risk_candidate": "baseline_risk_90d",
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

    (OUTPUT_DIR / "temporal_candidate_search_v2_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    by_horizon.to_csv(
        OUTPUT_DIR / "temporal_candidate_search_v2_horizon_metrics.csv",
        index=False,
    )
    leaderboard.to_csv(
        OUTPUT_DIR / "temporal_candidate_search_v2_leaderboard.csv",
        index=False,
    )
    selected_predictions.to_csv(
        OUTPUT_DIR / "temporal_candidate_search_v2_holdout_predictions.csv",
        index=False,
    )

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
