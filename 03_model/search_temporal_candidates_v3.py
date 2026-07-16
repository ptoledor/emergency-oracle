"""Third temporal search: stable global robust level selected on development only."""

from __future__ import annotations

import json
import math

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error

import search_temporal_candidates_v2 as v2
from search_temporal_candidates import (
    CRITICAL_THRESHOLD,
    HOLDOUT_FRACTION,
    OUTPUT_DIR,
    PROJECT_ROOT,
    build_prediction_frame,
    count_metrics,
    risk_metrics,
)
from temporal_gate import evaluate_promotion_gate


v2.SHRINK_WINDOWS = (28, 34, 42, 48, 56, 64, 70, 84)
OFFSET_WINDOWS = tuple(range(36, 62, 2))
OFFSETS = (-0.50, -0.25, 0.25, 0.50)
BLEND_WEIGHTS = tuple(value / 10.0 for value in range(1, 10))
STABILITY_WEIGHT = 0.25
N_STABILITY_FOLDS = 4


def add_v3_candidates(predictions: pd.DataFrame) -> pd.DataFrame:
    additions: dict[str, np.ndarray] = {}

    for window in OFFSET_WINDOWS:
        base = predictions[f"count_quantile_q50_{window}d"].to_numpy(dtype=float)
        for offset in OFFSETS:
            suffix = str(offset).replace("-", "m").replace(".", "p")
            additions[f"count_median{window}_offset_{suffix}"] = np.clip(
                base + offset,
                0.0,
                None,
            )

    for weight in BLEND_WEIGHTS:
        additions[f"count_blend_q50_42_52_w{int(weight * 100)}"] = (
            weight * predictions["count_quantile_q50_42d"].to_numpy(dtype=float)
            + (1.0 - weight)
            * predictions["count_quantile_q50_52d"].to_numpy(dtype=float)
        )
        additions[f"count_blend_q50_42_56_w{int(weight * 100)}"] = (
            weight * predictions["count_quantile_q50_42d"].to_numpy(dtype=float)
            + (1.0 - weight)
            * predictions["count_quantile_q50_56d"].to_numpy(dtype=float)
        )

    additions["count_ensemble_q50_40_60"] = predictions[
        [f"count_quantile_q50_{window}d" for window in (40, 44, 48, 52, 56, 60)]
    ].mean(axis=1).to_numpy(dtype=float)
    additions["count_ensemble_q50_42_52_56"] = predictions[
        [
            "count_quantile_q50_42d",
            "count_quantile_q50_52d",
            "count_quantile_q50_56d",
        ]
    ].mean(axis=1).to_numpy(dtype=float)

    return pd.concat([predictions, pd.DataFrame(additions, index=predictions.index)], axis=1)


def select_stable_global(
    development: pd.DataFrame,
    candidates: list[str],
) -> tuple[str, pd.DataFrame]:
    origins = np.sort(development["origin_index"].unique())
    fold_origin_sets = np.array_split(origins, N_STABILITY_FOLDS)
    actual = development["EVENTOS"].to_numpy(dtype=float)
    rows: list[dict[str, float | str]] = []

    for candidate in candidates:
        prediction = development[candidate].to_numpy(dtype=float)
        overall = float(mean_absolute_error(actual, prediction))
        fold_scores: list[float] = []
        for fold_origins in fold_origin_sets:
            mask = development["origin_index"].isin(fold_origins).to_numpy()
            fold_scores.append(
                float(mean_absolute_error(actual[mask], prediction[mask]))
            )
        stability_std = float(np.std(fold_scores, ddof=1))
        score = overall + STABILITY_WEIGHT * stability_std
        rows.append({
            "candidate": candidate,
            "development_mae": overall,
            "fold_mae_std": stability_std,
            "selection_score": score,
            "worst_fold_mae": float(max(fold_scores)),
        })

    leaderboard = pd.DataFrame(rows).sort_values(
        ["selection_score", "development_mae"],
        ignore_index=True,
    )
    return str(leaderboard.iloc[0]["candidate"]), leaderboard


def make_horizon_metrics(
    holdout: pd.DataFrame,
    selected: str,
) -> pd.DataFrame:
    rows: list[dict[str, float | int | str]] = []
    for horizon, group in holdout.groupby("horizon", sort=True):
        actual = group["EVENTOS"].to_numpy(dtype=float)
        candidate = group[selected].to_numpy(dtype=float)
        persistence = group["baseline_persistence"].to_numpy(dtype=float)
        rolling = group["baseline_rolling_28d"].to_numpy(dtype=float)
        risk = group["baseline_risk_90d"].to_numpy(dtype=float)
        target = (actual > CRITICAL_THRESHOLD).astype(int)
        from sklearn.metrics import brier_score_loss
        rows.append({
            "horizon": int(horizon),
            "selected_candidate": selected,
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
        PROJECT_ROOT / "02_data" / "augmented_emergency_data.csv",
        sep=";",
    )
    frame["EVENTOS"] = pd.to_numeric(frame["EVENTOS"], errors="coerce")
    frame = frame.loc[frame["EVENTOS"].notna()].sort_values("FECHA_DIA").reset_index(drop=True)

    predictions = build_prediction_frame(frame)
    predictions = v2.add_robust_level_candidates(predictions, frame)
    predictions = add_v3_candidates(predictions)

    origins = np.sort(predictions["origin_index"].unique())
    holdout_count = max(1, int(math.ceil(len(origins) * HOLDOUT_FRACTION)))
    holdout_start = int(origins[-holdout_count])
    development = predictions.loc[predictions["origin_index"] < holdout_start].copy()
    holdout = predictions.loc[predictions["origin_index"] >= holdout_start].copy()

    candidates = [
        column
        for column in predictions.columns
        if column.startswith("count_") and column != "count_mean_28d"
    ]
    selected, leaderboard = select_stable_global(development, candidates)

    actual = holdout["EVENTOS"].to_numpy(dtype=float)
    candidate_count = holdout[selected].to_numpy(dtype=float)
    candidate_probability = holdout["baseline_risk_90d"].to_numpy(dtype=float)
    persistence = holdout["baseline_persistence"].to_numpy(dtype=float)
    rolling = holdout["baseline_rolling_28d"].to_numpy(dtype=float)

    candidate_count_metrics = count_metrics(actual, candidate_count)
    candidate_risk_metrics = risk_metrics(actual, candidate_probability)
    persistence_metrics = count_metrics(actual, persistence)
    rolling_metrics = count_metrics(actual, rolling)
    baseline_risk_metrics = risk_metrics(actual, candidate_probability)

    by_horizon = make_horizon_metrics(holdout, selected)
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
    selected_predictions["candidate_name"] = selected

    summary = {
        "protocol": "stable global development selection + final 25% holdout",
        "selected_count_candidate": selected,
        "selected_risk_candidate": "baseline_risk_90d",
        "stability_weight": STABILITY_WEIGHT,
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

    (OUTPUT_DIR / "temporal_candidate_search_v3_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    leaderboard.to_csv(
        OUTPUT_DIR / "temporal_candidate_search_v3_leaderboard.csv", index=False
    )
    by_horizon.to_csv(
        OUTPUT_DIR / "temporal_candidate_search_v3_horizon_metrics.csv", index=False
    )
    selected_predictions.to_csv(
        OUTPUT_DIR / "temporal_candidate_search_v3_holdout_predictions.csv", index=False
    )

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
