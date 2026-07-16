"""Pure helpers for leakage-aware temporal evaluation and model promotion gates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence


@dataclass(frozen=True)
class PromotionGateResult:
    """Decision and diagnostics for a candidate model promotion gate."""

    passes: bool
    best_count_baseline: str
    best_count_baseline_mae: float
    count_relative_improvement: float
    brier_relative_change: float
    horizons_improved: int
    horizons_required: int
    reasons: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "passes": self.passes,
            "best_count_baseline": self.best_count_baseline,
            "best_count_baseline_mae": self.best_count_baseline_mae,
            "count_relative_improvement": self.count_relative_improvement,
            "brier_relative_change": self.brier_relative_change,
            "horizons_improved": self.horizons_improved,
            "horizons_required": self.horizons_required,
            "reasons": list(self.reasons),
        }


def build_origin_horizon_pairs(
    n_rows: int,
    min_train_days: int,
    horizons: Sequence[int],
) -> list[tuple[int, int, int]]:
    """Return every valid ``(origin_index, horizon, target_index)`` tuple.

    The first origin is the final observation of the minimum training window.
    Every origin is evaluated at every requested horizon when the target remains
    inside the dataset. This prevents the former cyclic-horizon evaluation where
    each origin contributed only one horizon.
    """

    if n_rows <= 0:
        return []
    if min_train_days < 1:
        raise ValueError("min_train_days must be positive")

    normalized_horizons = tuple(sorted({int(value) for value in horizons}))
    if not normalized_horizons or normalized_horizons[0] < 1:
        raise ValueError("horizons must contain positive integers")

    first_origin = min_train_days - 1
    last_origin = n_rows - normalized_horizons[-1] - 1
    if last_origin < first_origin:
        return []

    return [
        (origin, horizon, origin + horizon)
        for origin in range(first_origin, last_origin + 1)
        for horizon in normalized_horizons
    ]


def evaluate_promotion_gate(
    *,
    candidate_mae: float,
    count_baseline_mae: Mapping[str, float],
    candidate_brier: float,
    probability_baseline_brier: float,
    candidate_horizon_mae: Mapping[int, float],
    baseline_horizon_mae: Mapping[int, float],
    min_count_relative_improvement: float = 0.03,
    max_brier_relative_degradation: float = 0.02,
    min_horizon_fraction_improved: float = 2.0 / 3.0,
) -> PromotionGateResult:
    """Apply an explicit promotion gate against temporal baselines.

    A candidate passes only when it improves the best count baseline by the
    configured margin, does not materially worsen probability calibration, and
    improves a minimum share of forecast horizons.
    """

    finite_baselines = {
        str(name): float(value)
        for name, value in count_baseline_mae.items()
        if float(value) >= 0.0
    }
    if not finite_baselines:
        raise ValueError("at least one count baseline MAE is required")
    if probability_baseline_brier < 0.0:
        raise ValueError("probability baseline Brier must be non-negative")

    best_name, best_mae = min(finite_baselines.items(), key=lambda item: item[1])
    if best_mae == 0.0:
        count_improvement = 0.0 if candidate_mae == 0.0 else float("-inf")
    else:
        count_improvement = (best_mae - float(candidate_mae)) / best_mae

    if probability_baseline_brier == 0.0:
        brier_change = 0.0 if candidate_brier == 0.0 else float("inf")
    else:
        brier_change = (
            float(candidate_brier) - probability_baseline_brier
        ) / probability_baseline_brier

    common_horizons = sorted(
        set(candidate_horizon_mae).intersection(baseline_horizon_mae)
    )
    if not common_horizons:
        raise ValueError("candidate and baseline horizon metrics do not overlap")

    horizons_improved = sum(
        float(candidate_horizon_mae[horizon])
        < float(baseline_horizon_mae[horizon])
        for horizon in common_horizons
    )
    horizons_required = max(
        1,
        int(len(common_horizons) * min_horizon_fraction_improved + 0.999999),
    )

    reasons: list[str] = []
    if count_improvement < min_count_relative_improvement:
        reasons.append(
            "count_mae_improvement_below_"
            f"{min_count_relative_improvement:.3f}"
        )
    if brier_change > max_brier_relative_degradation:
        reasons.append(
            "brier_degradation_above_"
            f"{max_brier_relative_degradation:.3f}"
        )
    if horizons_improved < horizons_required:
        reasons.append(
            f"only_{horizons_improved}_of_{len(common_horizons)}_horizons_improved"
        )

    return PromotionGateResult(
        passes=not reasons,
        best_count_baseline=best_name,
        best_count_baseline_mae=float(best_mae),
        count_relative_improvement=float(count_improvement),
        brier_relative_change=float(brier_change),
        horizons_improved=int(horizons_improved),
        horizons_required=int(horizons_required),
        reasons=tuple(reasons),
    )
