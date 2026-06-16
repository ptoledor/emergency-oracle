"""Regression and classification metrics with safe edge-case handling."""

from __future__ import annotations

import math
from collections.abc import Sequence

import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    f1_score,
    log_loss,
    mean_absolute_error,
    mean_poisson_deviance,
    mean_squared_error,
    precision_score,
    r2_score,
    recall_score,
    roc_auc_score,
)


def _finite_pairs(
    first: Sequence[float], second: Sequence[float]
) -> tuple[np.ndarray, np.ndarray]:
    left = np.asarray(first, dtype=float)
    right = np.asarray(second, dtype=float)
    mask = np.isfinite(left) & np.isfinite(right)
    return left[mask], right[mask]


def regression_metrics(
    y_true: Sequence[float],
    y_pred: Sequence[float],
    y_train: Sequence[float] | None = None,
) -> dict[str, float]:
    actual, predicted = _finite_pairs(y_true, y_pred)
    if len(actual) == 0:
        return {
            key: math.nan
            for key in ("mae", "rmse", "r2", "mase", "poisson_deviance")
        }

    mae = float(mean_absolute_error(actual, predicted))
    rmse = float(mean_squared_error(actual, predicted) ** 0.5)
    r2 = float(r2_score(actual, predicted)) if len(actual) >= 2 else math.nan

    mase = math.nan
    if y_train is not None:
        train = np.asarray(y_train, dtype=float)
        train = train[np.isfinite(train)]
        if len(train) >= 2:
            scale = float(np.mean(np.abs(np.diff(train))))
            if scale > 0:
                mase = mae / scale

    poisson = math.nan
    if np.all(actual >= 0):
        poisson = float(
            mean_poisson_deviance(actual, np.clip(predicted, 1e-12, None))
        )

    return {
        "mae": mae,
        "rmse": rmse,
        "r2": r2,
        "mase": mase,
        "poisson_deviance": poisson,
    }


def classification_metrics(
    y_true: Sequence[int],
    y_probability: Sequence[float],
    decision_threshold: float = 0.5,
    dates: Sequence[object] | None = None,
) -> dict[str, float]:
    actual, probability = _finite_pairs(y_true, y_probability)
    probability = np.clip(probability, 1e-12, 1 - 1e-12)
    if len(actual) == 0:
        return {
            key: math.nan
            for key in (
                "roc_auc",
                "pr_auc",
                "brier",
                "log_loss",
                "precision",
                "recall",
                "f1",
                "alerts_per_week",
            )
        }

    actual = actual.astype(int)
    predicted = (probability >= decision_threshold).astype(int)
    has_both_classes = len(np.unique(actual)) == 2
    roc_auc = (
        float(roc_auc_score(actual, probability)) if has_both_classes else math.nan
    )
    pr_auc = (
        float(average_precision_score(actual, probability))
        if np.any(actual == 1)
        else math.nan
    )

    alerts_per_week = math.nan
    if dates is not None:
        parsed_dates = pd.to_datetime(pd.Series(dates), errors="coerce").dropna()
        if len(parsed_dates):
            days = max(1, int((parsed_dates.max() - parsed_dates.min()).days) + 1)
            alerts_per_week = float(predicted.sum() * 7 / days)

    return {
        "roc_auc": roc_auc,
        "pr_auc": pr_auc,
        "brier": float(brier_score_loss(actual, probability)),
        "log_loss": float(log_loss(actual, probability, labels=[0, 1])),
        "precision": float(precision_score(actual, predicted, zero_division=0)),
        "recall": float(recall_score(actual, predicted, zero_division=0)),
        "f1": float(f1_score(actual, predicted, zero_division=0)),
        "alerts_per_week": alerts_per_week,
    }
