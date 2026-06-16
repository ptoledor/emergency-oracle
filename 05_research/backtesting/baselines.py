"""Leakage-safe count baselines for temporal evaluation."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class BaselinePredictions:
    count: np.ndarray
    probability: np.ndarray


class HistoricalMedianBaseline:
    name = "historical_median"

    def predict(
        self,
        train_target: pd.Series,
        test_target: pd.Series,
        critical_threshold: float,
    ) -> BaselinePredictions:
        history = pd.to_numeric(train_target, errors="coerce").dropna().to_numpy()
        if len(history) == 0:
            raise ValueError("Historical median baseline needs known training targets")
        count = np.full(len(test_target), float(np.median(history)))
        probability = np.full(
            len(test_target), float(np.mean(history > critical_threshold))
        )
        return BaselinePredictions(count=count, probability=probability)


class MovingAverageBaseline:
    name = "moving_average_28d"

    def __init__(self, window: int = 28, update_with_observed: bool = True) -> None:
        if window <= 0:
            raise ValueError("window must be greater than zero")
        self.window = window
        self.update_with_observed = update_with_observed

    def predict(
        self,
        train_target: pd.Series,
        test_target: pd.Series,
        critical_threshold: float,
    ) -> BaselinePredictions:
        history = list(pd.to_numeric(train_target, errors="coerce").dropna())
        if not history:
            raise ValueError("Moving average baseline needs known training targets")

        counts: list[float] = []
        probabilities: list[float] = []
        observed = pd.to_numeric(test_target, errors="coerce").to_numpy()
        for actual in observed:
            window_values = np.asarray(history[-self.window :], dtype=float)
            counts.append(float(np.mean(window_values)))
            probabilities.append(
                float(np.mean(window_values > critical_threshold))
            )
            if self.update_with_observed and np.isfinite(actual):
                history.append(float(actual))

        return BaselinePredictions(
            count=np.asarray(counts),
            probability=np.asarray(probabilities),
        )
