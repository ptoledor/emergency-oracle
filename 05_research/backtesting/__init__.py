"""Leakage-aware temporal backtesting utilities."""

from .baselines import HistoricalMedianBaseline, MovingAverageBaseline
from .evaluator import BacktestEvaluator, PredictionBundle
from .splits import (
    BacktestPlan,
    TemporalBacktestConfig,
    TemporalFold,
    build_backtest_plan,
)

__all__ = [
    "BacktestEvaluator",
    "BacktestPlan",
    "HistoricalMedianBaseline",
    "MovingAverageBaseline",
    "PredictionBundle",
    "TemporalBacktestConfig",
    "TemporalFold",
    "build_backtest_plan",
]
