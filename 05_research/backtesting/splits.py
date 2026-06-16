"""Temporal split planning with a date-blocked final holdout."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class TemporalBacktestConfig:
    date_column: str
    target_column: str
    holdout_start: str
    holdout_end: str | None = None
    outer_min_train_size: int = 365
    outer_test_size: int = 28
    outer_step_size: int = 28
    outer_gap_size: int = 0
    outer_max_train_size: int | None = None
    inner_min_train_size: int = 180
    inner_test_size: int = 28
    inner_step_size: int = 28
    inner_gap_size: int = 0
    inner_max_train_size: int | None = None
    max_outer_folds: int | None = None
    max_inner_folds: int | None = None

    def __post_init__(self) -> None:
        positive = {
            "outer_min_train_size": self.outer_min_train_size,
            "outer_test_size": self.outer_test_size,
            "outer_step_size": self.outer_step_size,
            "inner_min_train_size": self.inner_min_train_size,
            "inner_test_size": self.inner_test_size,
            "inner_step_size": self.inner_step_size,
        }
        for name, value in positive.items():
            if value <= 0:
                raise ValueError(f"{name} must be greater than zero")
        if self.outer_gap_size < 0 or self.inner_gap_size < 0:
            raise ValueError("gap sizes cannot be negative")
        if self.outer_max_train_size is not None and self.outer_max_train_size <= 0:
            raise ValueError("outer_max_train_size must be greater than zero")
        if self.inner_max_train_size is not None and self.inner_max_train_size <= 0:
            raise ValueError("inner_max_train_size must be greater than zero")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class TemporalFold:
    name: str
    train_indices: np.ndarray
    test_indices: np.ndarray
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    test_start: pd.Timestamp
    test_end: pd.Timestamp
    inner_folds: list["TemporalFold"] = field(default_factory=list)

    def to_dict(self, include_inner: bool = True) -> dict[str, Any]:
        result = {
            "name": self.name,
            "train_size": int(len(self.train_indices)),
            "test_size": int(len(self.test_indices)),
            "train_start": self.train_start.isoformat(),
            "train_end": self.train_end.isoformat(),
            "test_start": self.test_start.isoformat(),
            "test_end": self.test_end.isoformat(),
        }
        if include_inner:
            result["inner_folds"] = [
                fold.to_dict(include_inner=False) for fold in self.inner_folds
            ]
        return result


@dataclass
class BacktestPlan:
    frame: pd.DataFrame
    config: TemporalBacktestConfig
    outer_folds: list[TemporalFold]
    development_indices: np.ndarray
    holdout_indices: np.ndarray
    unknown_target_indices: np.ndarray
    ignored_after_holdout_indices: np.ndarray

    def holdout_fold(self) -> TemporalFold:
        if len(self.holdout_indices) == 0:
            raise ValueError("Blocked holdout contains no known targets")
        return _make_fold(
            "holdout",
            self.development_indices,
            self.holdout_indices,
            self.frame,
            self.config.date_column,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "config": self.config.to_dict(),
            "known_development_rows": int(len(self.development_indices)),
            "known_holdout_rows": int(len(self.holdout_indices)),
            "unknown_target_rows": int(len(self.unknown_target_indices)),
            "ignored_after_holdout_rows": int(
                len(self.ignored_after_holdout_indices)
            ),
            "outer_folds": [fold.to_dict() for fold in self.outer_folds],
            "holdout": (
                self.holdout_fold().to_dict(include_inner=False)
                if len(self.holdout_indices)
                else None
            ),
        }


def _make_fold(
    name: str,
    train_indices: np.ndarray,
    test_indices: np.ndarray,
    frame: pd.DataFrame,
    date_column: str,
) -> TemporalFold:
    train_dates = frame.loc[train_indices, date_column]
    test_dates = frame.loc[test_indices, date_column]
    return TemporalFold(
        name=name,
        train_indices=np.asarray(train_indices, dtype=int),
        test_indices=np.asarray(test_indices, dtype=int),
        train_start=pd.Timestamp(train_dates.min()),
        train_end=pd.Timestamp(train_dates.max()),
        test_start=pd.Timestamp(test_dates.min()),
        test_end=pd.Timestamp(test_dates.max()),
    )


def _rolling_folds(
    eligible_indices: np.ndarray,
    frame: pd.DataFrame,
    date_column: str,
    prefix: str,
    min_train_size: int,
    test_size: int,
    step_size: int,
    gap_size: int,
    max_train_size: int | None,
    max_folds: int | None,
) -> list[TemporalFold]:
    folds: list[TemporalFold] = []
    test_start = min_train_size + gap_size
    fold_number = 1

    while test_start + test_size <= len(eligible_indices):
        train_end = test_start - gap_size
        train_start = 0 if max_train_size is None else max(0, train_end - max_train_size)
        train_indices = eligible_indices[train_start:train_end]
        test_indices = eligible_indices[test_start : test_start + test_size]
        folds.append(
            _make_fold(
                f"{prefix}_{fold_number:02d}",
                train_indices,
                test_indices,
                frame,
                date_column,
            )
        )
        fold_number += 1
        test_start += step_size

    if max_folds is not None:
        folds = folds[-max_folds:]
    return folds


def build_backtest_plan(
    data: pd.DataFrame, config: TemporalBacktestConfig
) -> BacktestPlan:
    """Build nested rolling-origin folds without using unknown targets."""
    missing = {
        config.date_column,
        config.target_column,
    }.difference(data.columns)
    if missing:
        raise KeyError(f"Missing required columns: {sorted(missing)}")

    frame = data.copy()
    frame[config.date_column] = pd.to_datetime(
        frame[config.date_column], errors="coerce"
    )
    if frame[config.date_column].isna().any():
        count = int(frame[config.date_column].isna().sum())
        raise ValueError(f"{count} rows contain invalid dates")
    frame[config.target_column] = pd.to_numeric(
        frame[config.target_column], errors="coerce"
    )
    frame = frame.sort_values(config.date_column, kind="stable").reset_index(drop=True)
    if frame[config.date_column].duplicated().any():
        raise ValueError(
            "Temporal backtesting expects one row per date; aggregate duplicates first"
        )

    holdout_start = pd.Timestamp(config.holdout_start)
    holdout_end = (
        pd.Timestamp(config.holdout_end)
        if config.holdout_end is not None
        else frame[config.date_column].max()
    )
    if holdout_end < holdout_start:
        raise ValueError("holdout_end cannot be before holdout_start")

    known = frame[config.target_column].notna()
    before_holdout = frame[config.date_column] < holdout_start
    in_holdout = frame[config.date_column].between(holdout_start, holdout_end)
    after_holdout = frame[config.date_column] > holdout_end

    development_indices = frame.index[known & before_holdout].to_numpy(dtype=int)
    holdout_indices = frame.index[known & in_holdout].to_numpy(dtype=int)
    unknown_indices = frame.index[~known].to_numpy(dtype=int)
    ignored_after = frame.index[known & after_holdout].to_numpy(dtype=int)

    outer_folds = _rolling_folds(
        development_indices,
        frame,
        config.date_column,
        "outer",
        config.outer_min_train_size,
        config.outer_test_size,
        config.outer_step_size,
        config.outer_gap_size,
        config.outer_max_train_size,
        config.max_outer_folds,
    )
    if not outer_folds:
        raise ValueError(
            "No outer folds fit. Reduce train/test sizes or move holdout_start."
        )

    for outer in outer_folds:
        outer.inner_folds = _rolling_folds(
            outer.train_indices,
            frame,
            config.date_column,
            f"{outer.name}_inner",
            config.inner_min_train_size,
            config.inner_test_size,
            config.inner_step_size,
            config.inner_gap_size,
            config.inner_max_train_size,
            config.max_inner_folds,
        )
        if not outer.inner_folds:
            raise ValueError(
                f"No inner folds fit inside {outer.name}. "
                "Reduce inner train/test sizes."
            )

    return BacktestPlan(
        frame=frame,
        config=config,
        outer_folds=outer_folds,
        development_indices=development_indices,
        holdout_indices=holdout_indices,
        unknown_target_indices=unknown_indices,
        ignored_after_holdout_indices=ignored_after,
    )
