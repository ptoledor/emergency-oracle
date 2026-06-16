"""Command-line entry point for baseline temporal backtests."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from .evaluator import BacktestEvaluator
from .splits import TemporalBacktestConfig, build_backtest_plan


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", type=Path)
    parser.add_argument("--sep", default=",", help="CSV delimiter; use ';' for project data.")
    parser.add_argument("--date-column", required=True)
    parser.add_argument("--target-column", required=True)
    parser.add_argument("--holdout-start", required=True)
    parser.add_argument("--holdout-end")
    parser.add_argument("--output", type=Path, default=Path("backtest_results"))
    parser.add_argument("--critical-threshold", type=float, default=7.0)
    parser.add_argument("--decision-threshold", type=float, default=0.5)
    parser.add_argument("--outer-min-train-size", type=int, default=365)
    parser.add_argument("--outer-test-size", type=int, default=28)
    parser.add_argument("--outer-step-size", type=int, default=28)
    parser.add_argument("--outer-gap-size", type=int, default=0)
    parser.add_argument("--inner-min-train-size", type=int, default=180)
    parser.add_argument("--inner-test-size", type=int, default=28)
    parser.add_argument("--inner-step-size", type=int, default=28)
    parser.add_argument("--inner-gap-size", type=int, default=0)
    parser.add_argument("--max-outer-folds", type=int)
    parser.add_argument("--max-inner-folds", type=int)
    parser.add_argument(
        "--evaluate-holdout",
        action="store_true",
        help="Explicitly open and score the blocked final holdout.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    data = pd.read_csv(args.csv, sep=args.sep)
    config = TemporalBacktestConfig(
        date_column=args.date_column,
        target_column=args.target_column,
        holdout_start=args.holdout_start,
        holdout_end=args.holdout_end,
        outer_min_train_size=args.outer_min_train_size,
        outer_test_size=args.outer_test_size,
        outer_step_size=args.outer_step_size,
        outer_gap_size=args.outer_gap_size,
        inner_min_train_size=args.inner_min_train_size,
        inner_test_size=args.inner_test_size,
        inner_step_size=args.inner_step_size,
        inner_gap_size=args.inner_gap_size,
        max_outer_folds=args.max_outer_folds,
        max_inner_folds=args.max_inner_folds,
    )
    plan = build_backtest_plan(data, config)
    evaluator = BacktestEvaluator(
        plan,
        critical_threshold=args.critical_threshold,
        decision_threshold=args.decision_threshold,
    )
    cv_result = evaluator.evaluate_cv()
    cv_result.export(args.output, plan)
    print(cv_result.summary.to_string(index=False))

    if args.evaluate_holdout:
        holdout_result = evaluator.evaluate_holdout()
        holdout_result.export(args.output, plan)
        print("\nBlocked holdout:")
        print(holdout_result.summary.to_string(index=False))


if __name__ == "__main__":
    main()
