from __future__ import annotations

import argparse
import json
import pickle
from dataclasses import replace
from pathlib import Path

import pandas as pd

from models.benchmark import (
    BenchmarkConfig,
    DEFAULT_METADATA,
    run_benchmark,
)
from weather_features import FEATURE_GROUPS


DEFAULT_SOURCE = Path("02_data/augmented_emergency_data.csv")
DEFAULT_WEATHER = Path("05_research/data/historical_forecast_features.csv")
DEFAULT_DATASET = Path("05_research/data/weather_experiment_dataset.csv")
DEFAULT_OUTPUT = Path("05_research/results/weather_ablation")


def build_experiment_dataset(
    source_path: Path,
    weather_path: Path,
    output_path: Path,
) -> pd.DataFrame:
    source = pd.read_csv(source_path, sep=";")
    weather = pd.read_csv(weather_path, sep=";")
    source["FECHA_DIA"] = pd.to_datetime(source["FECHA_DIA"]).dt.strftime("%Y-%m-%d")
    weather["FECHA_DIA"] = pd.to_datetime(weather["FECHA_DIA"]).dt.strftime("%Y-%m-%d")
    merged = source.merge(weather, on="FECHA_DIA", how="left", validate="one_to_one")
    merged.loc[pd.to_numeric(merged["EVENTOS"], errors="coerce").eq(0), "EVENTOS"] = pd.NA
    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_path, sep=";", index=False)
    return merged


def read_base_features(metadata_path: Path) -> list[str]:
    with metadata_path.open("rb") as stream:
        return list(pickle.load(stream)["feature_cols"])


def eligible_weather_features(
    frame: pd.DataFrame,
    columns: list[str],
    minimum_coverage: float = 0.80,
    maximum_initial_gap: float = 0.10,
) -> list[str]:
    eligible = []
    for column in columns:
        if column not in frame:
            continue
        values = pd.to_numeric(frame[column], errors="coerce")
        coverage = float(values.notna().mean())
        valid_positions = values.notna().to_numpy().nonzero()[0]
        if not len(valid_positions):
            continue
        initial_gap = float(valid_positions[0] / max(len(values), 1))
        if coverage >= minimum_coverage and initial_gap <= maximum_initial_gap:
            eligible.append(column)
    return eligible


def select_summary(
    output_dir: Path,
    experiments: list[dict[str, object]],
) -> dict[str, object]:
    results = []
    for experiment in experiments:
        directory = output_dir / str(experiment["name"])
        counts = pd.read_csv(directory / "count_metrics.csv")
        classes = pd.read_csv(directory / "classification_metrics.csv")
        best_count = counts.sort_values("mae").iloc[0]
        best_class = classes.sort_values(
            ["roc_auc", "brier"], ascending=[False, True]
        ).iloc[0]
        results.append(
            {
                **experiment,
                "count_model": str(best_count["model"]),
                "classification_model": str(best_class["model"]),
                "mae": float(best_count["mae"]),
                "rmse": float(best_count["rmse"]),
                "r2": float(best_count["r2"]),
                "roc_auc": float(best_class["roc_auc"]),
                "brier": float(best_class["brier"]),
                "accuracy": float(best_class["accuracy"]),
                "precision": float(best_class["precision"]),
                "recall": float(best_class["recall"]),
                "f1": float(best_class["f1"]),
            }
        )

    baseline = next(item for item in results if item["name"] == "baseline")
    candidates = [item for item in results if item["name"] != "baseline"]
    for item in results:
        item["mae_improvement"] = baseline["mae"] - item["mae"]
        item["roc_auc_improvement"] = item["roc_auc"] - baseline["roc_auc"]
        item["folds_improved"] = 0

    for item in candidates:
        base_folds = pd.read_csv(output_dir / "baseline" / "fold_metrics.csv")
        candidate_folds = pd.read_csv(output_dir / str(item["name"]) / "fold_metrics.csv")
        base_count = base_folds[
            (base_folds["task"] == "count")
            & (base_folds["model"] == baseline["count_model"])
        ].set_index("fold")["mae"]
        candidate_count = candidate_folds[
            (candidate_folds["task"] == "count")
            & (candidate_folds["model"] == item["count_model"])
        ].set_index("fold")["mae"]
        item["folds_improved"] = int((candidate_count < base_count).sum())

    winner = sorted(
        candidates,
        key=lambda item: (
            item["folds_improved"] >= 3,
            item["mae_improvement"] > 0,
            item["roc_auc_improvement"] > 0,
            item["mae_improvement"],
            item["roc_auc_improvement"],
        ),
        reverse=True,
    )[0]
    winner["promoted"] = bool(
        winner["folds_improved"] >= 3
        and winner["mae"] < baseline["mae"]
        and winner["roc_auc"] > baseline["roc_auc"]
    )
    summary = {"baseline": baseline, "winner": winner, "experiments": results}
    with (output_dir / "experiment_summary.json").open("w", encoding="utf-8") as stream:
        json.dump(summary, stream, indent=2, ensure_ascii=True)
    pd.DataFrame(results).to_csv(output_dir / "experiment_summary.csv", index=False)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Run weather feature ablations.")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--weather", type=Path, default=DEFAULT_WEATHER)
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--metadata-path", type=Path, default=DEFAULT_METADATA)
    parser.add_argument("--rf-estimators", type=int, default=400)
    args = parser.parse_args()

    merged = build_experiment_dataset(args.source, args.weather, args.dataset)
    base_features = read_base_features(args.metadata_path)
    available_groups = {
        name: eligible_weather_features(merged, columns)
        for name, columns in FEATURE_GROUPS.items()
    }
    experiments = [{"name": "baseline", "features": base_features}]
    for name, columns in available_groups.items():
        experiments.append({"name": name, "features": base_features + columns})
    all_new = [column for columns in available_groups.values() for column in columns]
    experiments.append({"name": "all_weather", "features": base_features + all_new})

    args.output_dir.mkdir(parents=True, exist_ok=True)
    base_config = BenchmarkConfig(
        dataset=str(args.dataset),
        output_dir="",
        date_column="FECHA_DIA",
        target_column="EVENTOS",
        critical_threshold=7.0,
        decision_threshold=0.30,
        n_splits=5,
        inner_splits=4,
        feature_source="",
        metadata_path=str(args.metadata_path),
        zero_policy="include",
        rf_estimators=args.rf_estimators,
        random_state=42,
    )
    for experiment in experiments:
        output = args.output_dir / str(experiment["name"])
        config = replace(
            base_config,
            output_dir=str(output),
            feature_source=",".join(experiment["features"]),
        )
        run_benchmark(config)
        print(f"completed={experiment['name']}")

    summary = select_summary(args.output_dir, experiments)
    print(f"winner={summary['winner']['name']}")
    print(f"promoted={summary['winner']['promoted']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
