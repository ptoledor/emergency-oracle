from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


DEFAULT_SOURCE = Path("02_data/augmented_emergency_data.csv")
DEFAULT_AUDIT = Path("05_research/data_quality/output/daily_target_audit.csv")
DEFAULT_OUTPUT = Path("05_research/data/experimental_target_dataset.csv")


def read_delimited(path: Path) -> tuple[pd.DataFrame, str]:
    with path.open("r", encoding="utf-8-sig") as stream:
        first_line = stream.readline()
    separator = ";" if first_line.count(";") > first_line.count(",") else ","
    return pd.read_csv(path, sep=separator), separator


def build_dataset(source_path: Path, audit_path: Path) -> pd.DataFrame:
    source, _ = read_delimited(source_path)
    audit, _ = read_delimited(audit_path)

    source["FECHA_DIA"] = pd.to_datetime(
        source["FECHA_DIA"], errors="raise"
    ).dt.normalize()
    audit["local_date"] = pd.to_datetime(
        audit["local_date"], errors="raise"
    ).dt.normalize()
    if source["FECHA_DIA"].duplicated().any():
        raise ValueError("Source dataset has duplicate FECHA_DIA values.")
    if audit["local_date"].duplicated().any():
        raise ValueError("Audit dataset has duplicate local_date values.")

    audit_columns = audit[
        ["local_date", "day_state", "target_count", "coverage_complete"]
    ].rename(
        columns={
            "local_date": "FECHA_DIA",
            "day_state": "TARGET_COVERAGE_STATE",
            "target_count": "EVENTOS_AUDITADOS",
            "coverage_complete": "TARGET_COVERAGE_COMPLETE",
        }
    )
    result = source.merge(
        audit_columns, on="FECHA_DIA", how="left", validate="one_to_one"
    )
    result = result.rename(columns={"EVENTOS": "EVENTOS_ORIGINAL"})
    result["EVENTOS"] = pd.to_numeric(
        result["EVENTOS_AUDITADOS"], errors="coerce"
    )
    missing_audit = result["TARGET_COVERAGE_STATE"].isna()
    result.loc[missing_audit, "TARGET_COVERAGE_STATE"] = "coverage_unknown"
    result.loc[missing_audit, "TARGET_COVERAGE_COMPLETE"] = False

    invalid_zero = (
        result["EVENTOS"].eq(0)
        & result["TARGET_COVERAGE_STATE"].ne("observed_zero")
    )
    if invalid_zero.any():
        raise ValueError("Zero targets must be explicitly marked observed_zero.")
    invalid_unknown = (
        result["TARGET_COVERAGE_STATE"].eq("coverage_unknown")
        & result["EVENTOS"].notna()
    )
    if invalid_unknown.any():
        raise ValueError("coverage_unknown rows must keep EVENTOS empty.")

    ordered = [
        "FECHA_DIA",
        "EVENTOS",
        "EVENTOS_ORIGINAL",
        "TARGET_COVERAGE_STATE",
        "TARGET_COVERAGE_COMPLETE",
    ]
    remaining = [column for column in result.columns if column not in ordered]
    return result[ordered + remaining].sort_values("FECHA_DIA").reset_index(drop=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Join the audited daily target to the experimental feature set."
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--audit", type=Path, default=DEFAULT_AUDIT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dataset = build_dataset(args.source, args.audit)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_csv(args.output, sep=";", index=False, encoding="utf-8")

    observed = dataset["EVENTOS"].notna()
    print(f"rows={len(dataset)}")
    print(f"observed_rows={int(observed.sum())}")
    print(f"unknown_rows={int((~observed).sum())}")
    print(f"observed_zero_rows={int(dataset['EVENTOS'].eq(0).sum())}")
    print(f"output={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
