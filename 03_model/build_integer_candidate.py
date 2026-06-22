"""Build and score an integer-output candidate from a versioned XGBoost model."""

from __future__ import annotations

import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from model_components import IntegerRoundedRegressor


def load_pickle(path: Path):
    with path.open("rb") as stream:
        return pickle.load(stream)


def dump_pickle(value, path: Path) -> None:
    with path.open("wb") as stream:
        pickle.dump(value, stream)


def main() -> int:
    models_dir = PROJECT_ROOT / "03_model" / "saved_models"
    source = "repeated_5fold_30seeds_xgboost"
    candidate = "integer_repeated_5fold_30seeds_xgboost"

    base_model = load_pickle(models_dir / f"regressor_{source}.pkl")
    classifier = load_pickle(models_dir / f"classifier_{source}.pkl")
    metadata = dict(load_pickle(models_dir / f"metadata_{source}.pkl"))
    predictions = pd.read_csv(
        models_dir / f"{source}_oof_predictions.csv", sep=";"
    )

    expected = pd.to_numeric(
        predictions["PRED_EVENTOS_KFOLD_OOF"], errors="raise"
    ).to_numpy(dtype=float)
    actual = pd.to_numeric(predictions["EVENTOS"], errors="raise").to_numpy(dtype=float)
    rounded = np.floor(np.clip(expected, 0, None) + 0.5).astype(int)
    mse = float(mean_squared_error(actual, rounded))

    metadata.update(
        {
            "model_role": "integer_output_candidate",
            "validation_protocol": "Repeated 5-Fold Cross-Validation (30 seeds) · Integer Output",
            "operational_use": False,
            "is_primary": False,
            "regressor_type": "IntegerRoundedXGBRegressor",
            "base_regressor_type": "XGBRegressor",
            "rounding_rule": "nearest_non_negative_integer_half_up",
            "continuous_mae": float(metadata["mae"]),
            "continuous_mse": float(metadata["mse"]),
            "continuous_rmse": float(metadata["rmse"]),
            "continuous_r2": float(metadata["r2"]),
            "mae": float(mean_absolute_error(actual, rounded)),
            "mse": mse,
            "rmse": float(mse**0.5),
            "r2": float(r2_score(actual, rounded)),
            "prediction_std": float(np.std(rounded, ddof=1)),
            "target_std": float(np.std(actual, ddof=1)),
            "variability_ratio": float(np.std(rounded, ddof=1) / np.std(actual, ddof=1)),
        }
    )

    dump_pickle(
        IntegerRoundedRegressor(base_model),
        models_dir / f"regressor_{candidate}.pkl",
    )
    dump_pickle(classifier, models_dir / f"classifier_{candidate}.pkl")
    dump_pickle(metadata, models_dir / f"metadata_{candidate}.pkl")

    output = predictions.copy()
    output["PRED_EVENTOS_CONTINUOUS_OOF"] = expected
    output["PRED_EVENTOS_INTEGER_OOF"] = rounded
    output.to_csv(
        models_dir / f"{candidate}_oof_predictions.csv", sep=";", index=False
    )

    print(
        f"MAE={metadata['mae']:.6f} RMSE={metadata['rmse']:.6f} "
        f"R2={metadata['r2']:.6f} variability={metadata['variability_ratio']:.6f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
