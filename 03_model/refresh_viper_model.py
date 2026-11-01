"""Refresco mensual del modelo operacional con un nuevo dump de Viper Talcahuano.

Pasos (todos con los scripts validados el 2026-11-01):
1. build_viper_dataset.py   -> compiled_scraped_data_viper.csv + viper_coverage.csv
2. extend_weather.py        -> agrega al cache oficial los dias nuevos
3. clean_and_augment.py     -> augmented_emergency_data_viper_mensajes.csv
4. copia ambos archivos a los nombres canonicos que lee el serving
5. viper_wf.py              -> walk-forward 6x120 (OOF para calibrar)
6. build_viper_candidate.py --promote -> reescribe el modelo activo

Antes de reescribir, el modelo vigente se respalda en saved_models/archive/<train_end_date>/.
Uso: copiar los JSON nuevos a 01_scraper/talcahuano/ y correr
    .venv/bin/python 03_model/refresh_viper_model.py
"""
from __future__ import annotations

import pickle
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "02_data"
MODELS = ROOT / "03_model" / "saved_models"
SUFFIX = "signal_hydro_ensemble_v2_viper_mensajes"


def run(*args):
    print(f"\n>>> {' '.join(str(a) for a in args)}", flush=True)
    subprocess.run([sys.executable, *map(str, args)], cwd=ROOT, check=True)


def backup_active_model():
    metadata_path = MODELS / f"metadata_{SUFFIX}.pkl"
    if not metadata_path.exists():
        return None
    with metadata_path.open("rb") as stream:
        metadata = pickle.load(stream)
    target = MODELS / "archive" / metadata["train_end_date"]
    target.mkdir(parents=True, exist_ok=True)
    for path in MODELS.glob(f"*{SUFFIX}*"):
        shutil.copy2(path, target / path.name)
    print(f"Modelo vigente respaldado en {target.relative_to(ROOT)}")
    return metadata


def main():
    run(DATA / "build_viper_dataset.py")
    coverage = pd.read_csv(DATA / "viper_coverage.csv")
    end_date = str(coverage.iloc[:, 0].max())
    run(DATA / "extend_weather.py", "--end-date", end_date)
    with tempfile.TemporaryDirectory() as scratch:
        run(
            DATA / "clean_and_augment.py",
            "--input", DATA / "compiled_scraped_data_viper.csv",
            "--coverage-override", DATA / "viper_coverage.csv",
            "--target", "mensajes",
            "--output", DATA / "augmented_emergency_data_viper_mensajes.csv",
            "--fifth-audit", Path(scratch) / "fifth_audit.csv",
        )
    shutil.copy2(DATA / "augmented_emergency_data_viper_mensajes.csv", DATA / "augmented_emergency_data.csv")
    shutil.copy2(DATA / "compiled_scraped_data_viper.csv", DATA / "compiled_scraped_data.csv")

    run(ROOT / "03_model" / "viper_wf.py", "mensajes", "7")
    previous = backup_active_model()
    run(ROOT / "03_model" / "build_viper_candidate.py", "mensajes", "7", "official", "--promote")

    with (MODELS / f"metadata_{SUFFIX}.pkl").open("rb") as stream:
        current = pickle.load(stream)
    print(f"\nEntrenado hasta {current['train_end_date']} ({current['train_samples']} dias)")
    for key in ("mae", "rmse", "r2", "roc_auc"):
        before = f"{previous[key]:.3f} -> " if previous else ""
        print(f"  {key}: {before}{current[key]:.3f}")


if __name__ == "__main__":
    main()
