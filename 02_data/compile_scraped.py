"""Compile the scraper's daily CSVs into 02_data/compiled_scraped_data.csv.

Replaces the first cells of the former process_data.ipynb. Messages before
2022-01-04 are excluded: the historical dataset starts there, and earlier
posts use a different format.

Usage: python 02_data/compile_scraped.py
"""

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
SCRAPED_DIR = BASE_DIR / "01_scraper" / "scraped_data"
OUTPUT_PATH = BASE_DIR / "02_data" / "compiled_scraped_data.csv"
START_TIMESTAMP = "2022-01-04"


def compile_scraped(scraped_dir=SCRAPED_DIR):
    files = sorted(scraped_dir.glob("tweets_*.csv"))
    if not files:
        raise FileNotFoundError(f"No hay archivos tweets_*.csv en {scraped_dir}")
    messages = pd.concat(
        [pd.read_csv(path, sep=";", decimal=",", dtype=str) for path in files],
        ignore_index=True,
    )
    return messages[messages["Fecha"] >= START_TIMESTAMP].reset_index(drop=True)


def main():
    messages = compile_scraped()
    messages.to_csv(OUTPUT_PATH, sep=";", index=False)
    print(
        f"{len(messages)} mensajes ({messages['Fecha'].min()} .. {messages['Fecha'].max()}) "
        f"guardados en {OUTPUT_PATH}"
    )


if __name__ == "__main__":
    main()
