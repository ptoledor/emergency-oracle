"""Fetch hourly Open-Meteo forecasts as issued 0-6 days before each hour.

The Previous Runs API stores, for every hour, the value forecast by the run
issued N days earlier (`<variable>_previous_dayN`). The un-suffixed variable is
the most recent run (lead 0), comparable to the Historical Forecast API used in
training. Output: 05_research/data/previous_runs_hourly.csv (one row per hour).
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
import time
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from features import OPEN_METEO_HOURLY_VARIABLES


LATITUDE = -36.731106
LONGITUDE = -73.11023
PREVIOUS_RUNS_URL = "https://previous-runs-api.open-meteo.com/v1/forecast"
MAX_LEAD_DAYS = 6
OUTPUT = PROJECT_ROOT / "05_research" / "data" / "previous_runs_hourly.csv"


def lead_column(variable, lead):
    return variable if lead == 0 else f"{variable}_previous_day{lead}"


def fetch_chunk(start, end):
    hourly = [
        lead_column(variable, lead)
        for lead in range(MAX_LEAD_DAYS + 1)
        for variable in OPEN_METEO_HOURLY_VARIABLES
    ]
    params = {
        "latitude": LATITUDE, "longitude": LONGITUDE,
        "start_date": str(start), "end_date": str(end),
        "hourly": ",".join(hourly), "timezone": "America/Santiago",
    }
    last_error = None
    for attempt in range(4):
        try:
            response = requests.get(PREVIOUS_RUNS_URL, params=params, timeout=180)
            response.raise_for_status()
            return pd.DataFrame(response.json()["hourly"])
        except Exception as exc:
            last_error = exc
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Open-Meteo previous runs failed for {start}..{end}: {last_error}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2024-02-01")
    parser.add_argument("--end", default="2026-04-22")
    parser.add_argument("--chunk-days", type=int, default=90)
    args = parser.parse_args()

    start = dt.date.fromisoformat(args.start)
    end = dt.date.fromisoformat(args.end)
    chunks = []
    while start <= end:
        chunk_end = min(start + dt.timedelta(days=args.chunk_days - 1), end)
        print(f"Fetching {start} .. {chunk_end}", flush=True)
        chunks.append(fetch_chunk(start, chunk_end))
        start = chunk_end + dt.timedelta(days=1)
        time.sleep(1)

    hourly = pd.concat(chunks, ignore_index=True).drop_duplicates("time")
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    hourly.to_csv(OUTPUT, sep=";", index=False)
    missing = hourly.drop(columns="time").isna().mean()
    print(f"Saved {len(hourly)} hours to {OUTPUT}")
    print("Columns with missing values (share):")
    print(missing[missing > 0].sort_values(ascending=False).to_string(float_format="%.3f"))


if __name__ == "__main__":
    main()
