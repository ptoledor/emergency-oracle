"""
Monitor en tiempo real del scraper.
Uso: python scraper/monitor.py
"""
import json
import os
import time
from pathlib import Path
from datetime import date, timedelta

DATA_DIR = Path(__file__).parent.parent / "data"
PROGRESS_FILE = DATA_DIR / "raw" / "progress.json"
OUTPUT_RAW = DATA_DIR / "raw" / "tweets_raw.csv"
LOG_FILE = Path(__file__).parent / "scraper.log"

START_DATE = date(2024, 1, 1)
END_DATE = date(2025, 1, 1)

ALL_DAYS = []
d = START_DATE
while d < END_DATE:
    ALL_DAYS.append(d.strftime("%Y-%m-%d"))
    d += timedelta(days=1)


def clear():
    os.system("cls" if os.name == "nt" else "clear")


def load_data():
    done = set()
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            done = set(json.load(f).get("done", []))

    tweet_counts = {}
    if OUTPUT_RAW.exists():
        with open(OUTPUT_RAW, encoding="utf-8") as f:
            next(f)  # header
            for line in f:
                fecha = line.split(";")[0][:7]
                tweet_counts[fecha] = tweet_counts.get(fecha, 0) + 1

    return done, tweet_counts


def tail_log(n=15):
    if not LOG_FILE.exists():
        return []
    with open(LOG_FILE, encoding="utf-8", errors="replace") as f:
        lines = f.readlines()
    return [l.rstrip() for l in lines[-n:] if l.strip()]


def render():
    done, tweet_counts = load_data()
    done_2024 = [d for d in ALL_DAYS if d in done]
    pending_2024 = [d for d in ALL_DAYS if d not in done]

    total = len(ALL_DAYS)
    n_done = len(done_2024)
    pct = n_done / total * 100

    bar_len = 40
    filled = int(bar_len * n_done / total)
    bar = "#" * filled + "-" * (bar_len - filled)

    print("=" * 60)
    print("  SCRAPER MONITOR — @Central_CBT 2024")
    print("=" * 60)
    print(f"  Progreso: [{bar}] {pct:.1f}%")
    print(f"  Dias done: {n_done}/{total}  |  Pendientes: {len(pending_2024)}")
    if pending_2024:
        print(f"  Proximo pendiente: {pending_2024[-1]}")
    print()

    print("  Tweets por mes (2024):")
    months = [f"2024-{m:02d}" for m in range(1, 13)]
    for m in months:
        count = tweet_counts.get(m, 0)
        days_done = sum(1 for d in done_2024 if d.startswith(m))
        days_total = sum(1 for d in ALL_DAYS if d.startswith(m))
        bar2_len = 20
        bar2_filled = int(bar2_len * days_done / days_total) if days_total else 0
        bar2 = "#" * bar2_filled + "." * (bar2_len - bar2_filled)
        print(f"  {m}  [{bar2}] {days_done:2d}/{days_total} dias  {count:4d} tweets")

    total_tweets = sum(tweet_counts.get(m, 0) for m in months)
    print(f"\n  Total tweets 2024: {total_tweets}")

    print()
    print("  Ultimas lineas del log:")
    print("  " + "-" * 56)
    for line in tail_log(12):
        # trim long lines
        trimmed = line[:100]
        print(f"  {trimmed}")

    print()
    print(f"  [actualiza cada 20s — Ctrl+C para salir]")
    print("=" * 60)


def main():
    try:
        while True:
            clear()
            render()
            time.sleep(20)
    except KeyboardInterrupt:
        print("\nMonitor detenido.")


if __name__ == "__main__":
    main()
