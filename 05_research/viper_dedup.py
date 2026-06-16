from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests


DEFAULT_INPUT = Path("02_data/tweets_procesados.csv")
DEFAULT_CACHE = Path("05_research/data/viper_coordinate_cache.json")
DEFAULT_OUTPUT_DIR = Path("05_research/data/viper_dedup")
SANTIAGO_TZ = "America/Santiago"


@dataclass
class CoordinateResult:
    original_url: str
    final_url: str | None
    x: float | None
    y: float | None
    coordinate_kind: str | None
    status: str
    error: str | None = None


def _empty_result(url: str, status: str, error: str | None = None) -> dict:
    return asdict(
        CoordinateResult(
            original_url=url,
            final_url=None,
            x=None,
            y=None,
            coordinate_kind=None,
            status=status,
            error=error,
        )
    )


def stable_message_id(row: pd.Series) -> str:
    raw = f"{row.get('Fecha', '')}|{row.get('Texto', '')}"
    return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()[:16]


def load_cache(path: Path) -> dict[str, dict]:
    if path.exists():
        with path.open("r", encoding="utf-8") as stream:
            return json.load(stream)
    return {}


def save_cache(path: Path, cache: dict[str, dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        json.dump(cache, stream, indent=2, ensure_ascii=True)


def candidate_pairs(text: str) -> Iterable[tuple[str, float, float]]:
    patterns = [
        (
            "lat_lon_json",
            re.compile(
                r'"(?:latitud|latitude|lat)"\s*:\s*(-?\d{1,3}(?:\.\d+)?)'
                r'\s*,\s*'
                r'"(?:longitud|longitude|lng|lon)"\s*:\s*(-?\d{1,3}(?:\.\d+)?)',
                re.IGNORECASE,
            ),
        ),
        (
            "lat_lon_named",
            re.compile(
                r"(?:latitud|latitude|lat)\D{0,20}(-?\d{1,3}(?:\.\d+)?)"
                r".{0,80}?"
                r"(?:longitud|longitude|lng|lon)\D{0,20}(-?\d{1,3}(?:\.\d+)?)",
                re.IGNORECASE | re.DOTALL,
            ),
        ),
        (
            "lon_lat_named",
            re.compile(
                r"(?:longitud|longitude|lng|lon)\D{0,20}(-?\d{1,3}(?:\.\d+)?)"
                r".{0,80}?"
                r"(?:latitud|latitude|lat)\D{0,20}(-?\d{1,3}(?:\.\d+)?)",
                re.IGNORECASE | re.DOTALL,
            ),
        ),
        (
            "xy_named",
            re.compile(
                r"\b(?:x|utm_x|este|easting)\D{0,20}(-?\d+(?:\.\d+)?)"
                r".{0,80}?"
                r"\b(?:y|utm_y|norte|northing)\D{0,20}(-?\d+(?:\.\d+)?)",
                re.IGNORECASE | re.DOTALL,
            ),
        ),
    ]
    for kind, pattern in patterns:
        for match in pattern.finditer(text):
            first = float(match.group(1))
            second = float(match.group(2))
            if kind == "lon_lat_named":
                yield "lat_lon", second, first
            else:
                yield kind, first, second

    for match in re.finditer(
        r"(-3[0-9](?:\.\d+)?)\s*,\s*(-7[0-9](?:\.\d+)?)",
        text,
    ):
        yield "lat_lon_pair", float(match.group(1)), float(match.group(2))

    for match in re.finditer(
        r"\b([56]\d{5}(?:\.\d+)?)\s*,\s*([5-6]\d{6}(?:\.\d+)?)\b",
        text,
    ):
        yield "utm_pair", float(match.group(1)), float(match.group(2))


def select_coordinate(html: str) -> tuple[float | None, float | None, str | None]:
    for kind, first, second in candidate_pairs(html):
        if kind.startswith("lat_lon") and -38.5 <= first <= -35.0 and -75.0 <= second <= -71.0:
            return first, second, "lat_lon"
        if kind in {"xy_named", "utm_pair"} and 500000 <= first <= 800000 and 5800000 <= second <= 6100000:
            return first, second, "utm_32718"
    return None, None, None


def fetch_coordinate(url: str, timeout: int = 25) -> dict:
    if not isinstance(url, str) or not url.startswith("http"):
        return _empty_result(str(url), "missing_url")
    try:
        response = requests.get(
            url,
            timeout=timeout,
            allow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
                )
            },
        )
        response.raise_for_status()
        x, y, kind = select_coordinate(response.text)
        return asdict(
            CoordinateResult(
                original_url=url,
                final_url=response.url,
                x=x,
                y=y,
                coordinate_kind=kind,
                status="ok" if x is not None and y is not None else "no_coordinate",
            )
        )
    except Exception as error:
        return _empty_result(url, "error", str(error)[:300])


def fetch_coordinate_with_curl(url: str, timeout: int = 25) -> dict:
    if not isinstance(url, str) or not url.startswith("http"):
        return _empty_result(str(url), "missing_url")
    command = [
        "curl.exe",
        "-L",
        "--silent",
        "--show-error",
        "--ssl-no-revoke",
        "--max-time",
        str(timeout),
        "--user-agent",
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
        ),
        "--write-out",
        "\n__FINAL_URL__:%{url_effective}",
        url,
    ]
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout + 5,
        )
        body = completed.stdout or ""
        final_url = None
        marker = "\n__FINAL_URL__:"
        if marker in body:
            body, final_url = body.rsplit(marker, 1)
            final_url = final_url.strip()
        if completed.returncode != 0:
            error = (completed.stderr or body or f"curl exit {completed.returncode}").strip()
            return _empty_result(url, "error", error[:300])
        x, y, kind = select_coordinate(body)
        return asdict(
            CoordinateResult(
                original_url=url,
                final_url=final_url or url,
                x=x,
                y=y,
                coordinate_kind=kind,
                status="ok" if x is not None and y is not None else "no_coordinate",
            )
        )
    except Exception as error:
        return _empty_result(url, "error", str(error)[:300])


def enrich_urls(
    urls: list[str],
    cache_path: Path,
    sleep_seconds: float,
    limit: int | None,
    resolver: str,
    retry_failed: bool,
) -> pd.DataFrame:
    cache = load_cache(cache_path)
    retry_statuses = {"error", "no_coordinate"} if retry_failed else set()
    pending = [
        url for url in urls
        if url and (url not in cache or cache[url].get("status") in retry_statuses)
    ]
    if limit is not None:
        pending = pending[:limit]
    fetcher = fetch_coordinate_with_curl if resolver == "curl" else fetch_coordinate
    for index, url in enumerate(pending, start=1):
        cache[url] = fetcher(url)
        if index % 25 == 0:
            save_cache(cache_path, cache)
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    save_cache(cache_path, cache)
    return pd.DataFrame(cache.values())


def build_deduplicated_messages(
    messages: pd.DataFrame,
    coordinates: pd.DataFrame,
    window_hours: float,
    url_fallback: bool = True,
) -> pd.DataFrame:
    frame = messages.copy()
    frame["message_id"] = frame.apply(stable_message_id, axis=1)
    frame["FECHA_LOCAL"] = (
        pd.to_datetime(frame["Fecha"], utc=True, errors="coerce")
        .dt.tz_convert(SANTIAGO_TZ)
    )
    frame["FECHA_DIA_LOCAL"] = frame["FECHA_LOCAL"].dt.strftime("%Y-%m-%d")
    frame = frame.merge(
        coordinates[
            ["original_url", "final_url", "x", "y", "coordinate_kind", "status"]
        ].rename(columns={"original_url": "URL"}),
        on="URL",
        how="left",
    )
    frame["has_coordinate"] = frame["x"].notna() & frame["y"].notna()
    frame["incident_rank"] = 1
    frame["dedup_reason"] = ""
    window = pd.Timedelta(hours=window_hours)

    def mark_duplicate_group(group: pd.DataFrame, reason_prefix: str) -> None:
        first_time = None
        first_id = None
        for row_index, row in group.iterrows():
            current_time = row["FECHA_LOCAL"]
            if first_time is None or current_time - first_time > window:
                first_time = current_time
                first_id = row["message_id"]
                if not frame.loc[row_index, "dedup_reason"]:
                    frame.loc[row_index, "dedup_reason"] = f"first_at_{reason_prefix}_window"
            else:
                frame.loc[row_index, "incident_rank"] = 0
                frame.loc[
                    row_index, "dedup_reason"
                ] = f"duplicate_{reason_prefix}_2h_of:{first_id}"

    with_coordinates = frame.loc[frame["has_coordinate"]].copy()
    with_coordinates = with_coordinates.sort_values("FECHA_LOCAL")
    for (_, x, y), group in with_coordinates.groupby(
        ["coordinate_kind", "x", "y"], dropna=False
    ):
        mark_duplicate_group(group, "xy")

    if url_fallback:
        without_coordinates = frame.loc[
            frame["has_coordinate"].eq(False)
            & frame["URL"].fillna("").astype(str).str.startswith("http")
        ].copy()
        without_coordinates = without_coordinates.sort_values("FECHA_LOCAL")
        for _, group in without_coordinates.groupby("URL", dropna=False):
            if len(group) > 1:
                mark_duplicate_group(group, "url")

    no_coordinate = frame["has_coordinate"].eq(False)
    frame.loc[
        no_coordinate & frame["dedup_reason"].eq(""),
        "dedup_reason",
    ] = "no_coordinate_keep"
    return frame.sort_values("FECHA_LOCAL").reset_index(drop=True)


def write_daily_target(deduplicated: pd.DataFrame, output_dir: Path) -> pd.DataFrame:
    known = deduplicated.copy()
    known["incident_count"] = known["incident_rank"].astype(int)
    daily = (
        known.groupby("FECHA_DIA_LOCAL", dropna=False)
        .agg(
            target_count=("incident_count", "sum"),
            raw_message_count=("message_id", "count"),
            messages_with_coordinate=("has_coordinate", "sum"),
            duplicate_xy_2h_count=("incident_rank", lambda values: int((values == 0).sum())),
        )
        .reset_index()
        .rename(columns={"FECHA_DIA_LOCAL": "FECHA_DIA"})
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    daily.to_csv(output_dir / "daily_viper_target.csv", index=False)
    deduplicated.to_csv(output_dir / "messages_viper_deduplicated.csv", index=False)
    return daily


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deduplicate CBT messages by Viper coordinates within a 2h window."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--window-hours", type=float, default=2.0)
    parser.add_argument("--sleep-seconds", type=float, default=0.15)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--no-fetch", action="store_true")
    parser.add_argument("--resolver", choices=["requests", "curl"], default="requests")
    parser.add_argument("--retry-failed", action="store_true")
    parser.add_argument("--no-url-fallback", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    messages = pd.read_csv(args.input, sep=";")
    urls = sorted(
        url for url in messages.get("URL", pd.Series(dtype=str)).dropna().unique()
        if isinstance(url, str) and url.startswith("http")
    )
    if args.no_fetch:
        cache = load_cache(args.cache)
        coordinates = pd.DataFrame(cache.values())
    else:
        coordinates = enrich_urls(
            urls,
            args.cache,
            args.sleep_seconds,
            args.limit,
            args.resolver,
            args.retry_failed,
        )
    if coordinates.empty:
        coordinates = pd.DataFrame(
            columns=["original_url", "final_url", "x", "y", "coordinate_kind", "status"]
        )
    deduplicated = build_deduplicated_messages(
        messages,
        coordinates,
        args.window_hours,
        url_fallback=not args.no_url_fallback,
    )
    daily = write_daily_target(deduplicated, args.output_dir)
    summary = {
        "messages": int(len(deduplicated)),
        "unique_urls": int(len(urls)),
        "coordinates_ok": int((coordinates.get("status") == "ok").sum()) if "status" in coordinates else 0,
        "duplicates_removed": int((deduplicated["incident_rank"] == 0).sum()),
        "duplicates_by_xy": int(deduplicated["dedup_reason"].astype(str).str.startswith("duplicate_xy_").sum()),
        "duplicates_by_url": int(deduplicated["dedup_reason"].astype(str).str.startswith("duplicate_url_").sum()),
        "daily_rows": int(len(daily)),
        "window_hours": args.window_hours,
    }
    with (args.output_dir / "summary.json").open("w", encoding="utf-8") as stream:
        json.dump(summary, stream, indent=2, ensure_ascii=True)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
