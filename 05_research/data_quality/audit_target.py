from __future__ import annotations

import argparse
import csv
import json
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Iterable, Sequence
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo


DEFAULT_TIMEZONE = "America/Santiago"
OPERATIONAL_RE = re.compile(r"\bESTADO\s+DE\s+UNIDADES\b", re.IGNORECASE)
INCIDENT_CODE_RE = re.compile(r"\b\d{1,2}-\d{1,2}(?:-\d{1,2})?\b")
URL_RE = re.compile(r"https?://[^\s;,]+", re.IGNORECASE)
TIME_PREFIX_RE = re.compile(
    r"^\s*(?:EMERGENCIA\s*:\s*)?\d{1,2}:\d{2}\s*,?\s*",
    re.IGNORECASE,
)
DISPATCH_PREFIX_RE = re.compile(
    r"^\s*SALE\s+[A-Z]{1,3}-?\d{1,2}\s+A\s+",
    re.IGNORECASE,
)
UNIT_SUFFIX_RE = re.compile(
    r"(?:,\s*)?(?:[A-Z]{1,3}-?\d{1,2})(?:\s+[A-Z]{1,3}-?\d{1,2})*\s*$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class SourceDay:
    source_date: date
    path: Path
    progress_done: bool
    file_exists: bool
    parse_ok: bool
    row_count: int
    status: str
    coverage_usable: bool
    error: str = ""


@dataclass
class Message:
    message_id: str
    source_date: date
    timestamp_utc: datetime
    timestamp_local: datetime
    text: str
    is_operational: bool
    incident_code: str
    location_key: str
    normalized_text: str
    urls: tuple[str, ...]
    duplicate_group_id: str = ""
    duplicate_reason: str = ""
    is_duplicate_candidate: bool = False

    @property
    def local_date(self) -> date:
        return self.timestamp_local.date()

    @property
    def is_incident_like(self) -> bool:
        return not self.is_operational and bool(
            self.incident_code or re.search(r"\bEMERGENCIA\b", self.text, re.IGNORECASE)
        )


def parse_timestamp(value: str) -> datetime:
    cleaned = value.strip()
    if not cleaned:
        raise ValueError("empty timestamp")
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    parsed = datetime.fromisoformat(cleaned)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalized.upper()
    normalized = URL_RE.sub(" ", normalized)
    normalized = re.sub(r"[^A-Z0-9]+", " ", normalized)
    return " ".join(normalized.split())


def normalize_url(value: str) -> str:
    trimmed = value.rstrip(".,)]}")
    parts = urlsplit(trimmed)
    return urlunsplit(
        (parts.scheme.lower(), parts.netloc.lower(), parts.path.rstrip("/"), parts.query, "")
    )


def extract_urls(text: str) -> tuple[str, ...]:
    return tuple(sorted({normalize_url(match.group(0)) for match in URL_RE.finditer(text)}))


def extract_incident_code(text: str) -> str:
    match = INCIDENT_CODE_RE.search(text)
    return match.group(0) if match else ""


def extract_location_key(text: str, incident_code: str) -> str:
    if not incident_code:
        return ""
    tail = text[text.upper().find(incident_code.upper()) + len(incident_code) :]
    close_parenthesis = tail.find(")")
    if close_parenthesis >= 0:
        tail = tail[close_parenthesis + 1 :]
    tail = TIME_PREFIX_RE.sub("", tail)
    tail = DISPATCH_PREFIX_RE.sub("", tail)
    tail = UNIT_SUFFIX_RE.sub("", tail)
    return normalize_text(tail)


def read_semicolon_csv(path: Path) -> list[dict[str, str]]:
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            with path.open("r", encoding=encoding, newline="") as handle:
                reader = csv.DictReader(handle, delimiter=";")
                if not reader.fieldnames:
                    return []
                normalized_fields = {field.strip().lower() for field in reader.fieldnames}
                if not {"fecha", "texto"}.issubset(normalized_fields):
                    raise ValueError(f"unexpected columns: {reader.fieldnames}")
                rows = []
                for row in reader:
                    rows.append(
                        {
                            "Fecha": next(
                                (value for key, value in row.items() if key.strip().lower() == "fecha"),
                                "",
                            ),
                            "Texto": next(
                                (value for key, value in row.items() if key.strip().lower() == "texto"),
                                "",
                            ),
                        }
                    )
                return rows
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error:
        raise last_error
    raise ValueError(f"could not read {path}")


def load_progress(path: Path) -> set[date]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return {date.fromisoformat(value) for value in payload.get("done", [])}


def discover_source_days(
    scraped_dir: Path,
    progress_done: set[date],
    trust_empty_files: bool,
) -> list[SourceDay]:
    file_by_date: dict[date, Path] = {}
    for path in scraped_dir.glob("tweets_????-??-??.csv"):
        try:
            file_by_date[date.fromisoformat(path.stem.removeprefix("tweets_"))] = path
        except ValueError:
            continue

    all_dates = sorted(progress_done | set(file_by_date))
    results = []
    for source_date in all_dates:
        path = file_by_date.get(source_date, scraped_dir / f"tweets_{source_date}.csv")
        done = source_date in progress_done
        if not path.exists():
            results.append(
                SourceDay(
                    source_date,
                    path,
                    done,
                    False,
                    False,
                    0,
                    "missing_completed" if done else "missing",
                    False,
                )
            )
            continue
        try:
            rows = read_semicolon_csv(path)
        except Exception as exc:
            results.append(
                SourceDay(
                    source_date,
                    path,
                    done,
                    True,
                    False,
                    0,
                    "unreadable",
                    False,
                    str(exc),
                )
            )
            continue

        row_count = len(rows)
        if not done:
            status = "file_not_in_progress"
            usable = False
        elif row_count == 0 and not trust_empty_files:
            status = "empty_unverified"
            usable = False
        elif row_count == 0:
            status = "empty_trusted"
            usable = True
        else:
            status = "nonempty_completed"
            usable = True
        results.append(
            SourceDay(
                source_date,
                path,
                done,
                True,
                True,
                row_count,
                status,
                usable,
            )
        )
    return results


def load_daily_messages(
    source_days: Sequence[SourceDay],
    timezone_name: str,
) -> tuple[list[Message], list[dict[str, str]]]:
    local_tz = ZoneInfo(timezone_name)
    messages: list[Message] = []
    parse_errors: list[dict[str, str]] = []
    sequence = 0
    for source in source_days:
        if not source.parse_ok or not source.file_exists:
            continue
        for row_number, row in enumerate(read_semicolon_csv(source.path), start=2):
            try:
                timestamp_utc = parse_timestamp(row.get("Fecha", ""))
            except (TypeError, ValueError) as exc:
                parse_errors.append(
                    {
                        "source_date": source.source_date.isoformat(),
                        "source_file": str(source.path),
                        "row_number": str(row_number),
                        "error": str(exc),
                    }
                )
                continue
            text = row.get("Texto", "") or ""
            code = extract_incident_code(text)
            sequence += 1
            messages.append(
                Message(
                    message_id=f"M{sequence:06d}",
                    source_date=source.source_date,
                    timestamp_utc=timestamp_utc,
                    timestamp_local=timestamp_utc.astimezone(local_tz),
                    text=text,
                    is_operational=bool(OPERATIONAL_RE.search(text)),
                    incident_code=code,
                    location_key=extract_location_key(text, code),
                    normalized_text=normalize_text(text),
                    urls=extract_urls(text),
                )
            )
    messages.sort(key=lambda item: (item.timestamp_utc, item.message_id))
    return messages, parse_errors


def _candidate_keys(message: Message) -> list[tuple[str, str]]:
    keys: list[tuple[str, str]] = []
    keys.extend(("url", url) for url in message.urls)
    if message.incident_code and message.location_key:
        keys.append(("code_location", f"{message.incident_code}|{message.location_key}"))
    if message.normalized_text:
        keys.append(("exact_text", message.normalized_text))
    return keys


def mark_duplicate_candidates(messages: Sequence[Message], window_minutes: int) -> None:
    window = timedelta(minutes=window_minutes)
    previous_by_key: dict[tuple[str, str], Message] = {}
    group_by_message: dict[str, str] = {}
    group_reasons: dict[str, set[str]] = defaultdict(set)
    group_number = 0

    for message in messages:
        if message.is_operational or not message.is_incident_like:
            continue
        matches: list[tuple[Message, str]] = []
        for kind, value in _candidate_keys(message):
            key = (kind, value)
            previous = previous_by_key.get(key)
            if previous and message.timestamp_utc - previous.timestamp_utc <= window:
                matches.append((previous, kind))
            previous_by_key[key] = message
        if not matches:
            continue

        existing_groups = {
            group_by_message[previous.message_id]
            for previous, _ in matches
            if previous.message_id in group_by_message
        }
        if existing_groups:
            group_id = sorted(existing_groups)[0]
        else:
            group_number += 1
            group_id = f"D{group_number:05d}"
        group_by_message[message.message_id] = group_id
        for previous, reason in matches:
            group_by_message[previous.message_id] = group_id
            group_reasons[group_id].add(reason)

    for message in messages:
        group_id = group_by_message.get(message.message_id, "")
        if group_id:
            message.duplicate_group_id = group_id
            message.duplicate_reason = "|".join(sorted(group_reasons[group_id]))
            message.is_duplicate_candidate = True


def required_utc_source_dates(local_day: date, timezone_name: str) -> list[date]:
    local_tz = ZoneInfo(timezone_name)
    start_local = datetime.combine(local_day, time.min, tzinfo=local_tz)
    end_local = datetime.combine(local_day + timedelta(days=1), time.min, tzinfo=local_tz)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc) - timedelta(microseconds=1)
    cursor = start_utc.date()
    required = []
    while cursor <= end_utc.date():
        required.append(cursor)
        cursor += timedelta(days=1)
    return required


def build_daily_target(
    messages: Sequence[Message],
    source_days: Sequence[SourceDay],
    compiled_counts: Counter[date],
    timezone_name: str,
) -> list[dict[str, object]]:
    source_by_date = {item.source_date: item for item in source_days}
    messages_by_day: dict[date, list[Message]] = defaultdict(list)
    for message in messages:
        messages_by_day[message.local_date].append(message)

    available_dates = set(messages_by_day)
    for source_day in source_days:
        available_dates.add(source_day.source_date)
    if not available_dates:
        return []

    first_day = min(available_dates)
    last_day = max(available_dates)
    rows = []
    cursor = first_day
    while cursor <= last_day:
        day_messages = messages_by_day.get(cursor, [])
        required_dates = required_utc_source_dates(cursor, timezone_name)
        source_statuses = [source_by_date.get(item) for item in required_dates]
        coverage_complete = all(item is not None and item.coverage_usable for item in source_statuses)

        non_operational = [item for item in day_messages if not item.is_operational]
        incident_like = [item for item in non_operational if item.is_incident_like]
        duplicate_ids = {
            item.duplicate_group_id for item in incident_like if item.duplicate_group_id
        }
        duplicate_members = sum(1 for item in incident_like if item.duplicate_group_id)
        reconstructed_count = len(incident_like) - duplicate_members + len(duplicate_ids)

        if not coverage_complete:
            state = "coverage_unknown"
            target_count: int | str = ""
        elif reconstructed_count == 0:
            state = "observed_zero"
            target_count = 0
        else:
            state = "observed_nonzero"
            target_count = reconstructed_count

        rows.append(
            {
                "local_date": cursor.isoformat(),
                "day_state": state,
                "target_count": target_count,
                "raw_message_count": len(day_messages),
                "operational_message_count": sum(
                    1 for item in day_messages if item.is_operational
                ),
                "non_operational_message_count": len(non_operational),
                "incident_like_message_count": len(incident_like),
                "duplicate_candidate_members": duplicate_members,
                "reconstructed_incident_count_lower_bound": reconstructed_count,
                "compiled_local_message_count": compiled_counts.get(cursor, 0),
                "required_utc_source_dates": "|".join(
                    item.isoformat() for item in required_dates
                ),
                "source_statuses": "|".join(
                    f"{required}:{source_by_date[required].status}"
                    if required in source_by_date
                    else f"{required}:absent"
                    for required in required_dates
                ),
                "coverage_complete": coverage_complete,
            }
        )
        cursor += timedelta(days=1)
    return rows


def load_compiled_counts(path: Path, timezone_name: str) -> tuple[Counter[date], int]:
    local_tz = ZoneInfo(timezone_name)
    counts: Counter[date] = Counter()
    invalid = 0
    for row in read_semicolon_csv(path):
        try:
            local_day = parse_timestamp(row.get("Fecha", "")).astimezone(local_tz).date()
        except (TypeError, ValueError):
            invalid += 1
            continue
        counts[local_day] += 1
    return counts, invalid


def write_csv(path: Path, rows: Sequence[dict[str, object]], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def source_rows(source_days: Sequence[SourceDay]) -> list[dict[str, object]]:
    return [
        {
            "source_date": item.source_date.isoformat(),
            "source_file": str(item.path),
            "progress_done": item.progress_done,
            "file_exists": item.file_exists,
            "parse_ok": item.parse_ok,
            "row_count": item.row_count,
            "status": item.status,
            "coverage_usable": item.coverage_usable,
            "error": item.error,
        }
        for item in source_days
    ]


def message_rows(messages: Sequence[Message]) -> list[dict[str, object]]:
    return [
        {
            "message_id": item.message_id,
            "source_date": item.source_date.isoformat(),
            "timestamp_utc": item.timestamp_utc.isoformat(),
            "timestamp_local": item.timestamp_local.isoformat(),
            "local_date": item.local_date.isoformat(),
            "is_operational": item.is_operational,
            "is_incident_like": item.is_incident_like,
            "incident_code": item.incident_code,
            "location_key": item.location_key,
            "urls": "|".join(item.urls),
            "duplicate_group_id": item.duplicate_group_id,
            "duplicate_reason": item.duplicate_reason,
            "is_duplicate_candidate": item.is_duplicate_candidate,
            "text": item.text,
        }
        for item in messages
    ]


def duplicate_rows(messages: Sequence[Message]) -> list[dict[str, object]]:
    return [
        row
        for row in message_rows(messages)
        if bool(row["duplicate_group_id"])
    ]


def summary_rows(
    source_days: Sequence[SourceDay],
    messages: Sequence[Message],
    daily_target: Sequence[dict[str, object]],
    parse_errors: Sequence[dict[str, str]],
    compiled_invalid: int,
    timezone_name: str,
    window_minutes: int,
    trust_empty_files: bool,
) -> list[dict[str, object]]:
    states = Counter(str(row["day_state"]) for row in daily_target)
    metrics = {
        "timezone": timezone_name,
        "duplicate_window_minutes": window_minutes,
        "trust_empty_files": trust_empty_files,
        "source_days": len(source_days),
        "source_days_usable": sum(item.coverage_usable for item in source_days),
        "source_days_missing_completed": sum(
            item.status == "missing_completed" for item in source_days
        ),
        "source_days_empty_unverified": sum(
            item.status == "empty_unverified" for item in source_days
        ),
        "messages_loaded": len(messages),
        "operational_messages": sum(item.is_operational for item in messages),
        "incident_like_messages": sum(item.is_incident_like for item in messages),
        "duplicate_candidate_messages": sum(
            item.is_duplicate_candidate for item in messages
        ),
        "message_parse_errors": len(parse_errors),
        "compiled_invalid_timestamps": compiled_invalid,
        "days_observed_zero": states["observed_zero"],
        "days_observed_nonzero": states["observed_nonzero"],
        "days_coverage_unknown": states["coverage_unknown"],
    }
    return [{"metric": key, "value": value} for key, value in metrics.items()]


def run_audit(args: argparse.Namespace) -> dict[str, Path]:
    progress_done = load_progress(args.progress)
    source_days = discover_source_days(
        args.scraped_dir, progress_done, args.trust_empty_files
    )
    messages, parse_errors = load_daily_messages(source_days, args.timezone)
    mark_duplicate_candidates(messages, args.duplicate_window_minutes)
    compiled_counts, compiled_invalid = load_compiled_counts(args.compiled, args.timezone)
    daily_target = build_daily_target(
        messages, source_days, compiled_counts, args.timezone
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "source_coverage": args.output_dir / "source_coverage.csv",
        "message_audit": args.output_dir / "message_audit.csv",
        "duplicate_candidates": args.output_dir / "duplicate_candidates.csv",
        "daily_target": args.output_dir / "daily_target_audit.csv",
        "parse_errors": args.output_dir / "parse_errors.csv",
        "summary": args.output_dir / "summary.csv",
    }
    source_data = source_rows(source_days)
    message_data = message_rows(messages)
    duplicate_data = duplicate_rows(messages)
    summary_data = summary_rows(
        source_days,
        messages,
        daily_target,
        parse_errors,
        compiled_invalid,
        args.timezone,
        args.duplicate_window_minutes,
        args.trust_empty_files,
    )
    write_csv(outputs["source_coverage"], source_data, list(source_data[0]) if source_data else [])
    write_csv(outputs["message_audit"], message_data, list(message_data[0]) if message_data else [])
    write_csv(
        outputs["duplicate_candidates"],
        duplicate_data,
        list(message_data[0]) if message_data else [],
    )
    write_csv(
        outputs["daily_target"],
        daily_target,
        list(daily_target[0]) if daily_target else [],
    )
    write_csv(
        outputs["parse_errors"],
        parse_errors,
        ["source_date", "source_file", "row_number", "error"],
    )
    write_csv(outputs["summary"], summary_data, ["metric", "value"])
    return outputs


def build_parser() -> argparse.ArgumentParser:
    project_root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(
        description=(
            "Audita cobertura y reconstruye un target diario experimental sin "
            "imputar cobertura desconocida como cero."
        )
    )
    parser.add_argument(
        "--compiled",
        type=Path,
        default=project_root / "02_data" / "compiled_scraped_data.csv",
    )
    parser.add_argument(
        "--scraped-dir",
        type=Path,
        default=project_root / "01_scraper" / "scraped_data",
    )
    parser.add_argument(
        "--progress",
        type=Path,
        default=project_root / "01_scraper" / "scraped_data" / "progress.json",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "output",
    )
    parser.add_argument("--timezone", default=DEFAULT_TIMEZONE)
    parser.add_argument("--duplicate-window-minutes", type=int, default=30)
    parser.add_argument(
        "--trust-empty-files",
        action="store_true",
        help="Trata CSV vacios completados como cobertura observada. No recomendado.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.duplicate_window_minutes < 1:
        parser.error("--duplicate-window-minutes debe ser mayor que cero")
    outputs = run_audit(args)
    for label, path in outputs.items():
        print(f"{label}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

