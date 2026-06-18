"""Deduplicación de mensajes incident-like y asignación de fecha local.

Usado por clean_and_augment.py, predict_tomorrow.py y audit_target.py
para mantener una sola implementación consistente.
"""
from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

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

DEFAULT_TIMEZONE = "America/Santiago"
DEFAULT_DUPLICATE_WINDOW_MINUTES = 30


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
    return tuple(sorted({normalize_url(m.group(0)) for m in URL_RE.finditer(text)}))


def extract_incident_code(text: str) -> str:
    match = INCIDENT_CODE_RE.search(text)
    return match.group(0) if match else ""


def extract_location_key(text: str, incident_code: str) -> str:
    if not incident_code:
        return ""
    tail = text[text.upper().find(incident_code.upper()) + len(incident_code):]
    close_paren = tail.find(")")
    if close_paren >= 0:
        tail = tail[close_paren + 1:]
    tail = TIME_PREFIX_RE.sub("", tail)
    tail = DISPATCH_PREFIX_RE.sub("", tail)
    tail = UNIT_SUFFIX_RE.sub("", tail)
    return normalize_text(tail)


def is_incident_like(text: str, incident_code: str) -> bool:
    is_operational = bool(OPERATIONAL_RE.search(text))
    return (not is_operational) and bool(
        incident_code or re.search(r"\bEMERGENCIA\b", text, re.IGNORECASE)
    )


def assign_local_date(timestamp_utc: datetime, timezone_name: str = DEFAULT_TIMEZONE) -> str:
    """Convierte un timestamp UTC a fecha local (YYYY-MM-DD) de forma segura."""
    if timestamp_utc.tzinfo is None:
        timestamp_utc = timestamp_utc.replace(tzinfo=timezone.utc)
    local_tz = ZoneInfo(timezone_name)
    return timestamp_utc.astimezone(local_tz).strftime("%Y-%m-%d")


def mark_duplicates(
    df_messages: pd.DataFrame,
    timestamp_col: str,
    text_col: str,
    window_minutes: int = DEFAULT_DUPLICATE_WINDOW_MINUTES,
) -> tuple[dict, dict]:
    """Marca duplicados incident-like dentro de una ventana temporal.

    Returns:
        (dup_flags, incident_flags) — diccionarios {row_index: bool}.
    """
    window = timedelta(minutes=window_minutes)
    msgs = []
    for idx, row in df_messages.iterrows():
        text = str(row[text_col]) if pd.notna(row[text_col]) else ""
        code = extract_incident_code(text)
        msgs.append({
            "idx": idx,
            "timestamp": pd.to_datetime(row[timestamp_col], utc=True),
            "text": text,
            "incident_code": code,
            "location_key": extract_location_key(text, code),
            "normalized_text": normalize_text(text),
            "urls": extract_urls(text),
            "is_incident_like": is_incident_like(text, code),
            "is_duplicate": False,
        })
    msgs.sort(key=lambda m: (m["timestamp"], m["idx"]))

    previous_by_key: dict[tuple[str, str], dict] = {}
    group_by_msg: dict = {}
    group_reasons: dict = {}
    group_number = 0

    for msg in msgs:
        if not msg["is_incident_like"]:
            continue
        keys: list[tuple[str, str]] = [("url", u) for u in msg["urls"]]
        if msg["normalized_text"]:
            keys.append(("exact_text", msg["normalized_text"]))

        matches = []
        for kind, value in keys:
            key = (kind, value)
            prev = previous_by_key.get(key)
            if prev and msg["timestamp"] - prev["timestamp"] <= window:
                matches.append((prev, kind))
            previous_by_key[key] = msg

        if not matches:
            continue

        existing = {group_by_msg[p["idx"]] for p, _ in matches if p["idx"] in group_by_msg}
        if existing:
            gid = sorted(existing)[0]
        else:
            group_number += 1
            gid = f"D{group_number:05d}"
        group_by_msg[msg["idx"]] = gid
        for prev, reason in matches:
            group_by_msg[prev["idx"]] = gid
            group_reasons.setdefault(gid, set()).add(reason)

    for msg in msgs:
        if msg["idx"] in group_by_msg:
            msg["is_duplicate"] = True

    dup_flags = {m["idx"]: m["is_duplicate"] for m in msgs}
    incident_flags = {m["idx"]: m["is_incident_like"] for m in msgs}
    return dup_flags, incident_flags
