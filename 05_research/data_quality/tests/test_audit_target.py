from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path
from zoneinfo import ZoneInfo


MODULE_DIR = Path(__file__).resolve().parents[1]
FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
sys.path.insert(0, str(MODULE_DIR))

from audit_target import (  # noqa: E402
    SourceDay,
    build_daily_target,
    discover_source_days,
    extract_incident_code,
    extract_location_key,
    load_daily_messages,
    mark_duplicate_candidates,
    parse_timestamp,
)

class TimestampTests(unittest.TestCase):
    def test_utc_timestamp_moves_to_previous_santiago_day(self) -> None:
        parsed = parse_timestamp("2022-05-07T03:00:00.000Z")
        local = parsed.astimezone(ZoneInfo("America/Santiago"))
        self.assertEqual(local.date(), date(2022, 5, 6))


class CoverageTests(unittest.TestCase):
    def test_empty_completed_file_is_unknown_by_default(self) -> None:
        root = FIXTURES_DIR / "empty"
        days = discover_source_days(root, {date(2022, 5, 7)}, False)
        self.assertEqual(days[0].status, "empty_unverified")
        self.assertFalse(days[0].coverage_usable)

    def test_missing_completed_file_is_not_zero(self) -> None:
        root = FIXTURES_DIR / "missing"
        days = discover_source_days(root, {date(2022, 5, 7)}, False)
        self.assertEqual(days[0].status, "missing_completed")
        self.assertFalse(days[0].coverage_usable)


class DuplicateTests(unittest.TestCase):
    def test_dispatch_with_same_code_and_location_is_candidate(self) -> None:
        daily = FIXTURES_DIR / "duplicate" / "tweets_2022-05-07.csv"
        source = SourceDay(
            date(2022, 5, 7),
            daily,
            True,
            True,
            True,
            2,
            "nonempty_completed",
            True,
        )
        messages, errors = load_daily_messages([source], "America/Santiago")
        self.assertFalse(errors)
        self.assertEqual(extract_incident_code(messages[0].text), "10-4")
        self.assertEqual(
            extract_location_key(messages[0].text, "10-4"),
            extract_location_key(messages[1].text, "10-4"),
        )
        mark_duplicate_candidates(messages, 30)
        self.assertTrue(all(item.is_duplicate_candidate for item in messages))
        self.assertEqual(messages[0].duplicate_group_id, messages[1].duplicate_group_id)


class TargetTests(unittest.TestCase):
    def test_unknown_coverage_leaves_target_blank(self) -> None:
        source = SourceDay(
            date(2022, 5, 7),
            Path("missing.csv"),
            True,
            False,
            False,
            0,
            "missing_completed",
            False,
        )
        rows = build_daily_target([], [source], {}, "America/Santiago")
        self.assertEqual(rows[0]["day_state"], "coverage_unknown")
        self.assertEqual(rows[0]["target_count"], "")

    def test_complete_coverage_can_produce_observed_zero(self) -> None:
        sources = [
            SourceDay(
                source_date,
                Path(f"tweets_{source_date}.csv"),
                True,
                True,
                True,
                0,
                "empty_trusted",
                True,
            )
            for source_date in (date(2022, 5, 7), date(2022, 5, 8))
        ]
        rows = build_daily_target([], sources, {}, "America/Santiago")
        self.assertEqual(rows[0]["day_state"], "observed_zero")
        self.assertEqual(rows[0]["target_count"], 0)


if __name__ == "__main__":
    unittest.main()
