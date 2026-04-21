#!/usr/bin/env python3
"""Backfill structured strategy-decision metadata from legacy reason strings."""

import json
import re
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import Config


FIELD_PATTERNS = {
    "blockers_json": re.compile(r"blockers=([^|]+)"),
    "cautions_json": re.compile(r"cautions=([^|]+)"),
    "candidate_signal_type": re.compile(r"signal_type=([^|]+)"),
    "candidate_signal_grade": re.compile(r"signal_grade=([^|]+)"),
    "candidate_confidence": re.compile(r"confidence=([^|]+)"),
}


def extract_list(reason, field_name):
    match = FIELD_PATTERNS[field_name].search(reason or "")
    if not match:
        return []
    return [item.strip() for item in match.group(1).split(",") if item.strip()]


def extract_scalar(reason, field_name):
    match = FIELD_PATTERNS[field_name].search(reason or "")
    if not match:
        return None
    return match.group(1).strip()


def main():
    with psycopg2.connect(Config.get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, reason
                FROM strategy_decisions_5m
                WHERE blockers_json IS NULL
                   OR cautions_json IS NULL
                   OR candidate_signal_type IS NULL
                   OR candidate_signal_grade IS NULL
                   OR candidate_confidence IS NULL
                   OR actionable_block_reason IS NULL;
                """
            )
            rows = cur.fetchall()
            updated = 0
            for row_id, reason in rows:
                blockers = extract_list(reason, "blockers_json")
                cautions = extract_list(reason, "cautions_json")
                candidate_type = extract_scalar(reason, "candidate_signal_type")
                candidate_grade = extract_scalar(reason, "candidate_signal_grade")
                candidate_confidence = extract_scalar(reason, "candidate_confidence")
                actionable_block_reason = None
                if reason and reason.startswith("Option-buyer filter blocked live alert"):
                    actionable_block_reason = "option_buyer_filter"

                cur.execute(
                    """
                    UPDATE strategy_decisions_5m
                    SET blockers_json = %s,
                        cautions_json = %s,
                        candidate_signal_type = COALESCE(candidate_signal_type, %s),
                        candidate_signal_grade = COALESCE(candidate_signal_grade, %s),
                        candidate_confidence = COALESCE(candidate_confidence, %s),
                        actionable_block_reason = COALESCE(actionable_block_reason, %s)
                    WHERE id = %s;
                    """,
                    (
                        json.dumps(blockers),
                        json.dumps(cautions),
                        candidate_type,
                        candidate_grade,
                        candidate_confidence,
                        actionable_block_reason,
                        row_id,
                    ),
                )
                updated += 1

    print(f"Backfilled {updated} strategy_decisions_5m rows.")


if __name__ == "__main__":
    main()
