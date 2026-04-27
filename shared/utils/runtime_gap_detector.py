"""Classify runtime loop gaps into real system sleep vs generic processing stalls."""

import time


class RuntimeGapDetector:
    def __init__(self, threshold_seconds=60, sleep_confirmation_seconds=20):
        self.threshold_seconds = threshold_seconds
        self.sleep_confirmation_seconds = sleep_confirmation_seconds
        self.last_wall_time = time.time()
        self.last_monotonic_time = time.monotonic()

    def check(self):
        now_wall = time.time()
        now_monotonic = time.monotonic()

        wall_gap = now_wall - self.last_wall_time
        active_gap = now_monotonic - self.last_monotonic_time
        suspended_gap = max(0.0, wall_gap - active_gap)

        self.last_wall_time = now_wall
        self.last_monotonic_time = now_monotonic

        if wall_gap <= self.threshold_seconds:
            return None

        kind = (
            "system_sleep"
            if suspended_gap >= self.sleep_confirmation_seconds
            else "processing_gap"
        )

        return {
            "kind": kind,
            "wall_gap": wall_gap,
            "active_gap": active_gap,
            "suspended_gap": suspended_gap,
        }
