from datetime import datetime

from strategies.shared.setup_helpers import (
    build_entry_plan,
    early_impulse_breakout_ready,
    price_led_hybrid_fallback_ready,
    recent_breakout_context,
    sensex_hybrid_fallback_ready,
    sensex_volume_flexible,
    watch_bucket,
)


def test_watch_bucket_prefers_confirmation_pending_for_retest():
    assert watch_bucket("RETEST", [], []) == "WATCH_CONFIRMATION_PENDING"


def test_recent_breakout_context_accepts_valid_same_day_reclaim():
    memory = {
        "direction": "CE",
        "time": datetime(2026, 4, 16, 10, 0),
        "level": 24000,
        "session_day": datetime(2026, 4, 16, 10, 0).date(),
    }

    assert recent_breakout_context(memory, "CE", datetime(2026, 4, 16, 10, 20), 24010, 23990, 10) is True


def test_build_entry_plan_handles_retest_defaults():
    plan = build_entry_plan("NIFTY", "CE", "RETEST", 24000, None, 20, 23970, 24040)

    assert plan["entry_above"] == 24000
    assert plan["invalidate_price"] == 23970
    assert plan["first_target_price"] == 24012.0


def test_early_impulse_breakout_ready_respects_extension_filter():
    ready = early_impulse_breakout_ready(
        "NIFTY", "CE", 76, "STRONG", True, True, False, "NONE",
        "OPENING", 24040, 24000, 20, 10,
    )

    assert ready is False


def test_sensex_volume_flexible_allows_opening_range_expansion():
    assert sensex_volume_flexible("SENSEX", "WEAK", 64, "OPENING", 30, 40) is True


def test_sensex_hybrid_fallback_ready_requires_orb_and_vwap_alignment():
    ready = sensex_hybrid_fallback_ready(
        "SENSEX", "CE", 62, "OPENING", 77050, 77020, 77040, 76980, 77045, 40, 50,
        True, True, False, True, "MILD",
    )

    assert ready is True


def test_price_led_hybrid_fallback_ready_accepts_trend_day_moderate_conflict():
    ready = price_led_hybrid_fallback_ready(
        "NIFTY", "CE", 70, 65, "MIDDAY", 24050, 24020, 24040, 23980, 24048, 25, 30,
        True, True, False, True, "MODERATE", "NORMAL", trend_day_context=True,
    )

    assert ready is True
