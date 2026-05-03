from datetime import datetime

from tools.review_option_signals import build_daily_summary, build_recommendations


def test_build_daily_summary_groups_buckets_and_setups():
    rows = [
        (
            datetime(2026, 4, 30, 9, 35),
            "NIFTY",
            "CE",
            24000,
            120.0,
            23980.0,
            78,
            "OPENING_DRIVE",
            "best liquidity",
            91.0,
            14.0,
            1,
            18.0,
            145.0,
            24000,
            120.0,
            91.0,
            14.0,
            18.0,
            145.0,
        ),
        (
            datetime(2026, 4, 30, 12, 5),
            "NIFTY",
            "PE",
            23850,
            98.0,
            23910.0,
            66,
            "BREAKOUT",
            "selected by edge",
            74.0,
            8.0,
            2,
            -6.0,
            104.0,
            23800,
            101.0,
            79.0,
            11.0,
            10.0,
            110.0,
        ),
    ]

    summary = build_daily_summary(rows)

    assert summary["total_signals"] == 2
    assert summary["chosen_top_rank_count"] == 1
    assert summary["better_alternative_count"] == 1
    assert summary["positive_pnl_count"] == 1
    assert summary["time_buckets"]["OPENING"]["signals"] == 1
    assert summary["time_buckets"]["MIDDAY"]["signals"] == 1
    assert summary["setups"]["OPENING_DRIVE"]["wins"] == 1


def test_build_recommendations_flags_bad_bucket_and_strike_selection():
    summary = {
        "total_signals": 6,
        "chosen_top_rank_count": 2,
        "better_alternative_count": 3,
        "positive_pnl_count": 1,
        "pnl_count": 6,
        "time_buckets": {
            "MIDDAY": {"signals": 3, "wins": 0, "sum_pnl": -12.0, "pnl_count": 3},
            "OPENING": {"signals": 3, "wins": 1, "sum_pnl": 3.0, "pnl_count": 3},
        },
        "setups": {
            "BREAKOUT": {"signals": 3, "wins": 0, "sum_pnl": -15.0, "pnl_count": 3},
            "OPENING_DRIVE": {"signals": 2, "wins": 1, "sum_pnl": 9.0, "pnl_count": 2},
        },
    }

    recommendations = build_recommendations(summary)

    assert any("top-ranked candidate" in item for item in recommendations)
    assert any("MIDDAY bucket weak" in item for item in recommendations)
