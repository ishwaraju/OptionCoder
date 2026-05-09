from datetime import datetime, timedelta

from services.oi_collector import OICollector


def _row(ts, strike, option_type, ltp, volume=1000, oi=5000, distance=0):
    return {
        "ts": ts,
        "atm_strike": 24200,
        "strike": strike,
        "distance_from_atm": distance,
        "option_type": option_type,
        "oi": oi,
        "volume": volume,
        "ltp": ltp,
        "iv": 18.0,
        "top_bid_price": ltp - 0.5,
        "top_bid_quantity": 75,
        "top_ask_price": ltp + 0.5,
        "top_ask_quantity": 75,
        "spread": 1.0,
    }


def test_option_market_state_marks_expanding_liquid_side_ready():
    current_ts = datetime(2026, 5, 8, 10, 5)
    previous_ts = current_ts - timedelta(minutes=1)
    collector = OICollector.__new__(OICollector)
    collector.instrument = "NIFTY"
    collector.db_reader = type(
        "ReaderStub",
        (),
        {
            "fetch_recent_option_band_snapshots": lambda *args, **kwargs: [
                [
                    _row(previous_ts, 24200, "CE", 100.0, volume=1000, oi=5000, distance=0),
                    _row(previous_ts, 24250, "CE", 80.0, volume=800, oi=4500, distance=1),
                    _row(previous_ts, 24150, "CE", 75.0, volume=700, oi=4000, distance=-1),
                    _row(previous_ts, 24200, "PE", 95.0, volume=900, oi=5200, distance=0),
                ]
            ]
        },
    )()
    option_data = {
        "underlying_price": 24218.0,
        "atm": 24200,
        "band_snapshots": [
            _row(current_ts, 24200, "CE", 108.0, volume=1200, oi=5100, distance=0),
            _row(current_ts, 24250, "CE", 84.0, volume=850, oi=4550, distance=1),
            _row(current_ts, 24150, "CE", 78.0, volume=720, oi=4020, distance=-1),
            _row(current_ts, 24200, "PE", 91.0, volume=910, oi=5150, distance=0),
        ],
    }

    rows = collector._build_option_market_state_rows(option_data, current_ts)

    ce = next(row for row in rows if row[2] == "CE")
    pe = next(row for row in rows if row[2] == "PE")
    assert ce[7] == 8.0
    assert ce[18] == 100.0
    assert ce[19] == "EXPANDING"
    assert ce[20] == "GOOD"
    assert ce[21] == "READY"
    assert pe[19] == "FADING"
    assert pe[21] == "AVOID"
