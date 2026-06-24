from datetime import datetime, timedelta

from strategies.shared.ict_structure import analyze_ict_structure


def _candle(i, open_, high, low, close, volume=1000):
    return {
        "time": datetime(2026, 6, 8, 10, 0) + timedelta(minutes=5 * i),
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


def test_bullish_liquidity_sweep_mss_fvg_retest_is_action_ready():
    candles = [
        _candle(0, 100, 104, 98, 102),
        _candle(1, 102, 105, 99, 103),
        _candle(2, 103, 106, 97, 101),
        _candle(3, 101, 104, 99, 100),
        _candle(4, 100, 103, 95, 101),  # sell-side sweep and reclaim
        _candle(5, 101, 108, 101, 107),  # displacement candle one
        _candle(6, 107, 114, 106, 113),  # bullish FVG: candle4 high 103, candle6 low 106
        _candle(7, 113, 116, 106, 115),  # retest FVG and reject
    ]

    context = analyze_ict_structure(candles, direction="CE", atr=8, buffer=2)

    assert context["direction"] == "CE"
    assert context["quality"] == "A"
    assert context["action_ready"] is True
    assert context["entry_zone"] == {"low": 103.0, "high": 106.0}
    assert "liquidity_sweep" in context["reason"]
    assert "mss" in context["reason"]
    assert "fvg_rejection" in context["reason"]


def test_bearish_liquidity_sweep_mss_fvg_retest_is_action_ready():
    candles = [
        _candle(0, 120, 122, 116, 118),
        _candle(1, 118, 123, 117, 121),
        _candle(2, 121, 124, 118, 122),
        _candle(3, 122, 123, 117, 119),
        _candle(4, 119, 126, 116, 118),  # buy-side sweep and reject
        _candle(5, 118, 118, 108, 110),  # displacement candle one
        _candle(6, 110, 110, 99, 101),   # bearish FVG: candle6 high 110, candle4 low 116
        _candle(7, 101, 112, 99, 100),   # retest FVG and reject
    ]

    context = analyze_ict_structure(candles, direction="PE", atr=8, buffer=2)

    assert context["direction"] == "PE"
    assert context["quality"] == "A"
    assert context["action_ready"] is True
    assert context["entry_zone"] == {"low": 110.0, "high": 116.0}
    assert context["invalidation"] == 126


def test_filled_fvg_does_not_become_action_ready():
    candles = [
        _candle(0, 100, 104, 98, 102),
        _candle(1, 102, 105, 99, 103),
        _candle(2, 103, 106, 97, 101),
        _candle(3, 101, 104, 99, 100),
        _candle(4, 100, 103, 95, 101),
        _candle(5, 101, 108, 101, 107),
        _candle(6, 107, 114, 109, 113),
        _candle(7, 113, 116, 102, 104),  # fully fills below bullish FVG low
    ]

    context = analyze_ict_structure(candles, direction="CE", atr=8, buffer=2)

    assert context["action_ready"] is False


def test_equal_low_sell_side_liquidity_sweep_gets_strength_context():
    candles = [
        _candle(0, 100, 104, 96, 101),
        _candle(1, 101, 105, 96.4, 102),
        _candle(2, 102, 106, 96.2, 103),
        _candle(3, 103, 104, 97, 99),
        _candle(4, 99, 102, 94.2, 100.8),  # takes equal lows and reclaims
        _candle(5, 101, 109, 101, 108),
        _candle(6, 108, 116, 106.5, 114),  # bullish FVG
        _candle(7, 114, 117, 105.8, 116),
    ]

    context = analyze_ict_structure(candles, direction="CE", atr=8, buffer=2)

    assert context["action_ready"] is True
    assert context["sweep"]["pool_type"] == "EQUAL_LOW"
    assert context["sweep"]["quality"] == "STRONG"
    assert context["sweep"]["touches"] >= 2
    assert "equal_liquidity_pool" in context["reason"]


def test_shallow_low_break_without_reclaim_is_not_liquidity_sweep():
    candles = [
        _candle(0, 100, 104, 96, 101),
        _candle(1, 101, 105, 96.3, 102),
        _candle(2, 102, 106, 96.1, 103),
        _candle(3, 103, 104, 97, 99),
        _candle(4, 99, 101, 95.8, 95.9),  # breaks pool but fails reclaim
        _candle(5, 96, 99, 94, 95),
        _candle(6, 95, 98, 93, 94),
        _candle(7, 94, 97, 93.5, 95),
    ]

    context = analyze_ict_structure(candles, direction="CE", atr=8, buffer=2)

    assert context["sweep"]["present"] is False
    assert context["action_ready"] is False


def test_equal_high_buy_side_liquidity_sweep_gets_strength_context():
    candles = [
        _candle(0, 120, 124, 116, 118),
        _candle(1, 118, 124.4, 117, 121),
        _candle(2, 121, 124.2, 118, 122),
        _candle(3, 122, 123, 117, 119),
        _candle(4, 119, 126.8, 116, 118),  # takes equal highs and rejects
        _candle(5, 118, 118, 108, 110),
        _candle(6, 110, 110, 99, 101),
        _candle(7, 101, 112, 99, 100),
    ]

    context = analyze_ict_structure(candles, direction="PE", atr=8, buffer=2)

    assert context["action_ready"] is True
    assert context["sweep"]["pool_type"] == "EQUAL_HIGH"
    assert context["sweep"]["quality"] == "STRONG"
    assert context["sweep"]["side"] == "BUY_SIDE"
