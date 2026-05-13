from config import Config


def slope_counts(values):
    rises = 0
    falls = 0
    for prev, curr in zip(values, values[1:]):
        if curr > prev:
            rises += 1
        elif curr < prev:
            falls += 1
    return rises, falls


def derive_opening_bias(recent_candles_5m, vwap, atr, trade_start, first_30_end, first_60_end):
    opening_window = []
    first_30 = []
    first_60 = []
    for candle in recent_candles_5m or []:
        candle_time = candle.get("time")
        if candle_time is None:
            continue
        clock = candle_time.time()
        if clock < trade_start or clock > first_60_end:
            continue
        opening_window.append(candle)
        if clock < first_30_end:
            first_30.append(candle)
        first_60.append(candle)

    if len(first_30) < 3 or len(first_60) < 6:
        return "UNKNOWN", "opening_window_incomplete"

    open_price = first_30[0].get("open")
    close_30 = first_30[-1].get("close")
    close_60 = first_60[-1].get("close")
    high_60 = max(candle.get("high", close_60) for candle in first_60)
    low_60 = min(candle.get("low", close_60) for candle in first_60)
    if None in {open_price, close_30, close_60}:
        return "UNKNOWN", "opening_window_invalid"

    atr_ref = max(float(atr or 0), 1.0)
    move_30 = float(close_30) - float(open_price)
    move_60 = float(close_60) - float(open_price)
    day_range_60 = float(high_60) - float(low_60)
    above_vwap_30 = vwap is not None and float(close_30) > float(vwap)
    below_vwap_30 = vwap is not None and float(close_30) < float(vwap)
    above_vwap_60 = vwap is not None and float(close_60) > float(vwap)
    below_vwap_60 = vwap is not None and float(close_60) < float(vwap)

    if (
        move_30 >= atr_ref * 0.45
        and above_vwap_30
        and (
            move_60 >= atr_ref * 0.75
            or (
                move_60 >= atr_ref * 0.2
                and above_vwap_60
                and float(close_60) >= float(low_60) + (day_range_60 * 0.55)
            )
        )
    ):
        return "OPEN_BULLISH", "opening_drive_up"
    if (
        move_30 <= -(atr_ref * 0.45)
        and below_vwap_30
        and (
            move_60 <= -(atr_ref * 0.75)
            or (
                move_60 <= -(atr_ref * 0.2)
                and below_vwap_60
                and float(close_60) <= float(low_60) + (day_range_60 * 0.45)
            )
        )
    ):
        return "OPEN_BEARISH", "opening_drive_down"
    if move_30 >= atr_ref * 0.45 and above_vwap_30:
        return "OPEN_BULLISH", "opening_first_30_bullish"
    if move_30 <= -(atr_ref * 0.45) and below_vwap_30:
        return "OPEN_BEARISH", "opening_first_30_bearish"
    return "OPEN_BALANCED", "opening_mixed"


def derive_active_day_state(recent_candles_5m, price, vwap, atr, pressure_metrics, opening_bias, time_regime):
    recent_candles_5m = recent_candles_5m or []
    if len(recent_candles_5m) < 6:
        return {"state": "UNKNOWN", "direction": "NONE", "detail": "insufficient_recent_context"}

    window = recent_candles_5m[-15:] if len(recent_candles_5m) >= 15 else recent_candles_5m[:]
    closes = [float(candle.get("close", 0) or 0) for candle in window]
    highs = [float(candle.get("high", close) or close) for candle, close in zip(window, closes)]
    lows = [float(candle.get("low", close) or close) for candle, close in zip(window, closes)]
    opens = [float(candle.get("open", close) or close) for candle, close in zip(window, closes)]
    if not closes:
        return {"state": "UNKNOWN", "direction": "NONE", "detail": "recent_window_invalid"}

    atr_ref = max(float(atr or 0), 1.0)
    net_move = closes[-1] - opens[0]
    recent_range = max(highs) - min(lows)
    close_rises, close_falls = slope_counts(closes[-5:] if len(closes) >= 5 else closes)
    high_rises, high_falls = slope_counts(highs[-5:] if len(highs) >= 5 else highs)
    low_rises, low_falls = slope_counts(lows[-5:] if len(lows) >= 5 else lows)
    above_vwap = vwap is not None and float(price) > float(vwap)
    below_vwap = vwap is not None and float(price) < float(vwap)
    pressure_bias = (pressure_metrics or {}).get("pressure_bias")
    bullish_pressure = pressure_bias in {"BULLISH", "NEUTRAL", None}
    bearish_pressure = pressure_bias in {"BEARISH", "NEUTRAL", None}

    bull_trend = (
        net_move >= atr_ref * 1.0
        and above_vwap
        and close_rises >= 3
        and high_rises >= 3
        and low_rises >= 2
        and bullish_pressure
    )
    bear_trend = (
        net_move <= -(atr_ref * 1.0)
        and below_vwap
        and close_falls >= 3
        and high_falls >= 2
        and low_falls >= 3
        and bearish_pressure
    )
    range_active = (
        recent_range <= atr_ref * 1.8
        and abs(net_move) <= atr_ref * 0.45
        and abs(close_rises - close_falls) <= 1
    )

    if bull_trend and opening_bias == "OPEN_BEARISH":
        return {"state": "REVERSAL_UNDERWAY", "direction": "CE", "detail": "bearish_open_reclaimed_to_bull"}
    if bear_trend and opening_bias == "OPEN_BULLISH":
        return {"state": "REVERSAL_UNDERWAY", "direction": "PE", "detail": "bullish_open_reversed_to_bear"}
    if bull_trend:
        return {"state": "BULL_TREND_ACTIVE", "direction": "CE", "detail": "rolling_bull_trend"}
    if bear_trend:
        return {"state": "BEAR_TREND_ACTIVE", "direction": "PE", "detail": "rolling_bear_trend"}
    if (
        time_regime in {"LATE_DAY", "ENDGAME"}
        and recent_range >= atr_ref * 2.4
        and close_rises >= 2
        and close_falls >= 2
        and pressure_bias == "NEUTRAL"
    ):
        return {"state": "EXPIRY_CHAOS_ACTIVE", "direction": "NONE", "detail": "late_range_expansion_with_flip_risk"}
    if range_active:
        return {"state": "RANGE_ACTIVE", "direction": "NONE", "detail": "rolling_range"}
    return {"state": "TRANSITION_ACTIVE", "direction": "NONE", "detail": "mixed_recent_structure"}


def apply_day_state_adjustment(score, scored_direction, cautions, components, day_state, append_cautions):
    adjusted_score = float(score or 0)
    state = (day_state or {}).get("state") or "UNKNOWN"
    direction = (day_state or {}).get("direction") or "NONE"
    detail = (day_state or {}).get("detail") or ""

    if state in {"BULL_TREND_ACTIVE", "BEAR_TREND_ACTIVE", "REVERSAL_UNDERWAY"} and direction in {"CE", "PE"}:
        if scored_direction == direction:
            adjusted_score = min(100.0, adjusted_score + (6.0 if state == "REVERSAL_UNDERWAY" else 8.0))
            components.append(f"day_state_{state.lower()}_aligned")
        elif scored_direction in {"CE", "PE"}:
            adjusted_score = max(0.0, adjusted_score - (6.0 if state == "REVERSAL_UNDERWAY" else 8.0))
            cautions = append_cautions(cautions, "day_state_opposes_direction")
            components.append(f"day_state_{state.lower()}_opposed")
    elif state == "RANGE_ACTIVE":
        cautions = append_cautions(cautions, "range_day_context")
        components.append("day_state_range")
    elif state == "EXPIRY_CHAOS_ACTIVE":
        adjusted_score = max(0.0, adjusted_score - 5.0)
        cautions = append_cautions(cautions, "expiry_chaos_context")
        components.append("day_state_expiry_chaos")
    elif state == "TRANSITION_ACTIVE" and detail:
        cautions = append_cautions(cautions, "transition_day_context")

    return round(adjusted_score, 2), cautions
