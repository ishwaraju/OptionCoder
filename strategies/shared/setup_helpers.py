def watch_bucket(signal_type, blockers, cautions):
    signal_type = (signal_type or "NONE").upper()
    blockers = blockers or []
    cautions = cautions or []
    if signal_type in {"BREAKOUT_CONFIRM", "RETEST", "REVERSAL"}:
        return "WATCH_CONFIRMATION_PENDING"
    if "direction_present_but_filters_incomplete" in blockers:
        return "WATCH_SETUP"
    if any(flag in cautions for flag in {"confirmation_watch_active", "retest_watch_active"}):
        return "WATCH_CONFIRMATION_PENDING"
    return "WATCH_CONTEXT"


def recent_breakout_context(breakout_memory, direction, candle_time, price, vwap, buffer):
    if not breakout_memory or candle_time is None or direction not in {"CE", "PE"}:
        return False
    memory = breakout_memory
    if memory["session_day"] != candle_time.date():
        return False
    if memory["direction"] != direction:
        return False
    minutes_apart = int((candle_time - memory["time"]).total_seconds() // 60)
    if minutes_apart < 0 or minutes_apart > 45:
        return False
    level = memory.get("level")
    if level is None or vwap is None:
        return False
    if direction == "CE":
        return price >= max(level - max(buffer, 5), vwap)
    return price <= min(level + max(buffer, 5), vwap)


def build_entry_plan(instrument, direction, signal_type, trigger_price, invalidate_price, atr, support, resistance):
    plan = {
        "entry_above": None,
        "entry_below": None,
        "invalidate_price": None,
        "first_target_price": None,
    }
    signal_type = (signal_type or "NONE").upper()
    if direction not in {"CE", "PE"}:
        return plan

    step = max((atr or 0) * 0.6, 8 if instrument == "NIFTY" else 15)
    if direction == "CE":
        plan["entry_above"] = trigger_price
        plan["invalidate_price"] = invalidate_price or support
        base_target = trigger_price or invalidate_price
        if base_target is not None:
            plan["first_target_price"] = round(base_target + step, 2)
    else:
        plan["entry_below"] = trigger_price
        plan["invalidate_price"] = invalidate_price or resistance
        base_target = trigger_price or invalidate_price
        if base_target is not None:
            plan["first_target_price"] = round(base_target - step, 2)

    if signal_type == "RETEST" and plan["invalidate_price"] is None and trigger_price is not None:
        offset = max((atr or 0) * 0.4, 6 if instrument == "NIFTY" else 12)
        plan["invalidate_price"] = round(trigger_price - offset, 2) if direction == "CE" else round(trigger_price + offset, 2)

    return plan


def confirmation_extension_limit(instrument, atr, buffer):
    floors = {
        "NIFTY": 12,
        "BANKNIFTY": 28,
        "SENSEX": 22,
    }
    base_floor = floors.get(instrument, 12)
    atr_part = (atr or 0) * 0.45
    buffer_part = (buffer or 0) * 1.2
    return max(base_floor, atr_part, buffer_part)


def entry_too_extended(instrument, direction, close_price, trigger_level, atr, buffer):
    if direction not in {"CE", "PE"} or close_price is None or trigger_level is None:
        return False

    extension_limit = confirmation_extension_limit(instrument, atr, buffer)
    if direction == "CE":
        return close_price >= trigger_level + extension_limit
    return close_price <= trigger_level - extension_limit


def early_impulse_breakout_ready(
    instrument,
    direction,
    score,
    volume_signal,
    candle_liquidity_ok,
    breakout_body_ok,
    breakout_structure_ok,
    pressure_conflict_level,
    time_regime,
    close_price,
    trigger_level,
    atr,
    buffer,
):
    if direction not in {"CE", "PE"}:
        return False
    if time_regime not in {"OPENING", "MID_MORNING"}:
        return False
    if volume_signal != "STRONG":
        return False
    if not candle_liquidity_ok:
        return False
    if pressure_conflict_level not in {"NONE", "MILD"}:
        return False
    if score < 74:
        return False
    if entry_too_extended(instrument, direction, close_price, trigger_level, atr, buffer):
        return False
    return breakout_body_ok or breakout_structure_ok


def sensex_volume_flexible(instrument, volume_signal, score, time_regime, candle_range, atr):
    if instrument != "SENSEX":
        return volume_signal in {"NORMAL", "STRONG"}
    if volume_signal in {"NORMAL", "STRONG"}:
        return True
    if time_regime not in {"OPENING", "MID_MORNING"}:
        return False
    if score < 62:
        return False
    if atr is None:
        return True
    return candle_range >= atr * 0.65


def sensex_hybrid_fallback_ready(
    instrument,
    direction,
    score,
    time_regime,
    price,
    vwap,
    orb_high,
    orb_low,
    candle_close,
    candle_range,
    atr,
    candle_liquidity_ok,
    breakout_body_ok,
    breakout_structure_ok,
    ha_ok,
    pressure_conflict_level,
):
    if instrument != "SENSEX" or direction not in {"CE", "PE"}:
        return False
    if time_regime not in {"OPENING", "MID_MORNING"}:
        return False
    if score < 60:
        return False
    if not candle_liquidity_ok or not ha_ok:
        return False
    if pressure_conflict_level == "HARD":
        return False
    if not (breakout_body_ok or breakout_structure_ok):
        return False
    if atr is not None and candle_range < atr * 0.75:
        return False
    trigger_level = orb_high if direction == "CE" else orb_low
    if trigger_level is not None and entry_too_extended(
        instrument,
        direction,
        candle_close or price,
        trigger_level,
        atr,
        max((atr or 0) * 0.2, 5),
    ):
        return False
    if direction == "CE":
        return (
            price is not None and vwap is not None and price > vwap
            and orb_high is not None and price > orb_high
        )
    return (
        price is not None and vwap is not None and price < vwap
        and orb_low is not None and price < orb_low
    )


def price_led_hybrid_fallback_ready(
    instrument,
    direction,
    score,
    entry_score,
    time_regime,
    price,
    vwap,
    orb_high,
    orb_low,
    candle_close,
    candle_range,
    atr,
    candle_liquidity_ok,
    breakout_body_ok,
    breakout_structure_ok,
    ha_ok,
    pressure_conflict_level,
    volume_signal,
    trend_day_context=False,
):
    if instrument not in {"NIFTY", "BANKNIFTY"} or direction not in {"CE", "PE"}:
        return False
    if time_regime not in {"MID_MORNING", "MIDDAY"}:
        return False
    min_score = 68 if instrument == "NIFTY" else 66
    min_entry_score = 64 if instrument == "NIFTY" else 60
    if score < min_score or entry_score < min_entry_score:
        return False
    if volume_signal not in {"NORMAL", "STRONG"} and not trend_day_context:
        return False
    if not candle_liquidity_ok or not ha_ok:
        return False
    if pressure_conflict_level not in {"NONE", "MILD"} and not (
        trend_day_context and pressure_conflict_level == "MODERATE"
    ):
        return False
    if not (breakout_body_ok or breakout_structure_ok):
        return False
    if atr is not None and candle_range < atr * 0.6:
        return False
    trigger_level = orb_high if direction == "CE" else orb_low
    if trigger_level is not None and entry_too_extended(
        instrument,
        direction,
        candle_close or price,
        trigger_level,
        atr,
        max((atr or 0) * 0.18, 5),
    ):
        return False
    if direction == "CE":
        return (
            price is not None and vwap is not None and price > vwap
            and orb_high is not None and price > orb_high
        )
    return (
        price is not None and vwap is not None and price < vwap
        and orb_low is not None and price < orb_low
    )
