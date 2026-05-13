def option_pnl_percent(entry_price, option_price):
    if entry_price in (None, 0) or option_price is None:
        return None
    try:
        return round(((float(option_price) - float(entry_price)) / float(entry_price)) * 100, 2)
    except Exception:
        return None


def drawdown_from_peak_percent(peak_price, option_price):
    if peak_price in (None, 0) or option_price is None:
        return None
    try:
        return round(((float(peak_price) - float(option_price)) / float(peak_price)) * 100, 2)
    except Exception:
        return None


def spread_widening_percent(entry_spread, current_spread):
    if entry_spread in (None, 0) or current_spread is None:
        return None
    try:
        return round(((float(current_spread) - float(entry_spread)) / float(entry_spread)) * 100.0, 2)
    except Exception:
        return None


def estimate_live_atr(recent_5m_candles, fallback=None):
    try:
        if recent_5m_candles:
            ranges = [
                abs(float(candle["high"]) - float(candle["low"]))
                for candle in recent_5m_candles[-4:]
                if candle.get("high") is not None and candle.get("low") is not None
            ]
            if ranges:
                return round(sum(ranges) / len(ranges), 2)
    except Exception:
        pass
    try:
        return round(float(fallback), 2) if fallback is not None else None
    except Exception:
        return None


def dynamic_trail_percent(base_trail_pct, setup_bucket, live_atr, underlying_price, time_regime=None):
    trail_pct = float(base_trail_pct or 0.0)
    if live_atr and underlying_price:
        atr_pct = (float(live_atr) / max(float(underlying_price), 1.0)) * 100.0
        if atr_pct >= 0.22:
            trail_pct += 2.0
        elif atr_pct >= 0.14:
            trail_pct += 1.0
        elif atr_pct <= 0.08:
            trail_pct = max(7.0, trail_pct - 1.0)

    setup_bucket = (setup_bucket or "").upper()
    if setup_bucket == "REVERSAL":
        trail_pct += 1.5
    elif setup_bucket == "CONTINUATION":
        trail_pct += 1.0
    elif setup_bucket == "BREAKOUT":
        trail_pct = max(7.0, trail_pct)

    time_regime = (time_regime or "").upper()
    if time_regime == "ENDGAME":
        trail_pct = max(7.0, trail_pct - 1.0)
    elif time_regime == "OPENING":
        trail_pct += 0.5

    return round(min(max(trail_pct, 7.0), 14.0), 2)


def update_psar_style_level(signal, existing_level, latest_1m, previous_1m, live_atr=None):
    signal = (signal or "").upper()
    level = float(existing_level) if existing_level is not None else None
    atr_buffer = max(float(live_atr or 0.0) * 0.12, 2.0)

    if signal == "CE":
        candidate = min(
            float(previous_1m.get("low") or latest_1m.get("low") or 0.0),
            float(latest_1m.get("low") or 0.0),
        ) - atr_buffer
        return round(max(level, candidate), 2) if level is not None else round(candidate, 2)
    if signal == "PE":
        candidate = max(
            float(previous_1m.get("high") or latest_1m.get("high") or 0.0),
            float(latest_1m.get("high") or 0.0),
        ) + atr_buffer
        return round(min(level, candidate), 2) if level is not None else round(candidate, 2)
    return level
