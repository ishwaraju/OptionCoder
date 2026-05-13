def pending_watch_max_minutes(instrument, pending):
    if not pending:
        return 20
    if pending.get("hybrid_mode"):
        if instrument == "BANKNIFTY":
            return 18
        if instrument == "SENSEX":
            return 16
        return 30
    if instrument == "SENSEX":
        return 14
    if instrument == "BANKNIFTY":
        return 16
    return 20


def pending_watch_retrigger_eligible(pending, latest, previous):
    if not pending:
        return False
    if int(pending.get("retrigger_count") or 0) >= 1:
        return False
    direction = (pending.get("direction") or "").upper()
    trigger_price = pending.get("trigger_price")
    if direction not in {"CE", "PE"} or trigger_price is None:
        return False
    latest_close = latest.get("close")
    previous_close = previous.get("close")
    latest_open = latest.get("open")
    if None in {latest_close, previous_close, latest_open}:
        return False
    if direction == "CE":
        return float(latest_close) >= float(trigger_price) and float(latest_close) >= float(latest_open) and float(latest_close) >= float(previous_close)
    return float(latest_close) <= float(trigger_price) and float(latest_close) <= float(latest_open) and float(latest_close) <= float(previous_close)


def pending_watch_elite_ready(pending):
    if not pending:
        return False
    signal_grade = (pending.get("signal_grade") or "").upper()
    confidence = (pending.get("confidence") or "").upper()
    setup = (pending.get("signal_type") or "").upper()
    score = float(pending.get("score") or 0)
    entry_score = float(pending.get("entry_score") or 0)
    return bool(
        pending.get("strong_watch_setup")
        and signal_grade in {"A", "A+"}
        and confidence == "HIGH"
        and setup in {"BREAKOUT_CONFIRM", "RETEST", "REVERSAL", "TRAP_REVERSAL"}
        and score >= 82
        and entry_score >= 76
    )


def candle_close_strength(candle, direction):
    high = candle.get("high")
    low = candle.get("low")
    close = candle.get("close")
    if high is None or low is None or close is None:
        return 0.0
    range_size = float(high) - float(low)
    if range_size <= 0:
        return 0.0
    if direction == "CE":
        return (float(close) - float(low)) / range_size
    return (float(high) - float(close)) / range_size


def candle_body_ratio(candle):
    high = candle.get("high")
    low = candle.get("low")
    open_price = candle.get("open")
    close = candle.get("close")
    if None in {high, low, open_price, close}:
        return 0.0
    range_size = float(high) - float(low)
    if range_size <= 0:
        return 0.0
    return abs(float(close) - float(open_price)) / range_size
